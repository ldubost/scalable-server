// SPDX-License-Identifier: AGPL-3.0-or-later
// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors

/*  Per-channel federation state and the per-origin pending logs (design §3).

    Everything goes through the existing ObjectBackend, so this works unchanged
    on `fs` and on S3. Only `get`, `put` (with `ifMatch`/`ifNoneMatch`), `append`
    and `remove` are used — no new storage primitives.

    Layout:

        channel/ab/abcdef….ndjson          the committed merged log  (UNCHANGED)
        fed/ab/abcdef….pending.<originId>  per-origin arrival log
        fed/ab/abcdef….state.json          merge state

    Why the pending log is separate (design §3.2)
    ---------------------------------------------
    R-9 says a local client's message must be durable before it is acknowledged.
    R-3 says nothing may be committed before the watermark allows it. A single
    file cannot satisfy both, so arrival and commitment are different files. The
    committed log stays byte-compatible with what every existing reader expects,
    which is what makes federation reversible: delete the `fed/` keys and an
    ordinary pad remains.

    One file per origin, so appends from different origins never interleave and a
    gap in `s` (R-17) is directly visible as a gap in one file.
*/

const Codec = require('../../common/federation/codec.js');
const Order = require('../../common/federation/order.js');

const STATE_VERSION = 1;

/*  Key layout mirrors the channel store's two-character shard so a `fed/`
    listing has the same shape as `channel/` and neither directory ends up with
    a hundred thousand entries. */
const shard = (channel) => `${channel.slice(0, 2)}/${channel}`;

const stateKey = (channel) => `fed/${shard(channel)}.state.json`;
const pendingKey = (channel, originId) =>
    // originIds are base64 and may contain '/', which is a key separator
    `fed/${shard(channel)}.pending.${encodeURIComponent(originId)}`;

const enoent = (err) => err && (err.code === 'ENOENT' || /ENOENT/.test(err.message || ''));

const mkState = (channel, validateKey) => ({
    v: STATE_VERSION,
    channel,
    validateKey,
    /*  The instance that created the replica set. In M1 it is the only member
        permitted to write; everyone else is a read-only mirror. M2 (L1) makes
        this the write-through anchor, and M3 (L2) drops the distinction. */
    origin: undefined,
    /*  This instance's own originId, so `origin === me` answers "am I the
        anchor?" without storage having to learn its own federation identity. */
    me: undefined,
    /*  'L1' — one anchor orders everything, mirrors forward their writes.
        'L2' — every member accepts writes and the merge derives the order.
        Recorded per channel so an instance can run both at once, and so
        raising a channel to L2 is a deliberate act rather than a global flag. */
    level: 'L1',
    members: [],
    /*  seq starts at -1 so the first message allocated is 0, matching the
        per-origin runs the merge checks for gaps against (R-17). Starting at 0
        made every origin's first message look like a gap at 1, which blocked
        the merge forever — and was masked while the gap check inferred its
        starting point from whatever happened to be pending. */
    self: { seq: -1, lamport: 0 },
    peers: {},
    committed: { lamport: 0, lastId: null, line: 0 },
    heads: [],
    evicted: []
});

const create = (Env) => {
    /*  Set by storage/index.js. On object storage this is the same backend the
        channel and blob stores use; on the filesystem path, where the legacy
        file store provides no ObjectBackend, it is one built for the purpose.
        Either way federation needs no storage primitives of its own. */
    const backend = Env.federationBackend || Env.storageBackend;
    if (!backend) { throw new Error('E_FEDLOG_NO_BACKEND'); }

    const FM = {};

    // --- state ------------------------------------------------------------

    /*  Read merge state. Returns `{ state, etag }`; etag is carried so the
        caller can write back conditionally and detect a concurrent writer.
        A channel with no state is simply not federated. */
    FM.readState = (channel, cb) => {
        backend.get(stateKey(channel), (err, body) => {
            if (err) {
                if (enoent(err)) { return void cb(void 0, undefined); }
                return void cb(err);
            }
            const state = Codec.decode(body);
            if (!state || state.v !== STATE_VERSION) {
                return void cb(new Error('E_FEDLOG_STATE_UNSUPPORTED'));
            }
            backend.head(stateKey(channel), (headErr, stat) => {
                cb(void 0, { state, etag: headErr ? undefined : stat?.etag });
            });
        });
    };

    /*  Write merge state conditionally.

        The per-channel single-writer property (`Env.getStorageId` plus the write
        queue) means a lost race here is a fault, not a normal case — so it is
        surfaced as EPRECONDITION rather than retried silently. Retrying would
        paper over the one situation we most need to know about: two nodes
        believing they own the same channel. */
    FM.writeState = (channel, state, etag, cb) => {
        const opts = etag ? { ifMatch: etag } : { ifNoneMatch: true };
        backend.put(stateKey(channel), Codec.encode(state), opts, (err, stat) => {
            if (err) { return void cb(err); }
            cb(void 0, stat?.etag);
        });
    };

    /*  Begin federating a channel. `ifNoneMatch` makes this idempotent-safe:
        a second attempt fails rather than resetting an existing replica's
        sequence counters back to zero. */
    FM.initState = (channel, opts, cb) => {
        const state = mkState(channel, opts.validateKey);
        state.origin = opts.origin || opts.self;
        state.me = opts.self;
        if (opts.level === 'L2') { state.level = 'L2'; }
        state.members = Array.from(new Set([opts.self].concat(opts.members || []))).sort();
        FM.writeState(channel, state, undefined, (err, etag) => {
            if (err) { return void cb(err); }
            cb(void 0, { state, etag });
        });
    };

    FM.isFederated = (channel, cb) => {
        backend.exists(stateKey(channel), (err, exists) => {
            cb(err, Boolean(exists));
        });
    };

    // --- pending logs -----------------------------------------------------

    /*  Append one envelope to its origin's pending log.

        Durability point for R-9: a local message is acknowledged only after this
        returns. The record is the encoded envelope plus a newline — the same
        ndjson discipline the channel log uses, so the same recovery reasoning
        applies (a torn final line is discarded on read). */
    FM.appendPending = (channel, env, cb) => {
        const line = Buffer.concat([Codec.encode(env), Buffer.from('\n')]);
        backend.append(pendingKey(channel, env.o), line, {}, (err) => cb(err));
    };

    /*  Read one origin's pending envelopes.

        A trailing partial line is dropped rather than treated as an error: it
        means a write was interrupted, the envelope was never acknowledged, and
        the peer will resend it. Failing the whole read instead would make one
        interrupted write block the channel forever.
    */
    FM.readPending = (channel, originId, cb) => {
        backend.get(pendingKey(channel, originId), (err, body) => {
            if (err) {
                if (enoent(err)) { return void cb(void 0, []); }
                return void cb(err);
            }
            const text = body.toString('utf8');
            const lines = text.split('\n');
            // a complete file ends in '\n', so the last element is '' — anything
            // else there is a torn write
            if (lines.length && lines[lines.length - 1] !== '') { lines.pop(); }
            const out = [];
            lines.forEach(l => {
                if (!l) { return; }
                const env = Codec.decode(Buffer.from(l, 'utf8'));
                if (env) { out.push(env); }
            });
            cb(void 0, out);
        });
    };

    /*  Every pending envelope this channel holds, from every origin, in the
        federation total order (R-36).

        This is what history serving appends after the committed prefix (R-8),
        and the sort is not cosmetic: replicas that serve this tail in arrival
        order can converge on permanently different documents. See
        common/federation/order.js and docs/experiments/chainpad-ordering.
    */
    FM.readAllPending = (channel, state, cb) => {
        const origins = Array.from(new Set(
            (state.members || []).concat(Object.keys(state.peers || {}))));
        const all = [];
        let pendingCount = origins.length;
        let failed = false;
        if (!pendingCount) { return void cb(void 0, []); }
        origins.forEach(origin => {
            FM.readPending(channel, origin, (err, envs) => {
                if (failed) { return; }
                if (err) { failed = true; return void cb(err); }
                Array.prototype.push.apply(all, envs);
                if (--pendingCount === 0) { cb(void 0, Order.sortTail(all)); }
            });
        });
    };

    /*  Drop an origin's pending log once everything in it is committed.
        Compaction is deliberately whole-file: the pending log is bounded by the
        watermark lag (spec §11.2 measured it at tens of messages), so rewriting
        it is cheaper than tracking offsets. */
    FM.clearPending = (channel, originId, cb) => {
        backend.remove(pendingKey(channel, originId), (err) => {
            if (err && !enoent(err)) { return void cb(err); }
            cb();
        });
    };

    /*  Rewrite an origin's pending log to only those envelopes not yet
        committed. Used after a merge pass commits a prefix. */
    FM.compactPending = (channel, originId, keep, cb) => {
        if (!keep.length) { return void FM.clearPending(channel, originId, cb); }
        const body = Buffer.from(
            keep.map(e => Codec.encode(e).toString('utf8')).join('\n') + '\n', 'utf8');
        backend.put(pendingKey(channel, originId), body, {}, (err) => cb(err));
    };

    /*  Remove every federation key for a channel (design §3.4). The committed
        log is deliberately left alone: un-federating leaves an ordinary pad. */
    FM.unfederate = (channel, state, cb) => {
        const origins = Array.from(new Set(
            (state?.members || []).concat(Object.keys(state?.peers || {}))));
        let n = origins.length + 1;
        let failed = false;
        const done = (err) => {
            if (failed) { return; }
            if (err) { failed = true; return void cb(err); }
            if (--n === 0) { cb(); }
        };
        origins.forEach(o => FM.clearPending(channel, o, done));
        backend.remove(stateKey(channel), (err) => {
            done(err && !enoent(err) ? err : undefined);
        });
    };

    FM.keys = { stateKey, pendingKey };
    return FM;
};

module.exports = { create, mkState, STATE_VERSION, stateKey, pendingKey };
