// SPDX-License-Identifier: AGPL-3.0-or-later
// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors

/*  The storage-side federation manager — `Env.FM`, a sibling of `Env.CM`.

    Owns everything about a federated channel that must happen where the log
    lives: enabling replication, reading history out as envelopes, and ingesting
    a remote envelope into the committed log.

    M1 (L0 mirror) scope. One origin writes; every other member is a read-only
    mirror, so the committed log is built by straight append and there is no
    merge. The Lamport clock is therefore just the position in the log, which has
    a useful side effect: `(s, l)` are *derived* from the committed log rather
    than stored, so a restart cannot desynchronise them. M3 replaces this with a
    real merge and the two stop coinciding.

    The committed log stays exactly the file every existing reader expects
    (`[0, senderId, "MSG", channel, content, time]` per line), so `computeIndex`,
    `offsetByHash`, `cpIndex` and `readMessagesBin` keep working untouched. That
    byte-compatibility is what makes federation reversible.
*/

const nThen = require('nthen');

const Ids = require('../../common/federation/ids.js');
const Order = require('../../common/federation/order.js');
const Envelope = require('../../common/federation/envelope.js');
const Constants = require('../../common/constants.js');
const Merge = require('./merge.js');
const Meta = require('../metadata.js');
const Metadata = require('../commands/metadata.js');
const FedLog = require('./fedlog.js');

const { CHECKPOINT_PATTERN } = Constants;
const ARRAY_LINE = /^\[/;

/*  A channel this instance has never stored is not an error here: a fresh
    mirror legitimately has no log yet, and its head is "nothing". Treating
    ENOENT as a failure would make the very first sync — the one that creates
    the log — impossible. */
const missing = (err) =>
    Boolean(err) && (err.code === 'ENOENT' || /ENOENT/.test(err.message || String(err)));

const create = (Env) => {
    const FM = {};
    /*  Declared ahead of use: enable defers to enableChecked once the metadata
        gate passes, and control dispatches to the per-type appliers below. */
    let enableChecked, applyMeta, applyDelete, saveState;
    const log = FedLog.create(Env);
    FM.log = log;

    /*  Which channels are federated, so the hot path costs a Map lookup rather
        than a storage round trip. Populated on enable and on first miss; an
        entry is only ever added for a channel that really has `fed/` state. */
    const cache = new Map();

    /*  The etag is cached with the state, not just the state.

        Conditional writes need it, and serving a cached state without one made
        every write fall back to `ifNoneMatch` — which then failed with
        KEY_EXISTS against the state it had just read. Keeping them together is
        what makes the cache safe to write from.
    */
    const remember = (channel, state, etag) => {
        cache.set(channel, state ? { state, etag } : null);
        return state;
    };

    FM.state = (channel, cb) => {
        if (cache.has(channel)) {
            const cached = cache.get(channel);
            return void cb(void 0, cached ? { state: cached.state, etag: cached.etag } : undefined);
        }
        log.readState(channel, (err, res) => {
            if (err) { return void cb(err); }
            remember(channel, res?.state, res?.etag);
            cb(void 0, res);
        });
    };

    // Synchronous, for the message path. Undefined means "not yet known".
    FM.cachedState = (channel) => cache.get(channel)?.state;
    FM.isFederatedCached = (channel) => Boolean(cache.get(channel));

    /*  Every channel this node holds federation state for, for a restarting
        instance to rebuild its routing from (R-52). Straight through to the log:
        the state files are the durable record, and nothing in memory here is. */
    FM.listFederated = (cb) => log.listFederated(cb);
    FM.forget = (channel) => cache.delete(channel);

    /*  Enable replication for a channel (design §5.2). The capability has
        already been verified by the caller against this channel's validateKey;
        by this point the question is only whether the state can be created. */
    FM.enable = (channel, opts, cb) => {
        /*  Two kinds of pad must never be federated, checked here because this
            is the only door in:

            R-22 — `selfdestruct` is scoped to one instance: the pad is destroyed
            when its last reader leaves. Replicating it would produce a pad that
            dies on one instance and lives on another, which is worse than either
            outcome.

            R-29 — a restricted pad's access list is enforced against *accounts*,
            and accounts are instance-scoped. A remote replica cannot evaluate it,
            so replicating one would silently drop the restriction.
        */
        Metadata.getMetadataRaw(Env, channel, (err, metadata) => {
            // no metadata yet is fine: a pad with no restrictions to violate
            if (err && !missing(err)) { return void cb(err); }
            if (metadata?.selfdestruct) { return void cb('ESELFDESTRUCT'); }
            if (metadata?.restricted) { return void cb('ERESTRICTED'); }
            enableChecked(channel, opts, cb);
        });
    };

    /*  Seed a replica's channel metadata from the anchor's.

        R-21 requires `channel`, `validateKey` and `created` to be identical
        across the replica set, and a mirror has none of them: its log is built
        by `FM.append`, which writes message lines and never goes through
        `handleFirstMessage`. Without this a mirrored pad has no validateKey to
        check writes against and no owner list for META commits to modify —
        which is exactly how the first version of this failed.

        `selfdestruct` is dropped rather than copied: it is scoped to one
        instance (R-22) and copying it would arm a timer on the replica.
    */
    const seedMetadata = (channel, metadata, cb) => {
        if (!metadata || typeof (metadata) !== 'object') { return void cb(); }
        Metadata.getMetadataRaw(Env, channel, (err, existing) => {
            if (err && !missing(err)) { return void cb(err); }
            // already has metadata: leave it alone, R-21 forbids reconciling it
            if (existing && existing.channel) { return void cb(); }

            const seed = Object.assign({}, metadata);
            delete seed.selfdestruct;
            seed.channel = channel;

            Env.store.writeMetadata(channel, JSON.stringify(seed), (e) => {
                if (e) { return void cb(e); }
                Env.metadata_cache[channel] = seed;
                Env.checkCache(channel);
                Env.Log.info('FEDERATION_METADATA_SEEDED', { channel });
                cb();
            });
        });
    };

    /*  Enabling a channel that is *already* federated must not fail.

        A user can legitimately press Federate twice, or federate the same pad to
        a second instance, and an existing replica set is not an error — it is the
        normal case for every call after the first. What must never happen is
        resetting the sequence counters of a live replica, which is why
        `initState` refuses to overwrite; so instead of clobbering, merge the new
        members into what is already there.
    */
    const alreadyExists = (err) =>
        /KEY_EXISTS|EEXIST|EPRECONDITION/.test(String(err?.code || err?.message || err));

    const addMembers = (channel, opts, cb) => {
        log.readState(channel, (err, res) => {
            if (err || !res) { return void cb(err || 'ENOSTATE'); }
            const state = res.state;

            if (state.validateKey && opts.validateKey &&
                state.validateKey !== opts.validateKey) {
                // R-21: a different key means a different pad, not a change
                return void cb('EVALIDATEKEY');
            }

            const before = state.members.length;
            const merged = new Set(state.members);
            (opts.members || []).forEach(m => merged.add(m));
            merged.add(opts.self);
            state.members = Array.from(merged).sort();

            if (state.members.length === before) {
                remember(channel, state, res.etag);
                /*  Already federated with everyone asked for. Not an error —
                    pressing Federate twice is a thing people do — so say so
                    plainly and let the caller show the existing link. */
                return void cb(void 0, state, { already: true });
            }
            log.writeState(channel, state, res.etag, (e, etag) => {
                if (e) { return void cb(e); }
                remember(channel, state, etag);
                Env.Log.info('FEDERATION_MEMBERS_ADDED',
                    { channel, members: state.members.length });
                cb(void 0, state, { already: false });
            });
        });
    };

    enableChecked = (channel, opts, cb) => {
        log.initState(channel, {
            validateKey: opts.validateKey,
            self: opts.self,
            origin: opts.origin,
            members: opts.members,
            level: opts.level
        }, (err, res) => {
            if (err) {
                // already federated: add the new member rather than failing
                if (alreadyExists(err)) { return void addMembers(channel, opts, cb); }
                return void cb(err);
            }
            remember(channel, res.state, res.etag);

            /*  A channel promoted to L2 may already have a committed history —
                the messages written before it was federated. Those reach a new
                replica through sync as `(origin, seq = log position)`, so this
                instance's allocator must continue *after* them. Starting from
                -1 would reissue sequence numbers the peers have already seen
                under different content, which reads as a fork rather than a
                duplicate.  */
            const afterSeed = (e) => {
                if (e) {
                    Env.Log.error('FEDERATION_METADATA_SEED_ERROR',
                        { channel, error: e.message || e });
                }
                if (res.state.level !== 'L2') { return void cb(void 0, res.state); }

                /*  Only the ORIGIN may adopt the existing log as its own.

                    A channel promoted to L2 already has history, and that
                    history was authored by the instance that held it. Those
                    messages reach a replica through sync as `(origin, position)`,
                    so the origin's allocator must continue after them or it
                    would reissue sequence numbers the peers have already seen.

                    A joining replica must NOT do this. It has authored nothing,
                    and setting its own `seq` from a log it merely received makes
                    it claim authorship of the origin's messages — after which the
                    two instances disagree about what any `(origin, seq)` refers
                    to, `haveThrough` becomes meaningless, and messages are
                    skipped silently on both sides. That is a divergence, and by
                    §11.1 it does not heal.
                */
                if (res.state.origin !== res.state.me) {
                    return void cb(void 0, res.state);
                }

                FM.head(channel, (e2, info) => {
                    if (e2) { return void cb(void 0, res.state); }
                    const last = (info?.count || 0) - 1;
                    if (last < 0) { return void cb(void 0, res.state); }
                    res.state.self.seq = last;
                    res.state.self.committedSeq = last;
                    res.state.self.lamport = Math.max(res.state.self.lamport, last);
                    res.state.committed = res.state.committed || {};
                    res.state.committed.lamport = Math.max(
                        res.state.committed.lamport || 0, last);
                    saveState(channel, res.state, res.etag,
                        () => cb(void 0, res.state));
                });
            };
            seedMetadata(channel, opts.metadata, afterSeed);
        });
    };

    FM.disable = (channel, cb) => {
        FM.state(channel, (err, res) => {
            if (err) { return void cb(err); }
            if (!res) { return void cb(); }
            log.unfederate(channel, res.state, (e) => {
                FM.forget(channel);
                cb(e);
            });
        });
    };

    /*  R-15: every replica revalidates content against the channel's
        validateKey for itself. A peer is never trusted about content, only
        about its own ordering claim — which is why a compromised instance key
        costs ordering and availability but not integrity.

        Delegated to the worker pool because signature verification is the
        expensive part of ingest and must not block the storage event loop. */
    const revalidate = (channel, content, validateKey, cb) => {
        /*  The `cp|<hash>|` prefix is added by the client and is not covered by
            the signature, so it must come off before verifying — exactly as the
            local write path does (`storage/channel-manager.js:195-196`).
            Verifying with it attached would reject every checkpoint. */
        const signedMsg = content.replace(CHECKPOINT_PATTERN, '');
        const coreId = Env.getCoreId(channel);
        Env.interface.sendQuery(coreId, 'VALIDATE_MESSAGE', {
            signedMsg, validateKey, channel
        }, (answer) => cb(answer?.error));
    };

    /*  Read the committed log as envelopes, from just after `sinceId` (or from
        the start when absent). Used to answer SYNC_REQ.

        Position in the log supplies both `s` and `l`, so the same message read
        twice yields the same envelope — which makes SYNC_RES idempotent and
        lets a mirror detect duplicates by id alone (R-18).
    */
    FM.since = (channel, sinceId, limit, cb) => {
        const out = [];
        let index = -1;
        let found = !sinceId;
        let failed;

        Env.store.readMessagesBin(channel, 0, (msgObj, readMore, abort) => {
            let line;
            try {
                line = msgObj.buff.toString('utf8');
            } catch (e) { return void readMore(); }
            if (!ARRAY_LINE.test(line)) { return void readMore(); }

            let parsed;
            try {
                parsed = JSON.parse(line);
            } catch (e) { return void readMore(); }

            const content = parsed[4];
            if (typeof (content) !== 'string') { return void readMore(); }

            index++;
            let id;
            try {
                id = Ids.fromContent(content);
            } catch (e) { return void readMore(); }

            /*  Everything up to and including sinceId is already held by the
                requester; start emitting after it. */
            if (!found) {
                if (id === sinceId) { found = true; }
                return void readMore();
            }

            out.push({ content, id, seq: index, time: parsed[5] });
            if (limit && out.length >= limit) { return void abort(); }
            readMore();
        }, (err) => {
            if (failed) { return; }
            if (err && !missing(err)) { return void cb(err); }
            /*  A `sinceId` we have never heard of means the requester's history
                diverges from ours, or was trimmed away here. Say so rather than
                silently replaying the whole channel as if it were new — that
                would look like success while producing a different document. */
            if (sinceId && !found) { return void cb('EUNKNOWNSINCE'); }
            cb(void 0, out);
        });
    };

    // Current head: the id of the last message in the committed log.
    FM.head = (channel, cb) => {
        let last;
        let count = 0;
        Env.store.readMessagesBin(channel, 0, (msgObj, readMore) => {
            let line;
            try {
                line = msgObj.buff.toString('utf8');
            } catch (e) { return void readMore(); }
            if (!ARRAY_LINE.test(line)) { return void readMore(); }
            try {
                const content = JSON.parse(line)[4];
                if (typeof (content) === 'string') {
                    last = Ids.fromContent(content);
                    count++;
                }
            } catch (e) { /* skip unreadable line */ }
            readMore();
        }, (err) => {
            if (err && !missing(err)) { return void cb(err); }
            cb(void 0, { head: last, count });
        });
    };

    /*  Every message id in the committed log, in order (R-6).

        The repair works on the log itself rather than on federation
        bookkeeping, because the messages it has to find are precisely the ones
        the bookkeeping never saw. Ids, not contents: a peer needs to know *which*
        messages we hold, and shipping the documents themselves to answer that
        would be both large and pointless.
    */
    FM.logIds = (channel, cb) => {
        const ids = [];
        Env.store.readMessagesBin(channel, 0, (msgObj, readMore) => {
            let line;
            try {
                line = msgObj.buff.toString('utf8');
            } catch (e) { return void readMore(); }
            if (!ARRAY_LINE.test(line)) { return void readMore(); }
            try {
                const content = JSON.parse(line)[4];
                if (typeof (content) === 'string') { ids.push(Ids.fromContent(content)); }
            } catch (e) { /* skip unreadable line */ }
            readMore();
        }, (err) => {
            if (err && !missing(err)) { return void cb(err); }
            cb(void 0, { ids });
        });
    };

    /*  Lift a set of messages out of the committed log and hand back their
        contents, so they can be federated properly (R-6).

        This is the one operation in the whole design that *removes* committed
        history, so it is deliberately narrow: it takes an explicit list of ids,
        returns exactly what it removed, and does nothing else. The caller
        re-federates those contents through the ordinary path, where the merge
        gives them positions both instances agree on — which is the entire point,
        since their problem was never having had one.

        Contents are read before the rewrite, not after: if the rewrite succeeds
        and the read fails, the messages are gone.
    */
    FM.excise = (channel, ids, cb) => {
        const wanted = new Set(Array.isArray(ids) ? ids : []);
        if (!wanted.size) { return void cb(void 0, { messages: [] }); }
        if (typeof (Env.store.deleteChannelLines) !== 'function') {
            return void cb(new Error('E_NO_EXCISE'));
        }

        const messages = [];
        Env.store.readMessagesBin(channel, 0, (msgObj, readMore) => {
            let line;
            try {
                line = msgObj.buff.toString('utf8');
            } catch (e) { return void readMore(); }
            if (!ARRAY_LINE.test(line)) { return void readMore(); }
            try {
                const parsed = JSON.parse(line);
                const content = parsed[4];
                if (typeof (content) === 'string') {
                    const id = Ids.fromContent(content);
                    if (wanted.has(id)) {
                        messages.push({ id, content, time: parsed[5] });
                    }
                }
            } catch (e) { /* skip unreadable line */ }
            readMore();
        }, (err) => {
            if (err && !missing(err)) { return void cb(err); }
            if (!messages.length) { return void cb(void 0, { messages: [] }); }

            Env.store.deleteChannelLines(channel, messages.map(m => m.id), (e) => {
                if (e) { return void cb(e); }
                Env.Log.info('FEDERATION_EXCISED', {
                    channel, count: messages.length
                });
                cb(void 0, { messages });
            });
        });
    };

    /*  Ingest one remote envelope into the committed log.

        The order of checks is deliberate: structural first (cheap, and catches
        a mismatched id per R-14), then duplicate detection (R-18 — a replayed
        envelope must be a no-op, not an error), then the expensive signature
        revalidation, and only then the append. `Env.queueStorage` serialises
        per channel, so ingest cannot interleave with a local write.
    */
    FM.ingest = (channel, env, cb) => {
        FM.state(channel, (err, res) => {
            if (err) { return void cb(err); }
            if (!res) { return void cb('ENOTFEDERATED'); }
            const state = res.state;

            const structural = Envelope.check(env, { channel });
            if (structural.error) { return void cb(structural.error); }

            /*  R-21: validateKey is immutable and identical across replicas. A
                peer offering a channel under a different key is offering a
                different pad. */
            if (!state.validateKey) { return void cb('ENOVALIDATEKEY'); }

            if (!state.members.includes(env.o)) { return void cb('ENOTAMEMBER'); }

            nThen(w => {
                // R-18: already committed? then this is a no-op, not a failure.
                FM.has(channel, env.id, w((e, present) => {
                    if (e) { w.abort(); return void cb(e); }
                    if (present) { w.abort(); return void cb(void 0, { duplicate: true }); }
                }));
            }).nThen(w => {
                revalidate(channel, env.m, state.validateKey, w((e) => {
                    if (e) {
                        w.abort();
                        Env.Log.error('FEDERATION_INVALID_CONTENT', {
                            channel, origin: env.o, error: e
                        });
                        return void cb('FAILED_VALIDATION');
                    }
                }));
            }).nThen(() => {
                FM.append(channel, env, cb);
            });
        });
    };

    // Is this id already in the committed log?
    FM.has = (channel, id, cb) => {
        let found = false;
        Env.store.readMessagesBin(channel, 0, (msgObj, readMore, abort) => {
            let line;
            try {
                line = msgObj.buff.toString('utf8');
            } catch (e) { return void readMore(); }
            if (!ARRAY_LINE.test(line)) { return void readMore(); }
            try {
                const content = JSON.parse(line)[4];
                if (typeof (content) === 'string' && Ids.fromContent(content) === id) {
                    found = true;
                    return void abort();
                }
            } catch (e) { /* skip */ }
            readMore();
        }, (err) => {
            if (err && !missing(err)) { return void cb(err); }
            cb(void 0, found);
        });
    };

    /*  Append a verified envelope to the committed log, in the exact line shape
        every existing reader expects. The origin's id is used as the sender so
        that the line is self-describing about where it came from; nothing reads
        it, but it makes a federated log legible in an incident. */
    /*  Push a message to the users connected here. Separated from `append`
        because at L2 the live tier and the committed tier are deliberately
        different moments: R-7 says broadcast immediately, R-3 says commit only
        at the watermark. Broadcasting at commit instead would make a remote
        edit appear a heartbeat late, which is the difference between federation
        feeling live and feeling broken. */
    const broadcast = (channel, env) => {
        const users = Array.from(Env.channel_cache[channel]?.users || []);
        if (!users.length) { return; }
        Env.interface.sendEvent(Env.getCoreId(channel), 'SEND_CHANNEL_MESSAGE', {
            users,
            message: [0, env.o, 'MSG', channel, env.m, env.t]
        });
    };

    FM.append = (channel, env, opts, cb) => {
        if (typeof (opts) === 'function') { cb = opts; opts = {}; }
        Env.queueStorage(channel, next => {
            const line = JSON.stringify([0, env.o, 'MSG', channel, env.m, env.t]);
            Env.store.messageBin(channel, Buffer.from(line + '\n', 'utf8'), (err) => {
                if (err) {
                    Env.Log.error('FEDERATION_INGEST_STORE_ERROR', err.message || err);
                    cb(err);
                    return void next();
                }
                /*  Drop the cached index: it was computed from a log this
                    append has just changed underneath it. Recomputed lazily. */
                const chan = Env.channel_cache[channel];
                if (chan) { delete chan.index; }

                /*  Push it to anyone connected to this channel here, so a user
                    reading the mirror sees remote edits arrive live rather than
                    only after a reload.

                    This reuses the existing fan-out verbatim — the same event a
                    local write emits (design §1.3) — which is the whole reason
                    remote ingestion needs no client change. */
                /*  At L1 a message becomes visible when it is appended, so this
                    is where it is announced. At L2 it was already announced on
                    arrival (R-7) and announcing again would duplicate it. */
                if (opts?.broadcast !== false) { broadcast(channel, env); }

                cb(void 0, { id: env.id });
                next();
            });
        });
    };

    /*  Wrap a locally-committed message as an envelope for publication. `seq`
        is the message's position in the committed log, so it is derived rather
        than counted and survives a restart. */
    FM.envelope = (channel, content, time, identity, cb) => {
        FM.state(channel, (err, res) => {
            if (err) { return void cb(err); }
            if (!res) { return void cb('ENOTFEDERATED'); }
            FM.head(channel, (e, info) => {
                if (e) { return void cb(e); }
                // position of this message: the log already contains it
                const seq = Math.max(0, info.count - 1);
                cb(void 0, Envelope.create({
                    channel, content, seq, lamport: seq, time
                }, identity));
            });
        });
    };

    /*  Is this instance the anchor for the channel (design §9, L1)?

        Under L1 exactly one instance orders a channel: the one that created the
        replica set. Everyone else forwards writes to it and applies its
        metadata one-way. `undefined` means "not federated", i.e. an ordinary
        pad with no restrictions at all.
    */
    FM.isAnchor = (channel) => {
        const state = cache.get(channel)?.state;
        if (!state) { return undefined; }
        // at L2 nobody anchors; local writes are always accepted here
        if (state.level === 'L2') { return true; }
        return state.origin === state.me;
    };

    /*  Apply a control commit that arrived from a peer (spec §5.4).

        Authorisation happened at the anchor: it verified that a legitimate
        owner account asked for this. A replica cannot check a remote account —
        accounts are instance-scoped — so this is the explicit trust-the-peer
        boundary R-28 describes, and the reason R-27 demands an allowlist rather
        than open peering. Only a peer in the replica set can get here at all.
    */
    FM.control = (channel, ctrl, from, cb) => {
        FM.state(channel, (err, res) => {
            if (err) { return void cb(err); }
            if (!res) { return void cb('ENOTFEDERATED'); }
            const state = res.state;

            if (from !== state.origin) {
                /*  Under L1 only the anchor emits control commits. A mirror
                    sending one is either confused or hostile; either way it does
                    not get to rewrite our metadata. */
                return void cb('ENOTANCHOR');
            }

            if (ctrl?.t === 'META') { return void applyMeta(channel, ctrl, cb); }
            if (ctrl?.t === 'DELETE') { return void applyDelete(channel, cb); }
            cb('EUNKNOWNCONTROL');
        });
    };

    /*  A metadata command, replayed verbatim on this replica.

        The line is applied through `Meta.handleCommand` exactly as a local
        command would be, so the resulting metadata is a pure function of the
        commands applied — which is what R-20 asks for. The owner check is
        deliberately not repeated: it was made at the anchor against an account
        this instance cannot see.
    */
    applyMeta = (channel, ctrl, cb) => {
        const line = ctrl.line;
        if (!Array.isArray(line) || typeof (line[0]) !== 'string') {
            return void cb('EBADCONTROL');
        }
        Env.queueMetadata(channel, (next) => {
            Metadata.getMetadataRaw(Env, channel, (err, metadata) => {
                if (err) { cb(err); return void next(); }

                let changed = false;
                try {
                    changed = Meta.handleCommand(metadata, line);
                } catch (e) {
                    cb('EBADCONTROL');
                    return void next();
                }
                // already applied (a replayed control commit is a no-op, R-18)
                if (!changed) { cb(); return void next(); }

                Env.store.writeMetadata(channel, JSON.stringify(line), (e) => {
                    if (e) { cb(e); return void next(); }
                    Env.metadata_cache[channel] = metadata;
                    Env.checkCache(channel);
                    Env.Log.info('FEDERATION_META_APPLIED',
                        { channel, command: line[0] });
                    cb();
                    next();
                });
            });
        });
    };

    /*  The owner deleted the pad at the anchor. Archive our replica too —
        archive rather than erase, matching what a local deletion does, so an
        operator can still recover from a mistaken or malicious delete. */
    applyDelete = (channel, cb) => {
        Env.Log.info('FEDERATION_DELETE_APPLIED', { channel });
        FM.disable(channel, (err) => {
            if (err) { return void cb(err); }
            Env.CM?.removeChannel?.(Env, channel);
            cb();
        });
    };

    // ================================================================
    //  L2: multi-master (M3)
    // ================================================================

    /*  Under L1 the anchor ordered everything and a replica appended what it was
        sent. At L2 every member accepts writes, so nothing is committed on
        arrival: it goes to the pending log and waits for the watermark.

        `FM.isAnchor` is undefined for an L2 channel — there is no anchor.
    */
    FM.isL2 = (channel) => cache.get(channel)?.state?.level === 'L2';

    const withState = (channel, cb) => {
        FM.state(channel, (err, res) => {
            if (err) { return void cb(err); }
            if (!res) { return void cb('ENOTFEDERATED'); }
            cb(void 0, res.state, res.etag);
        });
    };

    /*  Persist state after a merge step. Conditional on the etag we read, so two
        writers are detected rather than silently interleaved — the per-channel
        single-writer property means that is a fault, not a race to retry. */
    saveState = (channel, state, etag, cb) => {
        log.writeState(channel, state, etag, (err, newEtag) => {
            if (err) { return void cb(err); }
            remember(channel, state, newEtag);
            cb(void 0, newEtag);
        });
    };

    /*  Allocate ordering metadata for a message we are about to accept (R-9).

        Split from signing on purpose: the merge state lives here, but the
        instance key lives on the federation node. So storage assigns `(s, l)`
        and persists the advance; the federation node builds and signs the
        envelope and hands it straight back to `acceptRemote`. Storage never
        needs the key, and the key never needs the state.
    */
    FM.allocate = (channel, _cb) => {
      Env.queueFederation(channel, (next) => {
        const cb = (...args) => { next(); _cb(...args); };
        withState(channel, (err, state, etag) => {
            if (err) { return void cb(err); }
            if (state.level !== 'L2') { return void cb('ENOTL2'); }

            const lamport = Merge.nextLamport(state);
            const seq = (state.self?.seq ?? -1) + 1;

            /*  Persist the advance *before* returning it. If we crash here the
                worst case is a burnt sequence number, which is harmless; the
                opposite order could reissue one, which would look to peers like
                two different messages claiming the same position. */
            Merge.observe(state, state.me, { seq, lamport }, Date.now());
            saveState(channel, state, etag, (e) => {
                if (e) { return void cb(e); }
                const channelData = Env.channel_cache[channel] || {};
                cb(void 0, {
                    seq, lamport,
                    origin: state.me,
                    users: Array.from(channelData.users || [])
                });
            });
        });
      });
    };

    /*  An envelope from a peer on an L2 channel. Same durability point, same
        refusal to commit early. Content is revalidated here (R-15) rather than
        at commit time, so a forged message never reaches the pending log. */
    FM.acceptRemote = (channel, env, _cb) => {
      Env.queueFederation(channel, (next) => {
        const cb = (...args) => { next(); _cb(...args); };
        withState(channel, (err, state, etag) => {
            if (err) { return void cb(err); }

            const structural = Envelope.check(env, { channel });
            if (structural.error) { return void cb(structural.error); }
            if (!state.members.includes(env.o)) { return void cb('ENOTAMEMBER'); }

            /*  R-18: a duplicate envelope must be a no-op.

                The live push and a backfill overlap by design — subscribing to a
                pad that already has content delivers the same message both ways
                — so this is a normal event, not an attack. Appending it twice
                would put two identical lines in the committed log *here* and not
                on the peer, which breaks R-1: the two replicas would serve
                different sequences for the same set of messages.

                Checked against the pending log by id rather than by sequence
                number, because an envelope can legitimately arrive out of order
                and a `seq <= last seen` test would reject gap-fillers.  */
            log.readPending(channel, env.o, (ePend, existing) => {
                if (ePend) { return void cb(ePend); }
                if ((existing || []).some(x => x.id === env.id)) {
                    return void cb(void 0, { duplicate: true });
                }

            revalidate(channel, env.m, state.validateKey, (e) => {
                if (e) {
                    Env.Log.error('FEDERATION_INVALID_CONTENT',
                        { channel, origin: env.o, error: e });
                    return void cb('FAILED_VALIDATION');
                }
                log.appendPending(channel, env, (e2) => {
                    if (e2) { return void cb(e2); }
                    /*  R-7: live now, committed later. A reader on this instance
                        sees the edit as soon as it arrives rather than waiting
                        for the watermark. */
                    if (env.o !== state.me) { broadcast(channel, env); }
                    Merge.observe(state, env.o, { seq: env.s, lamport: env.l }, Date.now());
                    saveState(channel, state, etag, (e3) => {
                        if (e3) { return void cb(e3); }
                        cb();
                        FM.mergeLogged(channel);
                    });
                });
            });
            });
        });
      });
    };

    /*  A peer's clock, learned from a heartbeat rather than a message (R-4).
        This is what keeps the watermark moving on an idle pad — without it a
        quiet channel would never commit anything. */
    FM.observePeer = (channel, originId, info, _cb) => {
      Env.queueFederation(channel, (next) => {
        const cb = (...args) => { next(); _cb(...args); };
        withState(channel, (err, state, etag) => {
            if (err) { return void cb(err); }
            Merge.observe(state, originId, info, Date.now());
            saveState(channel, state, etag, (e) => {
                if (e) { return void cb(e); }
                cb();
                FM.mergeLogged(channel);
            });
        });
      });
    };

    /*  The merge loop (R-3): move everything below the watermark from pending to
        the committed log, in `(l, o, id)` order.

        Serialised per channel through the storage write queue, so two merge
        passes cannot interleave and write the same envelope twice.
    */
    /*  A merge failure is not cosmetic: it means this replica has stopped
        advancing its committed log while its peers carry on, which is exactly
        the divergence the design exists to prevent. Never swallow it. */
    FM.mergeLogged = (channel) => {
        FM.merge(channel, (err, n) => {
            if (err) {
                Env.Log.error('FEDERATION_MERGE_ERROR',
                    { channel, error: err.message || String(err) });
                return;
            }
            if (n) { Env.Log.verbose('FEDERATION_MERGED', { channel, committed: n }); }
        });
    };

    /*  Re-run the merge for every L2 channel we hold state for.

        Most merges are triggered by something arriving, but two cases have no
        trigger at all: a member being *evicted* (R-5), which raises the
        watermark without any message, and a gap that only clears because a
        peer went away. Without a sweep the committed log simply stops after a
        partition, which is the one thing eviction exists to prevent.
    */
    FM.sweep = () => {
        cache.forEach((entry, channel) => {
            if (entry?.state?.level === 'L2') { FM.mergeLogged(channel); }
        });
    };

    let merging = new Set();
    FM.merge = (channel, cb) => {
        cb ||= () => {};
        if (merging.has(channel)) { return void cb(); }
        merging.add(channel);
        const done = (err, n) => { merging.delete(channel); cb(err, n); };

        Env.queueFederation(channel, (next) => {
        const finish = (e, n) => { next(); done(e, n); };
        withState(channel, (err, state, etag) => {
            if (err) { return void finish(err); }
            if (state.level !== 'L2') { return void finish(); }

            log.readAllPending(channel, state, (e, pending) => {
                if (e) { return void finish(e); }
                if (!pending.length) { return void finish(void 0, 0); }

                const { ready, held, reordered } = Merge.partition(state, pending,
                    Date.now());

                /*  R-48: envelopes that arrived too late to be committed in
                    their proper place. They are held, not appended — appending
                    would diverge this instance from the peer that got them in
                    time, permanently and silently. Reported at error level
                    because the channel is now stalled for those origins and
                    only `RECONCILE` (R-6) can clear it. */
                if (reordered && reordered.length) {
                    Env.Log.error('FEDERATION_RECONCILE_REQUIRED', {
                        channel,
                        held: reordered.length,
                        tip: state.tip,
                        earliest: reordered.reduce((a, b) =>
                            (a && a.l <= b.l) ? a : b, null)
                    });
                }
                if (!ready.length) { return void finish(void 0, 0); }

                /*  Append in sort order, one at a time. Every replica computes
                    the same order over the same set, so every replica writes the
                    same lines in the same sequence — which is R-1, and what
                    §11.1 showed cannot be left to chance. */
                let i = 0;
                const step = () => {
                    if (i >= ready.length) {
                        /*  Keep what is still waiting — and, for our own
                            messages, anything a peer has not acknowledged yet.

                            Dropping an envelope the moment it commits locally
                            leaves nothing to resend when a peer turns out not to
                            have it, which is what made a lost message permanent.
                            Retention is bounded by the slowest peer's
                            acknowledgement, i.e. by the same watermark that
                            bounds everything else. */
                        const byOrigin = new Map();
                        held.forEach(h => {
                            if (!byOrigin.has(h.o)) { byOrigin.set(h.o, []); }
                            byOrigin.get(h.o).push(h);
                        });

                        let ackedByAll = Infinity;
                        (state.members || []).forEach(m => {
                            if (m === state.me) { return; }
                            const seen = state.peers?.[m]?.have?.[state.me];
                            ackedByAll = Math.min(ackedByAll,
                                typeof (seen) === 'number' ? seen : -1);
                        });
                        if (ackedByAll !== Infinity) {
                            /*  From `pending`, not from ready+held: envelopes
                                retained by an earlier pass are in neither, and
                                sourcing them from there would retire them the
                                moment they committed — which is the hole this
                                retention exists to close. */
                            pending.forEach(e => {
                                if (e.o !== state.me || e.s <= ackedByAll) { return; }
                                const list = byOrigin.get(e.o) || [];
                                if (!list.some(x => x.id === e.id)) { list.push(e); }
                                byOrigin.set(e.o, list);
                            });
                        }
                        const origins = Array.from(new Set(
                            pending.map(x => x.o)));
                        let pendingCompaction = origins.length;
                        if (!pendingCompaction) {
                            return void saveState(channel, state, etag,
                                (e2) => finish(e2, ready.length));
                        }
                        origins.forEach(o => {
                            log.compactPending(channel, o, byOrigin.get(o) || [], () => {
                                if (--pendingCompaction === 0) {
                                    saveState(channel, state, etag,
                                        (e2) => finish(e2, ready.length));
                                }
                            });
                        });
                        return;
                    }
                    const env = ready[i++];
                    // already broadcast when accepted; committing is silent
                    FM.append(channel, env, { broadcast: false }, (e2) => {
                        if (e2) { return void finish(e2); }
                        Merge.noteCommitted(state, env.o, env.s, env.l, env.id);
                        step();
                    });
                };
                step();
            });
        });
        });
    };

    /*  History as R-8 defines it: the committed prefix, then the uncommitted
        tail in `(l, o, id)` order.

        The tail's sort is not cosmetic. A replica that served it in arrival
        order could hand a joining client a different sequence from its peer,
        and the resulting documents would diverge permanently — see
        docs/experiments/chainpad-ordering.
    */
    FM.tail = (channel, cb) => {
        FM.state(channel, (err, res) => {
            if (err || !res || res.state.level !== 'L2') { return void cb(void 0, []); }
            log.readAllPending(channel, res.state, (e, pending) => {
                if (e) { return void cb(e); }
                const { held, ready } = Merge.partition(res.state, pending, Date.now());
                // everything not yet in the committed log, in the agreed order
                cb(void 0, Order.sortTail(ready.concat(held)));
            });
        });
    };

    /*  What this instance holds, per origin, for a peer to compare against
        (R-47). Cheap: it reads the pending log, which is bounded by the
        watermark. */
    FM.have = (channel, cb) => {
        FM.state(channel, (err, res) => {
            if (err || !res || res.state.level !== 'L2') { return void cb(void 0, {}); }
            log.readAllPending(channel, res.state, (e, pending) => {
                if (e) { return void cb(e); }
                cb(void 0, Merge.have(res.state, pending));
            });
        });
    };

    /*  Record what a peer says it holds, and return our own envelopes it lacks.

        Both halves in one call because they come from the same heartbeat: the
        peer's `have` is what lets us retire retained envelopes, and it is also
        what tells us which to resend.
    */
    FM.reconcile = (channel, originId, peerHave, cb) => {
      Env.queueFederation(channel, (next) => {
        const done = (...a) => { next(); cb(...a); };
        withState(channel, (err, state, etag) => {
            if (err) { return void done(err); }
            if (state.level !== 'L2') { return void done(void 0, { envelopes: [] }); }

            const peers = state.peers = state.peers || {};
            const peer = peers[originId] = peers[originId] || { seq: -1, lamport: 0 };
            peer.have = peerHave || {};
            peer.atime = Date.now();

            log.readAllPending(channel, state, (e, pending) => {
                if (e) { return void done(e); }
                const { from, to } = Merge.missingFor(state, peerHave, pending);
                saveState(channel, state, etag, () => {
                    if (from === null) { return void done(void 0, { envelopes: [] }); }

                    /*  Only resend what we still hold. Anything older has been
                        acknowledged by everyone, so a peer asking for it is
                        confused rather than behind — and replaying it would
                        reorder history rather than repair it. */
                    const out = pending.filter(x =>
                        x.o === state.me && x.s >= from && x.s <= to);
                    if (out.length) {
                        Env.Log.info('FEDERATION_RESEND', {
                            channel, peer: originId.slice(0, 8),
                            from, to, count: out.length
                        });
                    }
                    done(void 0, { envelopes: out });
                });
            });
        });
      });
    };

    /*  Trim history (R-11), gated by the R-38 precondition as a hard assertion
        (R-41).

        §11.3 measured a premature trim silently *destroying* the other branch's
        content — not merely reordering it — so this check is made here,
        immediately before trimming, rather than being left to callers. Failing
        it aborts and reports; it is never a warning.
    */
    FM.trim = (channel, toLamport, cb) => {
        withState(channel, (err, state) => {
            if (err) { return void cb(err); }
            const verdict = Merge.mayTrim(state, toLamport, Date.now());
            if (!verdict.ok) {
                Env.Log.error('FEDERATION_TRIM_REFUSED', {
                    channel, toLamport, reason: verdict.reason,
                    evicted: verdict.evicted, watermark: verdict.watermark
                });
                return void cb(verdict.reason);
            }
            Env.Log.info('FEDERATION_TRIM_OK', { channel, toLamport });
            cb(void 0, { ok: true, watermark: verdict.watermark });
        });
    };

    return FM;
};

module.exports = { create };
