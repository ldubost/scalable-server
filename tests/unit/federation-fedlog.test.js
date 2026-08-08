// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors
//
// SPDX-License-Identifier: AGPL-3.0-or-later

/*  storage/federation/fedlog.js against a real ObjectBackend.
 *
 *  This is where R-8 (history = committed prefix + sorted tail), R-9 (durable
 *  before acknowledgement) and R-36 (the tail is sorted, never served in arrival
 *  order) actually land, so it is tested against a live backend rather than a
 *  mock — the conditional-write and missing-key semantics are exactly what a
 *  mock would get wrong.
 */

const { test } = require('node:test');
const assert = require('node:assert');
const Fs = require('node:fs/promises');
const Os = require('node:os');
const Path = require('node:path');

const FsBackend = require('../../common/storage/backend/fs.js');
const FedLog = require('../../storage/federation/fedlog.js');
const { p, failure } = require('./backend-conformance.js');

const CHANNEL = 'abcdef0123456789abcdef0123456789';
const VK = Buffer.alloc(32, 5).toString('base64');
const A = Buffer.alloc(32, 1).toString('base64');
const B = Buffer.alloc(32, 2).toString('base64');

const withFM = (body) => async () => {
    const root = await Fs.mkdtemp(Path.join(Os.tmpdir(), 'cryptpad-fedlog-'));
    const backend = await p(FsBackend.create, { root });
    try {
        await body(FedLog.create({ federationBackend: backend }), backend, root);
    } finally {
        await Fs.rm(root, { recursive: true, force: true });
    }
};

// an envelope, only the fields fedlog cares about
const env = (o, s, l, id) => ({ v: 1, c: CHANNEL, m: 'x', id, o, s, l, t: 0, sig: 's' });

// --- state -----------------------------------------------------------------

test('state: a channel with no state is simply not federated', withFM(async FM => {
    assert.strictEqual(await p(FM.readState, CHANNEL), undefined);
    assert.strictEqual(await p(FM.isFederated, CHANNEL), false);
}));

test('state: init then read round-trips', withFM(async FM => {
    const { state } = await p(FM.initState, CHANNEL, { validateKey: VK, self: A, members: [B] });
    assert.deepStrictEqual(state.members, [A, B].sort());
    assert.strictEqual(state.validateKey, VK);

    const read = await p(FM.readState, CHANNEL);
    assert.strictEqual(read.state.channel, CHANNEL);
    assert.strictEqual(read.state.validateKey, VK);
    assert.ok(read.etag, 'an etag is returned for conditional writes');
    assert.strictEqual(await p(FM.isFederated, CHANNEL), true);
}));

/*  Re-initialising must not silently reset an existing replica's sequence
    counters — that would make every peer's R-17 gap detection fire. */
test('state: init refuses to clobber an existing replica', withFM(async FM => {
    await p(FM.initState, CHANNEL, { validateKey: VK, self: A });
    const err = await failure(p(FM.initState, CHANNEL, { validateKey: VK, self: A }));
    assert.ok(err, 'a second init must fail');
}));

/*  The per-channel single-writer property means a lost race is a fault, not a
    normal case, so it must surface rather than be retried away. */
test('state: a stale etag is refused', withFM(async FM => {
    const { etag } = await p(FM.initState, CHANNEL, { validateKey: VK, self: A });
    const { state } = await p(FM.readState, CHANNEL);

    state.self.seq = 1;
    const newEtag = await p(FM.writeState, CHANNEL, state, etag);
    assert.ok(newEtag);

    // writing again with the old etag must not succeed
    state.self.seq = 99;
    const err = await failure(p(FM.writeState, CHANNEL, state, etag));
    assert.ok(err, 'a concurrent writer must be detected');

    const after = await p(FM.readState, CHANNEL);
    assert.strictEqual(after.state.self.seq, 1, 'the stale write did not land');
}));

// --- pending logs ----------------------------------------------------------

test('pending: append then read back in order', withFM(async FM => {
    await p(FM.appendPending, CHANNEL, env(A, 0, 1, 'id0'));
    await p(FM.appendPending, CHANNEL, env(A, 1, 2, 'id1'));
    const envs = await p(FM.readPending, CHANNEL, A);
    assert.deepStrictEqual(envs.map(e => e.id), ['id0', 'id1']);
}));

test('pending: an absent log reads as empty, not an error', withFM(async FM => {
    assert.deepStrictEqual(await p(FM.readPending, CHANNEL, A), []);
}));

test('pending: origins do not interleave', withFM(async FM => {
    await p(FM.appendPending, CHANNEL, env(A, 0, 1, 'a0'));
    await p(FM.appendPending, CHANNEL, env(B, 0, 1, 'b0'));
    await p(FM.appendPending, CHANNEL, env(A, 1, 2, 'a1'));

    assert.deepStrictEqual((await p(FM.readPending, CHANNEL, A)).map(e => e.id), ['a0', 'a1']);
    assert.deepStrictEqual((await p(FM.readPending, CHANNEL, B)).map(e => e.id), ['b0']);
}));

/*  An interrupted append leaves a partial final line. The envelope was never
    acknowledged, so the peer will resend it; dropping the fragment is right, and
    failing the whole read would block the channel forever on one bad write. */
test('pending: a torn final line is discarded, earlier records survive',
    withFM(async (FM, backend) => {
        await p(FM.appendPending, CHANNEL, env(A, 0, 1, 'good'));
        await p(backend.append, FedLog.pendingKey(CHANNEL, A),
            Buffer.from('{"v":1,"partial'), {});

        const envs = await p(FM.readPending, CHANNEL, A);
        assert.deepStrictEqual(envs.map(e => e.id), ['good']);
    }));

// --- the tail (R-8, R-36) --------------------------------------------------

/*  The property the whole design rests on: the tail served to a joining client
    is in (l, o, id) order regardless of the order envelopes arrived in. Two
    replicas that serve it in arrival order can converge on permanently
    different documents — see docs/experiments/chainpad-ordering. */
test('R-36: the merged tail is sorted, not in arrival order', withFM(async FM => {
    const { state } = await p(FM.initState, CHANNEL,
        { validateKey: VK, self: A, members: [B] });

    // deliberately append out of order, and interleaved between origins
    await p(FM.appendPending, CHANNEL, env(A, 2, 9, 'zzz'));
    await p(FM.appendPending, CHANNEL, env(B, 0, 3, 'mmm'));
    await p(FM.appendPending, CHANNEL, env(A, 0, 3, 'aaa'));
    await p(FM.appendPending, CHANNEL, env(A, 1, 3, 'nnn'));

    const tail = await p(FM.readAllPending, CHANNEL, state);
    assert.deepStrictEqual(tail.map(e => `${e.l}/${e.o === A ? 'A' : 'B'}/${e.id}`), [
        '3/A/aaa',   // lamport 3, origin A, id aaa
        '3/A/nnn',   // same lamport and origin, id breaks the tie
        '3/B/mmm',   // same lamport, origin B sorts after A
        '9/A/zzz'    // higher lamport last
    ]);
}));

test('tail: empty when nothing is pending', withFM(async FM => {
    const { state } = await p(FM.initState, CHANNEL, { validateKey: VK, self: A });
    assert.deepStrictEqual(await p(FM.readAllPending, CHANNEL, state), []);
}));

// --- compaction and teardown ----------------------------------------------

test('compact: keeps only the uncommitted remainder', withFM(async FM => {
    await p(FM.appendPending, CHANNEL, env(A, 0, 1, 'old'));
    await p(FM.appendPending, CHANNEL, env(A, 1, 2, 'new'));

    await p(FM.compactPending, CHANNEL, A, [env(A, 1, 2, 'new')]);
    assert.deepStrictEqual((await p(FM.readPending, CHANNEL, A)).map(e => e.id), ['new']);

    await p(FM.compactPending, CHANNEL, A, []);
    assert.deepStrictEqual(await p(FM.readPending, CHANNEL, A), []);
}));

/*  Design §3.4: un-federating removes the fed/ keys and leaves the committed
    log untouched, so the pad remains an ordinary pad. There is no migration. */
test('unfederate: removes all fed/ keys and nothing else',
    withFM(async (FM, backend) => {
        const { state } = await p(FM.initState, CHANNEL,
            { validateKey: VK, self: A, members: [B] });
        await p(FM.appendPending, CHANNEL, env(A, 0, 1, 'a'));
        await p(FM.appendPending, CHANNEL, env(B, 0, 1, 'b'));

        // something that must survive
        await p(backend.put, `channel/ab/${CHANNEL}.ndjson`, Buffer.from('[0]\n'), {});

        await p(FM.unfederate, CHANNEL, state);

        assert.strictEqual(await p(FM.isFederated, CHANNEL), false);
        assert.deepStrictEqual(await p(FM.readPending, CHANNEL, A), []);
        assert.deepStrictEqual(await p(FM.readPending, CHANNEL, B), []);

        const listed = await p(backend.list, 'fed/', {});
        assert.strictEqual(listed.keys.length, 0, 'no fed/ keys remain');

        const body = await p(backend.get, `channel/ab/${CHANNEL}.ndjson`);
        assert.strictEqual(body.toString('utf8'), '[0]\n',
            'the committed log is left exactly as it was');
    }));

/*  originIds are base64 and can contain '/', which is the key separator. If that
    leaked into the key, two origins could collide or escape the fed/ prefix. */
test('keys: an originId containing / is encoded safely', () => {
    const slashy = Buffer.from(Array.from({ length: 32 }, (_, i) => i * 8 % 256))
        .toString('base64');
    const key = FedLog.pendingKey(CHANNEL, slashy);
    assert.ok(key.startsWith(`fed/${CHANNEL.slice(0, 2)}/${CHANNEL}.pending.`));
    assert.ok(!key.slice(`fed/${CHANNEL.slice(0, 2)}/`.length).includes('/'),
        `originId must not introduce a path separator: ${key}`);
});

/*  R-18: the live push and a backfill overlap by design, so the same envelope
    arriving twice must be a no-op. Appending it twice would put two identical
    lines in this replica's committed log and not the peer's, which breaks R-1 —
    the two would serve different sequences for the same set of messages.

    Detected by id, not by sequence number: an envelope can legitimately arrive
    out of order, and a `seq <= last seen` test would reject gap-fillers. */
test('R-18: a duplicate is detectable in the pending log by id', withFM(async FM => {
    await p(FM.appendPending, CHANNEL, env(A, 0, 1, 'dup'));
    await p(FM.appendPending, CHANNEL, env(A, 1, 2, 'other'));

    const held = await p(FM.readPending, CHANNEL, A);
    assert.ok(held.some(x => x.id === 'dup'), 'the id is visible for the check');

    // out-of-order arrival must NOT look like a duplicate
    assert.ok(!held.some(x => x.id === 'late'),
        'an unseen id is not mistaken for one already held');
}));

/*  Regression: only the ORIGIN may adopt an existing log as its own.

    A channel promoted to L2 already has history, authored by the instance that
    held it. That instance's allocator must continue after it. A joining replica
    must not: it has authored nothing, and claiming the origin's messages as its
    own makes the two disagree about what any (origin, seq) refers to — after
    which messages are skipped silently on both sides.

    This shipped, and diverged a real pad: 6 messages ended up only on one
    instance and 4 only on the other, with both believing they were in sync. */
test('L2: a replica does not claim the origin history as its own', withFM(async FM => {
    const ORIGIN = A, REPLICA = B;

    // the replica's state: origin is someone else
    const { state } = await p(FM.initState, CHANNEL, {
        validateKey: VK, self: REPLICA, origin: ORIGIN,
        members: [ORIGIN, REPLICA], level: 'L2'
    });

    assert.strictEqual(state.origin, ORIGIN);
    assert.strictEqual(state.me, REPLICA);
    assert.strictEqual(state.self.seq, -1,
        'a replica has authored nothing, so its sequence must start at -1');
    assert.notStrictEqual(state.self.committedSeq, 0,
        'and it must not claim to have committed messages of its own');
}));

test('L2: the origin does record itself as the origin', withFM(async FM => {
    const { state } = await p(FM.initState, CHANNEL, {
        validateKey: VK, self: A, origin: A, members: [A, B], level: 'L2'
    });
    assert.strictEqual(state.origin, state.me,
        'the instance that enables a pad is its origin');
}));
