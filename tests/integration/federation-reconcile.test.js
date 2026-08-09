// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors
//
// SPDX-License-Identifier: AGPL-3.0-or-later

/*  Repairing a split pad (spec R-6, R-53).
 *
 *  The situation this exists for was found on a live pair, not invented here.
 *  A message committed while core did not know its channel was federated never
 *  enters the federation order, so it has no sequence: no gap to detect, nothing
 *  to resend, and every mechanism in §4 blind to it. The two instances end up
 *  serving different documents while agreeing on every counter they exchange —
 *  identical `have` maps, identical committed tips, matching sequence counts.
 *  Measured on one real pad: 116 messages common and in identical order, plus 2
 *  held only by one instance and 7 only by the other.
 *
 *  The tests reproduce that exactly, by appending a validly signed message to a
 *  stopped instance's channel log. That is what an orphan *is* — a line in the
 *  log that federation never saw — and there is no way to produce one through
 *  the normal path now that the window is closed, which is rather the point.
 *
 *  What the repair must achieve is that **both instances hold everything**. It
 *  is append-only: a stored patch is somebody's work, and no server is in a
 *  position to decide that a copy of it should stop existing — it cannot read the
 *  content, and the party that can, the client, already resolves a chain it
 *  receives out of order.
 *
 *  An earlier version tried to give both instances one identical order by
 *  excising its own unfederated messages and re-federating them. It could not
 *  tell an orphan from a message merely *in flight*, so on a busy pad it cut out
 *  and re-sent perfectly good messages; the divergence outlived the retry
 *  interval and each round moved more of them. On a live pair a gap of 8 grew to
 *  117 in three minutes. These tests therefore assert the union and, explicitly,
 *  that nothing is ever lost — not that the two logs are identical.
 */

const { test } = require('node:test');
const assert = require('node:assert');
const Fs = require('node:fs/promises');
const Path = require('node:path');

const Capability = require('../../common/federation/capability.js');
const Rig = require('./rig.js');

const settle = (ms) => new Promise(r => setTimeout(r, ms));

const until = async (fn, ms, what) => {
    const deadline = Date.now() + ms;
    let last;
    while (Date.now() < deadline) {
        last = await fn();
        if (last) { return last; }
        await settle(500);
    }
    throw new Error(`timed out waiting for ${what}; last ${JSON.stringify(last)}`);
};

const federate = async (rig, a, b, channel, padKeys) => {
    const cap = Capability.mint({
        channel, from: a.originId, to: b.originId
    }, padKeys.secretKey);
    const enabled = await rig.enableOnOrigin(a, channel, padKeys.validateKey,
        [b.originId], 'L2');
    assert.ok(enabled.ok && !enabled.error, `enable: ${JSON.stringify(enabled)}`);
    const res = await rig.replicate(b, {
        channel, peer: a.originId, cap, validateKey: padKeys.validateKey
    });
    assert.deepStrictEqual(res, { ok: true }, `replicate: ${JSON.stringify(res)}`);
};

const channelPath = (inst, channel) =>
    Path.join(inst.dir, 'channel', channel.slice(0, 2), `${channel}.ndjson`);

/*  Append messages to an instance's log without federation ever seeing them.

    The instance must be stopped: this writes underneath the store, which is the
    only way to produce the state a restart window used to produce. Restarting is
    the caller's job, so several orphans can be planted in one stop.
*/
const plantOrphans = async (inst, channel, padKeys, texts) => {
    const path = channelPath(inst, channel);
    const contents = texts.map(t => Rig.mkMessage(padKeys.secretKey, t));
    const lines = contents.map(content => JSON.stringify(
        [0, 'orphan-sender-000000000000000000', 'MSG', channel, content, Date.now()]
    )).join('\n') + '\n';
    await Fs.appendFile(path, lines);
    return contents;
};

const setup = async (rig) => {
    const a = rig.get('A'), b = rig.get('B');
    const pad = Rig.randomChannel();
    const padKeys = Rig.mkPadKeys();

    const clientA = await rig.client(a, pad, padKeys);
    const seeded = [];
    for (let i = 0; i < 3; i++) { seeded.push(await clientA.send(`shared-${i}`)); }
    await settle(400);

    await federate(rig, a, b, pad, padKeys);
    await until(async () => {
        const h = await rig.history(b, pad);
        return h.length === seeded.length ? h : null;
    }, 40000, 'B to backfill the shared history');

    await clientA.close();
    return { a, b, pad, padKeys, seeded };
};

test('R-6: a pad split by orphans on both sides is repaired', async () => {
    const rig = await Rig.create(['A', 'B']);
    try {
        const { a, b, pad, padKeys, seeded } = await setup(rig);

        /*  The divergence: messages in each log that federation never saw.
            Asymmetric on purpose — the real one was 2 against 7.

            Both instances are stopped for this, and the split is verified on
            disk rather than through the servers: once they are running the
            repair may already have converged them, and a test that races the
            behaviour it is checking proves nothing either way. */
        await rig.killInstance(a);
        await rig.killInstance(b);
        const orphansA = await plantOrphans(a, pad, padKeys, ['only-on-A-1', 'only-on-A-2']);
        const orphansB = await plantOrphans(b, pad, padKeys,
            ['only-on-B-1', 'only-on-B-2', 'only-on-B-3']);

        const rawA = await Fs.readFile(channelPath(a, pad), 'utf8');
        const rawB = await Fs.readFile(channelPath(b, pad), 'utf8');
        orphansA.forEach(m => {
            assert.ok(rawA.includes(m), 'A must hold its orphan');
            assert.ok(!rawB.includes(m), 'and B must not — that is the split');
        });
        orphansB.forEach(m => {
            assert.ok(rawB.includes(m), 'B must hold its orphan');
            assert.ok(!rawA.includes(m), 'and A must not');
        });

        await rig.restartInstance(a);
        await rig.restartInstance(b);

        const all = seeded.concat(orphansA, orphansB);
        const holdsEverything = (h) => all.every(m => h.includes(m));

        const onA = await until(async () => {
            const h = await rig.history(a, pad);
            return holdsEverything(h) ? h : null;
        }, 120000, 'A to hold the union');
        const onB = await until(async () => {
            const h = await rig.history(b, pad);
            return holdsEverything(h) ? h : null;
        }, 120000, 'B to hold the union');

        /*  Both hold everything. Order may differ — each instance keeps its own
            messages where they are and appends what it was missing — and
            reconciling that is the client's job. */
        assert.strictEqual(onA.length, all.length,
            'A must hold the union and nothing more');
        assert.strictEqual(onB.length, all.length,
            'B must hold the union and nothing more');

        /*  Exactly once each. A repair races ordinary replication, so absorbing
            the same message twice is the failure this guards. */
        all.forEach(m => {
            assert.strictEqual(onA.filter(x => x === m).length, 1,
                'no message may be stored twice by a repair');
            assert.strictEqual(onB.filter(x => x === m).length, 1,
                'no message may be stored twice by a repair');
        });

        /*  And nothing was removed to achieve it: every message each instance
            held before the repair it must still hold. */
        seeded.concat(orphansA).forEach(m => assert.ok(onA.includes(m),
            'A must not have lost anything it held'));
        seeded.concat(orphansB).forEach(m => assert.ok(onB.includes(m),
            'B must not have lost anything it held'));
    } finally {
        await rig.stop();
    }
});

/*  One-sided divergence, which is the commoner shape: one instance was restarted
    and the other was not. */
test('R-6: orphans on one side only are repaired', async () => {
    const rig = await Rig.create(['A', 'B']);
    try {
        const { a, b, pad, padKeys, seeded } = await setup(rig);

        await rig.killInstance(b);
        const orphansB = await plantOrphans(b, pad, padKeys, ['stranded-1', 'stranded-2']);
        await rig.restartInstance(b);

        const all = seeded.concat(orphansB);
        const onA = await until(async () => {
            const h = await rig.history(a, pad);
            return all.every(m => h.includes(m)) ? h : null;
        }, 120000, 'A to receive what was stranded on B');
        const onB = await until(async () => {
            const h = await rig.history(b, pad);
            return all.every(m => h.includes(m)) ? h : null;
        }, 120000, 'B to still hold everything');

        assert.strictEqual(onA.length, all.length, 'A holds the union, nothing extra');
        assert.strictEqual(onB.length, all.length, 'B holds the union, nothing extra');
        orphansB.forEach(m => assert.ok(onB.includes(m),
            'and B still holds what was stranded on it'));
    } finally {
        await rig.stop();
    }
});

/*  The case that broke the first implementation, and the reason this file exists
    in its current form.

    A repair runs against a pad people are still typing in, so at the moment it
    compares logs some messages are legitimately *in flight*: federated, correct,
    simply not committed on the peer yet. They are indistinguishable from orphans
    by the only evidence available — the peer does not have them.

    The first version excised what the peer lacked and re-federated it. On a busy
    pad that meant cutting out perfectly good messages; the divergence outlived
    the retry interval, the next round did it again, and a gap of 8 messages grew
    to 117 in three minutes on a live pair. Appending cannot fail that way: the
    worst an in-flight message can suffer is being sent twice and recognised.
*/
test('R-6: a repair running against live traffic neither loses nor duplicates', async () => {
    const rig = await Rig.create(['A', 'B']);
    try {
        const { a, b, pad, padKeys, seeded } = await setup(rig);

        await rig.killInstance(b);
        const orphansB = await plantOrphans(b, pad, padKeys,
            ['stranded-1', 'stranded-2', 'stranded-3']);
        await rig.restartInstance(b);

        /*  Keep writing from both sides while the repair happens, so it is
            comparing logs that are moving under it. */
        const clientA = await rig.client(a, pad, padKeys);
        const clientB = await rig.client(b, pad, padKeys);
        const live = [];
        for (let i = 0; i < 8; i++) {
            live.push(await clientA.send(`live-A-${i}`));
            live.push(await clientB.send(`live-B-${i}`));
        }

        const all = seeded.concat(orphansB, live);
        const settled = async (inst) => {
            const h = await rig.history(inst, pad);
            return all.every(m => h.includes(m)) ? h : null;
        };

        const onA = await until(() => settled(a), 120000, 'A to hold everything');
        const onB = await until(() => settled(b), 120000, 'B to hold everything');

        /*  The failure mode was unbounded growth, so the count is the assertion
            that matters: exactly the union, on both, however much traffic
            crossed the repair. */
        assert.strictEqual(onA.length, all.length,
            `A grew beyond the union: ${onA.length} vs ${all.length}`);
        assert.strictEqual(onB.length, all.length,
            `B grew beyond the union: ${onB.length} vs ${all.length}`);

        all.forEach(m => {
            assert.strictEqual(onA.filter(x => x === m).length, 1,
                'a message caught mid-flight by a repair must be stored once');
            assert.strictEqual(onB.filter(x => x === m).length, 1,
                'a message caught mid-flight by a repair must be stored once');
        });

        await clientA.close();
        await clientB.close();
    } finally {
        await rig.stop();
    }
});

/*  A repair costs a log rewrite on the peer, so it must not run on a healthy pad —
    and detection must not mistake a moment of lag for a divergence. */
test('R-6: a pad that agrees is left alone', async () => {
    const rig = await Rig.create(['A', 'B']);
    try {
        const { a, b, pad, padKeys, seeded } = await setup(rig);

        const clientA = await rig.client(a, pad, padKeys);
        const clientB = await rig.client(b, pad, padKeys);
        const more = [];
        for (let i = 0; i < 3; i++) {
            more.push(await clientA.send(`A-${i}`), await clientB.send(`B-${i}`));
        }

        const all = seeded.concat(more);
        const onA = await until(async () => {
            const h = await rig.history(a, pad);
            return h.length === all.length ? h : null;
        }, 60000, 'A to hold every message');
        const onB = await until(async () => {
            const h = await rig.history(b, pad);
            return h.length === all.length ? h : null;
        }, 60000, 'B to hold every message');

        assert.deepStrictEqual(onB, onA);

        // and it stays that way: no repair fires and nothing is duplicated
        await settle(8000);
        const laterA = await rig.history(a, pad);
        const laterB = await rig.history(b, pad);
        assert.deepStrictEqual(laterA, onA, 'a healthy pad must not be rewritten');
        assert.deepStrictEqual(laterB, onB);

        await clientA.close();
        await clientB.close();
    } finally {
        await rig.stop();
    }
});
