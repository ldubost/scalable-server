// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors
//
// SPDX-License-Identifier: AGPL-3.0-or-later

/*  Federation milestone M3 acceptance — conformance level L2, multi-master
 *  (docs/federation-design.md §9).
 *
 *  The difference from M2 is that there is no anchor. Both instances accept
 *  writes locally and neither decides the order; the order is *derived* by the
 *  merge from Lamport clocks and the watermark (spec §4).
 *
 *  So the assertion is the one that matters most in the whole project: after
 *  concurrent editing, both instances' **committed id sequences are identical**.
 *  docs/experiments/chainpad-ordering showed that agreeing on the set but not
 *  the order produces permanently different documents, and that it does not
 *  self-heal — so "both have all the messages" would be a test that passes on a
 *  broken merge.
 */

const { test } = require('node:test');
const assert = require('node:assert');

const Capability = require('../../common/federation/capability.js');
const Rig = require('./rig.js');

const settle = (ms) => new Promise(r => setTimeout(r, ms));

const until = async (fn, ms, what) => {
    const deadline = Date.now() + ms;
    let last;
    while (Date.now() < deadline) {
        last = await fn();
        if (last) { return last; }
        await settle(250);
    }
    throw new Error(`timed out waiting for ${what}; last ${JSON.stringify(last)}`);
};

// A + B, both members of an L2 (multi-master) replica set for one pad.
const mkMultiMaster = async (rig) => {
    const a = rig.get('A'), b = rig.get('B');
    const pad = Rig.randomChannel();
    const padKeys = Rig.mkPadKeys();

    const clientA = await rig.client(a, pad, padKeys);
    const seeded = await clientA.send('seed');
    await settle(400);

    const cap = Capability.mint({
        channel: pad, from: a.originId, to: b.originId
    }, padKeys.secretKey);

    const enabled = await rig.enableOnOrigin(a, pad, padKeys.validateKey,
        [b.originId], 'L2');
    assert.ok(enabled.ok && !enabled.error,
        `the pad should go multi-master: ${JSON.stringify(enabled)}`);

    const res = await rig.replicate(b, {
        channel: pad, peer: a.originId, cap, validateKey: padKeys.validateKey
    });
    assert.deepStrictEqual(res, { ok: true }, 'B should join the replica set');

    await until(async () => {
        const h = await rig.history(b, pad);
        return h.length >= 1 ? h : null;
    }, 30000, 'B to backfill the seed');

    return { a, b, pad, padKeys, clientA, seeded };
};

test('M3: both instances accept writes and converge on one order', async () => {
    const rig = await Rig.create(['A', 'B']);
    try {
        const { a, b, pad, padKeys, clientA } = await mkMultiMaster(rig);
        const clientB = await rig.client(b, pad, padKeys);
        await settle(600);

        /*  Genuinely concurrent: each pair is written without either side
            having seen the other's. Neither instance is the authority, so the
            order has to come out of the merge. */
        const writes = [];
        for (let i = 0; i < 5; i++) {
            const [x, y] = await Promise.all([
                clientA.send(`A-${i}`),
                clientB.send(`B-${i}`)
            ]);
            writes.push(x, y);
        }

        const total = writes.length + 1;   // + the seed

        const onA = await until(async () => {
            const h = await rig.history(a, pad);
            return h.length === total ? h : null;
        }, 60000, `A to commit ${total} messages`);

        const onB = await until(async () => {
            const h = await rig.history(b, pad);
            return h.length === total ? h : null;
        }, 60000, `B to commit ${total} messages`);

        /*  The whole point. Not "same set" — same sequence. */
        assert.deepStrictEqual(onB, onA,
            'the two instances must serve an identical id sequence');

        writes.forEach(w => assert.ok(onA.includes(w), 'no write may be lost'));

        await clientA.close();
        await clientB.close();
    } finally {
        await rig.stop();
    }
});

/*  R-4/R-5: a pad where nobody is typing must still commit. The watermark only
    advances when every member reports its clock, and on an idle channel the
    only thing reporting is the heartbeat. */
test('M3: an idle channel still commits, via the heartbeat', async () => {
    const rig = await Rig.create(['A', 'B']);
    try {
        const { a, b, pad, padKeys, clientA } = await mkMultiMaster(rig);
        const clientB = await rig.client(b, pad, padKeys);
        await settle(600);

        // one write each, then silence
        const fromA = await clientA.send('quiet-A');
        const fromB = await clientB.send('quiet-B');

        const onA = await until(async () => {
            const h = await rig.history(a, pad);
            return h.length === 3 ? h : null;
        }, 60000, 'A to commit both writes while idle');
        const onB = await until(async () => {
            const h = await rig.history(b, pad);
            return h.length === 3 ? h : null;
        }, 60000, 'B to commit both writes while idle');

        assert.deepStrictEqual(onB, onA, 'an idle channel must still converge');
        assert.ok(onA.includes(fromA) && onA.includes(fromB));

        await clientA.close();
        await clientB.close();
    } finally {
        await rig.stop();
    }
});

/*  R-5: one instance going away must not freeze the other's history.
 *
 *  Note this is an *instance outage*, not a clean network partition: killing any
 *  node makes the rest of that instance exit too, because `common/interface.js`
 *  treats an internal disconnect as fatal ("Crash everything for now"). From the
 *  surviving instance's point of view the two are the same — the peer stops
 *  answering — which is what the eviction rule is written against.
 *
 *  Eviction takes EVICT_AFTER (60s), so this test is deliberately slow.
 */
test('M3: the surviving instance keeps committing when a peer disappears', async () => {
    const rig = await Rig.create(['A', 'B']);
    try {
        const { a, b, pad, clientA } = await mkMultiMaster(rig);
        await settle(500);

        const before = (await rig.history(a, pad)).length;

        // the peer goes away entirely
        await rig.killInstance(b);
        await settle(1000);

        const during = [];
        for (let i = 0; i < 3; i++) { during.push(await clientA.send(`during-${i}`)); }

        /*  Until B is evicted the watermark cannot advance past it, so these
            sit in the pending log. Once it is, the merge commits them — that is
            R-5 doing its job, and without it A's history would stop dead
            because one unrelated instance went down. */
        const onA = await until(async () => {
            const h = await rig.history(a, pad);
            return during.every(w => h.includes(w)) ? h : null;
        }, 120000, 'A to commit its own writes after evicting the absent peer');

        assert.ok(onA.length >= before + 3,
            'the surviving instance must keep committing during an outage');

        await clientA.close();
    } finally {
        await rig.stop();
    }
});
