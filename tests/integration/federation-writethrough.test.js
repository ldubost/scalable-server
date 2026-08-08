// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors
//
// SPDX-License-Identifier: AGPL-3.0-or-later

/*  Federation milestone M2 acceptance — conformance level L1, anchored
 *  write-through (docs/federation-design.md §9).
 *
 *      "Done when: users on both instances edit the same pad concurrently and
 *       converge; killing B loses nothing; killing A makes B read-only with a
 *       clear client-visible state."
 *
 *  The interesting property is not that a write from the mirror arrives — it is
 *  that the mirror never invents an order. B forwards to A, A is the single
 *  ordering authority, and both replicas end up with the same id sequence. That
 *  is what keeps R-1 true while there is still one writer; M3 is what removes
 *  the anchor.
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
        await settle(200);
    }
    throw new Error(`timed out waiting for ${what}; last ${JSON.stringify(last)}`);
};

// bring up A + B with `pad` replicated from A to B
const mkFederatedPad = async (rig) => {
    const a = rig.get('A'), b = rig.get('B');
    const pad = Rig.randomChannel();
    const padKeys = Rig.mkPadKeys();

    const clientA = await rig.client(a, pad, padKeys);
    const seeded = await clientA.send('seed');

    const cap = Capability.mint({
        channel: pad, from: a.originId, to: b.originId
    }, padKeys.secretKey);
    await rig.enableOnOrigin(a, pad, padKeys.validateKey, [b.originId]);
    const res = await rig.replicate(b, {
        channel: pad, peer: a.originId, cap, validateKey: padKeys.validateKey
    });
    assert.deepStrictEqual(res, { ok: true }, 'B should accept replication');

    await until(async () => {
        const h = await rig.history(b, pad);
        return h.length === 1 ? h : null;
    }, 30000, 'B to backfill');

    return { a, b, pad, padKeys, clientA, seeded };
};

test('M2: a write on the mirror is committed by the anchor and reaches both', async () => {
    const rig = await Rig.create(['A', 'B']);
    try {
        const { a, b, pad, padKeys, clientA, seeded } = await mkFederatedPad(rig);

        const clientB = await rig.client(b, pad, padKeys);
        await settle(500);

        // a write on the MIRROR
        const fromB = await clientB.send('written-on-B');

        const onA = await until(async () => {
            const h = await rig.history(a, pad);
            return h.length === 2 ? h : null;
        }, 30000, 'the anchor to commit the mirror write');
        assert.deepStrictEqual(onA, [seeded, fromB],
            'the anchor must hold the mirror write, after the seed');

        const onB = await until(async () => {
            const h = await rig.history(b, pad);
            return h.length === 2 ? h : null;
        }, 30000, 'the mirror to receive its own write back');

        /*  R-1: the same sequence on both, and the mirror's copy came back
            through the anchor rather than being appended locally. */
        assert.deepStrictEqual(onB, onA,
            'both replicas must serve the same id sequence');

        await clientA.close();
        await clientB.close();
    } finally {
        await rig.stop();
    }
});

test('M2: concurrent writes on both instances converge on one order', async () => {
    const rig = await Rig.create(['A', 'B']);
    try {
        const { a, b, pad, padKeys, clientA } = await mkFederatedPad(rig);
        const clientB = await rig.client(b, pad, padKeys);
        await settle(500);

        /*  Both sides write without waiting for each other. A commits its own
            directly; B's go through A. Whatever interleaving results, it must be
            ONE interleaving — the anchor decides it and both replicas replay it. */
        const writes = [];
        for (let i = 0; i < 4; i++) {
            const [x, y] = await Promise.all([
                clientA.send(`A-${i}`),
                clientB.send(`B-${i}`)
            ]);
            writes.push(x, y);
        }

        const expected = writes.length + 1;   // + the seed
        const onA = await until(async () => {
            const h = await rig.history(a, pad);
            return h.length === expected ? h : null;
        }, 40000, `the anchor to hold ${expected} messages`);

        const onB = await until(async () => {
            const h = await rig.history(b, pad);
            return h.length === expected ? h : null;
        }, 40000, `the mirror to hold ${expected} messages`);

        assert.deepStrictEqual(onB, onA,
            'both instances must converge on the same order after concurrent edits');
        // and every write actually survived, in some order
        writes.forEach(w => assert.ok(onA.includes(w), 'no write may be lost'));

        await clientA.close();
        await clientB.close();
    } finally {
        await rig.stop();
    }
});

/*  "Killing A makes B read-only with a clear client-visible state." The mirror
    must refuse rather than accept locally: accepting would fork the log, which
    is the one thing anchoring exists to prevent. */
test('M2: with the anchor down, the mirror refuses writes instead of forking', async () => {
    const rig = await Rig.create(['A', 'B']);
    try {
        const { a, b, pad, padKeys, clientA, seeded } = await mkFederatedPad(rig);
        const clientB = await rig.client(b, pad, padKeys);
        await settle(500);

        // take the anchor's federation node away
        await rig.killNode(a, 'federation');
        await settle(2000);

        let failed = false;
        try {
            await clientB.send('should-not-commit');
        } catch (e) {
            failed = true;
        }
        assert.ok(failed, 'the write must be reported as failed to the client');

        await settle(1500);
        const onB = await rig.history(b, pad);
        assert.deepStrictEqual(onB, [seeded],
            'the mirror must not have committed anything of its own');

        await clientA.close();
        await clientB.close();
    } finally {
        await rig.stop();
    }
});
