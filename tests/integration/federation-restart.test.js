// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors
//
// SPDX-License-Identifier: AGPL-3.0-or-later

/*  Federation survives a restart (spec R-52).
 *
 *  Everything the write path consults about federation is held in memory: core's
 *  set of federated channels, and the federation node's map of who replicates
 *  what. Both are consulted per message, so both are caches — and both are empty
 *  after a restart.
 *
 *  Nothing else put them back, and nothing could. A `SUBSCRIBE` carries a
 *  capability signed by the pad's own key, which is short-lived and single-use,
 *  so a restarted instance cannot ask its peers to remind it: only a browser
 *  holding the pad can mint one, and there may be nobody with the pad open for
 *  days. The state files are the only durable record, so they are what a restart
 *  has to rebuild from.
 *
 *  Until it did, a restart looked like nothing had happened — both instances
 *  came up, both served the pad, both accepted edits — and they simply stopped
 *  agreeing. That is the worst shape a failure can take, and it is why these
 *  tests assert on writes made *after* the restart rather than on any status
 *  the server reports about itself.
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

const federate = async (rig, a, b, channel, padKeys, level) => {
    const cap = Capability.mint({
        channel, from: a.originId, to: b.originId
    }, padKeys.secretKey);

    const enabled = await rig.enableOnOrigin(a, channel, padKeys.validateKey,
        [b.originId], level);
    assert.ok(enabled.ok && !enabled.error,
        `enable failed: ${JSON.stringify(enabled)}`);

    const res = await rig.replicate(b, {
        channel, peer: a.originId, cap, validateKey: padKeys.validateKey
    });
    assert.deepStrictEqual(res, { ok: true }, `replicate failed: ${JSON.stringify(res)}`);
};

/*  The plain case, and the one that was broken: federate a pad, restart the
    instance that holds it, then write. The write is the test — a status endpoint
    reporting "federated" proved nothing, because the state files always said so
    even while nothing replicated. */
test('R-52: an anchor keeps replicating after it restarts', async () => {
    const rig = await Rig.create(['A', 'B']);
    try {
        const a = rig.get('A'), b = rig.get('B');
        const pad = Rig.randomChannel();
        const padKeys = Rig.mkPadKeys();

        let clientA = await rig.client(a, pad, padKeys);
        await clientA.send('before the restart');
        await settle(400);

        await federate(rig, a, b, pad, padKeys, 'L2');
        await until(async () => {
            const h = await rig.history(b, pad);
            return h.length >= 1 ? h : null;
        }, 40000, 'B to backfill');

        await clientA.close();
        await rig.restartInstance(a);

        // a fresh client, as a browser reconnecting after the server came back
        clientA = await rig.client(a, pad, padKeys);
        const after = await clientA.send('after the restart');

        const onB = await until(async () => {
            const h = await rig.history(b, pad);
            return h.includes(after) ? h : null;
        }, 60000, 'B to receive a write made after A restarted');

        const onA = await rig.history(a, pad);
        assert.deepStrictEqual(onB, onA,
            'the two must still serve an identical sequence after a restart');

        await clientA.close();
    } finally {
        await rig.stop();
    }
});

/*  The other direction. A replica that forgets who it replicates from stops
    pulling *and* stops pushing, and at L2 the second half is the one that
    diverges the document. */
test('R-52: a replica keeps replicating after it restarts', async () => {
    const rig = await Rig.create(['A', 'B']);
    try {
        const a = rig.get('A'), b = rig.get('B');
        const pad = Rig.randomChannel();
        const padKeys = Rig.mkPadKeys();

        const clientA = await rig.client(a, pad, padKeys);
        await clientA.send('seed');
        await settle(400);

        await federate(rig, a, b, pad, padKeys, 'L2');
        await until(async () => {
            const h = await rig.history(b, pad);
            return h.length >= 1 ? h : null;
        }, 40000, 'B to backfill');

        await rig.restartInstance(b);

        // B writes: this only reaches A if B remembers A is in the replica set
        const clientB = await rig.client(b, pad, padKeys);
        const fromB = await clientB.send('written on the restarted replica');

        const onA = await until(async () => {
            const h = await rig.history(a, pad);
            return h.includes(fromB) ? h : null;
        }, 60000, 'A to receive a write made on the restarted replica');

        // and the other direction still works too
        const fromA = await clientA.send('and back again');
        const onB = await until(async () => {
            const h = await rig.history(b, pad);
            return h.includes(fromA) ? h : null;
        }, 60000, 'B to receive a later write from A');

        assert.deepStrictEqual(onB, await rig.history(a, pad),
            'both directions must survive a replica restart');
        assert.ok(onA.includes(fromB));

        await clientA.close();
        await clientB.close();
    } finally {
        await rig.stop();
    }
});

/*  Both at once, which is what an operator upgrading a pair actually does. It
    also removes the possibility that one surviving instance was carrying the
    other's memory. */
test('R-52: replication survives both instances restarting', async () => {
    const rig = await Rig.create(['A', 'B']);
    try {
        const a = rig.get('A'), b = rig.get('B');
        const pad = Rig.randomChannel();
        const padKeys = Rig.mkPadKeys();

        let clientA = await rig.client(a, pad, padKeys);
        await clientA.send('seed');
        await settle(400);
        await federate(rig, a, b, pad, padKeys, 'L2');
        await until(async () => {
            const h = await rig.history(b, pad);
            return h.length >= 1 ? h : null;
        }, 40000, 'B to backfill');
        await clientA.close();

        await rig.restartInstance(a);
        await rig.restartInstance(b);

        clientA = await rig.client(a, pad, padKeys);
        const clientB = await rig.client(b, pad, padKeys);

        const fromA = await clientA.send('A after both restarted');
        const fromB = await clientB.send('B after both restarted');

        const onA = await until(async () => {
            const h = await rig.history(a, pad);
            return h.includes(fromA) && h.includes(fromB) ? h : null;
        }, 90000, 'A to hold both post-restart writes');
        const onB = await until(async () => {
            const h = await rig.history(b, pad);
            return h.includes(fromA) && h.includes(fromB) ? h : null;
        }, 90000, 'B to hold both post-restart writes');

        assert.deepStrictEqual(onB, onA,
            'a restarted pair must converge on one order, as before');

        await clientA.close();
        await clientB.close();
    } finally {
        await rig.stop();
    }
});

/*  The case a restart actually produces, and the one the tests above missed.
 *
 *  All three passed while replication was still broken for real pads, because
 *  none of them wrote anything while the peer was away. Live messages resumed
 *  fine; it was the *backfill* that never happened.
 *
 *  The cause was a category error. On reconnect, an instance re-synced the
 *  channels in `following()` — the map of channels it mirrors *from an anchor*,
 *  which is an L1 notion and empty at L2, where there is no anchor and every
 *  member is a peer. So a multi-master pad asked for nothing, and everything
 *  written during the outage stayed on one side. Silent, and permanent once
 *  R-48's guard starts holding those late arrivals back.
 *
 *  A restart always produces this: something is written in the seconds a server
 *  is down. Live push cannot deliver it by definition, so it is the only path
 *  that matters.
 */
test('R-52: work done while a peer was away is backfilled when it returns', async () => {
    const rig = await Rig.create(['A', 'B']);
    try {
        const a = rig.get('A'), b = rig.get('B');
        const pad = Rig.randomChannel();
        const padKeys = Rig.mkPadKeys();

        const clientA = await rig.client(a, pad, padKeys);
        await clientA.send('before');
        await settle(400);

        await federate(rig, a, b, pad, padKeys, 'L2');
        await until(async () => {
            const h = await rig.history(b, pad);
            return h.length >= 1 ? h : null;
        }, 40000, 'B to backfill the seed');

        // B goes away, and A keeps working
        await rig.killInstance(b);
        await settle(1000);
        const duringOutage = [];
        for (let i = 0; i < 3; i++) {
            duringOutage.push(await clientA.send(`while-B-was-down-${i}`));
        }

        await rig.restartInstance(b);

        /*  Nothing pushes these: they were written when there was no session.
            Only the sync on reconnect can deliver them. */
        const onB = await until(async () => {
            const h = await rig.history(b, pad);
            return duringOutage.every(m => h.includes(m)) ? h : null;
        }, 90000, 'B to backfill what it missed while it was down');

        assert.deepStrictEqual(onB, await rig.history(a, pad),
            'and the two must agree on the order, not merely the set');

        await clientA.close();
    } finally {
        await rig.stop();
    }
});

/*  Restoring must not resurrect a pad somebody deliberately stopped federating.
    Worth pinning: rebuilding state from disk is exactly the kind of change that
    quietly undoes a deletion. */
test('R-52: a channel that was never federated is not federated by a restart', async () => {
    const rig = await Rig.create(['A', 'B']);
    try {
        const a = rig.get('A'), b = rig.get('B');
        const pad = Rig.randomChannel();
        const padKeys = Rig.mkPadKeys();

        const clientA = await rig.client(a, pad, padKeys);
        await clientA.send('private to A');
        await settle(400);
        await clientA.close();

        await rig.restartInstance(a);
        await settle(2000);

        const status = await fetch(`${a.frontUrl}/api/federation/channel/${pad}`)
            .then(r => r.json());
        assert.strictEqual(status.federated, false,
            'a restart must not federate anything on its own');

        const onB = await rig.history(b, pad);
        assert.deepStrictEqual(onB, [],
            'and the other instance must still hold nothing');
    } finally {
        await rig.stop();
    }
});
