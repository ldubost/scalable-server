// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors
//
// SPDX-License-Identifier: AGPL-3.0-or-later

/*  Federation milestone M1 acceptance (docs/federation-design.md §9).
 *
 *      "Done when: a pad edited on A is fully readable and live on B, and B's
 *       committed id sequence equals A's."
 *
 *  Two complete instances, real WebSocket clients, real pad messages. The
 *  assertion that matters is the *id sequence*, not just the content: spec R-1
 *  requires every replica to serve the same order, and
 *  docs/experiments/chainpad-ordering showed that replicas which agree on the
 *  set but not the order can converge on permanently different documents. A test
 *  that only checked "B has the same messages" would pass on a broken mirror.
 *
 *  Run `npm run build` first; this drives the bundles in ./build.
 */

const { test } = require('node:test');
const assert = require('node:assert');

const Capability = require('../../common/federation/capability.js');
const Rig = require('./rig.js');

const settle = (ms) => new Promise(r => setTimeout(r, ms));

/*  Wait for a condition rather than sleeping a fixed time: replication is
    asynchronous and a fixed sleep is either flaky or slow. */
const until = async (fn, ms, what) => {
    const deadline = Date.now() + ms;
    let last;
    while (Date.now() < deadline) {
        last = await fn();
        if (last) { return last; }
        await settle(200);
    }
    throw new Error(`timed out waiting for ${what}; last value ${JSON.stringify(last)}`);
};

test('M1: a pad edited on A is readable on B, in the same order', async () => {
    const rig = await Rig.create(['A', 'B']);
    try {
        const { a, b } = { a: rig.get('A'), b: rig.get('B') };

        // --- a pad on A, written by a real client ------------------------
        const pad = Rig.randomChannel();
        const padKeys = Rig.mkPadKeys();

        const clientA = await rig.client(a, pad, padKeys);
        const sent = [];
        for (let i = 0; i < 5; i++) {
            sent.push(await clientA.send(`message-${i}`));
        }

        // sanity: A really has them, in the order it accepted them
        const onA = await rig.history(a, pad);
        assert.deepStrictEqual(onA, sent,
            'the origin should hold exactly what was written, in order');

        // --- federate it -------------------------------------------------
        /*  The capability is minted with the pad's signing key, which only a
            client ever holds. That is the whole authorisation story (R-25):
            no account, no shared secret, nothing the server could forge. */
        const cap = Capability.mint({
            channel: pad,
            from: a.originId,
            to: b.originId
        }, padKeys.secretKey);

        await rig.enableOnOrigin(a, pad, padKeys.validateKey, [b.originId]);
        const res = await rig.replicate(b, {
            channel: pad, peer: a.originId, cap, validateKey: padKeys.validateKey
        });
        assert.deepStrictEqual(res, { ok: true }, 'B should accept the replication request');

        // --- backfill ----------------------------------------------------
        const onB = await until(async () => {
            const h = await rig.history(b, pad);
            return h.length === sent.length ? h : null;
        }, 30000, `B to backfill ${sent.length} messages`);

        /*  R-1: the same id sequence, not merely the same set. */
        assert.deepStrictEqual(onB, sent,
            'B must serve the same messages in the same order as A');

        // --- live --------------------------------------------------------
        const live = await clientA.send('after-subscribe');
        sent.push(live);

        const onBLive = await until(async () => {
            const h = await rig.history(b, pad);
            return h.length === sent.length ? h : null;
        }, 30000, 'B to receive the live message');

        assert.deepStrictEqual(onBLive, sent,
            'a message written on A after subscribing must reach B, in order');

        await clientA.close();
    } finally {
        await rig.stop();
    }
});

/*  "Fully readable and live on B" has to mean a user *connected to B* sees the
    edit arrive, not merely that B's log contains it. Reading history back would
    pass even if the mirror never told anybody. */
test('M1: a user connected to the mirror sees remote edits live', async () => {
    const rig = await Rig.create(['A', 'B']);
    try {
        const { a, b } = { a: rig.get('A'), b: rig.get('B') };
        const pad = Rig.randomChannel();
        const padKeys = Rig.mkPadKeys();

        const clientA = await rig.client(a, pad, padKeys);
        await clientA.send('before');

        const cap = Capability.mint({
            channel: pad, from: a.originId, to: b.originId
        }, padKeys.secretKey);
        await rig.enableOnOrigin(a, pad, padKeys.validateKey, [b.originId]);
        await rig.replicate(b, {
            channel: pad, peer: a.originId, cap, validateKey: padKeys.validateKey
        });

        await until(async () => {
            const h = await rig.history(b, pad);
            return h.length === 1 ? h : null;
        }, 30000, 'B to backfill');

        // a reader on the MIRROR, joined before the next edit happens
        const readerB = await rig.client(b, pad, padKeys);
        await settle(500);

        const live = await clientA.send('live-edit');

        const seen = await until(
            async () => readerB.received.includes(live) ? readerB.received : null,
            30000, 'the reader on B to receive the edit live');

        assert.ok(seen.includes(live),
            'a user connected to the mirror must receive remote edits without reloading');

        await clientA.close();
        await readerB.close();
    } finally {
        await rig.stop();
    }
});

test('M1: replication is refused without a valid capability', async () => {
    const rig = await Rig.create(['A', 'B']);
    try {
        const { a, b } = { a: rig.get('A'), b: rig.get('B') };
        const pad = Rig.randomChannel();
        const padKeys = Rig.mkPadKeys();

        const clientA = await rig.client(a, pad, padKeys);
        await clientA.send('secret');
        await rig.enableOnOrigin(a, pad, padKeys.validateKey, [b.originId]);

        // a capability signed by a different pad's key
        const wrong = Capability.mint({
            channel: pad, from: a.originId, to: b.originId
        }, Rig.mkPadKeys().secretKey);

        const res = await rig.replicate(b, {
            channel: pad, peer: a.originId, cap: wrong, validateKey: padKeys.validateKey
        });
        assert.ok(res.error, `expected refusal, got ${JSON.stringify(res)}`);

        await settle(1000);
        const onB = await rig.history(b, pad);
        assert.deepStrictEqual(onB, [],
            'B must not hold any of the pad it was not authorised to replicate');

        await clientA.close();
    } finally {
        await rig.stop();
    }
});
