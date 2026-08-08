// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors
//
// SPDX-License-Identifier: AGPL-3.0-or-later

/*  Federation M4: a pad's auxiliary channels (spec R-50).
 *
 *  A pad is not one channel. Its chat is a separate channel with its own random
 *  id, kept in the pad's own metadata as `chat2` — content no server can read —
 *  and encrypted and signed with the *pad's* keys. Federating the document
 *  alone therefore gives a pad whose text crosses instances and whose
 *  conversation does not, which reads as federation being broken rather than as
 *  a channel nobody federated.
 *
 *  The client is the only party that can enumerate them, so it drives this: it
 *  mints one capability per channel with the same pad key. What these tests pin
 *  is the server half — that a second channel sharing a pad's keys federates on
 *  exactly the same terms, and that a client can find out whether a pad is
 *  federated in order to catch up a chat created afterwards.
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

/*  Federate one channel with a capability minted from `padKeys`.

    Deliberately parameterised on the channel rather than on the pad: the whole
    point of R-50 is that the chat channel goes through this unchanged. */
const federateChannel = async (rig, a, b, channel, padKeys, level) => {
    const cap = Capability.mint({
        channel, from: a.originId, to: b.originId
    }, padKeys.secretKey);

    const enabled = await rig.enableOnOrigin(a, channel, padKeys.validateKey,
        [b.originId], level || 'L2');
    assert.ok(enabled.ok && !enabled.error,
        `enable ${channel} failed: ${JSON.stringify(enabled)}`);

    const res = await rig.replicate(b, {
        channel, peer: a.originId, cap, validateKey: padKeys.validateKey
    });
    assert.deepStrictEqual(res, { ok: true },
        `replicate ${channel} failed: ${JSON.stringify(res)}`);
};

test('R-50: a pad chat channel federates on the pad\'s own keys', async () => {
    const rig = await Rig.create(['A', 'B']);
    try {
        const a = rig.get('A'), b = rig.get('B');

        /*  One pad, two channels, one key set — exactly what the client has:
            `chat2` is a random id, not derived from anything, but its messages
            are signed with the pad's signKey and checked against the pad's
            validateKey. */
        const padKeys = Rig.mkPadKeys();
        const pad = Rig.randomChannel();
        const chat = Rig.randomChannel();

        const padA = await rig.client(a, pad, padKeys);
        const chatA = await rig.client(a, chat, padKeys);
        await padA.send('the document');
        await chatA.send('hello from the chat');
        await settle(400);

        await federateChannel(rig, a, b, pad, padKeys);
        await federateChannel(rig, a, b, chat, padKeys);

        const padOnB = await until(async () => {
            const h = await rig.history(b, pad);
            return h.length >= 1 ? h : null;
        }, 40000, 'B to receive the document');
        const chatOnB = await until(async () => {
            const h = await rig.history(b, chat);
            return h.length >= 1 ? h : null;
        }, 40000, 'B to receive the chat');

        assert.deepStrictEqual(padOnB, await rig.history(a, pad));
        assert.deepStrictEqual(chatOnB, await rig.history(a, chat),
            'the chat must replicate as faithfully as the document');

        await padA.close();
        await chatA.close();
    } finally {
        await rig.stop();
    }
});

/*  The chat is where people talk *while* editing, so it has to work in both
    directions — a chat that only carries one instance's messages is worse than
    no chat, because nobody can tell theirs are not arriving. */
test('R-50: both instances can write to a federated chat channel', async () => {
    const rig = await Rig.create(['A', 'B']);
    try {
        const a = rig.get('A'), b = rig.get('B');
        const padKeys = Rig.mkPadKeys();
        const chat = Rig.randomChannel();

        const chatA = await rig.client(a, chat, padKeys);
        await chatA.send('from A');
        await settle(400);

        await federateChannel(rig, a, b, chat, padKeys, 'L2');

        const chatB = await rig.client(b, chat, padKeys);
        await until(async () => {
            const h = await rig.history(b, chat);
            return h.length >= 1 ? h : null;
        }, 40000, 'B to backfill the chat');

        const fromB = await chatB.send('from B');

        const onA = await until(async () => {
            const h = await rig.history(a, chat);
            return h.length === 2 ? h : null;
        }, 60000, 'A to receive B\'s chat message');
        const onB = await until(async () => {
            const h = await rig.history(b, chat);
            return h.length === 2 ? h : null;
        }, 60000, 'B to commit both chat messages');

        assert.deepStrictEqual(onB, onA,
            'a chat channel converges on one order like any other');
        assert.ok(onA.includes(fromB), 'B\'s message must reach A');

        await chatA.close();
        await chatB.close();
    } finally {
        await rig.stop();
    }
});

/*  The catch-up path. A chat opened after the pad was federated has to be
    federated then and there, and the client can only decide that if it can ask
    whether the pad is federated and to whom. */
test('R-50: a client can ask whether a channel is federated, and with whom', async () => {
    const rig = await Rig.create(['A', 'B']);
    try {
        const a = rig.get('A'), b = rig.get('B');
        const padKeys = Rig.mkPadKeys();
        const pad = Rig.randomChannel();

        const status = async (inst, channel) => {
            const res = await fetch(`${inst.frontUrl}/api/federation/channel/${channel}`);
            return { code: res.status, body: await res.json().catch(() => ({})) };
        };

        // before: an honest "no", not an error
        const before = await status(a, pad);
        assert.strictEqual(before.code, 200);
        assert.strictEqual(before.body.federated, false,
            'an unfederated channel reports itself as such');

        const padA = await rig.client(a, pad, padKeys);
        await padA.send('seed');
        await settle(400);
        await federateChannel(rig, a, b, pad, padKeys, 'L2');

        const after = await until(async () => {
            const s = await status(a, pad);
            return s.body.federated ? s : null;
        }, 20000, 'A to report the pad as federated');

        assert.strictEqual(after.body.level, 'L2');
        assert.deepStrictEqual(after.body.members, [b.originId],
            'the members are what the client mints capabilities for');
        assert.strictEqual(after.body.self, a.originId,
            'and it needs its own id to scope them from');

        // the replica agrees, so a chat opened there federates back
        const onB = await until(async () => {
            const s = await status(b, pad);
            return s.body.federated ? s : null;
        }, 20000, 'B to report the pad as federated');
        assert.deepStrictEqual(onB.body.members, [a.originId]);

        await padA.close();
    } finally {
        await rig.stop();
    }
});

test('R-50: a malformed channel id is refused rather than answered', async () => {
    const rig = await Rig.create(['A', 'B']);
    try {
        const a = rig.get('A');
        const res = await fetch(`${a.frontUrl}/api/federation/channel/not-a-channel`);
        assert.strictEqual(res.status, 400);
    } finally {
        await rig.stop();
    }
});
