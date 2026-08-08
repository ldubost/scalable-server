// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors
//
// SPDX-License-Identifier: AGPL-3.0-or-later

/*  Federation M4 acceptance: a **real** pad.
 *
 *  Every other integration test writes synthetic messages — a signature over a
 *  short string. That proves the transport and the merge, and proves nothing
 *  about the thing users actually have: a ChainPad document, encrypted, with
 *  checkpoints, driven by the same library the browser runs.
 *
 *  So this test builds one. Real `chainpad`, real `chainpad-crypto`, real
 *  encryption with a real pad key, over the real WebSocket path — and asserts
 *  that the two instances converge on the same **document text**, not merely the
 *  same message ids. Converging on ids while rendering different text is exactly
 *  the failure docs/experiments/chainpad-ordering found ChainPad capable of.
 */

const { test } = require('node:test');
const assert = require('node:assert');

const Capability = require('../../common/federation/capability.js');
const Rig = require('./rig.js');
const { ChainPad, mkCryptor } = require('./realpad.js');

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

/*  A real editing session: ChainPad on top of the rig's WebSocket client, with
    messages encrypted and signed exactly as the browser does. */
const mkSession = async (rig, inst, channel, cryptor, name) => {
    const client = await rig.rawClient(inst, channel, cryptor.validateKey);

    const cp = ChainPad.create({
        userName: name,
        initialState: '',
        logLevel: 0,
        noPrune: true,
        // small, so a short test still crosses a checkpoint boundary
        checkpointInterval: 8,
        avgSyncMilliseconds: 1e9
    });

    // outbound: encrypt + sign, then send as an ordinary channel message
    cp.onMessage((msg, cb) => {
        const enc = cryptor.encrypt(msg);
        client.sendRaw(enc).then(() => setTimeout(cb, 0), () => setTimeout(() => cb('E'), 0));
    });

    // inbound: decrypt, hand to ChainPad
    client.onContent((content) => {
        let plain;
        try {
            plain = cryptor.decrypt(content, cryptor.validateKey);
        } catch (e) { return; }
        if (typeof (plain) === 'string') { cp.message(plain); }
    });

    cp.start();

    /*  ChainPad holds at most one unacknowledged message (`realtime.pending`),
        so a `sync()` while one is in flight is a no-op and the new work stays
        local. The browser gets around this with a periodic sync; these tests set
        `avgSyncMilliseconds` high to keep timing deterministic, so they have to
        drive the flush themselves — otherwise the last edit of a burst is never
        sent and the test blames the server for losing it. */
    const flush = async (ms = 15000) => {
        const deadline = Date.now() + ms;
        while (Date.now() < deadline) {
            if (cp.getUserDoc() === cp.getAuthDoc()) { return true; }
            cp.sync();
            await settle(150);
        }
        return cp.getUserDoc() === cp.getAuthDoc();
    };

    return {
        cp, client, name, flush,
        type: async (text) => {
            cp.contentUpdate(cp.getUserDoc() + text);
            await flush();
        },
        doc: () => cp.getAuthDoc(),
        close: async () => { cp.abort(); await client.close(); }
    };
};

/*  Track every session so they can be closed in a `finally`. ChainPad schedules
    its next sync ~11 days out with `avgSyncMilliseconds: 1e9`; a session left
    open on a failed assertion keeps that timer alive and the test runner never
    exits, which reads as a hang rather than the failure it is. */
const mkTracker = () => {
    const open = [];
    return {
        track: (s) => { open.push(s); return s; },
        closeAll: async () => {
            for (const s of open.splice(0)) {
                try { await s.close(); } catch (e) { /* already gone */ }
            }
        }
    };
};

const mkPad = async (rig) => {
    const a = rig.get('A'), b = rig.get('B');
    const channel = Rig.randomChannel();
    const cryptor = mkCryptor();
    return { a, b, channel, cryptor };
};

const federate = async (rig, a, b, channel, cryptor, level) => {
    const cap = Capability.mint({
        channel, from: a.originId, to: b.originId
    }, Buffer.from(cryptor.signKey, 'base64'));

    const enabled = await rig.enableOnOrigin(a, channel, cryptor.validateKey,
        [b.originId], level);
    assert.ok(enabled.ok && !enabled.error,
        `enable failed: ${JSON.stringify(enabled)}`);

    const res = await rig.replicate(b, {
        channel, peer: a.originId, cap, validateKey: cryptor.validateKey
    });
    assert.deepStrictEqual(res, { ok: true }, `replicate failed: ${JSON.stringify(res)}`);
};

test('M4: a real ChainPad document replicates to the other instance', async () => {
    const rig = await Rig.create(['A', 'B']);
    const t = mkTracker();
    try {
        const { a, b, channel, cryptor } = await mkPad(rig);

        const A = t.track(await mkSession(rig, a, channel, cryptor, 'A'));
        await A.type('Hello from A. ');
        await A.type('Second sentence. ');
        await settle(600);
        assert.match(A.doc(), /Hello from A/, 'the origin should hold its own text');

        await federate(rig, a, b, channel, cryptor, 'L2');

        const B = t.track(await mkSession(rig, b, channel, cryptor, 'B'));

        /*  Wait for the whole document, not for the first sentence to appear:
            the replica reconstructs it message by message, so a weaker
            condition here would race its own assertion. */
        const text = await until(async () => {
            const d = B.doc();
            return d && d === A.doc() && d.includes('Hello from A') ? d : null;
        }, 40000, 'the replica to reconstruct the document');

        assert.strictEqual(text, A.doc(),
            'both instances must render identical document text');
        assert.match(text, /Second sentence/,
            'the replica must hold every message, not just the first');
    } finally {
        await t.closeAll();
        await rig.stop();
    }
});

test('M4: both instances edit a real document and converge', async () => {
    const rig = await Rig.create(['A', 'B']);
    const t = mkTracker();
    try {
        const { a, b, channel, cryptor } = await mkPad(rig);

        const A = t.track(await mkSession(rig, a, channel, cryptor, 'A'));
        await A.type('start. ');
        await settle(500);

        await federate(rig, a, b, channel, cryptor, 'L2');
        const B = t.track(await mkSession(rig, b, channel, cryptor, 'B'));
        await until(async () => B.doc().includes('start') ? true : null,
            40000, 'B to catch up');

        // both type, without waiting for each other
        for (let i = 0; i < 4; i++) {
            await Promise.all([A.type(`a${i} `), B.type(`b${i} `)]);
            await settle(400);
        }

        /*  ChainPad converges the *document*; federation converges the *order*.
            Both have to hold, and only the document check would catch a merge
            that produced the same ids in a different sequence. */
        /*  Wait for convergence *and* for every edit to have landed. Checking
            only that the two strings match can succeed while the last edits are
            still in flight — both sides briefly agree on an incomplete
            document. */
        const all = [];
        for (let i = 0; i < 4; i++) { all.push(`a${i}`, `b${i}`); }
        const converged = await until(async () => {
            const da = A.doc(), db = B.doc();
            if (!da || da !== db) { return null; }
            return all.every(w => da.includes(w)) ? da : null;
        }, 90000, 'the two documents to converge with every edit present');

        all.forEach(w => assert.ok(converged.includes(w), `edit ${w} survived`));
    } finally {
        await t.closeAll();
        await rig.stop();
    }
});

/*  R-10. A real pad checkpoints; a federated one checkpoints on both instances.
    The checkpoint carries the whole document, so a mishandled one does not
    corrupt slightly — it replaces the document wholesale. */
test('M4/R-10: a document that crosses checkpoints still converges', async () => {
    const rig = await Rig.create(['A', 'B']);
    const t = mkTracker();
    try {
        const { a, b, channel, cryptor } = await mkPad(rig);

        const A = t.track(await mkSession(rig, a, channel, cryptor, 'A'));
        await A.type('x ');
        await settle(400);
        await federate(rig, a, b, channel, cryptor, 'L2');
        const B = t.track(await mkSession(rig, b, channel, cryptor, 'B'));
        await until(async () => B.doc().includes('x') ? true : null, 40000, 'B to catch up');

        // checkpointInterval is 8, so this crosses several from both sides
        for (let i = 0; i < 12; i++) {
            await Promise.all([A.type(`A${i} `), B.type(`B${i} `)]);
            await settle(250);
        }

        const all = [];
        for (let i = 0; i < 12; i++) { all.push(`A${i}`, `B${i}`); }
        const converged = await until(async () => {
            const da = A.doc(), db = B.doc();
            if (!da || da !== db) { return null; }
            return all.every(w => da.includes(w)) ? da : null;
        }, 120000, 'the two documents to converge across checkpoints');

        all.forEach(w => assert.ok(converged.includes(w), `${w} survived checkpointing`));
    } finally {
        await t.closeAll();
        await rig.stop();
    }
});
