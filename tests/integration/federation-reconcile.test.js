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
 *  The repair must converge the two on the *union*, in one order. Re-sending an
 *  orphan is not enough and is actively wrong: it would arrive with a fresh
 *  clock and be appended at the peer's tail while it sits mid-log here, which is
 *  the divergence rather than the cure. So each instance lifts its own orphans
 *  out of its log and federates them properly, and the merge places them.
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

        /*  The whole point, and the reason re-sending is not enough: not the
            same set, the same *sequence*. */
        assert.deepStrictEqual(onB, onA,
            'a repaired pad must leave both instances serving one identical order');

        /*  And nothing may be duplicated: the orphan is lifted out of the log
            before being federated, so it must appear exactly once. */
        all.forEach(m => {
            assert.strictEqual(onA.filter(x => x === m).length, 1,
                'every message appears exactly once after a repair');
        });
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

        assert.deepStrictEqual(onB, onA, 'both must agree on the order');
        assert.strictEqual(onA.length, all.length, 'and hold nothing extra');
    } finally {
        await rig.stop();
    }
});

/*  A repair rewrites committed history, so it must not run on a healthy pad —
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
