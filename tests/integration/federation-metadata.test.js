// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors
//
// SPDX-License-Identifier: AGPL-3.0-or-later

/*  Federation M2, second half: metadata and control commits.
 *
 *      R-20  metadata replicates as ordered META control commits
 *      R-22  a self-destructing pad is never federated
 *      R-29  a restricted pad is never federated
 *      §6    under L1, metadata is *anchor-authoritative*: only the anchor
 *            emits commits, mirrors apply them one-way
 *
 *  The refusals matter as much as the replication. A restricted pad's access
 *  list is evaluated against accounts, and accounts are instance-scoped — so
 *  replicating one would silently drop the restriction rather than enforce it
 *  remotely. Refusing is the only honest answer available at L1.
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

const federate = async (rig, opts) => {
    const a = rig.get('A'), b = rig.get('B');
    const pad = Rig.randomChannel();
    const padKeys = Rig.mkPadKeys();
    const owner = Rig.mkUserKeys();

    const clientA = await rig.client(a, pad, padKeys,
        Object.assign({ owners: [owner.edPublic] }, opts || {}));
    await clientA.send('seed');
    await settle(400);

    const cap = Capability.mint({
        channel: pad, from: a.originId, to: b.originId
    }, padKeys.secretKey);
    const enabled = await rig.enableOnOrigin(a, pad, padKeys.validateKey, [b.originId]);
    return { a, b, pad, padKeys, owner, clientA, cap, enabled };
};

test('M2/R-20: a metadata change on the anchor reaches the mirror', async () => {
    const rig = await Rig.create(['A', 'B']);
    try {
        const { a, b, pad, padKeys, owner, clientA, cap, enabled } = await federate(rig);
        assert.ok(enabled.ok && !enabled.error,
            `the pad should be federatable: ${JSON.stringify(enabled)}`);

        await rig.replicate(b, {
            channel: pad, peer: a.originId, cap, validateKey: padKeys.validateKey
        });
        await until(async () => {
            const h = await rig.history(b, pad);
            return h.length === 1 ? h : null;
        }, 30000, 'B to backfill');

        // the owner adds a second owner, at the anchor
        const newOwner = Rig.mkUserKeys();
        const rpcA = await rig.rpc(a, owner);
        await rpcA.send('SET_METADATA', {
            channel: pad, command: 'ADD_OWNERS', value: [newOwner.edPublic]
        });

        /*  R-20: the mirror must end up with the same owner list, by applying
            the same command — not by being handed the anchor's state. */
        const onB = await until(async () => {
            const m = await rig.metadata(b, pad);
            return m?.owners?.includes(newOwner.edPublic) ? m : null;
        }, 30000, 'the mirror to apply the metadata change');

        const onA = await rig.metadata(a, pad);
        assert.deepStrictEqual(onB.owners.slice().sort(), onA.owners.slice().sort(),
            'both replicas must agree on the owner list');

        rpcA.close();
        await clientA.close();
    } finally {
        await rig.stop();
    }
});

/*  §6: under L1 only the anchor orders metadata. A mirror that accepted a local
    change would diverge with no mechanism to reconcile — R-20 requires metadata
    to be a pure function of the replicated command sequence. */
test('M2: the mirror refuses local metadata changes', async () => {
    const rig = await Rig.create(['A', 'B']);
    try {
        const { a, b, pad, padKeys, owner, clientA, cap } = await federate(rig);
        await rig.replicate(b, {
            channel: pad, peer: a.originId, cap, validateKey: padKeys.validateKey
        });
        await until(async () => {
            const h = await rig.history(b, pad);
            return h.length === 1 ? h : null;
        }, 30000, 'B to backfill');

        const rpcB = await rig.rpc(b, owner);
        let refused = false;
        try {
            await rpcB.send('SET_METADATA', {
                channel: pad, command: 'ADD_OWNERS', value: [Rig.mkUserKeys().edPublic]
            });
        } catch (e) {
            refused = true;
        }
        assert.ok(refused, 'the mirror must refuse to mutate federated metadata');

        rpcB.close();
        await clientA.close();
    } finally {
        await rig.stop();
    }
});

test('M2/R-22: a self-destructing pad cannot be federated', async () => {
    const rig = await Rig.create(['A', 'B']);
    try {
        const { clientA, enabled } = await federate(rig, { selfdestruct: true });
        assert.ok(enabled.error, `expected refusal, got ${JSON.stringify(enabled)}`);
        assert.match(String(enabled.error), /SELFDESTRUCT/,
            'the refusal should name the reason');
        await clientA.close();
    } finally {
        await rig.stop();
    }
});

test('M2/R-29: a restricted pad cannot be federated', async () => {
    const rig = await Rig.create(['A', 'B']);
    try {
        const { clientA, enabled } = await federate(rig, { restricted: true });
        assert.ok(enabled.error, `expected refusal, got ${JSON.stringify(enabled)}`);
        assert.match(String(enabled.error), /RESTRICTED/,
            'the refusal should name the reason');
        await clientA.close();
    } finally {
        await rig.stop();
    }
});
