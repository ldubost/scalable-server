// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors
//
// SPDX-License-Identifier: AGPL-3.0-or-later

/*  Federation M5: blobs (spec R-44, R-45).
 *
 *  A pad that embeds an image references a blob from inside content no server
 *  can read. Replicating the channel log therefore replicates the *reference*
 *  and not the bytes, and the replica renders a broken document with no error
 *  anywhere — which is the failure mode this milestone exists to remove.
 *
 *  The mechanism is a fetch, not a replication: blobs are immutable and
 *  content-addressed, so there is nothing to order or merge. A replica asks its
 *  peers the first time somebody wants a blob it does not have.
 */

const { test } = require('node:test');
const assert = require('node:assert');
const Fs = require('node:fs/promises');
const Path = require('node:path');
const NodeCrypto = require('node:crypto');

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

const blobId = () => NodeCrypto.randomBytes(24).toString('hex');   // 48 hex chars

/*  Put a blob straight into an instance's store.

    Uploading through the real RPC path needs an account, a quota and a pinning
    session; none of that is what this test is about. The bytes land in exactly
    the place an upload would leave them, which is what federation reads.
*/
const putBlob = async (inst, id, body) => {
    const dir = Path.join(inst.dir, 'blob', id.slice(0, 2));
    await Fs.mkdir(dir, { recursive: true });
    await Fs.writeFile(Path.join(dir, id), body);
};

const getBlob = async (inst, id) => {
    const res = await fetch(`http://localhost:${inst.base + 30}/blob/${id.slice(0, 2)}/${id}`);
    if (!res.ok) { return { status: res.status }; }
    return { status: 200, body: Buffer.from(await res.arrayBuffer()) };
};

test('M5/R-44: a blob only one instance holds is fetched on demand by the other', async () => {
    const rig = await Rig.create(['A', 'B']);
    try {
        const a = rig.get('A'), b = rig.get('B');
        await rig.federated();

        const id = blobId();
        const body = NodeCrypto.randomBytes(3000);
        await putBlob(a, id, body);

        // the instance that has it serves it
        const onA = await getBlob(a, id);
        assert.strictEqual(onA.status, 200, 'the holder should serve its own blob');
        assert.ok(onA.body.equals(body));

        /*  B has never seen these bytes. The request itself is what triggers the
            transfer, so the first read is expected to succeed rather than 404. */
        const onB = await until(async () => {
            const r = await getBlob(b, id);
            return r.status === 200 ? r : null;
        }, 40000, 'B to fetch the blob from its peer');

        assert.ok(onB.body.equals(body),
            'the replica must serve byte-identical content');
    } finally {
        await rig.stop();
    }
});

test('M5: a blob larger than one frame is transferred intact', async () => {
    const rig = await Rig.create(['A', 'B']);
    try {
        const a = rig.get('A'), b = rig.get('B');
        await rig.federated();

        const id = blobId();
        // several chunks: the transfer is sliced at 64KB
        const body = NodeCrypto.randomBytes(200 * 1024);
        await putBlob(a, id, body);

        const onB = await until(async () => {
            const r = await getBlob(b, id);
            return r.status === 200 ? r : null;
        }, 60000, 'B to fetch a multi-chunk blob');

        assert.strictEqual(onB.body.length, body.length, 'no bytes lost or added');
        assert.ok(onB.body.equals(body), 'chunks must reassemble in order');
    } finally {
        await rig.stop();
    }
});

/*  Once fetched, a blob is stored locally: the replica must not go back to its
    peer for every read, and must keep serving it if the peer goes away. */
test('M5: a fetched blob is kept, and survives the peer disappearing', async () => {
    const rig = await Rig.create(['A', 'B']);
    try {
        const a = rig.get('A'), b = rig.get('B');
        await rig.federated();

        const id = blobId();
        const body = NodeCrypto.randomBytes(5000);
        await putBlob(a, id, body);

        await until(async () => {
            const r = await getBlob(b, id);
            return r.status === 200 ? r : null;
        }, 40000, 'B to fetch the blob');

        await rig.killInstance(a);
        await settle(1500);

        const again = await getBlob(b, id);
        assert.strictEqual(again.status, 200,
            'a fetched blob must be served from local storage, not re-fetched');
        assert.ok(again.body.equals(body));
    } finally {
        await rig.stop();
    }
});

test('M5: a blob nobody has is refused rather than hanging', async () => {
    const rig = await Rig.create(['A', 'B']);
    try {
        const b = rig.get('B');
        await rig.federated();

        const res = await getBlob(b, blobId());
        assert.notStrictEqual(res.status, 200,
            'an unknown blob must not be served');
    } finally {
        await rig.stop();
    }
});
