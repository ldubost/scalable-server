// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors
//
// SPDX-License-Identifier: AGPL-3.0-or-later

/*  Federation: a blob *uploaded to a pad* reaches the other instance (R-44).
 *
 *  Why this exists alongside `federation-blobs.test.js`
 *  ----------------------------------------------------
 *  That suite puts bytes straight into an instance's store, which is the right
 *  way to test the transfer mechanism and the wrong way to answer "does an image
 *  someone dropped into a pad show up on the other server". A real upload is a
 *  different thing: it goes through an authenticated RPC session, a quota check,
 *  a pending-upload slot, chunked encryption, and finally `UPLOAD_COMPLETE`,
 *  which is what moves the bytes into their final home and writes the sidecars.
 *  A blob placed on disk by hand has skipped every one of those.
 *
 *  That gap has already hidden one real bug: `Stores.create` forwarded
 *  `sendCommand` on the object-storage path but not the filesystem one, so HTTP
 *  uploads killed the storage worker while every blob test passed.
 *
 *  So these tests drive the *client's own* upload implementation
 *  (`tests/common/upload.js`, the same code the browser runs) against instance A,
 *  then fetch the result from instance B and decrypt it with the key that never
 *  left the client. Byte-identical plaintext on B is the only evidence that
 *  answers the question.
 */

const { test } = require('node:test');
const assert = require('node:assert');
const NodeCrypto = require('node:crypto');

const Util = require('../../common/common-util');
const Nacl = require('tweetnacl/nacl-fast');

const Rig = require('./rig.js');
const Upload = require('../common/upload.js');

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

const userKeys = () => {
    const kp = Nacl.sign.keyPair();
    return {
        publicKey: kp.publicKey,
        secretKey: kp.secretKey,
        edPublic: Util.encodeBase64(kp.publicKey),
        edPrivate: Util.encodeBase64(kp.secretKey)
    };
};

/*  The RPC surface `Upload.handleFile` expects, bound to one instance's session.
    Transcribed from `tests/blob.test.js` so the two stay comparable. */
const uploadCommands = (rpc) => ({
    uploadStatus: (id, size, cb) => {
        rpc.send('UPLOAD_STATUS', { id, size }, (e, res) => {
            if (e) { return void cb(e); }
            if (typeof (res[0]) !== 'boolean') { return void cb('INVALID_RESPONSE'); }
            cb(void 0, res[0]);
        });
    },
    uploadCancel: (id, size, cb) => {
        rpc.send('UPLOAD_CANCEL', { id, size }, (e) => cb(e));
    },
    uploadComplete: (id, owned, cb) => {
        rpc.send(`${owned ? 'OWNED_' : ''}UPLOAD_COMPLETE`, id, (e, res) => {
            if (e) { return void cb(e); }
            if (typeof (res[0]) !== 'string') { return void cb('INVALID_ID'); }
            cb(void 0, res[0]);
        });
    },
    uploadChunk: (id, chunk, cb) => {
        rpc.send.unauthenticated('UPLOAD', { chunk, id }, (e, msg) => cb(e, msg));
    }
});

/*  Blobs live on the *storage* node, not the front: `/blob` and `/upload-blob`
    are both mounted in storage/cluster.js. A deployment puts an http-server in
    front to proxy them, which the rig does not run — so both the upload and the
    read go straight to storage here. The federated fallback that fetches a
    missing blob from a peer is in that same middleware, so reading through
    anything else would test neither half. */
const blobOrigin = (inst) => `http://localhost:${inst.base + 30}`;

/*  Upload one blob to `inst` exactly as a browser would, and return the URL the
    client would then embed in the pad. `useWs` picks between the two transports:
    the WebSocket path carries its own session, the HTTP path does not and has to
    ask the primary for the caller's quota — which is precisely the difference
    that broke uploads once already. */
const uploadTo = async (rig, inst, bytes, key, useWs) => {
    const keys = userKeys();
    const session = await rig.rpc(inst, keys);
    const id = NodeCrypto.randomBytes(24).toString('hex');

    const url = await new Promise((resolve, reject) => {
        Upload.handleFile({
            USE_WS: useWs,
            owned: true,
            force: true,
            id,
            u8: bytes,
            key,
            rpcCmd: uploadCommands(session.rpc),
            origin: blobOrigin(inst),
            keys
        }, (err, out) => err ? reject(new Error(String(err))) : resolve(out));
    });

    return { id, url, session };
};

// what the browser does with the response: decrypt it and compare
const decrypt = (buf, key) => new Promise((resolve, reject) => {
    Upload.fileCrypto.decrypt(new Uint8Array(buf), key,
        (err, val) => err ? reject(new Error(String(err))) : resolve(val));
});

const fetchBlob = async (inst, url) => {
    const res = await fetch(blobOrigin(inst) + url);
    if (!res.ok) { return { status: res.status }; }
    return { status: 200, body: await res.arrayBuffer() };
};

/*  Over WebSocket, which is the transport this rig can represent.

    The HTTP transport is deliberately not covered here. `/upload-blob` is
    mounted on the storage node and its cookie handshake is designed to be
    reached through the `http-server` proxy, which a four-node rig does not run —
    so a test of it would be testing the harness, not the product. Its one real
    bug (`sendCommand` not forwarded on the filesystem path) is fixed and lives
    on the s3 branch; catching a repeat of that belongs in a blob test with an
    http-server, not here.

    What matters for federation is the same either way: the bytes land on one
    instance and the other must be able to fetch them. */
test('R-44: a blob uploaded over WebSocket is readable on the other instance', async () => {
    const rig = await Rig.create(['A', 'B']);
    try {
        const a = rig.get('A'), b = rig.get('B');
        await rig.federated();

        const plain = new Uint8Array(NodeCrypto.randomBytes(60 * 1024));
        const key = new Uint8Array(NodeCrypto.randomBytes(32));

        const up = await uploadTo(rig, a, plain, key, true);

        const onB = await until(async () => {
            const r = await fetchBlob(b, up.url);
            return r.status === 200 ? r : null;
        }, 60000, 'B to fetch the WebSocket-uploaded blob');

        const out = await decrypt(onB.body, key);
        const content = new Uint8Array(await out.content.arrayBuffer());
        assert.ok(Buffer.from(content).equals(Buffer.from(plain)),
            'a WebSocket upload must cross as faithfully as an HTTP one');

        await up.session.close();
    } finally {
        await rig.stop();
    }
});

/*  A blob big enough to be chunked by both mechanisms at once: the client
    encrypts in 128 KB plaintext chunks, and the federation transfer slices at
    64 KB. Their boundaries do not line up, which is the interesting case. */
test('R-44: a multi-chunk upload crosses with its chunk boundaries intact', async () => {
    const rig = await Rig.create(['A', 'B']);
    try {
        const a = rig.get('A'), b = rig.get('B');
        await rig.federated();

        const plain = new Uint8Array(NodeCrypto.randomBytes(400 * 1024));
        const key = new Uint8Array(NodeCrypto.randomBytes(32));

        const up = await uploadTo(rig, a, plain, key, true);

        const onB = await until(async () => {
            const r = await fetchBlob(b, up.url);
            return r.status === 200 ? r : null;
        }, 90000, 'B to fetch a multi-chunk upload');

        const out = await decrypt(onB.body, key);
        const content = new Uint8Array(await out.content.arrayBuffer());
        assert.strictEqual(content.length, plain.length);
        assert.ok(Buffer.from(content).equals(Buffer.from(plain)),
            'chunks must reassemble in order across both layers');

        await up.session.close();
    } finally {
        await rig.stop();
    }
});
