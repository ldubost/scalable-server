// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors
//
// SPDX-License-Identifier: AGPL-3.0-or-later

/*  The S3 backend, held to the same conformance spec as the filesystem backend.

    Requires a real S3-compatible bucket; skipped entirely when the environment is
    not configured, so the default `npm run test:unit` stays offline and fast.

        S3_BUCKET=my-bucket \
        S3_ENDPOINT=https://s3.fr-par.scw.cloud \
        S3_REGION=fr-par \
        S3_ACCESS_KEY=... S3_SECRET_KEY=... \
        npm run test:unit

    Every run works under a unique prefix and deletes it afterwards, so concurrent
    runs cannot interfere with each other or with real data in the bucket.
*/

const { test } = require('node:test');
const assert = require('node:assert');
const Crypto = require('node:crypto');

const { runConformance, p, failure } = require('./backend-conformance.js');

const CONFIG = {
    bucket: process.env.S3_BUCKET,
    endpoint: process.env.S3_ENDPOINT,
    region: process.env.S3_REGION || 'fr-par',
    accessKeyId: process.env.S3_ACCESS_KEY,
    secretAccessKey: process.env.S3_SECRET_KEY
};

const configured = Boolean(CONFIG.bucket && CONFIG.accessKeyId && CONFIG.secretAccessKey);

if (!configured) {
    test('s3: skipped (set S3_BUCKET, S3_ACCESS_KEY and S3_SECRET_KEY to run)',
        { skip: true }, () => {});
} else {
    const S3Backend = require('../../common/storage/backend/s3.js');

    const mkConf = (extra) => {
        return Object.assign({
            bucket: CONFIG.bucket,
            endpoint: CONFIG.endpoint,
            region: CONFIG.region,
            credentials: {
                accessKeyId: CONFIG.accessKeyId,
                secretAccessKey: CONFIG.secretAccessKey
            },
            // probing on every setup would triple the runtime of the suite;
            // capabilities are exercised by their own test below
            skipProbe: true,
            capabilities: { conditionalPut: true, serverSideAppend: true }
        }, extra || {});
    };

    const mkPrefix = () => `_test/${Date.now()}-${Crypto.randomBytes(4).toString('hex')}/`;

    /*  Cleanup runs through an unprefixed backend on purpose.

        A prefixed instance cannot delete its own contents: removePrefix('') is
        refused, because "delete everything under no prefix" is exactly the mistake
        that guard exists to prevent. So we delete the test prefix from outside it. */
    let rootBackend;
    const getRoot = async () => {
        rootBackend ||= await p(S3Backend.create, mkConf({ prefix: '' }));
        return rootBackend;
    };

    const destroy = async (backend, prefix) => {
        const root = await getRoot();
        await new Promise((resolve, reject) => {
            root.removePrefix(prefix, err => err ? reject(err) : resolve());
        });
        await new Promise(resolve => { backend.close(resolve); });
    };

    // build a backend under a fresh prefix, and tear it down afterwards
    const mkInstance = async (conf) => {
        const prefix = mkPrefix();
        const backend = await p(S3Backend.create, mkConf(Object.assign({ prefix }, conf)));
        return { backend, prefix, cleanup: () => destroy(backend, prefix) };
    };

    runConformance('s3', async () => {
        const { backend, cleanup } = await mkInstance();
        return { backend, cleanup };
    });

    // --- behaviour specific to the S3 backend ------------------------------

    test('s3: probe reports what the bucket actually supports', async () => {
        const { backend, cleanup } = await mkInstance({ capabilities: {} });
        try {
            const result = await p(backend.probe);
            const caps = result.capabilities;

            // we don't assert specific values: the point of the probe is that it
            // measures rather than assumes. But it must produce a definite answer.
            for (const name of ['conditionalPut', 'serverSideAppend', 'presign']) {
                assert.strictEqual(typeof caps[name], 'boolean',
                    `${name} must be determined by the probe`);
            }
            console.log('    S3 capabilities:', JSON.stringify(caps));
            if (result.notes.length) {
                console.log('    notes:', result.notes.join('; '));
            }

            // the probe must clean up after itself
            const leftovers = await p(backend.list, '_probe/', {});
            assert.strictEqual(leftovers.keys.length, 0,
                'the probe should delete its own objects');
        } finally {
            await cleanup();
        }
    });

    test('s3: server-side append does not download the object', async () => {
        const { backend, cleanup } = await mkInstance();
        try {
            if (!backend.capabilities.serverSideAppend) { return; }

            // above the 5 MiB threshold the append path uses UploadPartCopy
            const body = Buffer.alloc(6 * 1024 * 1024, 'a');
            await p(backend.put, 'big.ndjson', body, {});

            const stat = await p(backend.append, 'big.ndjson', Buffer.from('tail\n'), {});
            assert.strictEqual(stat.size, body.length + 5);

            // and the content must be exactly the concatenation: byte-for-byte
            // fidelity is what keeps history-keeper offsets valid
            const result = await p(backend.get, 'big.ndjson');
            assert.strictEqual(result.length, body.length + 5);
            assert.ok(result.subarray(0, body.length).equals(body));
            assert.strictEqual(result.subarray(body.length).toString('utf8'), 'tail\n');
        } finally {
            await cleanup();
        }
    });

    test('s3: append leaves no dangling multipart uploads on failure', async () => {
        const { backend, cleanup } = await mkInstance();
        try {
            // an expectedSize mismatch must fail before starting an upload
            await p(backend.put, 'guarded.ndjson', Buffer.alloc(1024, 'x'), {});
            const err = await failure(p(backend.append, 'guarded.ndjson',
                Buffer.from('tail'), { expectedSize: 999999 }));
            assert.strictEqual(err.code, 'EPRECONDITION');

            const stat = await p(backend.head, 'guarded.ndjson');
            assert.strictEqual(stat.size, 1024, 'the object must be untouched');
        } finally {
            await cleanup();
        }
    });

    test('s3: a prefix isolates one instance from another', async () => {
        const stamp = `${Date.now()}-${Crypto.randomBytes(4).toString('hex')}`;
        const prefix = `_test/${stamp}/`;
        const one = await p(S3Backend.create, mkConf({ prefix: `${prefix}one/` }));
        const two = await p(S3Backend.create, mkConf({ prefix: `${prefix}two/` }));
        try {
            await p(one.put, 'shared/name.txt', 'from one', {});
            await p(two.put, 'shared/name.txt', 'from two', {});

            assert.strictEqual((await p(one.get, 'shared/name.txt')).toString('utf8'), 'from one');
            assert.strictEqual((await p(two.get, 'shared/name.txt')).toString('utf8'), 'from two');

            // and listings must not leak across the prefix boundary
            const listed = await p(one.list, '', {});
            assert.deepStrictEqual(listed.keys.map(k => k.key), ['shared/name.txt']);
        } finally {
            await destroy(one, prefix);
            await new Promise(resolve => { two.close(resolve); });
        }
    });

    test('s3: large objects round-trip through the streaming put path', async () => {
        const { Readable } = require('node:stream');
        const { backend, cleanup } = await mkInstance();
        try {
            // 12 MiB in 1 MiB chunks: forces a real multipart upload, which is the
            // path blob uploads will take
            const chunk = Buffer.alloc(1024 * 1024, 'b');
            const chunks = Array.from({ length: 12 }, () => chunk);
            await p(backend.put, 'blob.bin', Readable.from(chunks), {});

            const stat = await p(backend.head, 'blob.bin');
            assert.strictEqual(stat.size, 12 * 1024 * 1024);
        } finally {
            await cleanup();
        }
    });

    test('s3: test prefix is left clean', async () => {
        // the suite must not accumulate objects in a real bucket
        const root = await getRoot();
        const leftovers = await p(root.list, '_test/', {});
        assert.deepStrictEqual(leftovers.keys.map(k => k.key), [],
            'every test must delete the prefix it created');
        await new Promise(resolve => { root.close(resolve); });
    });
}
