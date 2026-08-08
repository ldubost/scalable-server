// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors
//
// SPDX-License-Identifier: AGPL-3.0-or-later

/*  The cached append-log over a real object store.

    cache-manager.test.js covers the state machine exhaustively against the
    filesystem backend, which is fast and offline. This file is the integration
    proof: it runs the same flows against a real bucket, and in particular exercises
    the multipart-copy append path, which the filesystem backend cannot validate
    faithfully because appending is trivial there and genuinely hard on S3.

    Skipped unless S3_BUCKET / S3_ACCESS_KEY / S3_SECRET_KEY are set.
*/

const { test } = require('node:test');
const assert = require('node:assert');
const Fs = require('node:fs/promises');
const Os = require('node:os');
const Path = require('node:path');
const Crypto = require('node:crypto');

const Manager = require('../../common/storage/cache/manager.js');
const { p } = require('./backend-conformance.js');

const CONFIG = {
    bucket: process.env.S3_BUCKET,
    endpoint: process.env.S3_ENDPOINT,
    region: process.env.S3_REGION || 'fr-par',
    accessKeyId: process.env.S3_ACCESS_KEY,
    secretAccessKey: process.env.S3_SECRET_KEY
};

const configured = Boolean(CONFIG.bucket && CONFIG.accessKeyId && CONFIG.secretAccessKey);

if (!configured) {
    test('cache over s3: skipped (no S3 credentials configured)', { skip: true }, () => {});
} else {
    const S3Backend = require('../../common/storage/backend/s3.js');

    const quietLog = {
        info: () => {}, warn: () => {}, error: () => {}, verbose: () => {}, silly: () => {}
    };

    const setup = async (managerConf) => {
        const prefix = `_test/cache-${Date.now()}-${Crypto.randomBytes(4).toString('hex')}/`;
        const cache = await Fs.mkdtemp(Path.join(Os.tmpdir(), 'cryptpad-cache-s3-'));

        const backend = await p(S3Backend.create, {
            bucket: CONFIG.bucket,
            endpoint: CONFIG.endpoint,
            region: CONFIG.region,
            credentials: {
                accessKeyId: CONFIG.accessKeyId,
                secretAccessKey: CONFIG.secretAccessKey
            },
            prefix,
            skipProbe: true,
            capabilities: { conditionalPut: true, serverSideAppend: true }
        });

        const manager = await p(Manager.create, Object.assign({
            backend,
            cacheRoot: cache,
            prefix: 'channel/',
            Log: quietLog,
            flushDebounceMs: 60000,
            flushMaxDelayMs: 0,
            flushMaxBytes: 0
        }, managerConf || {}));

        // an unprefixed client, so cleanup can delete the prefix from outside it
        const root = await p(S3Backend.create, {
            bucket: CONFIG.bucket,
            endpoint: CONFIG.endpoint,
            region: CONFIG.region,
            credentials: {
                accessKeyId: CONFIG.accessKeyId,
                secretAccessKey: CONFIG.secretAccessKey
            },
            skipProbe: true
        });

        return {
            backend, manager, cache, prefix,
            append: async (relPath, text) => {
                const path = Path.join(cache, relPath);
                await Fs.mkdir(Path.dirname(path), { recursive: true });
                await Fs.appendFile(path, text);
                const stat = await Fs.stat(path);
                manager.touch(relPath, stat.size);
            },
            readObject: async key => (await p(backend.get, key)),
            cleanup: async () => {
                await new Promise(resolve => { root.removePrefix(prefix, () => resolve()); });
                await new Promise(resolve => { root.close(resolve); });
                await new Promise(resolve => { backend.close(resolve); });
                await Fs.rm(cache, { recursive: true, force: true });
            }
        };
    };

    const withCache = (body, conf) => {
        return async () => {
            const ctx = await setup(conf);
            try {
                await body(ctx);
            } finally {
                await ctx.cleanup();
            }
        };
    };

    test('cache over s3: append, flush and re-hydrate round-trips exactly',
        withCache(async ctx => {
            await ctx.append('ab/doc.ndjson', 'one\n');
            await ctx.append('ab/doc.ndjson', 'two\n');
            await p(ctx.manager.flush, 'ab/doc.ndjson');

            assert.strictEqual((await ctx.readObject('channel/ab/doc.ndjson')).toString('utf8'),
                'one\ntwo\n');

            // drop the cache entirely and fetch it back
            await p(ctx.manager.forget, 'ab/doc.ndjson');
            await p(ctx.manager.hydrate, 'ab/doc.ndjson');

            const cached = await Fs.readFile(Path.join(ctx.cache, 'ab/doc.ndjson'), 'utf8');
            assert.strictEqual(cached, 'one\ntwo\n');
        }));

    test('cache over s3: multipart-copy append preserves every byte',
        withCache(async ctx => {
            /*  Above 5 MiB the flush stops re-uploading the object and instead asks
                S3 to copy it server-side as part one of a multipart upload, with the
                new tail as part two. Any byte-level mistake here would silently
                invalidate every offset the history keeper has recorded, so this
                compares the whole object against the local file.  */
            const bulk = 'x'.repeat(64) + '\n';
            let expected = '';
            while (expected.length < 6 * 1024 * 1024) { expected += bulk; }

            await ctx.append('ab/large.ndjson', expected);
            await p(ctx.manager.flush, 'ab/large.ndjson');

            const tail = 'appended-after-the-threshold\n';
            await ctx.append('ab/large.ndjson', tail);
            await p(ctx.manager.flush, 'ab/large.ndjson');
            expected += tail;

            const stored = await ctx.readObject('channel/ab/large.ndjson');
            assert.strictEqual(stored.length, Buffer.byteLength(expected));
            assert.ok(stored.equals(Buffer.from(expected)),
                'the stored object must equal the local file byte for byte');

            const local = await Fs.readFile(Path.join(ctx.cache, 'ab/large.ndjson'));
            assert.ok(local.equals(stored), 'cache and store must agree');
        }));

    test('cache over s3: recovery flushes a tail left by a crash',
        withCache(async ctx => {
            await ctx.append('ab/crash.ndjson', 'flushed\n');
            await p(ctx.manager.flush, 'ab/crash.ndjson');

            // crash: append locally, lose the in-memory state
            await Fs.appendFile(Path.join(ctx.cache, 'ab/crash.ndjson'), 'unflushed\n');
            ctx.manager._entries.clear();

            const report = await p(ctx.manager.recover);
            assert.strictEqual(report.flushed, 1);
            assert.strictEqual((await ctx.readObject('channel/ab/crash.ndjson')).toString('utf8'),
                'flushed\nunflushed\n');
        }));

    test('cache over s3: a competing writer is detected, not clobbered',
        withCache(async ctx => {
            await ctx.append('ab/owned.ndjson', 'ours\n');
            await p(ctx.manager.flush, 'ab/owned.ndjson');

            // another node takes ownership and writes
            await p(ctx.backend.put, 'channel/ab/owned.ndjson', 'theirs\n', {});

            await ctx.append('ab/owned.ndjson', 'more of ours\n');
            let failed = false;
            await new Promise(resolve => {
                ctx.manager.flush('ab/owned.ndjson', err => { failed = Boolean(err); resolve(); });
            });

            assert.ok(failed, 'the conditional write must reject our flush');
            assert.strictEqual((await ctx.readObject('channel/ab/owned.ndjson')).toString('utf8'),
                'theirs\n', "the other writer's data must survive");
        }));

    // --- blobs over S3 ------------------------------------------------------

    const CachedBlob = require('../../storage/storage/cached-blob.js');
    const FsPromises = require('node:fs/promises');

    const setupBlob = async () => {
        const prefix = `_test/blob-${Date.now()}-${Crypto.randomBytes(4).toString('hex')}/`;
        const base = await FsPromises.mkdtemp(Path.join(Os.tmpdir(), 'cryptpad-blob-s3-'));

        const mk = (extra) => Object.assign({
            bucket: CONFIG.bucket,
            endpoint: CONFIG.endpoint,
            region: CONFIG.region,
            credentials: {
                accessKeyId: CONFIG.accessKeyId,
                secretAccessKey: CONFIG.secretAccessKey
            },
            skipProbe: true,
            capabilities: { conditionalPut: true, serverSideAppend: true }
        }, extra || {});

        const backend = await p(S3Backend.create, mk({ prefix }));
        const root = await p(S3Backend.create, mk({}));

        const sessions = {};
        const store = await p(CachedBlob.create, {
            backend,
            blobStagingPath: Path.join(base, 'blobstage'),
            localBlobPath: Path.join(base, 'localblob'),
            archivePath: Path.join(base, 'archive'),
            getSession: safeKey => (sessions[safeKey] ||= {
                currentUploadSize: 0,
                pendingUploadSize: 64 * 1024 * 1024
            })
        });

        return {
            backend, store,
            cleanup: async () => {
                await new Promise(resolve => { root.removePrefix(prefix, () => resolve()); });
                await new Promise(resolve => { root.close(resolve); });
                await new Promise(resolve => { backend.close(resolve); });
                await FsPromises.rm(base, { recursive: true, force: true });
            }
        };
    };

    test('blobs over s3: a large upload round-trips byte for byte', async () => {
        const ctx = await setupBlob();
        const SAFEKEY = 'k'.repeat(44);
        const BLOB = 'a'.repeat(48);
        try {
            /*  12 MiB pushed through the chunked upload path, which crosses the
                multipart threshold: this is the path every real file upload takes,
                and the one the filesystem backend cannot exercise faithfully.  */
            const chunk = Buffer.alloc(256 * 1024, 'z');
            for (let i = 0; i < 48; i++) {
                await p(ctx.store.upload, SAFEKEY, chunk.toString('base64'));
            }
            await p(ctx.store.complete, SAFEKEY, BLOB);

            assert.strictEqual(await p(ctx.store.size, BLOB), 12 * 1024 * 1024);

            const stored = await p(ctx.backend.get, `blob/aa/${BLOB}`);
            assert.strictEqual(stored.length, 12 * 1024 * 1024);
            assert.ok(stored.every(byte => byte === 'z'.charCodeAt(0)),
                'every byte of the upload must survive');
        } finally {
            await ctx.cleanup();
        }
    });

    test('blobs over s3: archive and restore happen server-side', async () => {
        const ctx = await setupBlob();
        const SAFEKEY = 'k'.repeat(44);
        const BLOB = 'b'.repeat(48);
        try {
            await p(ctx.store.upload, SAFEKEY,
                Buffer.from('owned contents').toString('base64'));
            await p(ctx.store.completeOwned, SAFEKEY, BLOB);

            await p(ctx.store.archive.blob, BLOB, 'ARCHIVE_OWNED');
            assert.strictEqual(await p(ctx.store.isBlobAvailable, BLOB), false);
            assert.strictEqual(await p(ctx.store.isBlobArchived, BLOB), true);

            await p(ctx.store.restore.blob, BLOB);
            assert.strictEqual(await p(ctx.store.isBlobAvailable, BLOB), true);
            assert.strictEqual((await p(ctx.backend.get, `blob/bb/${BLOB}`)).toString('utf8'),
                'owned contents');
        } finally {
            await ctx.cleanup();
        }
    });

    test('blobs over s3: presigned delivery serves the blob', async () => {
        const ctx = await setupBlob();
        const SAFEKEY = 'k'.repeat(44);
        const BLOB = 'c'.repeat(48);
        try {
            await p(ctx.store.upload, SAFEKEY,
                Buffer.from('downloadable').toString('base64'));
            await p(ctx.store.complete, SAFEKEY, BLOB);

            // this is how browsers fetch blobs in redirect mode: the object goes
            // straight from the store to the client, never through the server
            const url = await p(ctx.backend.presignGet, `blob/cc/${BLOB}`, 60);
            const res = await fetch(url);
            assert.strictEqual(res.status, 200);
            assert.strictEqual(await res.text(), 'downloadable');
        } finally {
            await ctx.cleanup();
        }
    });
}
