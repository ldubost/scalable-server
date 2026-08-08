// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors
//
// SPDX-License-Identifier: AGPL-3.0-or-later

/*  storage/storage/cached-blob.js — the blob store backed by an object store.

    Blobs are immutable once uploaded, so the interesting behaviour is at the
    boundaries: what happens while an upload is still in progress, what happens
    when it completes, and that a completed blob is never downloaded back to the
    server for routine operations like sizing or archiving.
*/

const { test } = require('node:test');
const assert = require('node:assert');
const Fs = require('node:fs/promises');
const FsSync = require('node:fs');
const Os = require('node:os');
const Path = require('node:path');

const CachedBlob = require('../../storage/storage/cached-blob.js');
const FsBackend = require('../../common/storage/backend/fs.js');
const { p, failure } = require('./backend-conformance.js');

const BLOB = 'a'.repeat(48);
const OTHER = 'b'.repeat(48);
const SAFEKEY = 'k'.repeat(44);

const setup = async () => {
    const base = await Fs.mkdtemp(Path.join(Os.tmpdir(), 'cryptpad-blob-'));
    const backend = await p(FsBackend.create, { root: Path.join(base, 'store') });

    // blob.js tracks per-uploader session state; a plain object is enough here
    const sessions = {};
    const store = await p(CachedBlob.create, {
        backend,
        blobStagingPath: Path.join(base, 'blobstage'),
        localBlobPath: Path.join(base, 'localblob'),
        archivePath: Path.join(base, 'archive'),
        getSession: safeKey => {
            return (sessions[safeKey] ||= {
                currentUploadSize: 0,
                pendingUploadSize: 1024 * 1024
            });
        }
    });

    return {
        base, backend, store, sessions,
        stagePath: Path.join(base, 'blobstage', SAFEKEY.slice(0, 2), SAFEKEY),
        objectExists: key => p(backend.exists, key),
        readObject: async key => (await p(backend.get, key)).toString('utf8'),
        // push content through the chunked upload path, as the HTTP layer does
        stage: async (content, safeKey) => {
            safeKey = safeKey || SAFEKEY;
            await p(store.upload, safeKey, Buffer.from(content).toString('base64'));
        },
        cleanup: () => Fs.rm(base, {
            recursive: true, force: true, maxRetries: 10, retryDelay: 20
        })
    };
};

const withStore = body => {
    return async () => {
        const ctx = await setup();
        try {
            await body(ctx);
        } finally {
            await ctx.cleanup();
        }
    };
};

// --- staging ---------------------------------------------------------------

test('blob: an upload in progress stays on local disk', withStore(async ctx => {
    await ctx.stage('some file contents');

    // nothing should have reached the store: a partial upload is worthless,
    // and most abandoned uploads never complete
    const listed = await p(ctx.backend.list, '', {});
    assert.deepStrictEqual(listed.keys, []);
    assert.strictEqual(await p(ctx.store.status, SAFEKEY), true,
        'the staged file should be visible to the uploader');
}));

test('blob: cancelling an upload discards the staged file', withStore(async ctx => {
    await ctx.stage('abandoned');
    await p(ctx.store.cancel, SAFEKEY, 1024);

    assert.strictEqual(await p(ctx.store.status, SAFEKEY), false);
    assert.deepStrictEqual((await p(ctx.backend.list, '', {})).keys, []);
}));

test('blob: upload cookies round-trip', withStore(async ctx => {
    await ctx.stage('contents');
    const cookie = await p(ctx.store.uploadCookie, SAFEKEY);
    assert.ok(cookie, 'a cookie should be issued');

    const checked = await new Promise(resolve => {
        ctx.store.checkUploadCookie(SAFEKEY, resolve);
    });
    assert.strictEqual(checked, cookie);
}));

// --- completion ------------------------------------------------------------

test('blob: completing an upload moves it to the store', withStore(async ctx => {
    await ctx.stage('file contents');
    const id = await p(ctx.store.complete, SAFEKEY, BLOB);

    assert.strictEqual(id, BLOB);
    assert.strictEqual(await ctx.readObject(`blob/aa/${BLOB}`), 'file contents');

    // and the staged copy is gone: keeping it would double the storage cost
    assert.strictEqual(FsSync.existsSync(ctx.stagePath), false);
}));

test('blob: a completed blob reports its size without downloading it',
    withStore(async ctx => {
        await ctx.stage('x'.repeat(5000));
        await p(ctx.store.complete, SAFEKEY, BLOB);

        assert.strictEqual(await p(ctx.store.size, BLOB), 5000);
        assert.strictEqual(await p(ctx.store.isBlobAvailable, BLOB), true);
    }));

test('blob: size of a missing blob is zero', withStore(async ctx => {
    assert.strictEqual(await p(ctx.store.size, BLOB), 0);
}));

test('blob: completion refuses to overwrite an existing blob', withStore(async ctx => {
    await ctx.stage('first');
    await p(ctx.store.complete, SAFEKEY, BLOB);

    await ctx.stage('second');
    const err = await failure(p(ctx.store.complete, SAFEKEY, BLOB));
    assert.strictEqual(err, 'RENAME_ERR', 'an id collision must be reported');
    assert.strictEqual(await ctx.readObject(`blob/aa/${BLOB}`), 'first');
}));

test('blob: completing without a staged file fails', withStore(async ctx => {
    const err = await failure(p(ctx.store.complete, SAFEKEY, BLOB));
    assert.strictEqual(err, 'RENAME_ERR');
}));

test('blob: an owned upload records its owner before the blob exists',
    withStore(async ctx => {
        await ctx.stage('owned contents');
        await p(ctx.store.completeOwned, SAFEKEY, BLOB);

        const lines = [];
        await new Promise((resolve, reject) => {
            ctx.store.readMetadata(BLOB, (err, parsed) => { lines.push(parsed); },
                err => err ? reject(err) : resolve());
        });

        assert.strictEqual(lines.length, 1);
        // safeKey characters are unescaped back into the real public key
        assert.deepStrictEqual(lines[0].owners, [SAFEKEY.replace(/-/g, '/')]);
        assert.strictEqual(await ctx.readObject(`blob/aa/${BLOB}`), 'owned contents');
    }));

test('blob: a large upload round-trips intact', withStore(async ctx => {
    // pushed through the same chunked path the HTTP layer uses
    const chunk = 'y'.repeat(64 * 1024);
    for (let i = 0; i < 8; i++) { await ctx.stage(chunk); }

    await p(ctx.store.complete, SAFEKEY, BLOB);
    const stored = await p(ctx.backend.get, `blob/aa/${BLOB}`);
    assert.strictEqual(stored.length, 8 * 64 * 1024);
    assert.ok(stored.every(byte => byte === 'y'.charCodeAt(0)));
}));

// --- metadata ---------------------------------------------------------------

test('blob: metadata appends rather than replacing', withStore(async ctx => {
    await p(ctx.store.writeMetadata, BLOB, JSON.stringify({ owners: ['one'] }));
    await p(ctx.store.writeMetadata, BLOB, JSON.stringify([-1, 'two']));

    const lines = [];
    await new Promise((resolve, reject) => {
        ctx.store.readMetadata(BLOB, (err, parsed) => { lines.push(parsed); },
            err => err ? reject(err) : resolve());
    });
    assert.strictEqual(lines.length, 2);
    assert.deepStrictEqual(lines[0], { owners: ['one'] });
}));

test('blob: reading absent metadata is not an error', withStore(async ctx => {
    // blobs uploaded anonymously have no metadata log at all
    const lines = [];
    await new Promise((resolve, reject) => {
        ctx.store.readMetadata(BLOB, (err, parsed) => { lines.push(parsed); },
            err => err ? reject(err) : resolve());
    });
    assert.deepStrictEqual(lines, []);
    assert.strictEqual(await p(ctx.store.hasMetadata, BLOB), false);
}));

// --- activity ---------------------------------------------------------------

test('blob: activity is recorded and readable', withStore(async ctx => {
    await ctx.stage('contents');
    await p(ctx.store.complete, SAFEKEY, BLOB);

    await p(ctx.store.updateActivity, BLOB);
    const when = await p(ctx.store.getActivity, BLOB);
    assert.ok(when instanceof Date);
    assert.ok(Math.abs(Date.now() - when.getTime()) < 5000);
}));

test('blob: activity writes are throttled', withStore(async ctx => {
    await ctx.stage('contents');
    await p(ctx.store.complete, SAFEKEY, BLOB);

    let writes = 0;
    const realPut = ctx.backend.put;
    ctx.backend.put = (key, body, opts, cb) => {
        if (key.endsWith('.activity')) { writes++; }
        realPut(key, body, opts, cb);
    };

    // this fires on every browser HEAD of every blob; one request to the store
    // per request from a browser would be absurd for a value measured in days
    for (let i = 0; i < 25; i++) { await p(ctx.store.updateActivity, BLOB); }
    assert.strictEqual(writes, 1, 'repeated activity updates must collapse');
}));

test('blob: getStats falls back to the blob when activity is unrecorded',
    withStore(async ctx => {
        await ctx.stage('contents');
        await p(ctx.store.complete, SAFEKEY, BLOB);

        const stats = await p(ctx.store.getStats, BLOB);
        assert.ok(stats.mtime instanceof Date);
        assert.strictEqual(stats.size, 8);
    }));

// --- archive / restore / remove ---------------------------------------------

test('blob: archive moves the blob and its metadata server-side',
    withStore(async ctx => {
        await ctx.stage('contents');
        await p(ctx.store.completeOwned, SAFEKEY, BLOB);
        await p(ctx.store.updateActivity, BLOB);

        await p(ctx.store.archive.blob, BLOB, 'ARCHIVE_OWNED');

        assert.strictEqual(await ctx.objectExists(`blob/aa/${BLOB}`), false);
        assert.strictEqual(await ctx.objectExists(`archive/blob/aa/${BLOB}`), true);
        assert.strictEqual(
            await ctx.objectExists(`archive/blob/aa/${BLOB}.metadata.ndjson`), true);
        assert.strictEqual(await p(ctx.store.isBlobArchived, BLOB), true);
    }));

test('blob: archiving records a reason', withStore(async ctx => {
    await ctx.stage('contents');
    await p(ctx.store.complete, SAFEKEY, BLOB);
    await p(ctx.store.archive.blob, BLOB, 'ARCHIVE_OWNED');

    const reason = await new Promise(resolve => {
        ctx.store.getPlaceholder(BLOB, resolve);
    });
    assert.strictEqual(reason, 'ARCHIVE_OWNED');
}));

test('blob: archiving a blob that is not there reports ENOENT',
    withStore(async ctx => {
        const err = await failure(p(ctx.store.archive.blob, BLOB, 'ARCHIVE_OWNED'));
        assert.strictEqual(err, 'ENOENT');
    }));

test('blob: restore brings a blob and its metadata back', withStore(async ctx => {
    await ctx.stage('contents');
    await p(ctx.store.completeOwned, SAFEKEY, BLOB);
    await p(ctx.store.archive.blob, BLOB, 'ARCHIVE_OWNED');

    await p(ctx.store.restore.blob, BLOB);

    assert.strictEqual(await ctx.readObject(`blob/aa/${BLOB}`), 'contents');
    assert.strictEqual(await p(ctx.store.hasMetadata, BLOB), true);
    // and it no longer looks deleted
    const reason = await new Promise(resolve => { ctx.store.getPlaceholder(BLOB, resolve); });
    assert.strictEqual(reason, undefined);
}));

test('blob: remove deletes the blob and its activity record', withStore(async ctx => {
    await ctx.stage('contents');
    await p(ctx.store.complete, SAFEKEY, BLOB);
    await p(ctx.store.updateActivity, BLOB);

    await p(ctx.store.remove.blob, BLOB);

    assert.strictEqual(await ctx.objectExists(`blob/aa/${BLOB}`), false);
    assert.strictEqual(await ctx.objectExists(`blob/aa/${BLOB}.activity`), false);
}));

test('blob: removing archived data clears every trace', withStore(async ctx => {
    await ctx.stage('contents');
    await p(ctx.store.completeOwned, SAFEKEY, BLOB);
    await p(ctx.store.archive.blob, BLOB, 'ARCHIVE_OWNED');

    await p(ctx.store.remove.archived.blob, BLOB);

    assert.strictEqual(await ctx.objectExists(`archive/blob/aa/${BLOB}`), false);
    assert.strictEqual(
        await ctx.objectExists(`archive/blob/aa/${BLOB}.metadata.ndjson`), false);
}));

test('blob: removing archived data that is absent is not an error',
    withStore(async ctx => {
        await p(ctx.store.remove.archived.blob, BLOB);
    }));

// --- listing ----------------------------------------------------------------

test('blob: lists blobs with sizes', withStore(async ctx => {
    await ctx.stage('one');
    await p(ctx.store.complete, SAFEKEY, BLOB);
    await ctx.stage('two!');
    await p(ctx.store.complete, SAFEKEY, OTHER);

    const seen = [];
    await new Promise((resolve, reject) => {
        ctx.store.list.blobs((err, data, next) => {
            if (err) { return reject(err); }
            seen.push(data);
            next();
        }, err => err ? reject(err) : resolve());
    });

    assert.deepStrictEqual(seen.map(b => b.blobId).sort(), [BLOB, OTHER].sort());
    assert.strictEqual(seen.find(b => b.blobId === BLOB).size, 3);
}));

test('blob: listing folds metadata into its blob rather than listing it separately',
    withStore(async ctx => {
        await ctx.stage('one');
        await p(ctx.store.completeOwned, SAFEKEY, BLOB);

        const seen = [];
        await new Promise((resolve, reject) => {
            ctx.store.list.blobs((err, data, next) => { seen.push(data); next(); },
                err => err ? reject(err) : resolve());
        });
        assert.strictEqual(seen.length, 1, 'a blob and its metadata are one entry');
    }));

test('blob: orphaned metadata is listed with no time, so eviction can clear it',
    withStore(async ctx => {
        // metadata whose blob never arrived
        await p(ctx.store.writeMetadata, BLOB, JSON.stringify({ owners: ['x'] }));

        const seen = [];
        await new Promise((resolve, reject) => {
            ctx.store.list.blobs((err, data, next) => { seen.push(data); next(); },
                err => err ? reject(err) : resolve());
        });
        assert.strictEqual(seen.length, 1);
        assert.strictEqual(seen[0].mtime, 0);
    }));

test('blob: sweeps activity records left behind by deleted blobs',
    withStore(async ctx => {
        await ctx.stage('contents');
        await p(ctx.store.complete, SAFEKEY, BLOB);
        await p(ctx.store.updateActivity, BLOB);

        // delete the blob behind the store's back, as the old bug did
        await p(ctx.backend.remove, `blob/aa/${BLOB}`);
        assert.strictEqual(await ctx.objectExists(`blob/aa/${BLOB}.activity`), true);

        const removed = await p(ctx.store.remove.loneActivity);
        assert.strictEqual(removed, 1);
        assert.strictEqual(await ctx.objectExists(`blob/aa/${BLOB}.activity`), false);
    }));

test('blob: rejects malformed ids', withStore(async ctx => {
    assert.strictEqual(await failure(p(ctx.store.size, 'nope')), 'INVALID_ID');
    assert.strictEqual(await failure(p(ctx.store.isBlobAvailable, 'nope')), 'INVALID_ID');
    assert.strictEqual(
        await failure(p(ctx.store.archive.blob, 'nope', 'reason')), 'INVALID_ID');
}));
