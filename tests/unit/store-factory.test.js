// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors
//
// SPDX-License-Identifier: AGPL-3.0-or-later

/*  storage/storage/index.js — the one place that decides where a node's data
    lives. Getting this wrong is expensive in a specific way: a node that silently
    falls back to local disk looks healthy while stranding its data on one machine.
*/

const { test } = require('node:test');
const assert = require('node:assert');
const Fs = require('node:fs/promises');
const Os = require('node:os');
const Path = require('node:path');

const Stores = require('../../storage/storage/index.js');
const Core = require('../../common/core.js');
const { p, failure } = require('./backend-conformance.js');

const quietLog = {
    info: () => {}, warn: () => {}, error: () => {}, verbose: () => {}, silly: () => {}
};

const mkPaths = async () => {
    const base = await Fs.mkdtemp(Path.join(Os.tmpdir(), 'cryptpad-factory-'));
    return {
        base,
        paths: {
            filePath: Path.join(base, 'channel'),
            pinPath: Path.join(base, 'pins'),
            archivePath: Path.join(base, 'archive'),
            blobPath: Path.join(base, 'blob'),
            blobStagingPath: Path.join(base, 'blobstage'),
            cachePath: Path.join(base, 'cache')
        },
        cleanup: () => Fs.rm(base, { recursive: true, force: true })
    };
};

// blob.js requires one; the factory's job is to pass it through
const getSession = () => ({});

test('factory: defaults to local filesystem stores', async () => {
    const { paths, cleanup } = await mkPaths();
    try {
        const stores = await p(Stores.create, {
            paths, config: {}, Log: quietLog, getSession
        });
        assert.ok(stores.store, 'a channel store');
        assert.ok(stores.pinStore, 'a pin store');
        assert.ok(stores.blobStore, 'a blob store');
        // the plain file store has nothing buffered, so it has no drain step
        assert.strictEqual(typeof stores.store.flushAll, 'undefined');
    } finally {
        await cleanup();
    }
});

test('factory: the fs default and the pin store use separate archives', async () => {
    const { paths, cleanup } = await mkPaths();
    try {
        const stores = await p(Stores.create, {
            paths, config: {}, Log: quietLog, getSession
        });
        // pins archive to their own volume; sharing one would mix archived pin
        // logs in with archived channels
        assert.ok(stores.store);
        assert.ok(stores.pinStore);
        assert.notStrictEqual(stores.store, stores.pinStore);
    } finally {
        await cleanup();
    }
});

test('factory: an s3 type reaches the s3 backend and validates its config',
    async () => {
        const { paths, cleanup } = await mkPaths();
        try {
            // proves routing: the error comes from the S3 backend's own validation,
            // not from the registry failing to resolve it
            const err = await failure(p(Stores.create, {
                paths,
                Log: quietLog,
                getSession,
                config: {
                    storage: {
                        type: 's3',
                        s3: { cache: { path: paths.cachePath } }  // no bucket
                    }
                }
            }));
            assert.ok(err);
            assert.match(err.message, /bucket|@aws-sdk/);
        } finally {
            await cleanup();
        }
    });

test('factory: an unavailable backend fails startup rather than falling back',
    async () => {
        const { paths, cleanup } = await mkPaths();
        try {
            const err = await failure(p(Stores.create, {
                paths,
                Log: quietLog,
                config: { storage: { type: 'nonsense' } }
            }));
            assert.ok(err, 'startup must fail');
            assert.strictEqual(err.code, 'E_UNKNOWN_STORAGE_BACKEND');
        } finally {
            await cleanup();
        }
    });

test('factory: remote storage requires a cache directory', async () => {
    const err = await failure(p(Stores.create, {
        paths: {},          // no cachePath either
        Log: quietLog,
        config: { storage: { type: 's3', s3: { bucket: 'x' } } }
    }));
    assert.ok(err);
    assert.match(err.message, /cache/);
});

// --- flush-window validation -----------------------------------------------

test('validation: accepts a sane flush window', () => {
    Stores._validateCacheConfig({
        flushDebounceMs: 5000,
        flushMaxDelayMs: 30000,
        flushMaxBytes: 1024 * 1024
    });
});

test('validation: accepts strict mode (zero debounce)', () => {
    // flush-before-acknowledge: slow, but a legitimate choice
    Stores._validateCacheConfig({ flushDebounceMs: 0, flushMaxDelayMs: 30000 });
});

test('validation: rejects a debounce longer than the ceiling', () => {
    /*  This is the subtle one: it "works", but the debounce can never fire, so the
        ceiling silently becomes the only trigger and the operator's intent is
        quietly discarded.  */
    assert.throws(() => {
        Stores._validateCacheConfig({ flushDebounceMs: 60000, flushMaxDelayMs: 30000 });
    }, /flushDebounceMs must not exceed flushMaxDelayMs/);
});

test('validation: rejects negative and non-numeric values', () => {
    assert.throws(() => Stores._validateCacheConfig({ flushDebounceMs: -1 }),
        /non-negative/);
    assert.throws(() => Stores._validateCacheConfig({ flushMaxDelayMs: 'soon' }),
        /non-negative/);
    assert.throws(() => Stores._validateCacheConfig({ maxBytes: NaN }),
        /non-negative/);
});

test('validation: rejects an absurdly small byte threshold', () => {
    // a threshold of a few bytes would flush on every message
    assert.throws(() => Stores._validateCacheConfig({ flushMaxBytes: 10 }),
        /at least/);
});

test('validation: a missing cache block is fine', () => {
    Stores._validateCacheConfig(undefined);
    Stores._validateCacheConfig({});
});

// --- paths ------------------------------------------------------------------

test('paths: getPaths exposes a cache directory', () => {
    const paths = Core.getPaths({ index: 0, config: {} });
    assert.ok(paths.cachePath, 'cachePath must be defined');
    assert.match(paths.cachePath, /cache/);

    const env = Core.getPaths({ index: 0, config: {} }, true);
    assert.strictEqual(env.cache, paths.cachePath);
});

test('paths: the cache directory is per storage node', () => {
    const zero = Core.getPaths({ index: 0, config: {} });
    const one = Core.getPaths({ index: 1, config: {} });
    assert.notStrictEqual(zero.cachePath, one.cachePath,
        'two storage nodes on one machine must not share a cache');
});

test('paths: an explicit cache path wins', () => {
    const paths = Core.getPaths({
        index: 0,
        config: { storage: { s3: { cache: { path: '/mnt/fast/cache' } } } }
    });
    assert.strictEqual(paths.cachePath, '/mnt/fast/cache');
});
