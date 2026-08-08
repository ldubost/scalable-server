// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors
//
// SPDX-License-Identifier: AGPL-3.0-or-later

/*  Store factory.

    One place decides whether a node's channel, pin and blob stores live on the
    local filesystem or in an object store. Every caller above this receives the
    same API either way.

    The three processes that make up a storage node each build their own handles
    (the primary in storage/index.js, the fork workers in storage/worker.js, the
    cluster workers in storage/cluster.js), so this is shared between them rather
    than duplicated three times.
*/

const nThen = require("nthen");
const Path = require("node:path");

const File = require("./file.js");
const CachedFile = require("./cached-file.js");
const Blob = require("./blob.js");
const CachedBlob = require("./cached-blob.js");
const Backends = require("../../common/storage/backend/index.js");

const Stores = module.exports;

const MIN_FLUSH_BYTES = 64 * 1024;

/*  A misconfigured flush window is a silent durability regression: it does not
    fail, it just widens how much recent editing a node can lose. So the values are
    checked at boot and rejected outright rather than clamped into something the
    operator did not ask for. */
const validateCacheConfig = (cache) => {
    if (!cache) { return; }

    const num = (name, min) => {
        const value = cache[name];
        if (typeof (value) === 'undefined') { return; }
        if (typeof (value) !== 'number' || isNaN(value) || value < 0) {
            throw new Error(`config.storage.s3.cache.${name} must be a non-negative number`);
        }
        if (typeof (min) === 'number' && value > 0 && value < min) {
            throw new Error(`config.storage.s3.cache.${name} must be at least ${min}`);
        }
    };

    num('flushDebounceMs');
    num('flushMaxDelayMs');
    num('flushMaxBytes', MIN_FLUSH_BYTES);
    num('maxBytes');
    num('maxIdleMs');

    const debounce = cache.flushDebounceMs;
    const ceiling = cache.flushMaxDelayMs;
    if (typeof (debounce) === 'number' && typeof (ceiling) === 'number' &&
        ceiling > 0 && debounce > ceiling) {
        throw new Error(
            'config.storage.s3.cache.flushDebounceMs must not exceed flushMaxDelayMs: ' +
            'a debounce longer than the ceiling means the ceiling is what always fires');
    }
};

/*  Build the channel store, the pin store and the blob store for a node.

        Stores.create(Env, {
            paths: { filePath, pinPath, archivePath, blobPath, blobStagingPath, cachePath },
            config,           // the whole server config
            Log,
            getSession,       // blob upload sessions (primary/cluster only)
            monitoring,
            only              // optional subset, e.g. ['blob'] for HTTP workers
        }, cb)
*/
Stores.create = (opts, _cb) => {
    const cb = typeof (_cb) === 'function' ? _cb : () => {};

    const config = opts.config || {};
    const storageConfig = config.storage || {};
    const type = storageConfig.type || 'fs';
    const paths = opts.paths || {};
    const Log = opts.Log;

    /*  HTTP workers serve blobs but never touch channels, so they can skip
        building channel and pin stores — and, more to the point, the cache
        managers and flush timers that come with them. */
    const only = Array.isArray(opts.only) ? opts.only : null;
    const wants = name => !only || only.indexOf(name) !== -1;

    const result = {};

    // ---------------------------------------------------------------- local
    if (type === 'fs') {
        return void nThen(w => {
            if (wants('channel')) { File.create({
                filePath: paths.filePath,
                archivePath: paths.archivePath
            }, w((err, store) => {
                if (err) { w.abort(); return void cb(err); }
                result.store = store;
            })); }
            if (wants('pin')) { File.create({
                filePath: paths.pinPath,
                archivePath: paths.archivePath,
                // pins archive to their own volume, or archived pin logs would
                // get mixed in with channels
                volumeId: 'pins'
            }, w((err, store) => {
                if (err) { w.abort(); return void cb(err); }
                result.pinStore = store;
            })); }
            if (wants('blob')) { Blob.create({
                blobPath: paths.blobPath,
                blobStagingPath: paths.blobStagingPath,
                archivePath: paths.archivePath,
                getSession: opts.getSession,
                /*  Required in the storage *cluster*, where an HTTP upload has
                    no RPC session of its own and has to ask the primary for the
                    caller's quota (`UPLOAD_GET_SESSION`). Without it
                    `blob.js:upload` reaches `Env.sendCommand(...)` on an Env
                    that has none and the worker dies with a TypeError — which
                    surfaces as ECONNRESET at the proxy and an upload that stalls
                    with no error anywhere useful.

                    The object-storage branch below already forwards this; the
                    filesystem branch did not, so HTTP uploads crashed the worker
                    on `fs` while WebSocket uploads (which carry their own
                    session) worked. */
                sendCommand: opts.sendCommand
            }, w((err, store) => {
                if (err) { w.abort(); return void cb(err); }
                result.blobStore = store;
            })); }
        }).nThen(() => {
            cb(void 0, result);
        });
    }

    // --------------------------------------------------------------- remote
    const s3 = storageConfig[type] || {};
    const cacheConf = s3.cache || {};

    try {
        validateCacheConfig(cacheConf);
    } catch (err) {
        return void cb(err);
    }

    const cacheRoot = cacheConf.path || paths.cachePath;
    if (!cacheRoot) {
        return void cb(new Error(
            'config.storage.s3.cache.path is required: object storage needs a local ' +
            'cache directory to hold documents while they are being edited'));
    }

    const cacheOpts = {
        flushDebounceMs: cacheConf.flushDebounceMs,
        flushMaxDelayMs: cacheConf.flushMaxDelayMs,
        flushMaxBytes: cacheConf.flushMaxBytes,
        flushConcurrency: cacheConf.flushConcurrency,
        maxBytes: cacheConf.maxBytes,
        maxIdleMs: cacheConf.maxIdleMs,
        appendThresholdBytes: s3.upload && s3.upload.appendThresholdMB ?
            s3.upload.appendThresholdMB * 1024 * 1024 : undefined,
        monitoring: opts.monitoring,
        Log
    };

    nThen(w => {
        Backends.create(storageConfig, { root: paths.filePath }, w((err, backend) => {
            if (err) { w.abort(); return void cb(err); }
            result.backend = backend;
        }));
    }).nThen(w => {
        if (wants('channel')) { CachedFile.create(Object.assign({
            backend: result.backend,
            cachePath: Path.join(cacheRoot, 'channel'),
            archivePath: paths.archivePath,
            volumeId: 'datastore',
            keyPrefix: 'channel/',
            archiveKeyPrefix: 'archive/datastore/',
            metricsPrefix: 's3',
            isPinned: opts.isPinned
        }, cacheOpts), w((err, store) => {
            if (err) { w.abort(); return void cb(err); }
            result.store = store;
        })); }

        if (wants('pin')) { CachedFile.create(Object.assign({
            backend: result.backend,
            cachePath: Path.join(cacheRoot, 'pins'),
            archivePath: paths.archivePath,
            volumeId: 'pins',
            keyPrefix: 'pins/',
            archiveKeyPrefix: 'archive/pins/',
            metricsPrefix: 's3_pins'
        }, cacheOpts), w((err, store) => {
            if (err) { w.abort(); return void cb(err); }
            result.pinStore = store;
        })); }

        /*  Blobs are immutable once uploaded, so they need none of the cached
            append-log machinery: uploads stage on local disk and are streamed to
            the store on completion.  */
        if (wants('blob')) { CachedBlob.create({
            backend: result.backend,
            keyPrefix: 'blob/',
            archiveKeyPrefix: 'archive/blob/',
            blobStagingPath: paths.blobStagingPath,
            localBlobPath: paths.blobPath,
            archivePath: paths.archivePath,
            getSession: opts.getSession,
            sendCommand: opts.sendCommand
        }, w((err, store) => {
            if (err) { w.abort(); return void cb(err); }
            result.blobStore = store;
        })); }
    }).nThen(() => {
        cb(void 0, result);
    });
};

Stores._validateCacheConfig = validateCacheConfig;
