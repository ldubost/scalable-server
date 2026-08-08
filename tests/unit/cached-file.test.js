// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors
//
// SPDX-License-Identifier: AGPL-3.0-or-later

/*  storage/storage/cached-file.js — the channel store backed by an object store.

    The contract being tested is "indistinguishable from file.js": the same methods,
    the same results, the same error codes. Where practical each behaviour is
    asserted against *both* stores from one body, so the cached store cannot quietly
    drift from the engine it is meant to replace.
*/

const { test } = require('node:test');
const assert = require('node:assert');
const Fs = require('node:fs/promises');
const FsSync = require('node:fs');
const Os = require('node:os');
const Path = require('node:path');

const File = require('../../storage/storage/file.js');
const CachedFile = require('../../storage/storage/cached-file.js');
const FsBackend = require('../../common/storage/backend/fs.js');
const { p, failure } = require('./backend-conformance.js');

const quietLog = {
    info: () => {}, warn: () => {}, error: () => {}, verbose: () => {}, silly: () => {}
};

const CHANNEL = 'a'.repeat(32);
const OTHER = 'b'.repeat(32);

const setup = async (conf) => {
    const base = await Fs.mkdtemp(Path.join(Os.tmpdir(), 'cryptpad-cached-'));
    const store = Path.join(base, 'store');
    const backend = await p(FsBackend.create, { root: store });

    const cached = await p(CachedFile.create, Object.assign({
        backend,
        cachePath: Path.join(base, 'cache'),
        archivePath: Path.join(base, 'localarchive'),
        volumeId: 'datastore',
        keyPrefix: 'channel/',
        archiveKeyPrefix: 'archive/datastore/',
        Log: quietLog,
        flushDebounceMs: 60000,
        flushMaxDelayMs: 0,
        flushMaxBytes: 0
    }, conf || {}));

    // plain file.js stores built by parity tests, torn down with everything else
    const extra = [];

    return {
        base, store, backend, cached,
        cachePath: Path.join(base, 'cache'),
        // build a plain file.js store to compare against
        plain: async () => {
            const store = await p(File.create, {
                filePath: Path.join(base, 'plain'),
                archivePath: Path.join(base, 'plainarchive')
            });
            extra.push(store);
            return store;
        },
        objectExists: key => p(backend.exists, key),
        readObject: async key => (await p(backend.get, key)).toString('utf8'),
        cleanup: async () => {
            // close write streams and stop timers, as a shutdown would
            [cached, ...extra].forEach(store => {
                store.closeInactiveChannels(new Set());
                store.shutdown();
            });
            // stream destruction is asynchronous, so the directory may briefly
            // still be in use when we try to remove it
            await Fs.rm(base, {
                recursive: true, force: true, maxRetries: 10, retryDelay: 20
            });
        }
    };
};

const withStore = (body, conf) => {
    return async () => {
        const ctx = await setup(conf);
        try {
            await body(ctx);
        } finally {
            await ctx.cleanup();
        }
    };
};

// collect every message in a channel, the way the history keeper does
const readAll = (store, channel) => {
    return new Promise((resolve, reject) => {
        const out = [];
        store.getMessages(channel, msg => { out.push(msg); }, err => {
            if (err) { return reject(err); }
            resolve(out);
        });
    });
};

const readFrom = (store, channel, start) => {
    return new Promise((resolve, reject) => {
        const out = [];
        store.readMessagesBin(channel, start, (msgObj, readMore) => {
            out.push({ offset: msgObj.offset, text: msgObj.buff.toString('utf8') });
            readMore();
        }, err => {
            if (err) { return reject(err); }
            resolve(out);
        });
    });
};

// --- parity with file.js ---------------------------------------------------

test('cached-file: messages round-trip like file.js', withStore(async ctx => {
    const plain = await ctx.plain();

    for (const store of [plain, ctx.cached]) {
        await p(store.message, CHANNEL, 'one');
        await p(store.message, CHANNEL, 'two');
    }

    assert.deepStrictEqual(await readAll(ctx.cached, CHANNEL), ['one', 'two']);
    assert.deepStrictEqual(await readAll(ctx.cached, CHANNEL), await readAll(plain, CHANNEL));
}));

test('cached-file: byte offsets match file.js exactly', withStore(async ctx => {
    /*  The history keeper stores byte offsets and later re-reads from them. If the
        cached store produced different offsets, reconnecting clients would resume
        mid-message. */
    const plain = await ctx.plain();

    const messages = ['alpha', 'beta', 'a longer message', 'cp|checkpoint'];
    for (const store of [plain, ctx.cached]) {
        for (const msg of messages) { await p(store.message, CHANNEL, msg); }
    }

    const fromPlain = await readFrom(plain, CHANNEL, 0);
    const fromCached = await readFrom(ctx.cached, CHANNEL, 0);
    assert.deepStrictEqual(fromCached, fromPlain);

    // and resuming from a recorded offset must land on a message boundary
    const resumeAt = fromPlain[2].offset;
    assert.deepStrictEqual(
        (await readFrom(ctx.cached, CHANNEL, resumeAt)).map(m => m.text),
        ['a longer message', 'cp|checkpoint']
    );
}));

test('cached-file: rejects invalid channel ids like file.js', withStore(async ctx => {
    const err = await failure(p(ctx.cached.message, 'too-short', 'x'));
    assert.ok(err, 'an invalid id must be rejected');
    assert.strictEqual(err.message, 'EINVAL');
}));

// --- the object store side -------------------------------------------------

test('cached-file: messages reach the store on flush', withStore(async ctx => {
    await p(ctx.cached.message, CHANNEL, 'one');
    await p(ctx.cached.message, CHANNEL, 'two');

    await p(ctx.cached.flushAll);

    assert.strictEqual(await ctx.readObject(`channel/aa/${CHANNEL}.ndjson`),
        'one\ntwo\n');
}));

test('cached-file: a channel is served from the store after the cache is cleared',
    withStore(async ctx => {
        await p(ctx.cached.message, CHANNEL, 'persisted');
        await p(ctx.cached.flushAll);

        // wipe the cache entirely, as an eviction or a fresh node would
        await Fs.rm(ctx.cachePath, { recursive: true, force: true });

        assert.deepStrictEqual(await readAll(ctx.cached, CHANNEL), ['persisted']);
    }));

test('cached-file: metadata is written through immediately', withStore(async ctx => {
    await p(ctx.cached.writeMetadata, CHANNEL, JSON.stringify({ owners: ['someone'] }));

    // no flush call: metadata must already be durable
    assert.strictEqual(await ctx.objectExists(`channel/aa/${CHANNEL}.metadata.ndjson`), true);
}));

test('cached-file: metadata reads survive a cache wipe', withStore(async ctx => {
    await p(ctx.cached.writeMetadata, CHANNEL, JSON.stringify({ owners: ['someone'] }));
    await Fs.rm(ctx.cachePath, { recursive: true, force: true });

    const lines = [];
    await new Promise((resolve, reject) => {
        ctx.cached.readDedicatedMetadata(CHANNEL, (err, line) => { lines.push(line); },
            err => err ? reject(err) : resolve());
    });
    assert.deepStrictEqual(lines, [{ owners: ['someone'] }]);
}));

// --- existence and size ----------------------------------------------------

test('cached-file: isChannelAvailable sees stored and unflushed channels',
    withStore(async ctx => {
        assert.strictEqual(await p(ctx.cached.isChannelAvailable, CHANNEL), false);

        await p(ctx.cached.message, CHANNEL, 'one');
        // unflushed, but it exists as far as callers are concerned
        assert.strictEqual(await p(ctx.cached.isChannelAvailable, CHANNEL), true);

        await p(ctx.cached.flushAll);
        await Fs.rm(ctx.cachePath, { recursive: true, force: true });
        // now only in the store
        assert.strictEqual(await p(ctx.cached.isChannelAvailable, CHANNEL), true);
    }));

test('cached-file: getChannelSize counts unflushed bytes', withStore(async ctx => {
    await p(ctx.cached.message, CHANNEL, 'one');   // 'one\n'
    assert.strictEqual(await p(ctx.cached.getChannelSize, CHANNEL), 4);

    await p(ctx.cached.flushAll);
    assert.strictEqual(await p(ctx.cached.getChannelSize, CHANNEL), 4);
}));

test('cached-file: getChannelSize answers from the store without hydrating',
    withStore(async ctx => {
        await p(ctx.cached.message, CHANNEL, 'one');
        await p(ctx.cached.flushAll);
        await Fs.rm(ctx.cachePath, { recursive: true, force: true });

        assert.strictEqual(await p(ctx.cached.getChannelSize, CHANNEL), 4);
        // asking for a size must not drag the log back into the cache
        assert.strictEqual(
            FsSync.existsSync(Path.join(ctx.cachePath, 'aa', `${CHANNEL}.ndjson`)),
            false, 'a size query should not hydrate');
    }));

// --- archive / restore / remove --------------------------------------------

test('cached-file: archive moves the object server-side', withStore(async ctx => {
    await p(ctx.cached.message, CHANNEL, 'one');
    await p(ctx.cached.flushAll);

    await p(ctx.cached.archiveChannel, CHANNEL, 'EXPIRED');

    assert.strictEqual(await ctx.objectExists(`channel/aa/${CHANNEL}.ndjson`), false);
    assert.strictEqual(await ctx.objectExists(`archive/datastore/aa/${CHANNEL}.ndjson`), true);
    assert.strictEqual(await p(ctx.cached.isChannelArchived, CHANNEL), true);
    // the local copy must be gone too, or a read would resurrect it
    assert.strictEqual(
        FsSync.existsSync(Path.join(ctx.cachePath, 'aa', `${CHANNEL}.ndjson`)), false);
}));

test('cached-file: archive leaves a placeholder explaining why', withStore(async ctx => {
    await p(ctx.cached.message, CHANNEL, 'one');
    await p(ctx.cached.flushAll);
    await p(ctx.cached.archiveChannel, CHANNEL, 'EXPIRED');

    const reason = await new Promise(resolve => {
        ctx.cached.getPlaceholder(CHANNEL, resolve);
    });
    assert.strictEqual(reason, 'EXPIRED');
}));

test('cached-file: restore brings an archived channel back', withStore(async ctx => {
    await p(ctx.cached.message, CHANNEL, 'one');
    await p(ctx.cached.flushAll);
    await p(ctx.cached.archiveChannel, CHANNEL, 'EXPIRED');

    await p(ctx.cached.restoreArchivedChannel, CHANNEL);

    assert.strictEqual(await ctx.objectExists(`channel/aa/${CHANNEL}.ndjson`), true);
    assert.deepStrictEqual(await readAll(ctx.cached, CHANNEL), ['one']);
}));

test('cached-file: restore refuses to clobber a live channel', withStore(async ctx => {
    await p(ctx.cached.message, CHANNEL, 'archived');
    await p(ctx.cached.flushAll);
    await p(ctx.cached.archiveChannel, CHANNEL, 'EXPIRED');

    // the channel came back to life in the meantime
    await p(ctx.cached.message, CHANNEL, 'live');
    await p(ctx.cached.flushAll);

    const err = await failure(p(ctx.cached.restoreArchivedChannel, CHANNEL));
    assert.strictEqual(err, 'UNARCHIVE_CHANNEL_CONFLICT');
    assert.strictEqual(await ctx.readObject(`channel/aa/${CHANNEL}.ndjson`), 'live\n');
}));

test('cached-file: remove deletes the object and the cached copy',
    withStore(async ctx => {
        await p(ctx.cached.message, CHANNEL, 'one');
        await p(ctx.cached.writeMetadata, CHANNEL, JSON.stringify({ owners: [] }));
        await p(ctx.cached.flushAll);

        await p(ctx.cached.removeChannel, CHANNEL);

        assert.strictEqual(await ctx.objectExists(`channel/aa/${CHANNEL}.ndjson`), false);
        assert.strictEqual(await ctx.objectExists(`channel/aa/${CHANNEL}.metadata.ndjson`), false);
        assert.strictEqual(await p(ctx.cached.isChannelAvailable, CHANNEL), false);
    }));

// --- rewrites --------------------------------------------------------------

test('cached-file: trimChannel rewrites the stored object', withStore(async ctx => {
    const Extras = require('../../storage/hk-util.js');
    // build messages in the shape trimChannel expects
    const mk = text => JSON.stringify([0, 'sender', 'MSG', null, text]);
    await p(ctx.cached.message, CHANNEL, mk('first'));
    await p(ctx.cached.message, CHANNEL, mk('second'));
    await p(ctx.cached.message, CHANNEL, mk('third'));
    await p(ctx.cached.flushAll);

    const hash = Extras.getHash('second');
    await p(ctx.cached.trimChannel, CHANNEL, hash);

    const remaining = await readAll(ctx.cached, CHANNEL);
    assert.strictEqual(remaining.length, 2, 'the first message should be gone');

    // the store must reflect the rewrite, not the pre-trim log
    const stored = await ctx.readObject(`channel/aa/${CHANNEL}.ndjson`);
    assert.strictEqual(stored.split('\n').filter(Boolean).length, 2);
}));

// --- listing ---------------------------------------------------------------

test('cached-file: listChannels enumerates the store', withStore(async ctx => {
    await p(ctx.cached.message, CHANNEL, 'one');
    await p(ctx.cached.message, OTHER, 'two');
    await p(ctx.cached.flushAll);

    const seen = [];
    await new Promise((resolve, reject) => {
        ctx.cached.listChannels((err, data, next) => {
            if (err) { return reject(err); }
            seen.push(data.channel);
            next();
        }, err => err ? reject(err) : resolve());
    });

    assert.deepStrictEqual(seen.sort(), [CHANNEL, OTHER].sort());
}));

test('cached-file: listChannels reports sizes without a stat per file',
    withStore(async ctx => {
        await p(ctx.cached.message, CHANNEL, 'one');
        await p(ctx.cached.flushAll);

        const seen = [];
        await new Promise((resolve, reject) => {
            ctx.cached.listChannels((err, data, next) => {
                seen.push(data); next();
            }, err => err ? reject(err) : resolve());
        });

        assert.strictEqual(seen[0].size, 4);
        assert.ok(typeof seen[0].mtime === 'number');
    }));

// --- lifecycle -------------------------------------------------------------

test('cached-file: closeChannel flushes before closing', withStore(async ctx => {
    await p(ctx.cached.message, CHANNEL, 'one');
    await p(ctx.cached.closeChannel, CHANNEL);

    // this is what happens when the last editor leaves: the document must be
    // fully in the store afterwards
    assert.strictEqual(await ctx.readObject(`channel/aa/${CHANNEL}.ndjson`), 'one\n');
}));

test('cached-file: recover flushes a tail left by a crash', withStore(async ctx => {
    await p(ctx.cached.message, CHANNEL, 'flushed');
    await p(ctx.cached.flushAll);

    // simulate a crash: append behind the store's back, drop in-memory state
    await Fs.appendFile(Path.join(ctx.cachePath, 'aa', `${CHANNEL}.ndjson`), 'unflushed\n');
    const report = await new Promise((resolve, reject) => {
        // a fresh store over the same cache, as a restarted node would have
        ctx.cached.recover((err, r) => err ? reject(err) : resolve(r));
    });

    assert.ok(report.flushed >= 1);
    assert.strictEqual(await ctx.readObject(`channel/aa/${CHANNEL}.ndjson`),
        'flushed\nunflushed\n');
}));

test('cached-file: offsets stay local and are never uploaded', withStore(async ctx => {
    await p(ctx.cached.message, CHANNEL, 'one');
    await p(ctx.cached.writeOffset, CHANNEL, { start: 0, created: Date.now() });
    await p(ctx.cached.flushAll);

    const offset = await p(ctx.cached.getOffset, CHANNEL);
    assert.strictEqual(offset.start, 0);

    // offsets are a local optimisation: uploading them would be waste
    assert.strictEqual(await ctx.objectExists(`channel/aa/${CHANNEL}.ndjson.offset`), false);
}));

/*  Regression: the dirty mark used to be applied only after an async stat, so a
    write could call back before the manager knew anything was outstanding. A flush
    racing that return — the shutdown drain, most importantly — would skip the write
    and the message would exist only on local disk.  */
test('cached-file: a flush immediately after a write cannot miss it',
    withStore(async ctx => {
        await p(ctx.cached.message, CHANNEL, 'written');
        // no delay, no scheduler: drain right now, as a SIGTERM would
        await p(ctx.cached.flushAll);

        assert.strictEqual(await ctx.readObject(`channel/aa/${CHANNEL}.ndjson`),
            'written\n', 'the drain must see a write that just called back');
    }));

test('cached-file: exposes cache statistics', withStore(async ctx => {
    await p(ctx.cached.message, CHANNEL, 'one');
    assert.strictEqual(ctx.cached.cacheStats().dirtyCount, 1);
    await p(ctx.cached.flushAll);
    assert.strictEqual(ctx.cached.cacheStats().dirtyCount, 0);
}));
