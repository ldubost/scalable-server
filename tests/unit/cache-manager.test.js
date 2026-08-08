// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors
//
// SPDX-License-Identifier: AGPL-3.0-or-later

/*  The cached append-log.

    These tests run against the filesystem backend, which implements the same
    ObjectBackend contract as S3 and lets the whole state machine be exercised
    offline in milliseconds. The "store" and the "cache" are two separate
    directories here, exactly as a bucket and a local cache are in production.

    The properties worth protecting, in order of how much a bug would cost:

      1. no acknowledged write is ever lost (flush, recovery, shutdown drain)
      2. the local copy is byte-identical to the object (offsets stay valid)
      3. a second writer is detected, never silently clobbered
*/

const { test } = require('node:test');
const assert = require('node:assert');
const Fs = require('node:fs/promises');
const FsSync = require('node:fs');
const Os = require('node:os');
const Path = require('node:path');

const FsBackend = require('../../common/storage/backend/fs.js');
const Manager = require('../../common/storage/cache/manager.js');
const Journal = require('../../common/storage/cache/journal.js');
const { p, failure } = require('./backend-conformance.js');

const quietLog = {
    info: () => {}, warn: () => {}, error: () => {}, verbose: () => {}, silly: () => {}
};

const setup = async (managerConf) => {
    const base = await Fs.mkdtemp(Path.join(Os.tmpdir(), 'cryptpad-cache-'));
    const store = Path.join(base, 'store');
    const cache = Path.join(base, 'cache');
    const backend = await p(FsBackend.create, { root: store });
    const manager = await p(Manager.create, Object.assign({
        backend,
        cacheRoot: cache,
        prefix: 'channel/',
        Log: quietLog,
        // long timers by default: tests drive flushes explicitly unless testing
        // the scheduler itself
        flushDebounceMs: 60000,
        flushMaxDelayMs: 0,
        flushMaxBytes: 0
    }, managerConf || {}));

    return {
        base, store, cache, backend, manager,
        // append to the cached file the way the file store would, then tell the
        // manager about it
        append: async (relPath, text) => {
            const path = Path.join(cache, relPath);
            await Fs.mkdir(Path.dirname(path), { recursive: true });
            await Fs.appendFile(path, text);
            const stat = await Fs.stat(path);
            manager.touch(relPath, stat.size);
        },
        readObject: async (key) => (await p(backend.get, key)).toString('utf8'),
        readCached: async (relPath) => Fs.readFile(Path.join(cache, relPath), 'utf8'),
        cleanup: () => Fs.rm(base, { recursive: true, force: true })
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

// --- hydration -------------------------------------------------------------

test('cache: hydrates an existing object into the cache', withCache(async ctx => {
    await p(ctx.backend.put, 'channel/ab/abcd.ndjson', 'line one\nline two\n', {});

    const result = await p(ctx.manager.hydrate, 'ab/abcd.ndjson');
    assert.strictEqual(result.downloaded, true);
    assert.strictEqual(await ctx.readCached('ab/abcd.ndjson'), 'line one\nline two\n');
}));

test('cache: hydration is byte-exact', withCache(async ctx => {
    // offsets recorded against this file must stay valid, so the local copy has
    // to be the object byte for byte - no normalisation of any kind
    const body = Buffer.from('a\nbb\r\nccc\n\néè\n');
    await p(ctx.backend.put, 'channel/ab/bytes.ndjson', body, {});

    await p(ctx.manager.hydrate, 'ab/bytes.ndjson');
    const cached = await Fs.readFile(Path.join(ctx.cache, 'ab/bytes.ndjson'));
    assert.ok(cached.equals(body), 'cached bytes must equal the stored bytes');
}));

test('cache: hydrating an absent object prepares a new one', withCache(async ctx => {
    const result = await p(ctx.manager.hydrate, 'ab/new.ndjson');
    assert.strictEqual(result.created, true);
    // nothing is written locally until something appends to it
    assert.strictEqual(FsSync.existsSync(Path.join(ctx.cache, 'ab/new.ndjson')), false);
}));

test('cache: hydration never clobbers a local copy', withCache(async ctx => {
    // the store has an old version; the cache has one with unflushed appends
    await p(ctx.backend.put, 'channel/ab/abcd.ndjson', 'old\n', {});
    await ctx.append('ab/abcd.ndjson', 'old\nlocal-append\n');

    await p(ctx.manager.hydrate, 'ab/abcd.ndjson');

    // the unflushed appends must survive: overwriting here would lose messages
    // that were already acknowledged to clients
    assert.strictEqual(await ctx.readCached('ab/abcd.ndjson'), 'old\nlocal-append\n');
}));

test('cache: concurrent hydration of the same object is safe', withCache(async ctx => {
    await p(ctx.backend.put, 'channel/ab/race.ndjson', 'payload\n', {});

    await Promise.all([
        p(ctx.manager.hydrate, 'ab/race.ndjson'),
        p(ctx.manager.hydrate, 'ab/race.ndjson'),
        p(ctx.manager.hydrate, 'ab/race.ndjson')
    ]);

    assert.strictEqual(await ctx.readCached('ab/race.ndjson'), 'payload\n');
    // no temp files left behind by the losers
    const files = await Fs.readdir(Path.join(ctx.cache, 'ab'));
    assert.deepStrictEqual(files.filter(f => f.includes('hydrating')), []);
}));

// --- flushing --------------------------------------------------------------

test('cache: flush uploads a newly created object', withCache(async ctx => {
    await ctx.append('ab/fresh.ndjson', 'one\n');
    await p(ctx.manager.flush, 'ab/fresh.ndjson');

    assert.strictEqual(await ctx.readObject('channel/ab/fresh.ndjson'), 'one\n');
}));

test('cache: flush uploads appended messages', withCache(async ctx => {
    await ctx.append('ab/log.ndjson', 'one\n');
    await p(ctx.manager.flush, 'ab/log.ndjson');
    await ctx.append('ab/log.ndjson', 'two\n');
    await p(ctx.manager.flush, 'ab/log.ndjson');

    assert.strictEqual(await ctx.readObject('channel/ab/log.ndjson'), 'one\ntwo\n');
}));

test('cache: flushing when nothing changed is a no-op', withCache(async ctx => {
    await ctx.append('ab/log.ndjson', 'one\n');
    await p(ctx.manager.flush, 'ab/log.ndjson');

    const before = ctx.manager.stats().counters.flushes;
    await p(ctx.manager.flush, 'ab/log.ndjson');
    assert.strictEqual(ctx.manager.stats().counters.flushes, before,
        'a clean object should not be re-uploaded');
}));

test('cache: the object matches the local file exactly after many appends',
    withCache(async ctx => {
        let expected = '';
        for (let i = 0; i < 50; i++) {
            const line = `message ${i} ${'x'.repeat(i)}\n`;
            expected += line;
            await ctx.append('ab/many.ndjson', line);
            // flush at irregular intervals, as the scheduler would
            if (i % 7 === 0) { await p(ctx.manager.flush, 'ab/many.ndjson'); }
        }
        await p(ctx.manager.flush, 'ab/many.ndjson');

        assert.strictEqual(await ctx.readObject('channel/ab/many.ndjson'), expected);
        assert.strictEqual(await ctx.readCached('ab/many.ndjson'), expected);
    }));

test('cache: concurrent flushes collapse without losing the tail',
    withCache(async ctx => {
        await ctx.append('ab/log.ndjson', 'one\n');

        // three flushes requested at once, plus an append racing them
        const flushes = [
            p(ctx.manager.flush, 'ab/log.ndjson'),
            p(ctx.manager.flush, 'ab/log.ndjson'),
            p(ctx.manager.flush, 'ab/log.ndjson')
        ];
        await ctx.append('ab/log.ndjson', 'two\n');
        await Promise.all(flushes);
        await p(ctx.manager.flush, 'ab/log.ndjson');

        assert.strictEqual(await ctx.readObject('channel/ab/log.ndjson'), 'one\ntwo\n');
    }));

/*  Regression: a write that lands while an upload is in flight used to have its
    dirty flag cleared when that upload finished. The bytes stayed on local disk,
    but flushAll() no longer knew about them — so a *graceful* shutdown reported a
    clean drain and dropped messages that had already been acknowledged to clients.
    Recovery would not save it either, because a clean shutdown is followed by a
    clean start.  */
test('cache: a write during an in-flight flush is not lost by the drain',
    withCache(async ctx => {
        await ctx.append('ab/racy.ndjson', 'first\n');

        // hold the upload open so we can append while it is in flight
        let release;
        const held = new Promise(resolve => { release = resolve; });
        const realPut = ctx.backend.put;
        let intercepted = false;
        ctx.backend.put = (key, body, opts, cb) => {
            if (!intercepted) {
                intercepted = true;
                return void held.then(() => { realPut(key, body, opts, cb); });
            }
            realPut(key, body, opts, cb);
        };

        const flushing = p(ctx.manager.flush, 'ab/racy.ndjson');
        await new Promise(resolve => setTimeout(resolve, 20));

        // this append happens strictly during the upload of 'first\n'
        await ctx.append('ab/racy.ndjson', 'second\n');
        release();
        await flushing;

        // the drain must still see the second message as outstanding
        await p(ctx.manager.flushAll);
        assert.strictEqual(await ctx.readObject('channel/ab/racy.ndjson'),
            'first\nsecond\n',
            'a message written during a flush must survive a graceful shutdown');
    }));

test('cache: server-side append path produces identical bytes', withCache(async ctx => {
    // force the append path by dropping the threshold to zero
    await ctx.append('ab/big.ndjson', 'header\n');
    await p(ctx.manager.flush, 'ab/big.ndjson');
    await ctx.append('ab/big.ndjson', 'tail one\n');
    await ctx.append('ab/big.ndjson', 'tail two\n');
    await p(ctx.manager.flush, 'ab/big.ndjson');

    assert.strictEqual(await ctx.readObject('channel/ab/big.ndjson'),
        'header\ntail one\ntail two\n');
}, { appendThresholdBytes: 0 }));

test('cache: a failed flush leaves the object dirty for the next attempt',
    withCache(async ctx => {
        await ctx.append('ab/log.ndjson', 'one\n');

        // make the backend fail
        const realPut = ctx.backend.put;
        ctx.backend.put = (key, body, opts, cb) => {
            cb(Object.assign(new Error('network down'), { code: 'ECONNRESET' }));
        };

        const err = await failure(p(ctx.manager.flush, 'ab/log.ndjson'));
        assert.ok(err, 'the flush should report the failure');
        assert.strictEqual(ctx.manager.stats().dirtyCount, 1,
            'the object must stay dirty so the data is retried, not dropped');

        // recover: the retry must succeed and lose nothing
        ctx.backend.put = realPut;
        await p(ctx.manager.flush, 'ab/log.ndjson');
        assert.strictEqual(await ctx.readObject('channel/ab/log.ndjson'), 'one\n');
        assert.strictEqual(ctx.manager.stats().dirtyCount, 0);
    }));

// --- the flush scheduler ---------------------------------------------------

test('cache: an idle debounce triggers a flush', withCache(async ctx => {
    await ctx.append('ab/log.ndjson', 'one\n');
    assert.strictEqual(ctx.manager.stats().dirtyCount, 1);

    await new Promise(resolve => setTimeout(resolve, 120));
    assert.strictEqual(await ctx.readObject('channel/ab/log.ndjson'), 'one\n');
    assert.strictEqual(ctx.manager.stats().dirtyCount, 0);
}, { flushDebounceMs: 30 }));

test('cache: continuous writing still flushes at the ceiling', withCache(async ctx => {
    // every append restarts the debounce, so only the ceiling can save us
    const started = Date.now();
    for (let i = 0; i < 10; i++) {
        await ctx.append('ab/busy.ndjson', `line ${i}\n`);
        await new Promise(resolve => setTimeout(resolve, 20));
    }
    const elapsed = Date.now() - started;
    assert.ok(elapsed > 120, 'the writes should outlast the ceiling');

    await new Promise(resolve => setTimeout(resolve, 60));
    const stored = await ctx.readObject('channel/ab/busy.ndjson');
    assert.ok(stored.length > 0,
        'a continuously edited document must still reach the store');
}, { flushDebounceMs: 10000, flushMaxDelayMs: 100 }));

test('cache: a large unflushed tail flushes immediately', withCache(async ctx => {
    await ctx.append('ab/bulk.ndjson', 'x'.repeat(200));
    // no waiting on the debounce: the byte threshold should have fired already
    await new Promise(resolve => setTimeout(resolve, 40));
    assert.strictEqual((await ctx.readObject('channel/ab/bulk.ndjson')).length, 200);
}, { flushDebounceMs: 60000, flushMaxBytes: 100 }));

test('cache: flushAll drains everything outstanding', withCache(async ctx => {
    await ctx.append('ab/one.ndjson', 'one\n');
    await ctx.append('ab/two.ndjson', 'two\n');
    await ctx.append('cd/three.ndjson', 'three\n');
    assert.strictEqual(ctx.manager.stats().dirtyCount, 3);

    const report = await p(ctx.manager.flushAll);
    assert.strictEqual(report.flushed, 3);
    assert.strictEqual(ctx.manager.stats().dirtyCount, 0);
    assert.strictEqual(await ctx.readObject('channel/cd/three.ndjson'), 'three\n');
}));

// --- conflict detection ----------------------------------------------------

test('cache: a second writer is detected, not clobbered', withCache(async ctx => {
    await p(ctx.backend.put, 'channel/ab/owned.ndjson', 'original\n', {});
    await p(ctx.manager.hydrate, 'ab/owned.ndjson');
    await ctx.append('ab/owned.ndjson', 'ours\n');

    // somebody else writes the object behind our back
    await p(ctx.backend.put, 'channel/ab/owned.ndjson', 'theirs\n', {});

    const err = await failure(p(ctx.manager.flush, 'ab/owned.ndjson'));
    assert.ok(err, 'the flush must fail rather than overwrite');
    assert.strictEqual(await ctx.readObject('channel/ab/owned.ndjson'), 'theirs\n',
        "the other writer's data must survive");
}));

test('cache: a conflicting copy is quarantined rather than dropped',
    withCache(async ctx => {
        await p(ctx.backend.put, 'channel/ab/owned.ndjson', 'original\n', {});
        await p(ctx.manager.hydrate, 'ab/owned.ndjson');
        await ctx.append('ab/owned.ndjson', 'ours\n');
        await p(ctx.backend.put, 'channel/ab/owned.ndjson', 'theirs\n', {});

        await failure(p(ctx.manager.flush, 'ab/owned.ndjson'));

        // our version must be preserved for an operator to inspect
        const conflictDir = Path.join(ctx.cache, '.conflict', 'ab');
        const files = await Fs.readdir(conflictDir);
        assert.strictEqual(files.length, 1);
        const preserved = await Fs.readFile(Path.join(conflictDir, files[0]), 'utf8');
        assert.strictEqual(preserved, 'original\nours\n');

        assert.strictEqual(ctx.manager.stats().counters.conflicts, 1);
    }));

test('cache: creating an object that already exists is a conflict',
    withCache(async ctx => {
        // we believe this is a new channel...
        await p(ctx.manager.hydrate, 'ab/new.ndjson');
        await ctx.append('ab/new.ndjson', 'ours\n');
        // ...but another node created it first
        await p(ctx.backend.put, 'channel/ab/new.ndjson', 'theirs\n', {});

        const err = await failure(p(ctx.manager.flush, 'ab/new.ndjson'));
        assert.ok(err);
        assert.strictEqual(await ctx.readObject('channel/ab/new.ndjson'), 'theirs\n');
    }));

// --- crash recovery --------------------------------------------------------

test('recovery: flushes a tail left behind by a crash', withCache(async ctx => {
    await ctx.append('ab/crash.ndjson', 'flushed\n');
    await p(ctx.manager.flush, 'ab/crash.ndjson');

    // simulate a crash: append locally, then throw the in-memory state away
    await Fs.appendFile(Path.join(ctx.cache, 'ab/crash.ndjson'), 'unflushed\n');
    ctx.manager._entries.clear();

    const report = await p(ctx.manager.recover);
    assert.strictEqual(report.flushed, 1);
    assert.strictEqual(await ctx.readObject('channel/ab/crash.ndjson'),
        'flushed\nunflushed\n');
}));

test('recovery: works even when the journal never recorded the write',
    withCache(async ctx => {
        await ctx.append('ab/crash.ndjson', 'flushed\n');
        await p(ctx.manager.flush, 'ab/crash.ndjson');

        // a crash between the append and any state update: the journal still says
        // clean. Sizes, not flags, are what recovery trusts.
        await Fs.appendFile(Path.join(ctx.cache, 'ab/crash.ndjson'), 'unflushed\n');
        const state = await p(Journal.read, Path.join(ctx.cache, 'ab/crash.ndjson'));
        assert.strictEqual(state.dirty, false, 'precondition: journal says clean');
        ctx.manager._entries.clear();

        const report = await p(ctx.manager.recover);
        assert.strictEqual(report.flushed, 1);
        assert.strictEqual(await ctx.readObject('channel/ab/crash.ndjson'),
            'flushed\nunflushed\n');
    }));

test('recovery: recovers an object that was never flushed at all',
    withCache(async ctx => {
        await ctx.append('ab/orphan.ndjson', 'never flushed\n');
        ctx.manager._entries.clear();

        const report = await p(ctx.manager.recover);
        assert.strictEqual(report.flushed, 1);
        assert.strictEqual(await ctx.readObject('channel/ab/orphan.ndjson'),
            'never flushed\n');
    }));

test('recovery: leaves an up-to-date cache alone', withCache(async ctx => {
    await ctx.append('ab/clean.ndjson', 'one\n');
    await p(ctx.manager.flush, 'ab/clean.ndjson');
    ctx.manager._entries.clear();

    const report = await p(ctx.manager.recover);
    assert.strictEqual(report.clean, 1);
    assert.strictEqual(report.flushed, 0);
}));

test('recovery: discards a local copy that is behind the store',
    withCache(async ctx => {
        await ctx.append('ab/stale.ndjson', 'short\n');
        await p(ctx.manager.flush, 'ab/stale.ndjson');

        // the object moved on without us: ownership went elsewhere and came back
        await p(ctx.backend.put, 'channel/ab/stale.ndjson', 'much longer content\n', {});
        ctx.manager._entries.clear();

        const report = await p(ctx.manager.recover);
        assert.strictEqual(report.stale, 1);
        assert.strictEqual(FsSync.existsSync(Path.join(ctx.cache, 'ab/stale.ndjson')), false,
            'a stale copy must be dropped so the next read re-hydrates');
    }));

test('recovery: ignores journal sidecars and conflict copies', withCache(async ctx => {
    await ctx.append('ab/one.ndjson', 'one\n');
    await p(ctx.manager.flush, 'ab/one.ndjson');
    ctx.manager._entries.clear();

    const report = await p(ctx.manager.recover);
    // exactly one object, not the object plus its sidecar
    assert.strictEqual(report.scanned, 1);
}));

// --- eviction --------------------------------------------------------------

test('eviction: never evicts a dirty object', withCache(async ctx => {
    await ctx.append('ab/dirty.ndjson', 'unflushed\n');

    const report = await p(ctx.manager.evict);
    assert.strictEqual(report.evicted, 0);
    assert.ok(FsSync.existsSync(Path.join(ctx.cache, 'ab/dirty.ndjson')),
        'the only copy of unflushed data must never be evicted');
}, { maxBytes: 1 }));

test('eviction: removes clean objects over the size limit', withCache(async ctx => {
    await ctx.append('ab/one.ndjson', 'x'.repeat(500));
    await p(ctx.manager.flush, 'ab/one.ndjson');

    const report = await p(ctx.manager.evict);
    assert.strictEqual(report.evicted, 1);
    assert.strictEqual(FsSync.existsSync(Path.join(ctx.cache, 'ab/one.ndjson')), false);
    // and the object itself is still in the store
    assert.strictEqual((await ctx.readObject('channel/ab/one.ndjson')).length, 500);
}, { maxBytes: 100 }));

test('eviction: respects pinned objects', withCache(async ctx => {
    await ctx.append('ab/pinned.ndjson', 'x'.repeat(500));
    await p(ctx.manager.flush, 'ab/pinned.ndjson');

    const report = await p(ctx.manager.evict);
    assert.strictEqual(report.evicted, 0,
        'a channel with connected editors should not be evicted');
}, { maxBytes: 100, isPinned: () => true }));

test('eviction: an evicted object re-hydrates on demand', withCache(async ctx => {
    await ctx.append('ab/gone.ndjson', 'content\n');
    await p(ctx.manager.flush, 'ab/gone.ndjson');
    await p(ctx.manager.forget, 'ab/gone.ndjson');

    await p(ctx.manager.hydrate, 'ab/gone.ndjson');
    assert.strictEqual(await ctx.readCached('ab/gone.ndjson'), 'content\n');
}));

test('eviction: forget refuses to drop a dirty object', withCache(async ctx => {
    await ctx.append('ab/dirty.ndjson', 'unflushed\n');
    const err = await failure(p(ctx.manager.forget, 'ab/dirty.ndjson'));
    assert.ok(err, 'forget must refuse while there are unflushed bytes');
}));

// --- metrics ---------------------------------------------------------------

test('metrics: reports the dirty set and the exposure window', withCache(async ctx => {
    assert.strictEqual(ctx.manager.stats().dirtyCount, 0);

    await ctx.append('ab/one.ndjson', 'hello\n');
    await ctx.append('ab/two.ndjson', 'world\n');

    const stats = ctx.manager.stats();
    assert.strictEqual(stats.dirtyCount, 2, 'the headline number: unflushed files');
    assert.strictEqual(stats.dirtyBytes, 12);
    assert.ok(stats.oldestDirtyAgeMs >= 0, 'the live exposure window');

    await p(ctx.manager.flushAll);
    assert.strictEqual(ctx.manager.stats().dirtyCount, 0);
    assert.strictEqual(ctx.manager.stats().oldestDirtyAgeMs, 0);
}));

test('metrics: registers pull-based gauges that read live state', async () => {
    const gauges = {};
    const base = await Fs.mkdtemp(Path.join(Os.tmpdir(), 'cryptpad-cache-'));
    try {
        const backend = await p(FsBackend.create, { root: Path.join(base, 'store') });
        const manager = await p(Manager.create, {
            backend,
            cacheRoot: Path.join(base, 'cache'),
            prefix: 'channel/',
            Log: quietLog,
            flushDebounceMs: 60000,
            monitoring: {
                registerGauge: (name, fn) => { gauges[name] = fn; }
            }
        });

        assert.deepStrictEqual(Object.keys(gauges).sort(), [
            's3_cached_objects', 's3_dirty_bytes', 's3_dirty_channels',
            's3_flush_inflight', 's3_oldest_dirty_age_ms'
        ]);

        assert.strictEqual(gauges.s3_dirty_channels(), 0);

        const path = Path.join(base, 'cache', 'ab', 'x.ndjson');
        await Fs.mkdir(Path.dirname(path), { recursive: true });
        await Fs.appendFile(path, 'data\n');
        manager.touch('ab/x.ndjson', 5);

        assert.strictEqual(gauges.s3_dirty_channels(), 1);
        assert.strictEqual(gauges.s3_dirty_bytes(), 5);
        assert.ok(gauges.s3_oldest_dirty_age_ms() >= 0);
    } finally {
        await Fs.rm(base, { recursive: true, force: true });
    }
});

// --- shutdown --------------------------------------------------------------

test('shutdown: close() drains before returning', withCache(async ctx => {
    await ctx.append('ab/one.ndjson', 'one\n');
    await ctx.append('ab/two.ndjson', 'two\n');

    await p(ctx.manager.close);

    // a graceful stop must leave nothing behind
    assert.strictEqual(await ctx.readObject('channel/ab/one.ndjson'), 'one\n');
    assert.strictEqual(await ctx.readObject('channel/ab/two.ndjson'), 'two\n');
    assert.strictEqual(ctx.manager.stats().dirtyCount, 0);
}));
