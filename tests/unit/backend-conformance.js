// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors
//
// SPDX-License-Identifier: AGPL-3.0-or-later

/*  The ObjectBackend conformance suite.

    This spec is written once and run against every backend implementation, so that
    the local filesystem backend and any remote backend cannot drift apart. When the
    S3 plugin lands, its test file should be a handful of lines: build a backend
    pointed at a bucket (MinIO locally, the real provider in a nightly job) and hand
    it to runConformance().

    Anything a backend cannot do is declared in `capabilities`, and the relevant
    tests skip rather than fail — but a backend that claims a capability must
    implement it exactly as specified here.
*/

const { test } = require('node:test');
const assert = require('node:assert');
const { Readable } = require('node:stream');

// promisify a (err, value) callback API for readability in tests
const p = (fn, ...args) => {
    return new Promise((resolve, reject) => {
        fn(...args, (err, value) => {
            if (err) { return reject(err); }
            resolve(value);
        });
    });
};

// resolve with the error instead of rejecting, for negative cases
const failure = async promise => {
    try {
        await promise;
    } catch (err) {
        return err;
    }
    return null;
};

const readStream = stream => {
    return new Promise((resolve, reject) => {
        const chunks = [];
        stream.on('data', chunk => chunks.push(chunk));
        stream.on('error', reject);
        stream.on('end', () => resolve(Buffer.concat(chunks)));
    });
};

/*  runConformance(label, setup)

    `setup` is an async function returning { backend, cleanup }. It is called once
    per test so that tests are independent of each other's leftovers.
*/
const runConformance = (label, setup) => {
    // each test gets a fresh store, and always cleans up even if it throws
    const withBackend = body => {
        return async () => {
            const { backend, cleanup } = await setup();
            try {
                await body(backend);
            } finally {
                if (cleanup) { await cleanup(); }
            }
        };
    };

    test(`${label}: put and get round-trip`, withBackend(async backend => {
        await p(backend.put, 'a/b/hello.txt', Buffer.from('world'), {});
        const body = await p(backend.get, 'a/b/hello.txt');
        assert.strictEqual(body.toString('utf8'), 'world');
    }));

    test(`${label}: put accepts a string body`, withBackend(async backend => {
        await p(backend.put, 'str.txt', 'hello', {});
        const body = await p(backend.get, 'str.txt');
        assert.strictEqual(body.toString('utf8'), 'hello');
    }));

    test(`${label}: put accepts a stream body`, withBackend(async backend => {
        await p(backend.put, 'stream.txt', Readable.from([
            Buffer.from('one '), Buffer.from('two')
        ]), {});
        const body = await p(backend.get, 'stream.txt');
        assert.strictEqual(body.toString('utf8'), 'one two');
    }));

    test(`${label}: put overwrites by default`, withBackend(async backend => {
        await p(backend.put, 'over.txt', 'first', {});
        await p(backend.put, 'over.txt', 'second', {});
        const body = await p(backend.get, 'over.txt');
        assert.strictEqual(body.toString('utf8'), 'second');
    }));

    test(`${label}: head reports size and a stable etag`, withBackend(async backend => {
        await p(backend.put, 'sized.bin', Buffer.alloc(1234, 7), {});
        const stat = await p(backend.head, 'sized.bin');
        assert.strictEqual(stat.size, 1234);
        assert.ok(stat.etag, 'etag should be set');
        assert.strictEqual(typeof stat.mtime, 'number');

        // the etag must be stable while the object is unchanged
        const again = await p(backend.head, 'sized.bin');
        assert.strictEqual(again.etag, stat.etag);
    }));

    test(`${label}: etag changes when content changes`, withBackend(async backend => {
        await p(backend.put, 'mutate.txt', 'aaa', {});
        const before = await p(backend.head, 'mutate.txt');
        await p(backend.put, 'mutate.txt', 'aaaa', {});
        const after = await p(backend.head, 'mutate.txt');
        assert.notStrictEqual(after.etag, before.etag);
    }));

    test(`${label}: missing keys report ENOENT`, withBackend(async backend => {
        const headErr = await failure(p(backend.head, 'nope/missing.txt'));
        assert.strictEqual(headErr && headErr.code, 'ENOENT');

        const getErr = await failure(p(backend.get, 'nope/missing.txt'));
        assert.strictEqual(getErr && getErr.code, 'ENOENT');

        const streamErr = await failure(p(backend.getStream, 'nope/missing.txt', {}));
        assert.strictEqual(streamErr && streamErr.code, 'ENOENT');
    }));

    test(`${label}: exists distinguishes present from absent`, withBackend(async backend => {
        await p(backend.put, 'here.txt', 'x', {});
        assert.strictEqual(await p(backend.exists, 'here.txt'), true);
        assert.strictEqual(await p(backend.exists, 'not-here.txt'), false);
    }));

    test(`${label}: getStream honours start and end`, withBackend(async backend => {
        await p(backend.put, 'range.txt', '0123456789', {});

        const tail = await readStream(await p(backend.getStream, 'range.txt', { start: 4 }));
        assert.strictEqual(tail.toString('utf8'), '456789');

        // 'end' is inclusive, matching fs.createReadStream and HTTP Range
        const mid = await readStream(await p(backend.getStream, 'range.txt', { start: 2, end: 4 }));
        assert.strictEqual(mid.toString('utf8'), '234');
    }));

    test(`${label}: invalid keys are rejected`, withBackend(async backend => {
        for (const key of ['../escape', 'a/../../escape', '/absolute', '']) {
            const err = await failure(p(backend.put, key, 'x', {}));
            assert.ok(err, `expected ${JSON.stringify(key)} to be rejected`);
            assert.strictEqual(err.code, 'EINVAL', `expected EINVAL for ${JSON.stringify(key)}`);
        }
    }));

    test(`${label}: append creates then extends an object`, withBackend(async backend => {
        await p(backend.append, 'log.ndjson', Buffer.from('one\n'), {});
        await p(backend.append, 'log.ndjson', Buffer.from('two\n'), {});
        const body = await p(backend.get, 'log.ndjson');
        assert.strictEqual(body.toString('utf8'), 'one\ntwo\n');
    }));

    test(`${label}: append reports the resulting size`, withBackend(async backend => {
        const stat = await p(backend.append, 'log2.ndjson', Buffer.from('12345'), {});
        assert.strictEqual(stat.size, 5);
        const stat2 = await p(backend.append, 'log2.ndjson', Buffer.from('678'), {});
        assert.strictEqual(stat2.size, 8);
    }));

    test(`${label}: append honours expectedSize`, withBackend(async backend => {
        await p(backend.append, 'guard.ndjson', Buffer.from('12345'), {});

        const err = await failure(p(backend.append, 'guard.ndjson', Buffer.from('x'), {
            expectedSize: 99
        }));
        assert.strictEqual(err && err.code, 'EPRECONDITION');

        // the failed append must not have modified the object
        const stat = await p(backend.head, 'guard.ndjson');
        assert.strictEqual(stat.size, 5);

        // and the correct expectation still succeeds
        const ok = await p(backend.append, 'guard.ndjson', Buffer.from('x'), {
            expectedSize: 5
        });
        assert.strictEqual(ok.size, 6);
    }));

    test(`${label}: copy duplicates without removing the source`, withBackend(async backend => {
        await p(backend.put, 'src.txt', 'payload', {});
        await p(backend.copy, 'src.txt', 'dst/copied.txt', {});

        assert.strictEqual((await p(backend.get, 'dst/copied.txt')).toString('utf8'), 'payload');
        assert.strictEqual(await p(backend.exists, 'src.txt'), true);
    }));

    test(`${label}: move relocates and removes the source`, withBackend(async backend => {
        await p(backend.put, 'moving.txt', 'payload', {});
        await p(backend.move, 'moving.txt', 'archive/moved.txt', {});

        assert.strictEqual((await p(backend.get, 'archive/moved.txt')).toString('utf8'), 'payload');
        assert.strictEqual(await p(backend.exists, 'moving.txt'), false);
    }));

    test(`${label}: copy and move refuse to clobber unless told to`, withBackend(async backend => {
        await p(backend.put, 'one.txt', 'one', {});
        await p(backend.put, 'two.txt', 'two', {});

        const copyErr = await failure(p(backend.copy, 'one.txt', 'two.txt', {}));
        assert.strictEqual(copyErr && copyErr.code, 'EEXIST');
        assert.strictEqual((await p(backend.get, 'two.txt')).toString('utf8'), 'two');

        const moveErr = await failure(p(backend.move, 'one.txt', 'two.txt', {}));
        assert.strictEqual(moveErr && moveErr.code, 'EEXIST');
        assert.strictEqual((await p(backend.get, 'two.txt')).toString('utf8'), 'two');

        // ...and do overwrite when explicitly asked
        await p(backend.copy, 'one.txt', 'two.txt', { overwrite: true });
        assert.strictEqual((await p(backend.get, 'two.txt')).toString('utf8'), 'one');
    }));

    test(`${label}: remove deletes an object`, withBackend(async backend => {
        await p(backend.put, 'doomed.txt', 'x', {});
        await p(backend.remove, 'doomed.txt');
        assert.strictEqual(await p(backend.exists, 'doomed.txt'), false);
    }));

    test(`${label}: remove of a missing key reports ENOENT`, withBackend(async backend => {
        const err = await failure(p(backend.remove, 'never-existed.txt'));
        assert.strictEqual(err && err.code, 'ENOENT');
    }));

    test(`${label}: removePrefix deletes a subtree`, withBackend(async backend => {
        await p(backend.put, 'tree/a/1.txt', '1', {});
        await p(backend.put, 'tree/a/2.txt', '2', {});
        await p(backend.put, 'tree/b/3.txt', '3', {});
        await p(backend.put, 'other/4.txt', '4', {});

        await p(backend.removePrefix, 'tree/');

        assert.strictEqual(await p(backend.exists, 'tree/a/1.txt'), false);
        assert.strictEqual(await p(backend.exists, 'tree/b/3.txt'), false);
        assert.strictEqual(await p(backend.exists, 'other/4.txt'), true);
    }));

    test(`${label}: removePrefix refuses an empty prefix`, withBackend(async backend => {
        await p(backend.put, 'precious.txt', 'x', {});
        const err = await failure(p(backend.removePrefix, ''));
        assert.ok(err, 'an empty prefix must not wipe the store');
        assert.strictEqual(await p(backend.exists, 'precious.txt'), true);
    }));

    test(`${label}: list returns keys under a prefix, recursively`, withBackend(async backend => {
        await p(backend.put, 'ch/aa/one.ndjson', '1', {});
        await p(backend.put, 'ch/ab/two.ndjson', '22', {});
        await p(backend.put, 'other/three.ndjson', '333', {});

        const result = await p(backend.list, 'ch/', {});
        const keys = result.keys.map(k => k.key).sort();
        assert.deepStrictEqual(keys, ['ch/aa/one.ndjson', 'ch/ab/two.ndjson']);

        // listings carry sizes, so callers never need a stat per entry
        const one = result.keys.find(k => k.key === 'ch/aa/one.ndjson');
        assert.strictEqual(one.size, 1);
    }));

    test(`${label}: list treats the prefix as a string, not a directory`, withBackend(async backend => {
        await p(backend.put, 'ch/aa/alpha.ndjson', 'a', {});
        await p(backend.put, 'ch/aa/beta.ndjson', 'b', {});

        const result = await p(backend.list, 'ch/aa/al', {});
        const keys = result.keys.map(k => k.key);
        assert.deepStrictEqual(keys, ['ch/aa/alpha.ndjson']);
    }));

    test(`${label}: list with a delimiter reports common prefixes`, withBackend(async backend => {
        await p(backend.put, 'ch/aa/one.ndjson', '1', {});
        await p(backend.put, 'ch/ab/two.ndjson', '2', {});
        await p(backend.put, 'ch/top.ndjson', '3', {});

        const result = await p(backend.list, 'ch/', { delimiter: '/' });
        assert.deepStrictEqual(result.keys.map(k => k.key), ['ch/top.ndjson']);
        assert.deepStrictEqual(result.prefixes.sort(), ['ch/aa/', 'ch/ab/']);
    }));

    test(`${label}: list of an absent prefix is empty, not an error`, withBackend(async backend => {
        const result = await p(backend.list, 'nothing/here/', {});
        assert.deepStrictEqual(result.keys, []);
    }));

    test(`${label}: list paginates with limit and cursor`, withBackend(async backend => {
        const names = ['a', 'b', 'c', 'd', 'e'];
        for (const name of names) {
            await p(backend.put, `page/${name}.txt`, name, {});
        }

        const seen = [];
        let cursor;
        // deliberately loop more times than needed to prove it terminates
        for (let i = 0; i < 10; i++) {
            const result = await p(backend.list, 'page/', { limit: 2, cursor });
            result.keys.forEach(k => seen.push(k.key));
            cursor = result.cursor;
            if (!cursor) { break; }
        }

        assert.deepStrictEqual(seen, names.map(n => `page/${n}.txt`));
    }));

    test(`${label}: conditional create fails when the key exists`, withBackend(async backend => {
        if (!backend.capabilities.conditionalPut) { return; }

        await p(backend.put, 'once.txt', 'first', { ifNoneMatch: true });

        const err = await failure(p(backend.put, 'once.txt', 'second', { ifNoneMatch: true }));
        assert.strictEqual(err && err.code, 'EEXIST');

        // the original content must survive the rejected write
        assert.strictEqual((await p(backend.get, 'once.txt')).toString('utf8'), 'first');
    }));

    test(`${label}: conditional update requires a matching etag`, withBackend(async backend => {
        if (!backend.capabilities.conditionalPut) { return; }

        await p(backend.put, 'cas.txt', 'v1', {});
        const stat = await p(backend.head, 'cas.txt');

        const err = await failure(p(backend.put, 'cas.txt', 'v2', { ifMatch: '"bogus"' }));
        assert.strictEqual(err && err.code, 'EPRECONDITION');
        assert.strictEqual((await p(backend.get, 'cas.txt')).toString('utf8'), 'v1');

        await p(backend.put, 'cas.txt', 'v2', { ifMatch: stat.etag });
        assert.strictEqual((await p(backend.get, 'cas.txt')).toString('utf8'), 'v2');
    }));

    test(`${label}: declares its capabilities`, withBackend(async backend => {
        const caps = backend.capabilities;
        assert.ok(caps, 'capabilities must be present');
        for (const name of ['conditionalPut', 'serverSideAppend', 'presign', 'atomicMove']) {
            assert.strictEqual(typeof caps[name], 'boolean', `${name} must be a boolean`);
        }
        assert.strictEqual(typeof backend.name, 'string');

        // a backend claiming presign must actually provide the method
        if (caps.presign) {
            assert.strictEqual(typeof backend.presignGet, 'function');
        }
    }));
};

module.exports = { runConformance, p, failure, readStream };
