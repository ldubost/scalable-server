// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors
//
// SPDX-License-Identifier: AGPL-3.0-or-later

/*  common/storage/basic.js used to call fs directly; it now delegates to an
    ObjectBackend. These tests pin the behaviour its callers (users, invitations,
    sessions, mfa, support, challenges) actually depend on, so that routing it
    through the backend — and later through S3 — cannot change it silently.
*/

const { test } = require('node:test');
const assert = require('node:assert');
const Fs = require('node:fs/promises');
const Os = require('node:os');
const Path = require('node:path');

const Basic = require('../../common/storage/basic.js');
const FsBackend = require('../../common/storage/backend/fs.js');
const { p, failure } = require('./backend-conformance.js');

// build an Env shaped like the real one: paths.base is the storage root
const mkEnv = async (extra) => {
    const base = await Fs.mkdtemp(Path.join(Os.tmpdir(), 'cryptpad-basic-'));
    const Env = Object.assign({ paths: { base } }, extra || {});
    return {
        Env,
        base,
        cleanup: () => Fs.rm(base, { recursive: true, force: true })
    };
};

const withEnv = (body, extra) => {
    return async () => {
        const { Env, base, cleanup } = await mkEnv(extra);
        try {
            await body(Env, base);
        } finally {
            await cleanup();
        }
    };
};

test('basic: write then read round-trips a string', withEnv(async (Env, base) => {
    const path = Path.join(base, 'users', 'ab', 'abcdef');
    await p(Basic.write, Env, path, JSON.stringify({ hello: 'world' }));

    const data = await p(Basic.read, Env, path);
    assert.strictEqual(typeof data, 'string', 'read must call back with a string');
    assert.deepStrictEqual(JSON.parse(data), { hello: 'world' });
}));

test('basic: write creates intermediate directories', withEnv(async (Env, base) => {
    const path = Path.join(base, 'sessions', 'ab', 'abcdef', 'token');
    await p(Basic.write, Env, path, 'payload');
    assert.strictEqual(await p(Basic.read, Env, path), 'payload');
}));

test('basic: write refuses to overwrite (wx semantics)', withEnv(async (Env, base) => {
    const path = Path.join(base, 'mfa', 'ab', 'abcdef.json');
    await p(Basic.write, Env, path, 'first');

    const err = await failure(p(Basic.write, Env, path, 'second'));
    assert.ok(err, 'a second write to the same path must fail');
    // callers do `cb(err.code)`, so the code is the part that matters
    assert.strictEqual(err.code, 'EEXIST');
    assert.strictEqual(await p(Basic.read, Env, path), 'first');
}));

test('basic: reading a missing key reports ENOENT', withEnv(async (Env, base) => {
    const err = await failure(p(Basic.read, Env, Path.join(base, 'users', 'zz', 'nope')));
    assert.strictEqual(err && err.code, 'ENOENT');
}));

test('basic: delete removes a key', withEnv(async (Env, base) => {
    const path = Path.join(base, 'users', 'ab', 'abcdef');
    await p(Basic.write, Env, path, 'x');
    await p(Basic.delete, Env, path);
    const err = await failure(p(Basic.read, Env, path));
    assert.strictEqual(err && err.code, 'ENOENT');
}));

test('basic: delete of a missing key reports ENOENT', withEnv(async (Env, base) => {
    const err = await failure(p(Basic.delete, Env, Path.join(base, 'users', 'ab', 'gone')));
    assert.strictEqual(err && err.code, 'ENOENT');
}));

test('basic: readDir lists directories then files, by name', withEnv(async (Env, base) => {
    await p(Basic.write, Env, Path.join(base, 'users', 'ab', 'abcdef'), '1');
    await p(Basic.write, Env, Path.join(base, 'users', 'cd', 'cdefgh'), '2');

    // first level: the two-character prefixes, as directory names
    const prefixes = await p(Basic.readDir, Env, Path.join(base, 'users'));
    assert.deepStrictEqual(prefixes.sort(), ['ab', 'cd']);

    // second level: the ids themselves
    const ids = await p(Basic.readDir, Env, Path.join(base, 'users', 'ab'));
    assert.deepStrictEqual(ids, ['abcdef']);
}));

test('basic: readDir of a missing directory is empty rather than an error',
    withEnv(async (Env, base) => {
        // fs.readdir used to report ENOENT here; object stores have no directories
        // to be missing. Every caller treats the two cases identically.
        const names = await p(Basic.readDir, Env, Path.join(base, 'users'));
        assert.deepStrictEqual(names, []);
    }));

test('basic: deleteDir removes a subtree and is quiet about absent ones',
    withEnv(async (Env, base) => {
        const dir = Path.join(base, 'sessions', 'ab', 'abcdef');
        await p(Basic.write, Env, Path.join(dir, 'one'), '1');
        await p(Basic.write, Env, Path.join(dir, 'two'), '2');

        await p(Basic.deleteDir, Env, dir);
        assert.deepStrictEqual(await p(Basic.readDir, Env, dir), []);

        // deleting again must not throw: the old implementation used force: true
        await p(Basic.deleteDir, Env, dir);
    }));

test('basic: archive moves a key aside, restore brings it back',
    withEnv(async (Env, base) => {
        const path = Path.join(base, 'block', 'ab', 'abcdef');
        const archivePath = Path.join(base, 'archive', 'block', 'ab', 'abcdef');

        await p(Basic.write, Env, path, 'payload');
        await p(Basic.archive, Env, path, archivePath);

        assert.strictEqual((await failure(p(Basic.read, Env, path))).code, 'ENOENT');
        assert.strictEqual(await p(Basic.read, Env, archivePath), 'payload');

        await p(Basic.restore, Env, archivePath, path);
        assert.strictEqual(await p(Basic.read, Env, path), 'payload');
    }));

test('basic: archive overwrites an older archived copy', withEnv(async (Env, base) => {
    const path = Path.join(base, 'block', 'ab', 'abcdef');
    const archivePath = Path.join(base, 'archive', 'block', 'ab', 'abcdef');

    await p(Basic.write, Env, archivePath, 'older');
    await p(Basic.write, Env, path, 'newer');

    // block writes archive the previous block on every password change,
    // so this has to succeed rather than fail with EEXIST
    await p(Basic.archive, Env, path, archivePath);
    assert.strictEqual(await p(Basic.read, Env, archivePath), 'newer');
}));

test('basic: restore does not clobber a live key', withEnv(async (Env, base) => {
    const path = Path.join(base, 'block', 'ab', 'abcdef');
    const archivePath = Path.join(base, 'archive', 'block', 'ab', 'abcdef');

    await p(Basic.write, Env, path, 'live');
    await p(Basic.write, Env, archivePath, 'archived');

    const err = await failure(p(Basic.restore, Env, archivePath, path));
    assert.ok(err, 'restoring over a live key must fail');
    assert.strictEqual(await p(Basic.read, Env, path), 'live');
}));

test('basic: a falsy path reports INVALID_PATH', withEnv(async (Env) => {
    // pathFromId returns undefined for ids that fail validation, and every caller
    // passes that straight through
    const err = await failure(p(Basic.read, Env, undefined));
    assert.strictEqual(err && err.code, 'INVALID_PATH');
}));

test('basic: uses an installed backend instead of building its own',
    withEnv(async (Env, base) => {
        const calls = [];
        const real = FsBackend.create({ root: base });
        Env.storageBackend = Object.assign({}, real, {
            get: (key, cb) => { calls.push(key); real.get(key, cb); }
        });

        await p(Basic.write, Env, Path.join(base, 'users', 'ab', 'abcdef'), 'x');
        await p(Basic.read, Env, Path.join(base, 'users', 'ab', 'abcdef'));

        // paths must resolve to keys relative to the backend root
        assert.deepStrictEqual(calls, ['users/ab/abcdef']);
    }));

test('basic: a path outside the storage root is a configuration error',
    withEnv(async (Env) => {
        const err = await failure(p(Basic.read, Env, '/etc/passwd'));
        assert.strictEqual(err && err.code, 'E_PATH_OUTSIDE_ROOT');
        // the message should name the offending path and the root
        assert.match(err.message, /outside the storage root/);
    }));

test('basic: refuses to fall back to disk when configured for remote storage',
    withEnv(async (Env, base) => {
        // no backend installed, but config says s3: writing to the local disk here
        // would look healthy while stranding data on one node
        const err = await failure(p(Basic.read, Env, Path.join(base, 'users', 'ab', 'x')));
        assert.strictEqual(err && err.code, 'E_NO_STORAGE_BACKEND');
    }, { config: { storage: { type: 's3' } } }));

test('basic: isValidId accepts ids and rejects path characters', () => {
    assert.ok(Basic.isValidId('abcDEF123-_+='));
    assert.ok(!Basic.isValidId('../escape'));
    assert.ok(!Basic.isValidId('with/slash'));
    assert.ok(!Basic.isValidId(''));
    assert.ok(!Basic.isValidId(undefined));
});
