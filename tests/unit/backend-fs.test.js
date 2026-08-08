// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors
//
// SPDX-License-Identifier: AGPL-3.0-or-later

const { test } = require('node:test');
const assert = require('node:assert');
const Fs = require('node:fs/promises');
const Os = require('node:os');
const Path = require('node:path');

const FsBackend = require('../../common/storage/backend/fs.js');
const Backends = require('../../common/storage/backend/index.js');
const { runConformance, p, failure } = require('./backend-conformance.js');

const mkTempRoot = async () => {
    return await Fs.mkdtemp(Path.join(Os.tmpdir(), 'cryptpad-backend-'));
};

runConformance('fs', async () => {
    const root = await mkTempRoot();
    const backend = await p(FsBackend.create, { root });
    return {
        backend,
        cleanup: () => Fs.rm(root, { recursive: true, force: true })
    };
});

// --- behaviour specific to the filesystem backend --------------------------

test('fs: creates its root directory if absent', async () => {
    const parent = await mkTempRoot();
    const root = Path.join(parent, 'does', 'not', 'exist', 'yet');
    try {
        await p(FsBackend.create, { root });
        const stat = await Fs.stat(root);
        assert.ok(stat.isDirectory());
    } finally {
        await Fs.rm(parent, { recursive: true, force: true });
    }
});

test('fs: keys map onto paths verbatim', async () => {
    const root = await mkTempRoot();
    try {
        const backend = await p(FsBackend.create, { root });
        await p(backend.put, 'channel/ab/abcdef.ndjson', 'payload', {});

        // the on-disk layout must match the key, so that an instance can be
        // migrated between fs and S3 without rewriting paths
        const onDisk = await Fs.readFile(Path.join(root, 'channel/ab/abcdef.ndjson'), 'utf8');
        assert.strictEqual(onDisk, 'payload');
    } finally {
        await Fs.rm(root, { recursive: true, force: true });
    }
});

test('fs: a failed write leaves no temp files behind', async () => {
    const root = await mkTempRoot();
    try {
        const backend = await p(FsBackend.create, { root });
        await p(backend.put, 'exclusive.txt', 'first', { ifNoneMatch: true });

        const err = await failure(p(backend.put, 'exclusive.txt', 'second', { ifNoneMatch: true }));
        assert.strictEqual(err.code, 'EEXIST');

        const entries = await Fs.readdir(root);
        assert.deepStrictEqual(entries, ['exclusive.txt'],
            'the rejected write should not leave a temp file');
    } finally {
        await Fs.rm(root, { recursive: true, force: true });
    }
});

test('fs: readers never observe a partially written object', async () => {
    const root = await mkTempRoot();
    try {
        const backend = await p(FsBackend.create, { root });
        const big = Buffer.alloc(2 * 1024 * 1024, 'a');
        const bigger = Buffer.alloc(2 * 1024 * 1024, 'b');

        await p(backend.put, 'atomic.bin', big, {});

        // overwrite while repeatedly reading; every read must see one whole version
        const reads = [];
        const writing = p(backend.put, 'atomic.bin', bigger, {});
        for (let i = 0; i < 20; i++) {
            reads.push(p(backend.get, 'atomic.bin').then(body => {
                assert.strictEqual(body.length, big.length, 'read a truncated object');
                const first = body[0];
                assert.ok(body.every(byte => byte === first), 'read a mixed object');
            }));
        }
        await writing;
        await Promise.all(reads);
    } finally {
        await Fs.rm(root, { recursive: true, force: true });
    }
});

// --- the registry ----------------------------------------------------------

test('registry: defaults to the fs backend', async () => {
    const root = await mkTempRoot();
    try {
        const backend = await p(Backends.create, {}, { root });
        assert.strictEqual(backend.name, 'fs');
    } finally {
        await Fs.rm(root, { recursive: true, force: true });
    }
});

test('registry: selects fs explicitly', async () => {
    const root = await mkTempRoot();
    try {
        const backend = await p(Backends.create, { type: 'fs' }, { root });
        assert.strictEqual(backend.name, 'fs');
    } finally {
        await Fs.rm(root, { recursive: true, force: true });
    }
});

test('registry: an unknown backend fails loudly rather than falling back', async () => {
    const err = await failure(p(Backends.create, { type: 'nonsense' }, { root: '/tmp' }));
    assert.ok(err, 'requesting an unavailable backend must be an error');
    assert.strictEqual(err.code, 'E_UNKNOWN_STORAGE_BACKEND');
    // the message should tell an operator what to do about it
    assert.match(err.message, /config\.storage\.type/);
});

test('registry: s3 resolves and validates its own config', async () => {
    // proves the lazy require resolves without reaching the network:
    // a missing bucket is rejected before any client is used
    const err = await failure(p(Backends.create, { type: 's3', s3: {} }, {}));
    assert.ok(err);
    assert.match(err.message, /bucket|@aws-sdk/);
});

test('registry: config section is passed to the backend', async () => {
    const root = await mkTempRoot();
    try {
        const backend = await p(Backends.create, {
            type: 'fs',
            fs: { root }
        }, { root: '/should/be/overridden' });
        assert.strictEqual(backend.root, Path.resolve(root));
    } finally {
        await Fs.rm(root, { recursive: true, force: true });
    }
});

test('registry: lists what is available', () => {
    const names = Backends.listAvailable();
    assert.ok(names.includes('fs'));
});
