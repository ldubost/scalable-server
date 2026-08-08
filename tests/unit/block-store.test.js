// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors
//
// SPDX-License-Identifier: AGPL-3.0-or-later

/*  Login blocks, now routed through the storage backend.

    A block is what lets a registered user log in, so the behaviour that matters
    most is the password-change path: the previous block must be archived (and
    restorable) and the new one must land, even when something went wrong with the
    archival.
*/

const { test } = require('node:test');
const assert = require('node:assert');
const Fs = require('node:fs/promises');
const Os = require('node:os');
const Path = require('node:path');

const BlockStore = require('../../storage/storage/block.js');
const { p, failure } = require('./backend-conformance.js');

// Block.check takes (Env, key, cb, noRedirect): the callback is not last,
// so it cannot go through the generic promise helper
const check = (Env, key) => new Promise((resolve, reject) => {
    BlockStore.check(Env, key, err => err ? reject(err) : resolve(), true);
});

// placeholders are written fire-and-forget, as they were before this store
// moved onto the backend; poll briefly rather than racing it
const placeholder = async (Env, key) => {
    for (let i = 0; i < 50; i++) {
        const reason = await new Promise(resolve => {
            BlockStore.readPlaceholder(Env, key, resolve);
        });
        if (reason) { return reason; }
        await new Promise(resolve => setTimeout(resolve, 10));
    }
    return undefined;
};

const KEY = 'a'.repeat(44);
const OTHER = 'b'.repeat(44);

const withEnv = body => {
    return async () => {
        const base = await Fs.mkdtemp(Path.join(Os.tmpdir(), 'cryptpad-block-'));
        const Env = {
            paths: {
                base,
                block: Path.join(base, 'block'),
                archive: Path.join(base, 'archive')
            }
        };
        try {
            await body(Env, base);
        } finally {
            await Fs.rm(base, { recursive: true, force: true });
        }
    };
};

test('block: write then check', withEnv(async Env => {
    await p(BlockStore.write, Env, KEY, Buffer.from('secret'));
    // check calls back with no error when the block is present
    await check(Env, KEY);
    assert.strictEqual(await p(BlockStore.isAvailable, Env, KEY), true);
}));

test('block: check reports an error for a missing block', withEnv(async Env => {
    const err = await failure(check(Env, KEY));
    assert.ok(err, 'callers rely on an error, not a false');
    assert.strictEqual(err.code, 'ENOENT');
}));

test('block: check rejects a malformed key', withEnv(async Env => {
    const err = await failure(p(BlockStore.isAvailable, Env, 'too-short'));
    assert.strictEqual(err, 'INVALID_ARGS');
}));

test('block: a password change archives the old block', withEnv(async Env => {
    await p(BlockStore.write, Env, KEY, Buffer.from('first'));
    await p(BlockStore.write, Env, KEY, Buffer.from('second'));

    assert.strictEqual(await p(BlockStore.isAvailable, Env, KEY), true);
    assert.strictEqual(await p(BlockStore.isArchived, Env, KEY), true);
}));

test('block: repeated password changes keep working', withEnv(async Env => {
    // the archive destination already exists from the second change onwards,
    // so this fails if archival refuses to overwrite
    for (const value of ['first', 'second', 'third', 'fourth']) {
        await p(BlockStore.write, Env, KEY, Buffer.from(value));
        assert.strictEqual(await p(BlockStore.isAvailable, Env, KEY), true);
    }
}));

test('block: archive then restore round-trips', withEnv(async Env => {
    await p(BlockStore.write, Env, KEY, Buffer.from('secret'));
    await p(BlockStore.archive, Env, KEY, 'ACCOUNT_DELETION');

    assert.strictEqual(await p(BlockStore.isAvailable, Env, KEY), false);
    assert.strictEqual(await p(BlockStore.isArchived, Env, KEY), true);

    await p(BlockStore.restore, Env, KEY);
    assert.strictEqual(await p(BlockStore.isAvailable, Env, KEY), true);
    assert.strictEqual(await p(BlockStore.isArchived, Env, KEY), false);
}));

test('block: archival records a reason', withEnv(async Env => {
    await p(BlockStore.write, Env, KEY, Buffer.from('secret'));
    await p(BlockStore.archive, Env, KEY, 'ACCOUNT_DELETION');

    assert.strictEqual(await placeholder(Env, KEY), 'ACCOUNT_DELETION');
}));

test('block: restoring clears the placeholder', withEnv(async Env => {
    await p(BlockStore.write, Env, KEY, Buffer.from('secret'));
    await p(BlockStore.archive, Env, KEY, 'ACCOUNT_DELETION');
    // let the fire-and-forget placeholder land before restoring
    await placeholder(Env, KEY);
    await p(BlockStore.restore, Env, KEY);

    const reason = await new Promise(resolve => {
        setTimeout(() => { BlockStore.readPlaceholder(Env, KEY, resolve); }, 30);
    });
    assert.strictEqual(reason, undefined,
        'a restored account should not still look deleted');
}));

test('block: accounts are independent', withEnv(async Env => {
    await p(BlockStore.write, Env, KEY, Buffer.from('one'));
    await p(BlockStore.write, Env, OTHER, Buffer.from('two'));

    await p(BlockStore.archive, Env, KEY, 'ACCOUNT_DELETION');

    assert.strictEqual(await p(BlockStore.isAvailable, Env, KEY), false);
    assert.strictEqual(await p(BlockStore.isAvailable, Env, OTHER), true);
}));

test('block: keys map onto the expected layout', withEnv(async (Env, base) => {
    await p(BlockStore.write, Env, KEY, Buffer.from('secret'));
    // the on-disk (and in-bucket) layout must stay block/<xx>/<key>
    const path = Path.join(base, 'block', KEY.slice(0, 2), KEY);
    assert.strictEqual((await Fs.readFile(path)).toString('utf8'), 'secret');
}));
