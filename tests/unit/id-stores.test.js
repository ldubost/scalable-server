// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors
//
// SPDX-License-Identifier: AGPL-3.0-or-later

/*  The id-keyed stores built on common/storage/basic.js: users, invitations and
    moderators. Moderator.getAllKeys in particular was synchronous (fs.readdirSync)
    and is now async, so its behaviour is pinned here.
*/

const { test } = require('node:test');
const assert = require('node:assert');
const Fs = require('node:fs/promises');
const Os = require('node:os');
const Path = require('node:path');

const User = require('../../storage/storage/user.js');
const Invite = require('../../storage/storage/invite.js');
const Moderator = require('../../storage/moderator.js');
const { p, failure } = require('./backend-conformance.js');

const withEnv = body => {
    return async () => {
        const base = await Fs.mkdtemp(Path.join(Os.tmpdir(), 'cryptpad-stores-'));
        const Env = { paths: { base } };
        try {
            await body(Env, base);
        } finally {
            await Fs.rm(base, { recursive: true, force: true });
        }
    };
};

// --- users -----------------------------------------------------------------

test('user: write, read and delete', withEnv(async Env => {
    await p(User.write, Env, 'abcdef', { alias: 'someone', blockId: 'block1' });

    const data = await p(User.read, Env, 'abcdef');
    assert.deepStrictEqual(data, { alias: 'someone', blockId: 'block1' });

    await p(User.delete, Env, 'abcdef');
    const err = await failure(p(User.read, Env, 'abcdef'));
    assert.strictEqual(err, 'ENOENT', 'User.read calls back with the error code');
}));

test('user: update replaces existing data', withEnv(async Env => {
    await p(User.write, Env, 'abcdef', { alias: 'before' });
    await p(User.update, Env, 'abcdef', { alias: 'after' });
    assert.deepStrictEqual(await p(User.read, Env, 'abcdef'), { alias: 'after' });
}));

test('user: getAll collects every user across prefixes', withEnv(async Env => {
    await p(User.write, Env, 'aaaaaa', { alias: 'one' });
    await p(User.write, Env, 'bbbbbb', { alias: 'two' });
    await p(User.write, Env, 'aabbcc', { alias: 'three' });

    const users = await p(User.getAll, Env);
    assert.deepStrictEqual(Object.keys(users).sort(), ['aaaaaa', 'aabbcc', 'bbbbbb']);
    assert.strictEqual(users.aaaaaa.alias, 'one');
}));

test('user: getAll on an empty store returns an empty map', withEnv(async Env => {
    assert.deepStrictEqual(await p(User.getAll, Env), {});
}));

// --- invitations -----------------------------------------------------------

test('invite: write, read, delete', withEnv(async Env => {
    await p(Invite.write, Env, 'inviteid', { alias: 'guest' });
    assert.deepStrictEqual(await p(Invite.read, Env, 'inviteid'), { alias: 'guest' });

    await p(Invite.delete, Env, 'inviteid');
    assert.ok(await failure(p(Invite.read, Env, 'inviteid')));
}));

test('invite: getAll on an empty store returns an empty map', withEnv(async Env => {
    assert.deepStrictEqual(await p(Invite.getAll, Env), {});
}));

// --- moderators ------------------------------------------------------------

test('moderator: getAllKeys is empty before any moderator exists', withEnv(async Env => {
    // this used to be a synchronous readdirSync wrapped in try/catch;
    // an absent support directory must still yield an empty list
    assert.deepStrictEqual(await p(Moderator.getAllKeys, Env), []);
}));

test('moderator: getAllKeys collects keys across prefixes', withEnv(async Env => {
    await p(Moderator.write, Env, 'aaaaaa', { name: 'one' });
    await p(Moderator.write, Env, 'bbbbbb', { name: 'two' });
    await p(Moderator.write, Env, 'aabbcc', { name: 'three' });

    const keys = await p(Moderator.getAllKeys, Env);
    assert.deepStrictEqual(keys.sort(), ['aaaaaa', 'aabbcc', 'bbbbbb']);
}));

test('moderator: getAllKeys reflects deletions', withEnv(async Env => {
    await p(Moderator.write, Env, 'aaaaaa', { name: 'one' });
    await p(Moderator.write, Env, 'bbbbbb', { name: 'two' });
    await p(Moderator.delete, Env, 'aaaaaa');

    assert.deepStrictEqual(await p(Moderator.getAllKeys, Env), ['bbbbbb']);
}));

test('moderator: getAll returns parsed records', withEnv(async Env => {
    await p(Moderator.write, Env, 'aaaaaa', { name: 'one' });
    const all = await p(Moderator.getAll, Env);
    assert.deepStrictEqual(all.aaaaaa, { name: 'one' });
}));
