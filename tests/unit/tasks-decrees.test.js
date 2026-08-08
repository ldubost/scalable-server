// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors
//
// SPDX-License-Identifier: AGPL-3.0-or-later

/*  Scheduled tasks and admin decrees, now routed through the storage backend.

    Neither is large or busy, but losing either is expensive in a quiet way: a lost
    task means a pad that was scheduled to expire never does, and a lost decree
    means instance configuration silently reverts. Both are written through to
    storage rather than cached.
*/

const { test } = require('node:test');
const assert = require('node:assert');
const Fs = require('node:fs/promises');
const Os = require('node:os');
const Path = require('node:path');

const Tasks = require('../../storage/storage/tasks.js');
const DecreesCore = require('../../common/decrees-core.js');
const { p } = require('./backend-conformance.js');

const quietLog = {
    info: () => {}, warn: () => {}, error: () => {}, verbose: () => {}, silly: () => {}
};

const CHANNEL = 'a'.repeat(32);

const mkEnv = async () => {
    const base = await Fs.mkdtemp(Path.join(Os.tmpdir(), 'cryptpad-tasks-'));
    const Env = {
        paths: {
            base,
            task: Path.join(base, 'tasks'),
            decree: Path.join(base, 'decrees')
        },
        Log: quietLog
    };
    return {
        Env, base,
        cleanup: () => Fs.rm(base, { recursive: true, force: true })
    };
};

// --- tasks ------------------------------------------------------------------

const mkTasks = async (Env, base, store) => {
    return await p(Tasks.create, {
        log: quietLog,
        taskPath: Path.join(base, 'tasks'),
        Env,
        store: store || { archiveChannel: (id, reason, cb) => { cb(); } }
    });
};

const withTasks = body => {
    return async () => {
        const { Env, base, cleanup } = await mkEnv();
        try {
            await body({ Env, base, mkTasks: store => mkTasks(Env, base, store) });
        } finally {
            await cleanup();
        }
    };
};

test('tasks: a written task can be listed', withTasks(async ctx => {
    const tasks = await ctx.mkTasks();
    // scheduled in the past so it is eligible to run
    await p(tasks.write, +new Date() - 1000, 'EXPIRE', [CHANNEL]);

    const listed = await p(tasks.list);
    assert.strictEqual(listed.length, 1);
}));

test('tasks: writing the same task twice is idempotent', withTasks(async ctx => {
    const tasks = await ctx.mkTasks();
    const time = +new Date() - 1000;

    // ids are the hash of the contents, so a repeat is the same record
    await p(tasks.write, time, 'EXPIRE', [CHANNEL]);
    await p(tasks.write, time, 'EXPIRE', [CHANNEL]);

    const listed = await p(tasks.list);
    assert.strictEqual(listed.length, 1, 'a duplicate task must not fail or double up');
}));

test('tasks: runAll executes a due task and removes it', withTasks(async ctx => {
    const archived = [];
    const tasks = await ctx.mkTasks({
        archiveChannel: (id, reason, cb) => { archived.push({ id, reason }); cb(); }
    });

    await p(tasks.write, +new Date() - 1000, 'EXPIRE', [CHANNEL]);
    await p(tasks.runAll);

    assert.deepStrictEqual(archived, [{ id: CHANNEL, reason: 'EXPIRED' }]);
    assert.deepStrictEqual(await p(tasks.list), [],
        'a task that has run must not run again');
}));

test('tasks: a task scheduled for the future is left alone', withTasks(async ctx => {
    const archived = [];
    const tasks = await ctx.mkTasks({
        archiveChannel: (id, reason, cb) => { archived.push(id); cb(); }
    });

    // far enough ahead that its whole day-bucket is skipped
    await p(tasks.write, +new Date() + (7 * 24 * 3600 * 1000), 'EXPIRE', [CHANNEL]);
    await p(tasks.runAll);

    assert.deepStrictEqual(archived, [], 'a future expiry must not fire early');
}));

test('tasks: an unknown command is dropped rather than retried forever',
    withTasks(async ctx => {
        const tasks = await ctx.mkTasks();
        await p(tasks.write, +new Date() - 1000, 'NONSENSE', [CHANNEL]);

        await p(tasks.runAll);
        assert.deepStrictEqual(await p(tasks.list), []);
    }));

test('tasks: listing an empty store is empty', withTasks(async ctx => {
    const tasks = await ctx.mkTasks();
    assert.deepStrictEqual(await p(tasks.list), []);
}));

test('tasks: concurrent runAll calls do not overlap', withTasks(async ctx => {
    const tasks = await ctx.mkTasks();
    await p(tasks.write, +new Date() - 1000, 'EXPIRE', [CHANNEL]);

    const first = new Promise(resolve => { tasks.runAll(resolve); });
    const second = await new Promise(resolve => { tasks.runAll(resolve); });

    // the second call bails out rather than running the same task twice
    assert.strictEqual(second, 'TASK_CONCURRENCY');
    await first;
}));

// --- decrees ----------------------------------------------------------------

/*  A minimal decree module: one command that records a value on Env, which is
    enough to prove that decrees are persisted, reloaded and applied in order. */
const mkDecreeModule = () => {
    // Decrees.create takes the command map directly
    return {
        SET_VALUE: (Env, args) => {
            if (Env.value === args[0]) { return false; }
            Env.value = args[0];
            return true;
        }
    };
};

const withDecrees = body => {
    return async () => {
        const { Env, base, cleanup } = await mkEnv();
        try {
            await body({ Env, base });
        } finally {
            await cleanup();
        }
    };
};

test('decrees: written decrees are reloaded', withDecrees(async ctx => {
    const decrees = DecreesCore.create('decree.ndjson', mkDecreeModule());

    await p(decrees.write, ctx.Env, ['SET_VALUE', ['hello'], 'admin', +new Date()]);

    // a fresh Env, as a restarted node would have
    const fresh = { paths: ctx.Env.paths, Log: quietLog };
    const toSend = await p(decrees.load, fresh);

    assert.strictEqual(fresh.value, 'hello', 'the decree must be applied on load');
    assert.strictEqual(toSend.length, 1);
}));

test('decrees: decrees accumulate and apply in order', withDecrees(async ctx => {
    const decrees = DecreesCore.create('decree.ndjson', mkDecreeModule());

    await p(decrees.write, ctx.Env, ['SET_VALUE', ['first'], 'admin', 1]);
    await p(decrees.write, ctx.Env, ['SET_VALUE', ['second'], 'admin', 2]);

    const fresh = { paths: ctx.Env.paths, Log: quietLog };
    await p(decrees.load, fresh);

    assert.strictEqual(fresh.value, 'second', 'the last decree wins');
}));

test('decrees: an absent log is not an error', withDecrees(async ctx => {
    const decrees = DecreesCore.create('decree.ndjson', mkDecreeModule());

    // a fresh instance has no decrees at all
    const toSend = await p(decrees.load, ctx.Env);
    assert.deepStrictEqual(toSend, []);
}));

test('decrees: a corrupt line does not prevent the rest from loading',
    withDecrees(async ctx => {
        const decrees = DecreesCore.create('decree.ndjson', mkDecreeModule());
        await p(decrees.write, ctx.Env, ['SET_VALUE', ['good'], 'admin', 1]);

        // append something unparseable, as a partial write would leave
        const path = Path.join(ctx.base, 'decrees', 'decree.ndjson');
        await Fs.appendFile(path, '{not json\n');

        const fresh = { paths: ctx.Env.paths, Log: quietLog };
        await p(decrees.load, fresh);
        assert.strictEqual(fresh.value, 'good',
            'one bad line must not cost the whole configuration');
    }));

test('decrees: the log is written through, not buffered', withDecrees(async ctx => {
    const decrees = DecreesCore.create('decree.ndjson', mkDecreeModule());
    await p(decrees.write, ctx.Env, ['SET_VALUE', ['durable'], 'admin', 1]);

    // readable straight from storage with no flush step
    const path = Path.join(ctx.base, 'decrees', 'decree.ndjson');
    const content = await Fs.readFile(path, 'utf8');
    assert.match(content, /durable/);
}));
