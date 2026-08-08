// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors
//
// SPDX-License-Identifier: AGPL-3.0-or-later

/*  The worker pool respawns a worker whenever one exits, which is what keeps a
    node alive when a worker crashes. That same behaviour would fight a graceful
    shutdown — killing the pool would just spawn replacements — so shutdown() has
    to mark workers as deliberately killed first. This test pins that down with
    real forked processes.
*/

const { test } = require('node:test');
const assert = require('node:assert');
const Fs = require('node:fs/promises');
const Os = require('node:os');
const Path = require('node:path');

const WorkerModule = require('../../common/worker-module.js');

const quietLog = { info: () => {}, error: () => {}, warn: () => {}, silly: () => {} };

// a minimal worker that speaks the protocol worker-module expects:
// the first message initialises it, subsequent ones are commands
const WORKER_SOURCE = `
process.on('message', obj => {
    if (!obj || !obj.txid) { return; }
    process.send({ txid: obj.txid, pid: obj.pid, value: { pid: process.pid } });
});
setInterval(() => {}, 1 << 30);
`;

const withPool = async (body) => {
    const dir = await Fs.mkdtemp(Path.join(Os.tmpdir(), 'cryptpad-worker-'));
    const workerPath = Path.join(dir, 'worker.js');
    await Fs.writeFile(workerPath, WORKER_SOURCE);

    const pool = WorkerModule({
        Log: quietLog,
        workerPath,
        maxWorkers: 2,
        maxJobs: 5,
        commandTimers: {},
        config: {},
        Env: {}
    });

    try {
        await body(pool);
    } finally {
        await new Promise(resolve => { pool.shutdown(resolve); });
        await Fs.rm(dir, { recursive: true, force: true });
    }
};

const isAlive = pid => {
    try {
        // signal 0 tests for existence without touching the process
        process.kill(pid, 0);
        return true;
    } catch (err) {
        return false;
    }
};

const waitFor = async (predicate, timeout = 2000) => {
    const deadline = Date.now() + timeout;
    while (Date.now() < deadline) {
        if (predicate()) { return true; }
        await new Promise(resolve => setTimeout(resolve, 20));
    }
    return predicate();
};

test('worker pool: spawns the configured number of workers', async () => {
    await withPool(async pool => {
        await waitFor(() => pool._workers.length === 2);
        assert.strictEqual(pool._workers.length, 2);
    });
});

test('worker pool: shutdown stops every worker', async () => {
    const dir = await Fs.mkdtemp(Path.join(Os.tmpdir(), 'cryptpad-worker-'));
    const workerPath = Path.join(dir, 'worker.js');
    await Fs.writeFile(workerPath, WORKER_SOURCE);

    const pool = WorkerModule({
        Log: quietLog,
        workerPath,
        maxWorkers: 2,
        maxJobs: 5,
        commandTimers: {},
        config: {},
        Env: {}
    });

    try {
        await waitFor(() => pool._workers.length === 2);
        const pids = pool._workers.map(state => state.pid);
        assert.strictEqual(pids.length, 2);

        await new Promise(resolve => { pool.shutdown(resolve); });

        // the pool must be empty and the processes actually gone
        assert.strictEqual(pool._workers.length, 0);
        for (const pid of pids) {
            await waitFor(() => !isAlive(pid));
            assert.strictEqual(isAlive(pid), false, `worker ${pid} should have exited`);
        }
    } finally {
        await Fs.rm(dir, { recursive: true, force: true });
    }
});

test('worker pool: shutdown does not respawn replacements', async () => {
    const dir = await Fs.mkdtemp(Path.join(Os.tmpdir(), 'cryptpad-worker-'));
    const workerPath = Path.join(dir, 'worker.js');
    await Fs.writeFile(workerPath, WORKER_SOURCE);

    const pool = WorkerModule({
        Log: quietLog,
        workerPath,
        maxWorkers: 2,
        maxJobs: 5,
        commandTimers: {},
        config: {},
        Env: {}
    });

    try {
        await waitFor(() => pool._workers.length === 2);
        await new Promise(resolve => { pool.shutdown(resolve); });

        // give the pool ample opportunity to spawn replacements
        await new Promise(resolve => setTimeout(resolve, 300));
        assert.strictEqual(pool._workers.length, 0,
            'a deliberate shutdown must not be undone by the respawn logic');
    } finally {
        await Fs.rm(dir, { recursive: true, force: true });
    }
});

test('worker pool: shutdown is safe to call twice', async () => {
    const dir = await Fs.mkdtemp(Path.join(Os.tmpdir(), 'cryptpad-worker-'));
    const workerPath = Path.join(dir, 'worker.js');
    await Fs.writeFile(workerPath, WORKER_SOURCE);

    const pool = WorkerModule({
        Log: quietLog,
        workerPath,
        maxWorkers: 1,
        maxJobs: 5,
        commandTimers: {},
        config: {},
        Env: {}
    });

    try {
        await waitFor(() => pool._workers.length === 1);
        await new Promise(resolve => { pool.shutdown(resolve); });
        // a second drain pass, or a signal arriving twice, must not hang
        await new Promise(resolve => { pool.shutdown(resolve); });
    } finally {
        await Fs.rm(dir, { recursive: true, force: true });
    }
});
