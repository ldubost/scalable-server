// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors
//
// SPDX-License-Identifier: AGPL-3.0-or-later

const { test } = require('node:test');
const assert = require('node:assert');

const Shutdown = require('../../common/shutdown.js');

const quietLog = { info: () => {}, error: () => {}, warn: () => {} };

const drain = (shutdown, reason) => {
    return new Promise(resolve => { shutdown.drain(reason || 'TEST', resolve); });
};

test('shutdown: runs hooks in registration order', async () => {
    const order = [];
    const shutdown = Shutdown.create({ Log: quietLog });

    shutdown.register('first', done => { order.push('first'); done(); });
    shutdown.register('second', done => {
        // asynchronous hooks must still be sequenced
        setTimeout(() => { order.push('second'); done(); }, 10);
    });
    shutdown.register('third', done => { order.push('third'); done(); });

    const outstanding = await drain(shutdown);
    assert.deepStrictEqual(order, ['first', 'second', 'third']);
    assert.deepStrictEqual(outstanding, [], 'a clean drain reports nothing outstanding');
});

test('shutdown: waits for a slow hook before continuing', async () => {
    const order = [];
    const shutdown = Shutdown.create({ Log: quietLog });

    shutdown.register('slow-flush', done => {
        setTimeout(() => { order.push('flushed'); done(); }, 30);
    });
    shutdown.register('close', done => { order.push('closed'); done(); });

    await drain(shutdown);
    // the whole point: nothing closes before the flush has finished
    assert.deepStrictEqual(order, ['flushed', 'closed']);
});

test('shutdown: reports hooks that exceed the timeout', async () => {
    const shutdown = Shutdown.create({ Log: quietLog, timeout: 40 });

    shutdown.register('fast', done => { done(); });
    shutdown.register('hangs', () => { /* never calls back */ });
    shutdown.register('never-reached', done => { done(); });

    const outstanding = await drain(shutdown);
    assert.deepStrictEqual(outstanding, ['hangs', 'never-reached'],
        'the timeout must name what did not finish');
});

test('shutdown: a throwing hook does not strand the sequence', async () => {
    const order = [];
    const shutdown = Shutdown.create({ Log: quietLog, timeout: 200 });

    shutdown.register('throws', () => { throw new Error('boom'); });
    shutdown.register('after', done => { order.push('after'); done(); });

    const outstanding = await drain(shutdown);
    assert.deepStrictEqual(order, ['after']);
    assert.deepStrictEqual(outstanding, []);
});

test('shutdown: a hook calling back twice does not skip a step', async () => {
    const order = [];
    const shutdown = Shutdown.create({ Log: quietLog, timeout: 200 });

    shutdown.register('sloppy', done => { done(); done(); });
    shutdown.register('one', done => { order.push('one'); done(); });
    shutdown.register('two', done => { order.push('two'); done(); });

    await drain(shutdown);
    assert.deepStrictEqual(order, ['one', 'two']);
});

test('shutdown: draining twice does not run hooks again', async () => {
    let runs = 0;
    const shutdown = Shutdown.create({ Log: quietLog, timeout: 200 });
    shutdown.register('once-only', done => { runs++; done(); });

    await drain(shutdown);
    const second = await drain(shutdown);

    assert.strictEqual(runs, 1);
    assert.deepStrictEqual(second, ['ALREADY_DRAINING']);
});

test('shutdown: isDraining reflects state', async () => {
    const shutdown = Shutdown.create({ Log: quietLog, timeout: 200 });
    let duringHook;
    shutdown.register('check', done => { duringHook = shutdown.isDraining(); done(); });

    assert.strictEqual(shutdown.isDraining(), false);
    await drain(shutdown);
    assert.strictEqual(duringHook, true, 'hooks run while the node is marked draining');
});

test('shutdown: an empty hook list drains cleanly', async () => {
    const shutdown = Shutdown.create({ Log: quietLog });
    assert.deepStrictEqual(await drain(shutdown), []);
});

test('shutdown: install() drains on a signal and exits 0', async () => {
    const shutdown = Shutdown.create({ Log: quietLog, timeout: 200 });
    let flushed = false;
    shutdown.register('flush', done => { flushed = true; done(); });

    const exited = new Promise(resolve => {
        shutdown.install({
            signals: ['SIGUSR2'],   // a signal the test runner will not use
            exit: resolve
        });
    });

    process.emit('SIGUSR2');
    const code = await exited;

    assert.strictEqual(flushed, true, 'the signal must trigger the drain');
    assert.strictEqual(code, 0, 'a clean drain exits 0');
});

test('shutdown: install() exits non-zero when the drain times out', async () => {
    const shutdown = Shutdown.create({ Log: quietLog, timeout: 30 });
    shutdown.register('hangs', () => {});

    const exited = new Promise(resolve => {
        shutdown.install({ signals: ['SIGUSR2'], exit: resolve });
    });

    process.emit('SIGUSR2');
    assert.strictEqual(await exited, 1, 'an incomplete drain must exit non-zero');
});

test('shutdown: a second signal forces an immediate exit', async () => {
    const shutdown = Shutdown.create({ Log: quietLog, timeout: 5000 });
    shutdown.register('slow', done => { setTimeout(done, 4000); });

    const codes = [];
    const forced = new Promise(resolve => {
        shutdown.install({
            signals: ['SIGUSR2'],
            exit: code => {
                codes.push(code);
                if (codes.length === 1) { resolve(code); }
            }
        });
    });

    process.emit('SIGUSR2');   // starts a slow drain
    process.emit('SIGUSR2');   // operator gives up

    // the forced exit must not wait for the slow hook
    assert.strictEqual(await forced, 1);
});

// clean up the listeners the tests above installed on the real process object
test.after?.(() => { process.removeAllListeners('SIGUSR2'); });
