// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors
//
// SPDX-License-Identifier: AGPL-3.0-or-later

/*  Graceful shutdown.

    Nodes used to exit whenever the process happened to die, which was survivable
    while every write went straight to a local disk. It stops being survivable once
    data is buffered before being sent somewhere else: an object-storage backend
    holds recent writes in a local cache until they are flushed, and those writes
    have already been acknowledged to clients.

    A node therefore needs a drain sequence, and the order matters:

      1. stop accepting new work, so the backlog stops growing while it is drained
      2. flush whatever is buffered
      3. close handles and stop background workers

    Hooks run sequentially in registration order. The whole sequence is bounded by a
    timeout, after which the node reports what was still outstanding and exits
    non-zero rather than hanging a deploy forever.
*/

const Util = require("./common-util");

const Shutdown = module.exports;

const DEFAULT_TIMEOUT = 30000;

Shutdown.create = (opts) => {
    opts = opts || {};
    const Log = opts.Log || {
        info: () => {}, error: () => {}, warn: () => {}
    };
    const label = opts.label || 'node';
    const timeout = typeof (opts.timeout) === 'number' ? opts.timeout : DEFAULT_TIMEOUT;

    const hooks = [];
    let draining = false;
    let finished = false;

    const api = {};

    /*  Register a step of the drain sequence. `fn` receives a callback and must
        call it exactly once; a step that never calls back is caught by the
        overall timeout rather than hanging forever.  */
    api.register = (name, fn) => {
        if (typeof (fn) !== 'function') { return; }
        hooks.push({ name, fn });
    };

    api.isDraining = () => draining;

    /*  Run every registered hook in order. Calls back with an array of the names
        that did not complete in time (empty when the drain was clean).  */
    api.drain = (reason, _cb) => {
        const cb = Util.once(typeof (_cb) === 'function' ? _cb : () => {});
        if (draining) {
            // a second request while draining: let the caller decide what to do
            return void cb(['ALREADY_DRAINING']);
        }
        draining = true;

        const started = +new Date();
        Log.info('SHUTDOWN_START', { node: label, reason, steps: hooks.length });

        const outstanding = hooks.map(hook => hook.name);

        const timer = setTimeout(() => {
            if (finished) { return; }
            finished = true;
            Log.error('SHUTDOWN_TIMEOUT', {
                node: label,
                reason,
                waited: +new Date() - started,
                outstanding
            });
            cb(outstanding.slice());
        }, timeout);

        let i = 0;
        const next = () => {
            if (finished) { return; }
            if (i >= hooks.length) {
                finished = true;
                clearTimeout(timer);
                Log.info('SHUTDOWN_COMPLETE', {
                    node: label,
                    reason,
                    elapsed: +new Date() - started
                });
                return void cb([]);
            }

            const hook = hooks[i++];
            const stepStarted = +new Date();
            // a hook that calls back twice must not advance the sequence twice
            const done = Util.once(() => {
                const idx = outstanding.indexOf(hook.name);
                if (idx !== -1) { outstanding.splice(idx, 1); }
                Log.info('SHUTDOWN_STEP', {
                    node: label,
                    step: hook.name,
                    elapsed: +new Date() - stepStarted
                });
                setImmediate(next);
            });

            try {
                hook.fn(done);
            } catch (err) {
                // a throwing hook shouldn't strand the rest of the sequence
                Log.error('SHUTDOWN_STEP_ERROR', {
                    node: label,
                    step: hook.name,
                    error: err && err.message
                });
                done();
            }
        };
        next();
    };

    /*  Install signal handlers. A second signal while draining exits immediately,
        so an operator is never stuck waiting on a drain that is not progressing. */
    api.install = (options) => {
        options = options || {};
        const signals = options.signals || ['SIGTERM', 'SIGINT'];
        const exit = options.exit || (code => { process.exit(code); });

        signals.forEach(signal => {
            process.on(signal, () => {
                if (draining) {
                    Log.error('SHUTDOWN_FORCED', { node: label, signal });
                    return void exit(1);
                }
                api.drain(signal, (outstanding) => {
                    exit(outstanding && outstanding.length ? 1 : 0);
                });
            });
        });
    };

    return api;
};
