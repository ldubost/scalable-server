// SPDX-FileCopyrightText: 2025 XWiki CryptPad Team <contact@cryptpad.org> and contributors
//
// SPDX-License-Identifier: AGPL-3.0-or-later
const { fork } = require('child_process');
const Crypto = require('crypto');
const cliArgs = require("minimist")(process.argv.slice(2));

if (cliArgs.h || cliArgs.help) {
    console.log(`Usage ${process.argv[1]}:`);
    console.log("\t--help, -h\tDisplay this help");
    console.log("\t--type,-t\tSet the core type (if unset, starts every core)");
    console.log("\t--index,-i\tSet the core node index (default: 0)");
    process.exit(1);
}

const { config: serverConfig, infra: infraConfig } = require('./common/load-config');

const Log = {
    debug: console.debug,
    error: console.error,
    info: console.log,
    verbose: console.info,
    warn: console.warn
};

let serverId;

/*  Children are tracked so that a shutdown can be forwarded to them and waited on.
    Nodes buffer writes (an object-storage backend holds recently acknowledged
    messages in a local cache until they are flushed), so killing the supervisor
    without giving them a chance to drain loses data. */
const children = [];
let shuttingDown = false;

// how long to wait for children to drain before giving up on them
const SHUTDOWN_TIMEOUT = 40000;

const stopChildren = (signal) => {
    if (shuttingDown) {
        // a second signal: the operator wants out now
        Log.error('Forcing shutdown.');
        children.forEach(child => { try { child.kill('SIGKILL'); } catch (e) {} });
        return void process.exit(1);
    }
    shuttingDown = true;

    const alive = children.filter(child => child.exitCode === null && !child.killed);
    Log.info(`Stopping ${alive.length} node(s)...`);
    if (!alive.length) { return void process.exit(0); }

    let pending = alive.length;
    let timer;
    const finish = (code) => {
        clearTimeout(timer);
        process.exit(code);
    };

    alive.forEach(child => {
        child.once('exit', () => {
            pending--;
            if (pending <= 0) { finish(0); }
        });
        try {
            child.kill(signal);
        } catch (err) {
            pending--;
            if (pending <= 0) { finish(0); }
        }
    });

    timer = setTimeout(() => {
        Log.error(`${pending} node(s) did not stop in time; killing.`);
        children.forEach(child => { try { child.kill('SIGKILL'); } catch (e) {} });
        finish(1);
    }, SHUTDOWN_TIMEOUT);
};

['SIGTERM', 'SIGINT'].forEach(signal => {
    process.on(signal, () => { stopChildren(signal); });
});

const startNode = (type, index, forking, cb) => {
    if (typeof (cb) !== 'function') { cb = () => { }; };

    const nodeFile = './build/' + type + '.js';
    const initConfig = {
        myId: `${type}:${index}`,
        index,
        config: serverConfig,
        infra: infraConfig
    };

    //Log.info(`Starting: ${initConfig.myId}`);
    if (forking) {
        let nodeProcess = fork(nodeFile);
        children.push(nodeProcess);
        nodeProcess.send(initConfig);
        nodeProcess.on('message', (message) => {
            if (message.msg === 'READY') {
                Log.info(`Started: ${type}:${message.index}`);
                if (message.dev) {
                    Log.info('DEV mode enabled');
                }
                cb();
            }
        });
        // FIXME
        nodeProcess.on('error', (err) => {
            if (shuttingDown) { return; }
            Log.error('Child process stopped due to error.');
            Log.error(err);
            process.exit(1);
        });
        nodeProcess.on('exit', (code, signal) => {
            // during a shutdown, children exiting is the expected outcome;
            // stopChildren() decides when everyone is done
            if (shuttingDown) { return; }
            Log.error(`Node ${type}:${index} exited unexpectedly ` +
                      `(code ${code}, signal ${signal}). Stopping the server.`);
            stopChildren('SIGTERM');
        });
    } else {
        require(nodeFile).start(initConfig);
    }
};

const coresReady = () => {
    const promises = [];
    infraConfig?.front?.forEach((data, index) => {
        promises.push(new Promise(resolve => {
            if (serverId && data.serverId !== serverId) { return resolve(); }
            startNode('front', index, true, resolve);
        }));
    });
    infraConfig?.storage?.forEach((data, index) => {
        promises.push(new Promise(resolve => {
            if (serverId && data.serverId !== serverId) { return resolve(); }
            startNode('storage', index, true, resolve);
        }));
    });
    // federation is optional; instances that do not federate configure none
    infraConfig?.federation?.forEach((data, index) => {
        promises.push(new Promise(resolve => {
            if (serverId && data.serverId !== serverId) { return resolve(); }
            startNode('federation', index, true, resolve);
        }));
    });
    promises.push(new Promise(resolve => {
        if (serverId && infraConfig?.public?.httpServerId !== serverId) { return resolve(); }
        startNode('http', 0, true, resolve);
    }));
    Promise.all(promises).then(() => {
        Log.info('CryptPad server ready');
    });
};

const startCores = () => {
    if (!serverConfig?.private?.nodes_key) {
        if (!serverConfig?.private) {
            serverConfig.private = { };
        }
        serverConfig.private.nodes_key = Crypto.randomBytes(32).toString('base64');
    }
    const corePromises = infraConfig?.core.map((data, index) => new Promise((resolve, reject) => {
        // hosted on another machine?
        if (serverId && data.serverId !== serverId) { return resolve(); }
        startNode('core', index, true, (err) => {
            if (err) {
                Log.error('START_CORE_ERROR', err);
                return reject(err);
            }
            return resolve();
        });
    }));

    Promise.all(corePromises)
        .then(() => { coresReady(); })
        .catch((e) => { return Log.error('START_CORE_ERROR', e); });
};

// Start process

if (cliArgs.type || cliArgs.t) {
    const type = cliArgs.type || cliArgs.t;
    const index = Number(cliArgs.index || cliArgs.i || 0);
    if (!serverConfig?.private?.nodes_key) {
        throw Error('E_MISSINGKEY');
    }
    startNode(type, index, false, (err) => {
        if (err) { return Log.error('START_NODE_ERROR', err); }
    });
} else {
    serverId = cliArgs.server || cliArgs.s;
    startCores();
}
