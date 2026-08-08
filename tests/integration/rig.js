// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors
//
// SPDX-License-Identifier: AGPL-3.0-or-later

/*  A harness for federation integration tests: several complete CryptPad
    instances in one test, each as real child processes, plus real WebSocket
    clients to drive them.

    Separate processes are not a stylistic choice — the node modules keep a
    module-level `Env`, so two instances cannot share one process.

    The persistent, browsable equivalent of this rig lives at
    /home/ludovic/dev/experiments/federation and is described in
    docs/federation-design.md §10.0. This one is throwaway: fresh ports, fresh
    temp directories, fresh instance keys per test.
*/

const { fork } = require('node:child_process');
const Net = require('node:net');
const Fs = require('node:fs/promises');
const Os = require('node:os');
const Path = require('node:path');
const NodeCrypto = require('node:crypto');

const Netflux = require('netflux-websocket');
const WebSocket = require('ws');
const Nacl = require('tweetnacl/nacl-fast');

const Rpc = require('../common/rpc.js');
const Util = require('../../common/common-util.js');

const Crypto = require('../../common/crypto.js')('sodiumnative');
const Identity = require('../../common/federation/identity.js');

const ROOT = Path.join(__dirname, '..', '..');
const BOOT_TIMEOUT = 60 * 1000;
const HK = '0123456789abcdef';

/*  Port allocation.

    Deriving a range from the pid was not enough: a previous suite whose children
    outlived it still holds its ports, and the next run then fails to bind and
    dies in a way that looks like a federation bug. So every port in a candidate
    block is probed before the block is used, and a busy block is skipped.

    Probing is a race in principle — something could take the port between the
    probe and the bind — but nothing else on the machine is allocating in this
    range, and the alternative (a lock file, or passing 0 and reading back the
    port) does not work here because the ports are baked into config the child
    processes read at startup.
*/
let portCursor = 3600 + (process.pid % 40) * 100;

const portFree = (port) => new Promise((resolve) => {
    const server = Net.createServer();
    server.once('error', () => resolve(false));
    server.once('listening', () => server.close(() => resolve(true)));
    server.listen(port, '127.0.0.1');
});

// every port an instance needs, relative to its base
const OFFSETS = [0, 1, 10, 20, 30, 40, 50];

const nextBase = async () => {
    for (let attempt = 0; attempt < 60; attempt++) {
        portCursor += 100;
        if (portCursor > 9500) { portCursor = 3600; }
        const base = portCursor;
        const checks = await Promise.all(OFFSETS.map(o => portFree(base + o)));
        if (checks.every(Boolean)) { return base; }
    }
    throw new Error('no free port block for a test instance');
};

const randomChannel = () => NodeCrypto.randomBytes(16).toString('hex');

/*  A pad's signing keypair. The private half normally lives in the URL fragment
    and never reaches a server; the public half is the channel's validateKey. */
const mkPadKeys = () => {
    const pair = Crypto.signKeyPair();
    return {
        secretKey: pair.secretKey,
        publicKey: pair.publicKey,
        validateKey: Crypto.encodeBase64(pair.publicKey)
    };
};

/*  A pad message as the server expects it: base64 of `signature ‖ payload`,
    which is what `crypto_sign_open` verifies against the validateKey
    (`core/worker.js:11-30`). The payload stands in for ciphertext; the server
    cannot read it either way. */
const mkMessage = (secretKey, text) => {
    const payload = Buffer.from(text, 'utf8');
    const sig = Crypto.detachedSign(payload, secretKey);
    return Buffer.concat([sig, payload]).toString('base64');
};

/*  An account keypair. Metadata authority is tied to accounts (spec §1.5), and
    accounts are instance-scoped — which is exactly why replicated metadata is a
    trust-the-peer boundary (R-28). */
const mkUserKeys = () => {
    const kp = Nacl.sign.keyPair();
    return {
        edPublic: Util.encodeBase64(kp.publicKey),
        edPrivate: Util.encodeBase64(kp.secretKey)
    };
};

const mkInfra = (base) => ({
    public: {
        origin: `http://localhost:${base}`,
        sandboxOrigin: `http://localhost:${base + 1}`,
        httpHost: 'localhost',
        httpPort: base,
        httpSafePort: base + 1,
        httpServerId: ''
    },
    front: [{ url: '', host: 'localhost', port: base + 10, serverId: '' }],
    core: [{ url: '', host: 'localhost', port: base + 20, serverId: '' }],
    storage: [{
        url: '', host: 'localhost', port: base + 30, wsPort: base + 40, serverId: ''
    }],
    federation: [{ url: '', host: '127.0.0.1', port: base + 50, serverId: '' }]
});

const mkConfig = (dir) => ({
    clientRoot: Path.join(ROOT, '..', 'cryptpad'),
    storage: { type: 'fs' },
    private: { nodes_key: NodeCrypto.randomBytes(32).toString('base64') },
    basePath: dir,
    filePath: Path.join(dir, 'channel'),
    blobPath: Path.join(dir, 'blob'),
    blobStagingPath: Path.join(dir, 'blobstage'),
    blockPath: Path.join(dir, 'block'),
    pinPath: Path.join(dir, 'pins'),
    archivePath: Path.join(dir, 'archive'),
    taskPath: Path.join(dir, 'tasks'),
    decreePath: Path.join(dir, 'decrees'),
    logoPath: Path.join(dir, 'logo'),
    challengePath: Path.join(dir, 'challenges'),
    disableIntegratedTasks: true,
    logToStdout: true
});

const startNode = (type, config, infra, sink) => {
    return new Promise((resolve, reject) => {
        const child = fork(Path.join(ROOT, 'build', `${type}.js`), {
            cwd: ROOT, silent: true
        });
        const tag = `${config._label}/${type}`;
        const onData = (buf) => { sink.lines.push(`[${tag}] ${String(buf)}`); };
        child.stdout.on('data', onData);
        child.stderr.on('data', onData);

        const timer = setTimeout(
            () => reject(new Error(`${tag} did not become ready`)), BOOT_TIMEOUT);
        child.on('message', (m) => {
            if (m?.msg !== 'READY') { return; }
            clearTimeout(timer);
            resolve(child);
        });
        child.on('exit', (code) => {
            clearTimeout(timer);
            reject(new Error(`${tag} exited early (${code})\n${sink.lines.join('')}`));
        });
        child.send({ myId: `${type}:0`, index: 0, config, infra });
    });
};

const httpJson = async (url, opts, ms = 20000) => {
    const deadline = Date.now() + ms;
    let last;
    while (Date.now() < deadline) {
        try {
            const res = await fetch(url, opts);
            const text = await res.text();
            try { return JSON.parse(text); } catch (e) { return { status: res.status, text }; }
        } catch (e) {
            last = e;
            await new Promise(r => setTimeout(r, 200));
        }
    }
    throw new Error(`${url} never answered: ${last?.message}`);
};

const create = async (labels) => {
    const sink = { lines: [] };
    const instances = new Map();
    const dirs = [];

    /*  Identities are minted before anything starts, because peering is by key
        and both peers.json files have to name keys that already exist. */
    const planned = [];
    for (const label of labels) {
        const dir = await Fs.mkdtemp(Path.join(Os.tmpdir(), `cryptpad-fed-${label}-`));
        dirs.push(dir);
        planned.push({
            label, dir,
            base: await nextBase(),
            identity: Identity.load(dir)
        });
    }

    // everyone peers with everyone; the lowest-based instance dials
    for (const inst of planned) {
        await Fs.writeFile(Path.join(inst.dir, 'peers.json'), JSON.stringify({
            version: 1,
            peers: planned.filter(o => o !== inst).map(other => ({
                originId: other.identity.originId,
                url: `ws://127.0.0.1:${other.base + 50}/federation`,
                name: other.label,
                dial: inst.base < other.base
            }))
        }));
    }

    for (const inst of planned) {
        const config = Object.assign(mkConfig(inst.dir), { _label: inst.label });
        const infra = mkInfra(inst.base);
        const children = [];
        children.push(await startNode('core', config, infra, sink));
        children.push(await startNode('storage', config, infra, sink));
        children.push(await startNode('federation', config, infra, sink));
        children.push(await startNode('front', config, infra, sink));

        instances.set(inst.label, {
            label: inst.label,
            dir: inst.dir,
            base: inst.base,
            originId: inst.identity.originId,
            frontUrl: `http://localhost:${inst.base + 10}`,
            wsUrl: `ws://localhost:${inst.base + 10}/cryptpad_websocket`,
            children
        });
    }

    const clients = [];
    const rig = {
        sink,
        get: (label) => instances.get(label),
        all: () => Array.from(instances.values()),

        /*  Wait until every instance reports a live federation session, so a test
            does not race the handshake. */
        federated: async (ms = 30000) => {
            const deadline = Date.now() + ms;
            while (Date.now() < deadline) {
                const ups = sink.lines.filter(l => /FEDERATION_SESSION_UP/.test(l));
                if (ups.length >= instances.size) { return true; }
                await new Promise(r => setTimeout(r, 200));
            }
            throw new Error(`federation sessions never came up\n${sink.lines.join('')}`);
        },

        /*  A real client: joins over the front node's WebSocket and writes signed
            pad messages, exactly as a browser would. */
        client: async (inst, channel, padKeys, opts) => {
            const validateKey = padKeys.validateKey;
            const network = await Netflux.connect('', () => new WebSocket(inst.wsUrl));
            network.historyKeeper = HK;
            const wc = await network.join(channel);

            /*  The first GET_HISTORY establishes the channel metadata, which is
                where validateKey comes from. Without it the server has no key to
                validate writes against.

                The history keeper answers with the stored lines as arrays, then
                a `{state: 1, channel}` object to say it is done — it is not a
                [command, payload] envelope, which is easy to get wrong. */
            await new Promise((resolve) => {
                const onMessage = (msg, sender) => {
                    if (sender !== HK) { return; }
                    let parsed;
                    try { parsed = JSON.parse(msg); } catch (e) { return; }
                    if (parsed && parsed.state === 1 && parsed.channel === channel) {
                        network.off('message', onMessage);
                        resolve();
                    }
                };
                network.on('message', onMessage);
                const metadata = { validateKey };
                if (opts?.owners) { metadata.owners = opts.owners; }
                if (opts?.expire) { metadata.expire = opts.expire; }
                if (opts?.selfdestruct) { metadata.selfdestruct = true; }
                if (opts?.restricted) { metadata.restricted = true; }
                network.sendto(HK, JSON.stringify(['GET_HISTORY', channel, {
                    txid: NodeCrypto.randomBytes(4).toString('hex'),
                    metadata
                }]));
                setTimeout(() => { network.off('message', onMessage); resolve(); }, 5000);
            });

            /*  Everything this client sees on the channel after joining, so a
                test can assert live delivery rather than reading history back —
                the difference between "the mirror stored it" and "a user
                watching the mirror saw it". */
            const received = [];
            // live messages arrive on the webchannel as the raw content string,
            // not as the [0, sender, 'MSG', channel, content, time] envelope
            wc.on('message', (msg) => { received.push(msg); });

            const client = {
                network, wc, received,
                send: async (text) => {
                    const content = mkMessage(padKeys.secretKey, text);
                    await wc.bcast(content);
                    // let it commit before the next write, so order is deterministic
                    await new Promise(r => setTimeout(r, 120));
                    return content;
                },
                close: async () => { try { network.disconnect(); } catch (e) { /* gone */ } }
            };
            clients.push(client);
            return client;
        },

        /*  Read a channel's history from an instance, as ids in order. Ids rather
            than raw content because that is what R-1 is stated over. */
        history: async (inst, channel) => {
            const network = await Netflux.connect('', () => new WebSocket(inst.wsUrl));
            network.historyKeeper = HK;
            try {
                await network.join(channel);
                return await new Promise((resolve) => {
                    const out = [];
                    const onMessage = (msg, sender) => {
                        if (sender !== HK) { return; }
                        let parsed;
                        try { parsed = JSON.parse(msg); } catch (e) { return; }
                        // the terminator is an object, the messages are arrays
                        if (parsed && parsed.state === 1 && parsed.channel === channel) {
                            return resolve(out);
                        }
                        if (!Array.isArray(parsed) || parsed[3] !== channel) { return; }
                        out.push(parsed[4]);
                    };
                    network.on('message', onMessage);
                    network.sendto(HK, JSON.stringify(['GET_HISTORY', channel, {
                        txid: NodeCrypto.randomBytes(4).toString('hex')
                    }]));
                    setTimeout(() => resolve(out), 6000);
                });
            } finally {
                try { network.disconnect(); } catch (e) { /* gone */ }
            }
        },

        // turn on replication for a channel at its origin
        enableOnOrigin: (inst, channel, validateKey, members, level) =>
            httpJson(`${inst.frontUrl}/api/federation/enable`, {
                method: 'POST',
                headers: { 'content-type': 'application/json' },
                body: JSON.stringify({ channel, validateKey, members, level })
            }),

        /*  Ask an instance to mirror a channel from a peer. Waits for the
            federation session first: replication needs a live session, and
            racing the handshake just produces a confusing ENOSESSION. */
        replicate: async (inst, body) => {
            await rig.federated();
            return httpJson(`${inst.frontUrl}/api/federation/replicate`, {
                method: 'POST',
                headers: { 'content-type': 'application/json' },
                body: JSON.stringify(body)
            });
        },

        /*  Kill one node of one instance, to test what the rest do without it.
            SIGKILL rather than SIGTERM: a partition is not a graceful shutdown,
            and the point is to see the failure handling, not the drain path. */
        killNode: async (inst, type) => {
            const idx = ['core', 'storage', 'federation', 'front'].indexOf(type);
            if (idx < 0) { throw new Error(`unknown node type ${type}`); }
            const child = inst.children[idx];
            try { child.kill('SIGKILL'); } catch (e) { /* already gone */ }
            await new Promise(r => setTimeout(r, 500));
        },

        /*  An authenticated RPC session, for the account-level operations —
            metadata changes are authorised by the *user's* account key, not the
            pad key. */
        rpc: async (inst, userKeys) => {
            const network = await Netflux.connect('', () => new WebSocket(inst.wsUrl));
            network.historyKeeper = HK;
            const rpc = await new Promise((resolve, reject) => {
                Rpc.create(network, userKeys.edPrivate, userKeys.edPublic,
                    (e, r) => e ? reject(e) : resolve(r));
            });
            const wrapper = {
                network,
                /*  The raw rpc as well as the promise wrapper: uploads are
                    driven by the client's own `tests/common/upload.js`, which
                    wants callbacks and `send.unauthenticated`. Wrapping that
                    would mean reimplementing the upload protocol here, which is
                    the one thing an upload test must not do. */
                rpc,
                send: (cmd, data) => new Promise((resolve, reject) => {
                    rpc.send(cmd, data, (e, out) => e ? reject(e) : resolve(out));
                }),
                close: () => { try { network.disconnect(); } catch (e) { /* gone */ } }
            };
            clients.push({ close: async () => wrapper.close() });
            return wrapper;
        },

        // read a channel's metadata as an instance currently sees it
        metadata: async (inst, channel) => {
            const network = await Netflux.connect('', () => new WebSocket(inst.wsUrl));
            network.historyKeeper = HK;
            try {
                await network.join(channel);
                return await new Promise((resolve) => {
                    const onMessage = (msg, sender) => {
                        if (sender !== HK) { return; }
                        let parsed;
                        try { parsed = JSON.parse(msg); } catch (e) { return; }
                        // the metadata line is an object carrying `channel`
                        if (parsed && parsed.channel === channel && !parsed.state) {
                            return resolve(parsed);
                        }
                        if (parsed && parsed.state === 1) { return resolve(undefined); }
                    };
                    network.on('message', onMessage);
                    network.sendto(HK, JSON.stringify(['GET_HISTORY', channel, {
                        txid: NodeCrypto.randomBytes(4).toString('hex')
                    }]));
                    setTimeout(() => resolve(undefined), 6000);
                });
            } finally {
                try { network.disconnect(); } catch (e) { /* gone */ }
            }
        },

        /*  Take a whole instance down. Killing one node would do it anyway —
            `common/interface.js` exits on an internal disconnect — but doing it
            explicitly says what the test means. */
        /*  A client that carries content the caller has already encrypted and
            signed, and hands inbound content back untouched.

            `client()` above signs a plain string for tests that only care about
            transport. This one exists for tests that drive a real ChainPad
            document, where the ciphertext must come from `chainpad-crypto` and
            the server must never see anything else. */
        rawClient: async (inst, channel, validateKey) => {
            const network = await Netflux.connect('', () => new WebSocket(inst.wsUrl));
            network.historyKeeper = HK;
            const wc = await network.join(channel);

            const handlers = [];
            const seen = new Set();
            /*  History arrives while this function is still running — the
                GET_HISTORY below is part of connecting — so anything delivered
                before the caller has attached a handler must be buffered and
                replayed, not dropped. Getting this wrong makes a replica look
                empty when in fact it holds the whole document. */
            const buffered = [];
            const deliver = (content) => {
                if (typeof (content) !== 'string' || seen.has(content)) { return; }
                seen.add(content);
                if (!handlers.length) { return void buffered.push(content); }
                handlers.forEach(h => h(content));
            };

            // live messages arrive on the webchannel as raw content
            wc.on('message', (msg) => deliver(msg));

            // history arrives from the history keeper as full message arrays
            network.on('message', (msg, sender) => {
                if (sender !== HK) { return; }
                let parsed;
                try { parsed = JSON.parse(msg); } catch (e) { return; }
                if (Array.isArray(parsed) && parsed[3] === channel) {
                    deliver(parsed[4]);
                }
            });

            // establish metadata (validateKey) and pull existing history
            await new Promise((resolve) => {
                const onMessage = (msg, sender) => {
                    if (sender !== HK) { return; }
                    let parsed;
                    try { parsed = JSON.parse(msg); } catch (e) { return; }
                    if (parsed && parsed.state === 1 && parsed.channel === channel) {
                        network.off('message', onMessage);
                        resolve();
                    }
                };
                network.on('message', onMessage);
                network.sendto(HK, JSON.stringify(['GET_HISTORY', channel, {
                    txid: NodeCrypto.randomBytes(4).toString('hex'),
                    metadata: { validateKey }
                }]));
                setTimeout(() => { network.off('message', onMessage); resolve(); }, 6000);
            });

            const client = {
                network, wc,
                sendRaw: (content) => wc.bcast(content),
                onContent: (h) => {
                    handlers.push(h);
                    // replay whatever arrived before anyone was listening
                    if (handlers.length === 1) {
                        buffered.splice(0).forEach(c => h(c));
                    }
                },
                close: async () => { try { network.disconnect(); } catch (e) { /* gone */ } }
            };
            clients.push(client);
            return client;
        },

        killInstance: async (inst) => {
            inst.children.forEach(c => {
                try { c.kill('SIGKILL'); } catch (e) { /* already gone */ }
            });
            await new Promise(r => setTimeout(r, 800));
        },

        stop: async () => {
            for (const c of clients) {
                try { await c.close(); } catch (e) { /* already gone */ }
            }
            const all = rig.all().flatMap(i => i.children);

            /*  Wait for the children to actually exit rather than sleeping a
                fixed time. A child still running when the next test starts holds
                its ports, and the resulting bind failure is indistinguishable
                from a federation bug — which cost real debugging time. */
            const gone = all.map(c => new Promise((resolve) => {
                if (c.exitCode !== null || c.signalCode) { return resolve(); }
                c.once('exit', () => resolve());
                try { c.kill('SIGTERM'); } catch (e) { resolve(); }
            }));
            await Promise.race([
                Promise.all(gone),
                new Promise(r => setTimeout(r, 5000))
            ]);
            all.forEach(c => { try { c.kill('SIGKILL'); } catch (e) { /* gone */ } });
            await new Promise(r => setTimeout(r, 300));
            if (process.env.FED_TEST_VERBOSE) { console.log(sink.lines.join('')); }
            for (const d of dirs) {
                await Fs.rm(d, { recursive: true, force: true });
            }
        }
    };
    return rig;
};

module.exports = { create, randomChannel, mkPadKeys, mkMessage, mkUserKeys, HK };
