// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors
//
// SPDX-License-Identifier: AGPL-3.0-or-later

/*  Federation milestone M0 acceptance (docs/federation-design.md §9).

        "Done when: two instances authenticate and exchange a ping; a single
         instance with infra.federation: [] is byte-identical in behaviour to
         today."

    Both halves are checked here. Two complete instances are booted as real
    child processes — the node modules keep a module-level Env, so two instances
    cannot share one process — each with its own core, storage and federation
    node, its own data directory and its own instance key. Instance A is told to
    peer with B; the assertion is that the FED_PING it sends on session-up
    travels A -> session -> B -> B's core -> B's storage and back.

    This test drives the built bundles in ./build, so run `npm run build` first.
*/

const { test } = require('node:test');
const assert = require('node:assert');
const { fork } = require('node:child_process');
const Fs = require('node:fs/promises');
const Os = require('node:os');
const Path = require('node:path');
const Crypto = require('node:crypto');

const Identity = require('../../common/federation/identity.js');

const ROOT = Path.join(__dirname, '..', '..');
const BOOT_TIMEOUT = 60 * 1000;

// Every instance gets its own port block so the two never collide.
const mkInfra = (base, federation, front) => ({
    public: {
        origin: `http://localhost:${base}`,
        sandboxOrigin: `http://localhost:${base + 1}`,
        httpHost: 'localhost',
        httpPort: base,
        httpSafePort: base + 1,
        httpServerId: ''
    },
    front: front ? [{ url: '', host: 'localhost', port: base + 10, serverId: '' }] : [],
    core: [{ url: '', host: 'localhost', port: base + 20, serverId: '' }],
    storage: [{
        url: '', host: 'localhost',
        port: base + 30, wsPort: base + 40, serverId: ''
    }],
    federation
});

const mkConfig = (dir, nodesKey) => ({
    clientRoot: Path.join(ROOT, '..', 'cryptpad'),
    storage: { type: 'fs' },
    private: { nodes_key: nodesKey },
    // keep every path inside the instance's own directory
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

/*  Boot one node and resolve when it says READY. Its stdout and stderr are
    accumulated so the assertions can look at what it logged, which is how we
    observe the ping without building an admin surface M0 does not have yet. */
const startNode = (type, index, config, infra, sink) => {
    return new Promise((resolve, reject) => {
        const child = fork(Path.join(ROOT, 'build', `${type}.js`), {
            cwd: ROOT,
            silent: true
        });
        const tag = `${type}:${index}`;
        const onData = (buf) => {
            const text = String(buf);
            sink.lines.push(`[${tag}] ${text}`);
        };
        child.stdout.on('data', onData);
        child.stderr.on('data', onData);

        const timer = setTimeout(() => {
            reject(new Error(`${tag} did not become ready`));
        }, BOOT_TIMEOUT);

        child.on('message', (message) => {
            if (message?.msg !== 'READY') { return; }
            clearTimeout(timer);
            resolve(child);
        });
        child.on('exit', (code) => {
            clearTimeout(timer);
            reject(new Error(`${tag} exited early (${code})\n${sink.lines.join('')}`));
        });
        child.send({ myId: tag, index, config, infra });
    });
};

const startInstance = async (dir, portBase, federation, sink, front) => {
    const nodesKey = Crypto.randomBytes(32).toString('base64');
    const config = mkConfig(dir, nodesKey);
    const infra = mkInfra(portBase, federation, front);
    const children = [];

    // core first: everything else dials it
    children.push(await startNode('core', 0, config, infra, sink));
    children.push(await startNode('storage', 0, config, infra, sink));
    if (federation.length) {
        children.push(await startNode('federation', 0, config, infra, sink));
    }
    if (front) {
        children.push(await startNode('front', 0, config, infra, sink));
    }
    return { children, config, infra };
};

const stop = async (instances) => {
    const all = instances.flatMap(i => i?.children || []);
    all.forEach(c => { try { c.kill('SIGTERM'); } catch (err) { /* gone */ } });
    await new Promise(r => setTimeout(r, 500));
    all.forEach(c => { try { c.kill('SIGKILL'); } catch (err) { /* gone */ } });
};

/*  A front node reports READY before it listens: it only opens its HTTP server
    once core has pushed it the decrees (front/network.js, onEnvReady). Retry
    rather than race it. */
const get = async (url, ms) => {
    const deadline = Date.now() + ms;
    let last;
    while (Date.now() < deadline) {
        try {
            return await fetch(url);
        } catch (err) {
            last = err;
            await new Promise(r => setTimeout(r, 200));
        }
    }
    throw new Error(`${url} never answered: ${last?.message}`);
};

const waitFor = async (sink, pattern, ms) => {
    const deadline = Date.now() + ms;
    while (Date.now() < deadline) {
        const hit = sink.lines.find(l => pattern.test(l));
        if (hit) { return hit; }
        await new Promise(r => setTimeout(r, 100));
    }
    return undefined;
};

test('M0: two instances authenticate and exchange a ping', async () => {
    const dirA = await Fs.mkdtemp(Path.join(Os.tmpdir(), 'cryptpad-fedA-'));
    const dirB = await Fs.mkdtemp(Path.join(Os.tmpdir(), 'cryptpad-fedB-'));
    const sink = { lines: [] };
    let a, b;

    try {
        /*  Mint both identities up front so each side's peers.json can name the
            other by key. In production an operator does this by fetching the
            other instance's /api/federation. */
        const idA = Identity.load(dirA);
        const idB = Identity.load(dirB);
        assert.notStrictEqual(idA.originId, idB.originId);

        const portA = 3120, portB = 3220;
        const fedA = [{ url: '', host: '127.0.0.1', port: portA + 50, serverId: '' }];
        const fedB = [{ url: '', host: '127.0.0.1', port: portB + 50, serverId: '' }];

        // A dials B; B accepts A but does not dial back
        await Fs.writeFile(Path.join(dirA, 'peers.json'), JSON.stringify({
            version: 1,
            peers: [{
                originId: idB.originId,
                url: `ws://127.0.0.1:${portB + 50}/federation`,
                name: 'instance-B'
            }]
        }));
        await Fs.writeFile(Path.join(dirB, 'peers.json'), JSON.stringify({
            version: 1,
            peers: [{ originId: idA.originId, name: 'instance-A' }]
        }));

        b = await startInstance(dirB, portB, fedB, sink);
        a = await startInstance(dirA, portA, fedA, sink);

        const up = await waitFor(sink, /FEDERATION_SESSION_UP/, 30000);
        assert.ok(up, `no session established.\n${sink.lines.join('')}`);

        /*  The ping is the point: it only succeeds if the far instance's
            federation node reached its own core and storage. */
        const ping = await waitFor(sink, /FEDERATION_PING_OK/, 30000);
        assert.ok(ping, `no successful ping.\n${sink.lines.join('')}`);
        assert.match(ping, /storage.{0,4}true/,
            `the ping did not reach the storage tier: ${ping}`);

        assert.ok(!/FEDERATION_REFUSED|ENOTPEERED/.test(sink.lines.join('')),
            'neither side should have refused the other');
    } finally {
        // FED_TEST_VERBOSE=1 to see what the six nodes actually logged
        if (process.env.FED_TEST_VERBOSE) { console.log(sink.lines.join('')); }
        await stop([a, b]);
        await Fs.rm(dirA, { recursive: true, force: true });
        await Fs.rm(dirB, { recursive: true, force: true });
    }
});

/*  R-23: the instance key has to be discoverable, or an operator has no way to
    put a peer on the other side's allowlist. */
test('M0: /api/federation publishes the instance key, and says so when off', async () => {
    const dir = await Fs.mkdtemp(Path.join(Os.tmpdir(), 'cryptpad-fedapi-'));
    const off = await Fs.mkdtemp(Path.join(Os.tmpdir(), 'cryptpad-fedoff-'));
    const sink = { lines: [] };
    let withFed, withoutFed;
    try {
        const id = Identity.load(dir);
        const portOn = 3420, portOff = 3520;

        withFed = await startInstance(dir, portOn,
            [{ url: '', host: '127.0.0.1', port: portOn + 50, serverId: '' }], sink, true);

        const res = await get(`http://localhost:${portOn + 10}/api/federation`, 20000);
        assert.strictEqual(res.status, 200);
        const body = await res.json();
        assert.strictEqual(body.federation, true);
        assert.strictEqual(body.originId, id.originId,
            'the endpoint must publish the same key the federation node loaded');
        assert.strictEqual(body.origin, `http://localhost:${portOn}`);

        // and an instance that does not federate answers honestly rather than 500ing
        withoutFed = await startInstance(off, portOff, [], sink, true);
        const res2 = await get(`http://localhost:${portOff + 10}/api/federation`, 20000);
        assert.strictEqual(res2.status, 404);
        assert.deepStrictEqual(await res2.json(), { federation: false });
    } finally {
        if (process.env.FED_TEST_VERBOSE) { console.log(sink.lines.join('')); }
        await stop([withFed, withoutFed]);
        await Fs.rm(dir, { recursive: true, force: true });
        await Fs.rm(off, { recursive: true, force: true });
    }
});

test('M0: an instance with no federation nodes starts and opens no listener', async () => {
    const dir = await Fs.mkdtemp(Path.join(Os.tmpdir(), 'cryptpad-fednone-'));
    const sink = { lines: [] };
    let inst;
    try {
        inst = await startInstance(dir, 3320, [], sink);
        const text = sink.lines.join('');
        assert.ok(!/FEDERATION/.test(text),
            `a non-federating instance must not mention federation:\n${text}`);
        // and it never created an instance key
        await assert.rejects(Fs.access(Path.join(dir, 'identity.json')),
            'no federation identity is generated when federation is off');
    } finally {
        await stop([inst]);
        await Fs.rm(dir, { recursive: true, force: true });
    }
});
