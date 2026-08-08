// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors
//
// SPDX-License-Identifier: AGPL-3.0-or-later

/*  Federation M0: instance identity, peering policy, and the mutual-key
    handshake (spec R-23, R-24, R-27).

    The handshake is the whole trust boundary of the federation node — past
    peer-session.js, callers assume the originId is proved and allowlisted — so
    the negative cases matter more than the happy path and are tested over a real
    WebSocket pair rather than by calling the state machine directly.
*/

const { test } = require('node:test');
const assert = require('node:assert');
const Fs = require('node:fs/promises');
const Os = require('node:os');
const Path = require('node:path');
const Http = require('node:http');
const WebSocket = require('ws');

const Identity = require('../../common/federation/identity.js');
const Handshake = require('../../common/federation/handshake.js');
const Codec = require('../../common/federation/codec.js');
const Policy = require('../../federation/policy.js');
const PeerSession = require('../../federation/peer-session.js');

const quietLog = {
    debug: () => {}, error: () => {}, info: () => {},
    verbose: () => {}, warn: () => {}
};

const withDir = body => {
    return async () => {
        const base = await Fs.mkdtemp(Path.join(Os.tmpdir(), 'cryptpad-fed-'));
        try {
            await body(base);
        } finally {
            await Fs.rm(base, { recursive: true, force: true });
        }
    };
};

// --- identity (R-23) -------------------------------------------------------

test('identity: generated once, then stable across loads', withDir(async base => {
    const first = Identity.load(base);
    assert.ok(first.generated, 'first load creates the keypair');
    assert.strictEqual(Buffer.from(first.originId, 'base64').length, 32);

    const second = Identity.load(base);
    assert.ok(!second.generated, 'a second load reuses the stored key');
    assert.strictEqual(second.originId, first.originId,
        'the instance identity must not change between restarts');
}));

test('identity: signs and verifies', withDir(async base => {
    const a = Identity.load(base);
    const msg = Buffer.from('federation');
    const sig = a.sign(msg);
    assert.strictEqual(sig.length, 64);
    assert.ok(a.verify(msg, sig, a.publicKey));
    assert.ok(!a.verify(Buffer.from('other'), sig, a.publicKey),
        'a signature must not verify against different bytes');
}));

test('identity: refuses a corrupt or mismatched file', withDir(async base => {
    const good = Identity.load(base);
    const path = Path.join(base, Identity.FILE);

    await Fs.writeFile(path, 'not json');
    assert.throws(() => Identity.load(base), /CORRUPT/);

    // a public key that disagrees with the secret means a half-written file
    const raw = JSON.parse(JSON.stringify({
        version: 1,
        publicKey: Buffer.alloc(32, 7).toString('base64'),
        secretKey: Buffer.from(good.originId, 'base64').toString('base64')
    }));
    await Fs.writeFile(path, JSON.stringify(raw));
    assert.throws(() => Identity.load(base));
}));

// --- policy (R-27) ---------------------------------------------------------

test('policy: absent file means "this instance does not federate"', withDir(async base => {
    const policy = Policy.load(base, quietLog);
    assert.strictEqual(policy.present, false);
    assert.deepStrictEqual(policy.peers, []);
    assert.strictEqual(policy.admits('anything'), false,
        'with no policy, no peer is admitted');
}));

test('policy: loads peers and rejects malformed ones', withDir(async base => {
    const goodKey = Buffer.alloc(32, 1).toString('base64');
    await Fs.writeFile(Path.join(base, Policy.FILE), JSON.stringify({
        version: 1,
        peers: [
            { originId: goodKey, url: 'ws://localhost:1/federation', name: 'good' },
            { originId: 'not-a-key', url: 'ws://localhost:2/federation' },
            { originId: goodKey, url: 'ws://localhost:3/federation' } // duplicate
        ]
    }));
    const policy = Policy.load(base, quietLog);
    assert.strictEqual(policy.peers.length, 1, 'bad and duplicate entries are dropped');
    assert.ok(policy.admits(goodKey));
    assert.strictEqual(policy.admits('not-a-key'), false);
    assert.strictEqual(policy.outbound().length, 1);
}));

test('policy: a peer with no url is accepted inbound but never dialled', withDir(async base => {
    const key = Buffer.alloc(32, 2).toString('base64');
    await Fs.writeFile(Path.join(base, Policy.FILE), JSON.stringify({
        version: 1, peers: [{ originId: key, name: 'inbound-only' }]
    }));
    const policy = Policy.load(base, quietLog);
    assert.ok(policy.admits(key));
    assert.deepStrictEqual(policy.outbound(), []);
}));

// --- the handshake state machine (R-24) ------------------------------------

const twoIdentities = async () => {
    const a = await Fs.mkdtemp(Path.join(Os.tmpdir(), 'cryptpad-fed-a-'));
    const b = await Fs.mkdtemp(Path.join(Os.tmpdir(), 'cryptpad-fed-b-'));
    return {
        A: Identity.load(a), B: Identity.load(b),
        cleanup: () => Promise.all([
            Fs.rm(a, { recursive: true, force: true }),
            Fs.rm(b, { recursive: true, force: true })
        ])
    };
};

test('handshake: completes and names both sides', async () => {
    const { A, B, cleanup } = await twoIdentities();
    try {
        const hello = Handshake.mkHello(A);
        const { challenge, error: e1 } = Handshake.onHello(B, hello);
        assert.ok(!e1, `onHello: ${e1}`);

        const { proof, error: e2 } = Handshake.onChallenge(A, hello, challenge, B.originId);
        assert.ok(!e2, `onChallenge: ${e2}`);

        const { originId, error: e3 } = Handshake.onProof(B, hello, challenge, proof);
        assert.ok(!e3, `onProof: ${e3}`);
        assert.strictEqual(originId, A.originId);
    } finally { await cleanup(); }
});

test('handshake: dialler rejects an instance other than the one it dialled', async () => {
    const { A, B, cleanup } = await twoIdentities();
    try {
        const hello = Handshake.mkHello(A);
        const { challenge } = Handshake.onHello(B, hello);
        // we believed this URL served some third instance
        const other = Buffer.alloc(32, 9).toString('base64');
        const { error } = Handshake.onChallenge(A, hello, challenge, other);
        assert.strictEqual(error, 'EWRONGPEER');
    } finally { await cleanup(); }
});

test('handshake: a proof for one session does not verify in another', async () => {
    const { A, B, cleanup } = await twoIdentities();
    try {
        const hello1 = Handshake.mkHello(A);
        const { challenge: c1 } = Handshake.onHello(B, hello1);
        const { proof: p1 } = Handshake.onChallenge(A, hello1, c1, B.originId);

        // a second session: different nonces, so the captured proof is useless
        const hello2 = Handshake.mkHello(A);
        const { challenge: c2 } = Handshake.onHello(B, hello2);
        const { error } = Handshake.onProof(B, hello2, c2, p1);
        assert.strictEqual(error, 'EBADSIG', 'replayed proof must be refused');
    } finally { await cleanup(); }
});

test('handshake: the listener proof cannot be replayed as the dialler proof', async () => {
    const { A, B, cleanup } = await twoIdentities();
    try {
        const hello = Handshake.mkHello(B);       // B dials
        const { challenge } = Handshake.onHello(A, hello);
        // challenge.sig is A's proof as listener; try to pass it off as B's
        const { error } = Handshake.onProof(A, hello, challenge, {
            type: 'PROOF', v: Handshake.VERSION, sig: challenge.sig
        });
        assert.strictEqual(error, 'EBADSIG',
            'the role must be bound into the signed transcript');
    } finally { await cleanup(); }
});

test('handshake: refuses itself, bad frames and clock skew', async () => {
    const { A, cleanup } = await twoIdentities();
    try {
        assert.strictEqual(Handshake.onHello(A, Handshake.mkHello(A)).error, 'ESELF');
        assert.strictEqual(Handshake.onHello(A, { type: 'NOPE' }).error, 'EBADFRAME');

        const skewed = Handshake.mkHello(A);
        skewed.originId = Buffer.alloc(32, 3).toString('base64');
        skewed.time = Date.now() - (60 * 60 * 1000);
        assert.strictEqual(Handshake.onHello(A, skewed).error, 'ECLOCKSKEW');
    } finally { await cleanup(); }
});

// --- a real session over a real socket --------------------------------------

/*  Stand up the listening half exactly as federation/server.js does, so the
    framing, the policy check and the command dispatch are all exercised. */
const mkListener = (Env) => {
    return new Promise(resolve => {
        const httpServer = Http.createServer();
        const wss = new WebSocket.Server({ server: httpServer });
        wss.on('connection', ws => {
            PeerSession.create(Env, ws, {
                role: 'listener',
                onReady: (s) => Env.ready.push(s),
                onClose: (s, reason) => Env.closed.push(reason || s?.originId)
            });
        });
        httpServer.listen(0, '127.0.0.1', () => {
            resolve({ port: httpServer.address().port, httpServer, wss });
        });
    });
};

const mkEnv = (identity, policy, commands) => ({
    Log: quietLog,
    identity,
    policy,
    federationCommands: commands || {},
    ready: [],
    closed: []
});

const mkPolicy = (ids) => {
    const byId = new Map(ids.map(id => [id, { originId: id, name: id.slice(0, 8) }]));
    return {
        path: 'test', present: true, peers: [...byId.values()],
        outbound: () => [],
        admits: (id) => byId.has(id),
        get: (id) => byId.get(id),
        describe: (id) => byId.get(id)?.name || String(id).slice(0, 12)
    };
};

const settle = (ms) => new Promise(r => setTimeout(r, ms));

test('session: two peers that list each other reach an authenticated session', async () => {
    const { A, B, cleanup } = await twoIdentities();
    const envB = mkEnv(B, mkPolicy([A.originId]));
    const { port, httpServer } = await mkListener(envB);
    try {
        const envA = mkEnv(A, mkPolicy([B.originId]));
        const ws = new WebSocket(`ws://127.0.0.1:${port}`);
        const session = await new Promise(resolve => {
            ws.on('open', () => {
                PeerSession.create(envA, ws, {
                    role: 'dialler',
                    expectedOriginId: B.originId,
                    onReady: resolve
                });
            });
        });
        assert.strictEqual(session.originId, B.originId);
        assert.ok(session.authenticated);

        await settle(50);
        assert.strictEqual(envB.ready.length, 1, 'the listener also authenticated');
        assert.strictEqual(envB.ready[0].originId, A.originId);
        ws.close();
    } finally {
        httpServer.close();
        await cleanup();
    }
});

test('session: a peer that is not on the allowlist is refused (R-27)', async () => {
    const { A, B, cleanup } = await twoIdentities();
    // B lists nobody
    const envB = mkEnv(B, mkPolicy([]));
    const { port, httpServer } = await mkListener(envB);
    try {
        const envA = mkEnv(A, mkPolicy([B.originId]));
        const ws = new WebSocket(`ws://127.0.0.1:${port}`);
        const errors = [];
        ws.on('message', (data) => {
            const f = Codec.decode(Buffer.from(data));
            if (f?.type === 'ERROR') { errors.push(f.error); }
        });
        await new Promise(resolve => {
            ws.on('open', () => {
                PeerSession.create(envA, ws, { role: 'dialler', expectedOriginId: B.originId });
                resolve();
            });
        });
        await settle(200);
        assert.strictEqual(envB.ready.length, 0, 'no session was established');
        assert.ok(envB.closed.includes('ENOTPEERED'),
            `listener closed with ENOTPEERED, got ${JSON.stringify(envB.closed)}`);
        assert.ok(errors.includes('ENOTPEERED'), 'and told the dialler why');
    } finally {
        httpServer.close();
        await cleanup();
    }
});

test('session: commands are refused before the handshake completes', async () => {
    const { A, B, cleanup } = await twoIdentities();
    let called = 0;
    const envB = mkEnv(B, mkPolicy([A.originId]), {
        'FED_PING': () => { called++; }
    });
    const { port, httpServer } = await mkListener(envB);
    try {
        const ws = new WebSocket(`ws://127.0.0.1:${port}`);
        await new Promise(resolve => ws.on('open', resolve));
        // straight to a command, no HELLO
        ws.send(Codec.encode({ type: 'FED_PING', nonce: 'x' }));
        await settle(200);
        assert.strictEqual(called, 0, 'no command may run on an unauthenticated socket');
        assert.strictEqual(envB.ready.length, 0);
        ws.close();
    } finally {
        httpServer.close();
        await cleanup();
    }
});

test('session: FED_PING dispatches once authenticated', async () => {
    const { A, B, cleanup } = await twoIdentities();
    const seen = [];
    const envB = mkEnv(B, mkPolicy([A.originId]), {
        'FED_PING': (Env, session, frame) => {
            seen.push({ from: session.originId, nonce: frame.nonce });
            session.send({ type: 'FED_PONG', nonce: frame.nonce, storage: true });
        }
    });
    const { port, httpServer } = await mkListener(envB);
    try {
        const pongs = [];
        const envA = mkEnv(A, mkPolicy([B.originId]), {
            'FED_PONG': (Env, session, frame) => { pongs.push(frame); }
        });
        const ws = new WebSocket(`ws://127.0.0.1:${port}`);
        const session = await new Promise(resolve => {
            ws.on('open', () => {
                PeerSession.create(envA, ws, {
                    role: 'dialler', expectedOriginId: B.originId, onReady: resolve
                });
            });
        });
        session.send({ type: 'FED_PING', nonce: 'abc' });
        await settle(150);

        assert.deepStrictEqual(seen, [{ from: A.originId, nonce: 'abc' }]);
        assert.strictEqual(pongs.length, 1);
        assert.strictEqual(pongs[0].nonce, 'abc');
        assert.strictEqual(pongs[0].storage, true);
        ws.close();
    } finally {
        httpServer.close();
        await cleanup();
    }
});

test('session: an oversized frame is dropped', async () => {
    const { A, B, cleanup } = await twoIdentities();
    const envB = mkEnv(B, mkPolicy([A.originId]));
    const { port, httpServer } = await mkListener(envB);
    try {
        const ws = new WebSocket(`ws://127.0.0.1:${port}`);
        await new Promise(resolve => ws.on('open', resolve));
        ws.send(Buffer.alloc(PeerSession.MAX_FRAME + 1, 0x61));
        await settle(200);
        assert.strictEqual(envB.ready.length, 0);
        assert.ok(envB.closed.includes('EFRAMETOOLARGE'));
    } finally {
        httpServer.close();
        await cleanup();
    }
});
