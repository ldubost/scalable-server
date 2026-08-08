// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors
//
// SPDX-License-Identifier: AGPL-3.0-or-later

/*  M1 primitives: ids (C-4 seam), capabilities (R-25) and envelopes (R-14…R-18).
 *
 *  These three carry the security of federation between them, so the negative
 *  cases are the point: a forged ordering claim, a capability replayed against a
 *  different pad, an id that does not match its content.
 */

const { test } = require('node:test');
const assert = require('node:assert');
const Fs = require('node:fs/promises');
const Os = require('node:os');
const Path = require('node:path');
const NodeCrypto = require('node:crypto');

const Crypto = require('../../common/crypto.js')('sodiumnative');
const Ids = require('../../common/federation/ids.js');
const Capability = require('../../common/federation/capability.js');
const Envelope = require('../../common/federation/envelope.js');
const Identity = require('../../common/federation/identity.js');

// A realistic message: base64 of (64-byte ed25519 signature ‖ ciphertext).
const mkContent = (seed) => {
    const sig = Buffer.alloc(64, seed);
    const ct = Buffer.from(`ciphertext-${seed}`);
    return Buffer.concat([sig, ct]).toString('base64');
};

const withIdentity = (body) => async () => {
    const dir = await Fs.mkdtemp(Path.join(Os.tmpdir(), 'cryptpad-env-'));
    try {
        await body(Identity.load(dir), dir);
    } finally {
        await Fs.rm(dir, { recursive: true, force: true });
    }
};

// --- ids -------------------------------------------------------------------

test('ids: derived from content, stable, 64 chars', () => {
    const c = mkContent(1);
    const id = Ids.fromContent(c);
    assert.strictEqual(id.length, Ids.ID_LENGTH);
    assert.strictEqual(id, Ids.fromContent(c), 'must be deterministic');
    assert.ok(Ids.isValid(id));
    assert.notStrictEqual(id, Ids.fromContent(mkContent(2)));
});

/*  The checkpoint prefix is added by the client and is not covered by the
    signature. If it leaked into the id, the same message would have two ids
    depending on how it was seen -- and R-14 would be unsatisfiable. */
test('ids: the checkpoint prefix does not change the id', () => {
    const c = mkContent(3);
    assert.strictEqual(Ids.fromContent(`cp|${c}`), Ids.fromContent(c));
    assert.strictEqual(Ids.fromContent(`cp|abc123|${c}`), Ids.fromContent(c));
    assert.ok(Ids.isCheckpoint(`cp|${c}`));
    assert.ok(!Ids.isCheckpoint(c));
});

test('ids: rejects malformed input and orders totally', () => {
    assert.throws(() => Ids.fromContent(''), /E_ID/);
    assert.throws(() => Ids.fromContent('short'), /E_ID/);
    assert.throws(() => Ids.fromContent(null), /E_ID/);
    assert.strictEqual(Ids.isValid('nope'), false);

    const a = Ids.fromContent(mkContent(1));
    const b = Ids.fromContent(mkContent(2));
    assert.strictEqual(Math.sign(Ids.compare(a, b)), -Math.sign(Ids.compare(b, a)));
    assert.strictEqual(Ids.compare(a, a), 0);
});

// --- capabilities (R-25) ---------------------------------------------------

/*  A pad's signing keypair: the private half normally lives in the URL fragment
    and never reaches a server; the public half IS the channel's validateKey. */
const mkPadKeys = () => {
    const pair = Crypto.signKeyPair();
    return { secretKey: pair.secretKey, validateKey: Crypto.encodeBase64(pair.publicKey) };
};

const originId = () => NodeCrypto.randomBytes(32).toString('base64');

test('capability: mints and verifies against the pad validateKey', () => {
    const pad = mkPadKeys();
    const from = originId(), to = originId();
    const cap = Capability.mint({ channel: 'abcdef', from, to }, pad.secretKey);

    const res = Capability.verify(cap, pad.validateKey, { channel: 'abcdef', from, to });
    assert.ok(!res.error, `verify failed: ${res.error}`);
});

test('capability: a different pad key does not verify', () => {
    const pad = mkPadKeys(), other = mkPadKeys();
    const cap = Capability.mint({ channel: 'abcdef', from: originId(), to: originId() },
        pad.secretKey);
    assert.strictEqual(Capability.verify(cap, other.validateKey).error, 'E_CAP_SIG');
});

/*  The signature being good is not enough: a capability authorising A->B must
    not be usable to make A replicate to C, nor to replicate a different pad. */
test('capability: cannot be repointed at another pad or another instance', () => {
    const pad = mkPadKeys();
    const from = originId(), to = originId(), attacker = originId();
    const cap = Capability.mint({ channel: 'abcdef', from, to }, pad.secretKey);

    assert.strictEqual(
        Capability.verify(cap, pad.validateKey, { channel: 'different' }).error,
        'E_CAP_CHANNEL');
    assert.strictEqual(
        Capability.verify(cap, pad.validateKey, { to: attacker }).error,
        'E_CAP_ORIGIN');

    // and tampering with the fields themselves breaks the signature
    const tampered = Object.assign({}, cap, { to: attacker });
    assert.strictEqual(Capability.verify(tampered, pad.validateKey).error, 'E_CAP_SIG');
});

test('capability: expiry is enforced in both directions', () => {
    const pad = mkPadKeys();
    const base = { channel: 'abcdef', from: originId(), to: originId() };

    // an expired capability is refused (the cheap check runs before the signature)
    const expired = Capability.mint(base, pad.secretKey);
    expired.expires = Date.now() - 1000;
    assert.strictEqual(Capability.verify(expired, pad.validateKey).error,
        'E_CAP_EXPIRED');

    /*  and expiry is genuinely covered by the signature: extending it to another
        still-valid time gets past the expiry checks and fails on the signature. */
    const extended = Capability.mint(
        Object.assign({ lifetime: 60 * 1000 }, base), pad.secretKey);
    extended.expires = extended.expires + 1000;   // still well within MAX_LIFETIME
    assert.strictEqual(Capability.verify(extended, pad.validateKey).error,
        'E_CAP_SIG', 'expiry must be part of the signed payload');

    // a legitimately-signed but over-long capability is still refused
    const longLived = Capability.mint(base, pad.secretKey);
    longLived.expires = Date.now() + (365 * 24 * 60 * 60 * 1000);
    longLived.sig = Crypto.encodeBase64(
        Crypto.detachedSign(Capability.payload(longLived), pad.secretKey));
    assert.strictEqual(Capability.verify(longLived, pad.validateKey).error,
        'E_CAP_LIFETIME');
});

test('capability: nonces are single-use', () => {
    const pad = mkPadKeys();
    const cap = Capability.mint({ channel: 'abcdef', from: originId(), to: originId() },
        pad.secretKey);
    const cache = Capability.mkNonceCache();
    assert.strictEqual(cache.claim(cap), true);
    assert.strictEqual(cache.claim(cap), false, 'a replayed capability is refused');
});

// --- envelopes (R-14…R-18) -------------------------------------------------

test('envelope: create then verify round-trips', withIdentity(async (identity) => {
    const content = mkContent(9);
    const env = Envelope.create({
        channel: 'abcdef', content, seq: 3, lamport: 42
    }, identity);

    assert.strictEqual(env.id, Ids.fromContent(content));
    assert.strictEqual(env.o, identity.originId);

    const res = Envelope.checkAndVerify(env, identity.publicKey,
        { channel: 'abcdef', origin: identity.originId });
    assert.ok(!res.error, `verify failed: ${res.error}`);
}));

/*  R-14: the id must be the one the content implies. Otherwise a peer could
    make one message take another's place in the total order. */
test('envelope: a mismatched id is refused', withIdentity(async (identity) => {
    const env = Envelope.create({
        channel: 'abcdef', content: mkContent(1), seq: 0, lamport: 1
    }, identity);
    env.id = Ids.fromContent(mkContent(2));
    assert.strictEqual(Envelope.check(env).error, 'E_ENV_ID_MISMATCH');
}));

/*  R-16: ordering metadata is signed. A peer that rewrites the sequence or the
    Lamport clock is trying to reposition a message in every replica's log. */
test('envelope: rewriting ordering metadata breaks the signature',
    withIdentity(async (identity) => {
        const env = Envelope.create({
            channel: 'abcdef', content: mkContent(4), seq: 5, lamport: 100
        }, identity);

        for (const field of ['s', 'l', 't']) {
            const tampered = Object.assign({}, env, { [field]: env[field] + 1 });
            assert.strictEqual(
                Envelope.verify(tampered, identity.publicKey).error, 'E_ENV_SIG',
                `tampering with ${field} must be detected`);
        }
        const reorigined = Object.assign({}, env, { c: 'other' });
        assert.strictEqual(Envelope.verify(reorigined, identity.publicKey).error,
            'E_ENV_SIG');
    }));

/*  The signature is checked against the ORIGIN's key, not the peer that relayed
    it. That is what makes it safe for instance C to forward A's envelopes. */
test('envelope: another instance cannot sign for this origin', async () => {
    const dirA = await Fs.mkdtemp(Path.join(Os.tmpdir(), 'cryptpad-envA-'));
    const dirB = await Fs.mkdtemp(Path.join(Os.tmpdir(), 'cryptpad-envB-'));
    try {
        const A = Identity.load(dirA), B = Identity.load(dirB);
        const env = Envelope.create({
            channel: 'abcdef', content: mkContent(7), seq: 1, lamport: 2
        }, A);
        // B claims the envelope is its own
        const forged = Object.assign({}, env, { o: B.originId });
        assert.strictEqual(Envelope.verify(forged, B.publicKey).error, 'E_ENV_SIG');
        assert.strictEqual(Envelope.verify(forged, A.publicKey).error, 'E_ENV_SIG');
    } finally {
        await Fs.rm(dirA, { recursive: true, force: true });
        await Fs.rm(dirB, { recursive: true, force: true });
    }
});

test('envelope: structural checks reject junk', withIdentity(async (identity) => {
    const good = Envelope.create({
        channel: 'abcdef', content: mkContent(2), seq: 0, lamport: 0
    }, identity);

    assert.strictEqual(Envelope.check(null).error, 'E_ENV_MALFORMED');
    assert.strictEqual(Envelope.check(Object.assign({}, good, { v: 99 })).error,
        'E_ENV_VERSION');
    assert.strictEqual(Envelope.check(Object.assign({}, good, { s: -1 })).error,
        'E_ENV_SEQ');
    assert.strictEqual(Envelope.check(Object.assign({}, good, { s: 1.5 })).error,
        'E_ENV_SEQ');
    assert.strictEqual(Envelope.check(Object.assign({}, good, { l: 'x' })).error,
        'E_ENV_LAMPORT');
    assert.strictEqual(Envelope.check(Object.assign({}, good, { o: 'short' })).error,
        'E_ENV_ORIGIN');
    assert.strictEqual(
        Envelope.check(good, { channel: 'elsewhere' }).error, 'E_ENV_CHANNEL');
}));

/*  Signing must be over canonical bytes: two envelopes with the same fields in
    a different key order have to produce the same signature input, or a
    browser and a server would disagree. */
test('envelope: ordering bytes are key-order independent',
    withIdentity(async (identity) => {
        const env = Envelope.create({
            channel: 'abcdef', content: mkContent(6), seq: 2, lamport: 3
        }, identity);
        const shuffled = { sig: env.sig, t: env.t, l: env.l, s: env.s,
            o: env.o, id: env.id, m: env.m, c: env.c, v: env.v };
        assert.deepStrictEqual(
            Envelope.orderingBytes(shuffled), Envelope.orderingBytes(env));
    }));
