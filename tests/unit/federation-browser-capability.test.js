// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors
//
// SPDX-License-Identifier: AGPL-3.0-or-later

/*  The browser mints replication capabilities; the server verifies them. If the
 *  two disagree about a single byte of the signed payload, federating a pad
 *  fails with `E_CAP_SIG` and the only place that shows up is a browser console.
 *
 *  So this reproduces the browser's minting exactly — the same field set, the
 *  same key-sorted encoding, the same `nacl.sign.detached` over UTF-8 bytes, and
 *  a real pad key derived by `chainpad-crypto` — and checks the server's
 *  verifier accepts it. It is the closest thing to running
 *  `experiments/federation/federate-pad.js` that does not need a browser.
 */

const { test } = require('node:test');
const assert = require('node:assert');
const Path = require('node:path');

const Capability = require('../../common/federation/capability.js');
const Codec = require('../../common/federation/codec.js');

const CLIENT = process.env.CRYPTPAD_CLIENT ||
    Path.join(__dirname, '..', '..', '..', 'cryptpad');
const PadCrypto = require(Path.join(CLIENT, 'node_modules', 'chainpad-crypto'));
const Nacl = require(Path.join(CLIENT, 'node_modules', 'tweetnacl'));

const originId = () => require('node:crypto').randomBytes(32).toString('base64');

/*  Exactly what federate-pad.js does in the browser, transcribed. Deliberately
    NOT calling Capability.mint: the point is to catch the two implementations
    drifting apart, which sharing code would hide. */
const browserMint = (channel, keys, from, to) => {
    const canonical = (obj) => JSON.stringify(obj, Object.keys(obj).sort());
    const nonce = Buffer.from(Nacl.randomBytes(16)).toString('base64');
    const expires = Date.now() + (10 * 60 * 1000);
    const payload = canonical({
        v: 1, t: 'replicate', channel, from, to, nonce, expires
    });
    const sig = Buffer.from(Nacl.sign.detached(
        new TextEncoder().encode(payload),
        Buffer.from(keys.signKey, 'base64')
    )).toString('base64');
    return { v: 1, channel, from, to, nonce, expires, sig };
};

// a real pad, derived as the client derives one from a v2 edit hash
const mkPad = () => {
    const keys = PadCrypto.createEditCryptor2(undefined, undefined, undefined);
    return {
        keys,
        channel: Buffer.from(keys.chanId.replace(/-/g, '+').replace(/_/g, '/'),
            'base64').toString('hex')
    };
};

test('browser-minted capability: the server accepts it', () => {
    const pad = mkPad();
    const from = originId(), to = originId();

    const cap = browserMint(pad.channel, pad.keys, from, to);
    const res = Capability.verify(cap, pad.keys.validateKey, {
        channel: pad.channel, from, to
    });
    assert.ok(!res.error,
        `the server rejected a browser-minted capability: ${res.error}`);
});

/*  The whole risk is the two ends encoding the signed payload differently. Pin
    that they agree byte for byte, so a change to either is caught here rather
    than in a console. */
test('browser and server agree on the signed bytes', () => {
    const cap = {
        v: 1, t: 'replicate',
        channel: 'abcdef0123456789abcdef0123456789',
        from: 'AAAA', to: 'BBBB',
        nonce: 'nonce', expires: 1234567890
    };
    const browser = JSON.stringify(cap, Object.keys(cap).sort());
    const server = Codec.canonical(cap).toString('utf8');
    assert.strictEqual(browser, server,
        'the browser and the server must sign identical bytes');
});

/*  The toolbar's Federate button signs in the *outer* frame using CryptPad's own
    helpers (`Util.decodeUTF8`, `Crypto.Nacl.sign.detached`) rather than
    TextEncoder. Pin that those produce the same bytes, so the button and the
    console helper cannot diverge from each other or from the server. */
test('the outer frame\'s helpers produce the same signed bytes', () => {
    const NaclUtil = require(Path.join(CLIENT, 'node_modules', 'tweetnacl-util'));
    const payload = JSON.stringify({ a: 1, b: 'two' });
    assert.deepStrictEqual(
        Buffer.from(NaclUtil.decodeUTF8(payload)),
        Buffer.from(new TextEncoder().encode(payload)),
        'Util.decodeUTF8 and TextEncoder must agree');

    const pad = mkPad();
    const from = originId(), to = originId();
    const cap = browserMint(pad.channel, pad.keys, from, to);

    // re-sign the same claim the way the outer frame does
    const claim = {
        v: 1, t: 'replicate', channel: pad.channel, from, to,
        nonce: cap.nonce, expires: cap.expires
    };
    const outerSig = Buffer.from(Nacl.sign.detached(
        NaclUtil.decodeUTF8(JSON.stringify(claim, Object.keys(claim).sort())),
        Buffer.from(pad.keys.signKey, 'base64')
    )).toString('base64');

    assert.strictEqual(outerSig, cap.sig,
        'the toolbar button and the console helper must sign identically');
});

test('browser-minted capability is refused for a different pad', () => {
    const pad = mkPad(), other = mkPad();
    const from = originId(), to = originId();

    const cap = browserMint(pad.channel, pad.keys, from, to);
    assert.strictEqual(
        Capability.verify(cap, other.keys.validateKey, {}).error, 'E_CAP_SIG',
        'a capability must not verify against another pad\'s key');
});

test('browser-minted capability is refused for a different peer', () => {
    const pad = mkPad();
    const from = originId(), to = originId(), attacker = originId();

    const cap = browserMint(pad.channel, pad.keys, from, to);
    assert.strictEqual(
        Capability.verify(cap, pad.keys.validateKey, { to: attacker }).error,
        'E_CAP_ORIGIN',
        'a capability scoped to one instance must not authorise another');
});

/*  A read-only pad hash yields no signKey, which is what stops a viewer
    federating somebody else's document. Worth pinning: it is the difference
    between "capability" and "anyone who can read it". */
test('a view-only pad has no signing key to mint with', () => {
    const keys = PadCrypto.createEditCryptor2(undefined, undefined, undefined);
    const view = PadCrypto.createViewCryptor2(keys.viewKeyStr, undefined);
    assert.ok(!view.signKey,
        'a view cryptor must not expose a signing key');
    assert.ok(keys.signKey, 'an edit cryptor does');
});
