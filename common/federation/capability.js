// SPDX-License-Identifier: AGPL-3.0-or-later
// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors

/*  Replication capabilities (spec R-25).

    The authorisation problem: instance B asks instance A to replicate a pad. A
    must decide whether whoever asked is allowed to make that happen. A cannot
    read the pad — it only ever sees ciphertext — and there is no cross-instance
    account system.

    The answer needs no new key material. A pad already has an ed25519 signing
    keypair: the private half lives in the URL fragment and never reaches any
    server, and the public half is the channel's `validateKey`, which every
    server already stores in order to validate messages. So:

        the browser, which has the signing key, signs a capability
        any server can verify it against the validateKey it already holds

    Proving the right to replicate a pad therefore requires exactly what proving
    the right to *write* to it requires, and the server learns nothing new.

    Minting happens in the client (`../cryptpad`); this module is shared so both
    ends verify with identical bytes.
*/

const Crypto = require('../crypto.js')('sodiumnative');
const Codec = require('./codec.js');

const VERSION = 1;

// Capabilities are short-lived: they authorise setting replication up, not
// keeping it running. The replica set, once recorded, is what persists.
const MAX_LIFETIME = 10 * 60 * 1000;

const err = (why) => ({ error: `E_CAP_${why}` });

/*  The exact bytes that get signed. Through the canonical encoder so that a
    browser and a server, on different runtimes, produce identical input.

    Every field that scopes the grant is included. In particular BOTH origins
    are named: a capability minted to let A replicate to B must not be
    replayable to make A replicate to some third instance C. */
const payload = (cap) => Codec.canonical({
    v: VERSION,
    t: 'replicate',
    channel: cap.channel,
    from: cap.from,        // originId granting the replica
    to: cap.to,            // originId receiving it
    nonce: cap.nonce,
    expires: cap.expires
});

const isB64 = (s, bytes) => {
    if (typeof (s) !== 'string' || !s) { return false; }
    try {
        const buf = Buffer.from(s, 'base64');
        return buf.length === bytes && buf.toString('base64') === s;
    } catch (e) { return false; }
};

/*  Mint a capability. Server-side this is only used by tests and tooling — in
    production step 1 of design §5.2 runs in the browser, which is the only
    place the signing key exists. */
const mint = (opts, signingKey) => {
    const cap = {
        v: VERSION,
        channel: opts.channel,
        from: opts.from,
        to: opts.to,
        nonce: require('node:crypto').randomBytes(16).toString('base64'),
        expires: Date.now() + Math.min(opts.lifetime || MAX_LIFETIME, MAX_LIFETIME)
    };
    cap.sig = Crypto.encodeBase64(Crypto.detachedSign(payload(cap), signingKey));
    return cap;
};

/*  Verify a capability against the channel's validateKey.

    `expect` pins what this server believes the capability should say, so a
    capability for a different channel or a different pair of instances is
    refused even though its signature is perfectly good. Verifying the signature
    alone would authorise the wrong thing.
*/
const verify = (cap, validateKey, expect) => {
    if (!cap || typeof (cap) !== 'object') { return err('MALFORMED'); }
    if (cap.v !== VERSION) { return err('VERSION'); }
    if (typeof (cap.channel) !== 'string' || !cap.channel) { return err('CHANNEL'); }
    if (!isB64(cap.from, 32) || !isB64(cap.to, 32)) { return err('ORIGIN'); }
    if (!isB64(cap.sig, 64)) { return err('SIG'); }
    if (typeof (cap.nonce) !== 'string' || !cap.nonce) { return err('NONCE'); }

    if (typeof (cap.expires) !== 'number' || !isFinite(cap.expires)) {
        return err('EXPIRY');
    }
    if (cap.expires <= Date.now()) { return err('EXPIRED'); }
    /*  Refuse a capability minted with an absurd lifetime even if it verifies:
        a long-lived one is a bearer token for replication. */
    if (cap.expires - Date.now() > MAX_LIFETIME) { return err('LIFETIME'); }

    if (expect) {
        if (expect.channel && cap.channel !== expect.channel) { return err('CHANNEL'); }
        if (expect.from && cap.from !== expect.from) { return err('ORIGIN'); }
        if (expect.to && cap.to !== expect.to) { return err('ORIGIN'); }
    }

    let key;
    try {
        key = Crypto.decodeBase64(validateKey);
    } catch (e) { return err('VALIDATEKEY'); }
    if (!key || key.length !== 32) { return err('VALIDATEKEY'); }

    let ok = false;
    try {
        ok = Crypto.detachedVerify(payload(cap), Crypto.decodeBase64(cap.sig), key);
    } catch (e) { ok = false; }
    if (!ok) { return err('SIG'); }

    return { cap };
};

/*  R-18-style replay defence for the setup path. A capability is single-use:
    its nonce is remembered until it would have expired anyway, so a captured
    one cannot be used twice. Bounded by MAX_LIFETIME, so the set cannot grow
    without limit. */
const mkNonceCache = () => {
    const seen = new Map();
    return {
        // true if this is the first time we have seen the nonce
        claim: (cap) => {
            const now = Date.now();
            for (const [k, exp] of seen) {
                if (exp <= now) { seen.delete(k); }
            }
            const key = `${cap.channel}:${cap.nonce}`;
            if (seen.has(key)) { return false; }
            seen.set(key, cap.expires);
            return true;
        },
        size: () => seen.size
    };
};

module.exports = { mint, verify, payload, mkNonceCache, VERSION, MAX_LIFETIME };
