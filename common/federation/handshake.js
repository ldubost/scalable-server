// SPDX-License-Identifier: AGPL-3.0-or-later
// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors

/*  Mutual instance-key authentication (spec R-24), shared by both ends.

    Three frames, and the point of each:

        dialler -> listener   HELLO      "I claim to be <originId>", + my nonce
        listener -> dialler   CHALLENGE  listener's nonce + its proof over both
        dialler -> listener   PROOF      dialler's proof over both

    Each side signs *both* nonces, so neither can be replayed into a different
    session and neither side can pick the whole signed payload. The transcript
    also carries both origin ids, which binds the proof to who the signer thinks
    it is talking to: a proof captured from a session with A cannot be replayed
    to B.

    This authenticates the *instance key*, not the TLS channel. R-24 says the
    session runs over TLS; that provides confidentiality and integrity, this
    provides the identity that the peering policy (R-27) decides about.
*/

const Crypto = require('../crypto.js')('sodiumnative');
const Codec = require('./codec.js');

const NONCE_BYTES = 32;
const VERSION = 1;

// how long a peer has to complete the handshake before we hang up
const TIMEOUT = 20 * 1000;
// how far apart the two clocks may be
const MAX_SKEW = 5 * 60 * 1000;

const nonce = () => require('node:crypto').randomBytes(NONCE_BYTES).toString('base64');

/*  The bytes both sides sign. Everything that identifies the session goes in,
    in a fixed order, through the canonical encoder so the two ends agree
    byte-for-byte. `role` differs between the two proofs, so the listener's
    signature can never be replayed back as the dialler's. */
const transcript = (role, hello, challenge) => Codec.canonical({
    v: VERSION,
    role,                        // 'dialler' | 'listener'
    dialler: hello.originId,
    listener: challenge.originId,
    dnonce: hello.nonce,
    lnonce: challenge.nonce
});

const badTime = (t) => {
    if (typeof (t) !== 'number' || !isFinite(t)) { return true; }
    return Math.abs(Date.now() - t) > MAX_SKEW;
};

const isB64 = (s, bytes) => {
    if (typeof (s) !== 'string') { return false; }
    const buf = Crypto.decodeBase64(s);
    return buf.length === bytes && Crypto.encodeBase64(buf) === s;
};

// ---------------------------------------------------------------- dialler

const mkHello = (identity) => ({
    type: 'HELLO',
    v: VERSION,
    originId: identity.originId,
    nonce: nonce(),
    time: Date.now()
});

/*  Dialler verifies the listener really holds the key for the originId we
    dialled, then produces its own proof. Returns { error } or { proof }. */
const onChallenge = (identity, hello, challenge, expectedOriginId) => {
    if (challenge?.type !== 'CHALLENGE' || challenge.v !== VERSION) {
        return { error: 'EBADFRAME' };
    }
    if (!isB64(challenge.originId, 32)) { return { error: 'EBADORIGIN' }; }
    if (!isB64(challenge.nonce, NONCE_BYTES)) { return { error: 'EBADNONCE' }; }
    if (badTime(challenge.time)) { return { error: 'ECLOCKSKEW' }; }

    /*  We dialled a URL because we believed it served a particular instance.
        If it answers with a different key, this is not the peer we authorised —
        no policy decision can rescue that. */
    if (expectedOriginId && challenge.originId !== expectedOriginId) {
        return { error: 'EWRONGPEER' };
    }
    if (challenge.originId === identity.originId) { return { error: 'ESELF' }; }

    const ok = identity.verify(
        transcript('listener', hello, challenge),
        Crypto.decodeBase64(challenge.sig),
        Crypto.decodeBase64(challenge.originId)
    );
    if (!ok) { return { error: 'EBADSIG' }; }

    return {
        proof: {
            type: 'PROOF',
            v: VERSION,
            sig: Crypto.encodeBase64(identity.sign(transcript('dialler', hello, challenge)))
        }
    };
};

// ---------------------------------------------------------------- listener

/*  Listener checks the HELLO is well-formed and answers with its own nonce and
    proof. It does NOT decide admission here: the policy (R-27) is consulted
    only once the dialler has proved the key, so an unauthenticated caller
    cannot probe the allowlist. */
const onHello = (identity, hello) => {
    if (hello?.type !== 'HELLO' || hello.v !== VERSION) {
        return { error: 'EBADFRAME' };
    }
    if (!isB64(hello.originId, 32)) { return { error: 'EBADORIGIN' }; }
    if (!isB64(hello.nonce, NONCE_BYTES)) { return { error: 'EBADNONCE' }; }
    if (badTime(hello.time)) { return { error: 'ECLOCKSKEW' }; }
    if (hello.originId === identity.originId) { return { error: 'ESELF' }; }

    const challenge = {
        type: 'CHALLENGE',
        v: VERSION,
        originId: identity.originId,
        nonce: nonce(),
        time: Date.now()
    };
    challenge.sig = Crypto.encodeBase64(
        identity.sign(transcript('listener', hello, challenge)));
    return { challenge };
};

/*  Final step: the dialler's proof. On success the caller may consult the
    peering policy for hello.originId. */
const onProof = (identity, hello, challenge, proof) => {
    if (proof?.type !== 'PROOF' || proof.v !== VERSION) {
        return { error: 'EBADFRAME' };
    }
    if (!isB64(proof.sig, 64)) { return { error: 'EBADSIG' }; }

    const ok = identity.verify(
        transcript('dialler', hello, challenge),
        Crypto.decodeBase64(proof.sig),
        Crypto.decodeBase64(hello.originId)
    );
    if (!ok) { return { error: 'EBADSIG' }; }
    return { originId: hello.originId };
};

module.exports = {
    VERSION, TIMEOUT, NONCE_BYTES,
    mkHello, onHello, onChallenge, onProof,
    transcript
};
