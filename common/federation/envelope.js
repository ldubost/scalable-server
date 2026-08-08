// SPDX-License-Identifier: AGPL-3.0-or-later
// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors

/*  Envelopes: the unit of replication (spec §5.1, R-14…R-18).

    An envelope wraps one pad message with the ordering metadata the receiving
    instance needs in order to place it in the same total order as everybody
    else. Its shape:

        {
          c   channel id
          m   the message content, verbatim: base64(sig ‖ ciphertext)
          id  getHash(m) -- content-derived, so every instance agrees      R-14
          o   originId of the instance that accepted m from its client
          s   per-(channel, origin) sequence number, gapless               R-17
          l   Lamport clock                                                §4.2
          t   origin's wall clock, advisory only -- NEVER an ordering input
          sig origin's signature over the ordering metadata                R-16
        }

    Two independent authorities, and keeping them separate is the whole design:

      * `m` authenticates itself against the channel's `validateKey`, which every
        replica re-checks for itself (R-15). No instance has to trust another
        about content. This is why replication is a pure data-plane operation.
      * `sig` authenticates only the *ordering* claim -- "I, origin o, accepted
        this message as my sequence s at Lamport time l". A malicious peer can
        lie about ordering for its own origin and nothing else; it cannot forge
        pad content, and it cannot forge another origin's ordering.

    `t` is carried for operators and diagnostics. It is deliberately excluded
    from the sort key: docs/experiments/chainpad-ordering showed ChainPad never
    receives server timestamps at all, so ordering by them would add a
    clock-skew dependency to buy nothing. See common/federation/order.js.
*/

const Crypto = require('../crypto.js')('sodiumnative');
const Codec = require('./codec.js');
const Ids = require('./ids.js');

const VERSION = 1;

const err = (why) => ({ error: `E_ENV_${why}` });

/*  The signed portion: identity and ordering, never the content. `m` is not
    signed here because it already carries its own ed25519 signature, and
    re-signing it would let a peer's key substitute for the pad's. `id` IS
    signed, and since id is derived from m (R-14), that binds the envelope to
    its message without granting the origin any authority over content. */
const orderingBytes = (env) => Codec.canonical({
    v: VERSION,
    c: env.c,
    id: env.id,
    o: env.o,
    s: env.s,
    l: env.l,
    t: env.t
});

const isB64 = (s, bytes) => {
    if (typeof (s) !== 'string' || !s) { return false; }
    try {
        const buf = Buffer.from(s, 'base64');
        return buf.length === bytes && buf.toString('base64') === s;
    } catch (e) { return false; }
};

const isSeq = (n) => typeof (n) === 'number' && Number.isInteger(n) && n >= 0;

/*  Build and sign an envelope for a message this instance has just accepted.
    `id` is computed, never supplied, so R-14 holds by construction. */
const create = (opts, identity) => {
    const env = {
        v: VERSION,
        c: opts.channel,
        m: opts.content,
        id: Ids.fromContent(opts.content),
        o: identity.originId,
        s: opts.seq,
        l: opts.lamport,
        t: typeof (opts.time) === 'number' ? opts.time : Date.now()
    };
    env.sig = Crypto.encodeBase64(identity.sign(orderingBytes(env)));
    return env;
};

/*  Structural validation of an envelope from the wire. Cheap, synchronous, and
    done before anything expensive: verifying the origin's signature, and above
    all revalidating `m` against the channel's validateKey (R-15), which is the
    costly part and is done by the caller through the worker pool. */
const check = (env, expect) => {
    if (!env || typeof (env) !== 'object') { return err('MALFORMED'); }
    if (env.v !== VERSION) { return err('VERSION'); }
    if (typeof (env.c) !== 'string' || !env.c) { return err('CHANNEL'); }
    if (typeof (env.m) !== 'string' || !env.m) { return err('CONTENT'); }
    if (!isB64(env.o, 32)) { return err('ORIGIN'); }
    if (!isSeq(env.s)) { return err('SEQ'); }
    if (!isSeq(env.l)) { return err('LAMPORT'); }
    if (typeof (env.t) !== 'number' || !isFinite(env.t)) { return err('TIME'); }
    if (!isB64(env.sig, 64)) { return err('SIG'); }
    if (!Ids.isValid(env.id)) { return err('ID'); }

    /*  R-14: the id must be the one the content implies. A peer that supplies a
        mismatched id is trying to make one message occupy another's position in
        the order, or to make the same content appear twice. */
    let derived;
    try {
        derived = Ids.fromContent(env.m);
    } catch (e) { return err('CONTENT'); }
    if (derived !== env.id) { return err('ID_MISMATCH'); }

    if (expect?.channel && env.c !== expect.channel) { return err('CHANNEL'); }
    if (expect?.origin && env.o !== expect.origin) { return err('ORIGIN'); }

    return { env };
};

/*  R-16: verify the ordering metadata was signed by the origin it names.

    Note this is verified against the *origin's* key, not the key of the peer
    that handed it to us. Envelopes are relayed: instance C may forward an
    envelope that originated at A. C cannot alter A's ordering claim, which is
    what makes relaying safe and anti-entropy (§4.7) possible.
*/
const verify = (env, originPublicKey) => {
    let ok = false;
    try {
        ok = Crypto.detachedVerify(
            orderingBytes(env), Crypto.decodeBase64(env.sig), originPublicKey);
    } catch (e) { ok = false; }
    return ok ? { env } : err('SIG');
};

// Convenience: check structure, then signature, in the order that fails cheapest.
const checkAndVerify = (env, originPublicKey, expect) => {
    const structural = check(env, expect);
    if (structural.error) { return structural; }
    return verify(env, originPublicKey);
};

module.exports = {
    create, check, verify, checkAndVerify, orderingBytes, VERSION
};
