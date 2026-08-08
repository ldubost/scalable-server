// SPDX-License-Identifier: AGPL-3.0-or-later
// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors

/*  The IdCodec seam (design C-4, §11).

    A federated message id is derived from its content and nothing else, so every
    instance that ever sees the message computes the same id. Today that is
    CryptPad's existing `getHash`: the first 64 base64 characters of the message,
    which are the first 48 bytes of a *deterministic* ed25519 signature
    (`storage/hk-util.js:29-37`, spec §1.2). NextGraph would use BLAKE3 here.

    Rules that keep the seam real (design §11):

      * No file outside this one may call `HKUtil.getHash` on a federated path,
        assume an id is 64 characters, or slice one.
      * Ids are opaque strings to every caller. Compare them with `compare`,
        never with `<` or `.slice()`.

    `fromContent` is intentionally the *only* way to obtain an id. R-14 requires
    that an envelope's `id` equal `getHash(m)`, and the cheapest way to guarantee
    that is to never let a caller supply one.
*/

// Matches HK.getHash: the first 64 base64 chars of the signed message.
const ID_LENGTH = 64;

const err = (why) => new Error(`E_ID: ${why}`);

/*  Derive the id of a message from its wire content -- base64 of
    `ed25519_signature ‖ ciphertext`, possibly prefixed with `cp|<hash>|`
    for a checkpoint.

    The checkpoint prefix is stripped first. It is added by the *client* and is
    not part of the signature, so leaving it in would give the same message two
    different ids depending on whether it was seen as a checkpoint. */
const CHECKPOINT_PREFIX = /^cp\|(([A-Za-z0-9+/=]+)\|)?/;

const fromContent = (content) => {
    if (typeof (content) !== 'string' || !content) {
        throw err('content must be a non-empty string');
    }
    const bare = content.replace(CHECKPOINT_PREFIX, '');
    if (bare.length < ID_LENGTH) {
        throw err('content is too short to carry a signature');
    }
    return bare.slice(0, ID_LENGTH);
};

// Is this a well-formed id? Used when an id arrives from a peer (R-14).
const isValid = (id) => {
    if (typeof (id) !== 'string' || id.length !== ID_LENGTH) { return false; }
    // base64 alphabet only; ids are a signature prefix, so never padded
    return /^[A-Za-z0-9+/]+$/.test(id);
};

/*  Total order on ids, for the (l, o, id) sort key. Deliberately a plain
    lexical comparison rather than localeCompare: every instance must agree, and
    localeCompare depends on the runtime's ICU data. */
const compare = (a, b) => {
    if (!isValid(a) || !isValid(b)) { throw err('cannot order malformed ids'); }
    return a < b ? -1 : (a > b ? 1 : 0);
};

// True when `content` carries a checkpoint marker (spec §4.5, R-10).
const isCheckpoint = (content) =>
    typeof (content) === 'string' && CHECKPOINT_PREFIX.test(content);

module.exports = {
    fromContent, isValid, compare, isCheckpoint,
    ID_LENGTH,
    name: 'sigprefix'
};
