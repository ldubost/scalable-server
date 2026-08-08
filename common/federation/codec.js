// SPDX-License-Identifier: AGPL-3.0-or-later
// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors

/*  The Codec seam (design C-4, §11).

    Everything that crosses the federation wire is encoded here and nowhere else.
    Today that is JSON; NextGraph uses BARE. Swapping this file is the whole
    change, which only holds if the rule is obeyed:

        No file outside this one may call JSON.stringify/JSON.parse on wire data.

    `canonical` matters more than it looks. Signatures (R-16) are taken over the
    encoded bytes, so both ends must encode the same object to the same bytes.
    JSON.stringify does not guarantee that across implementations, so signed
    payloads are encoded with sorted keys.
*/

const sortKeys = (value) => {
    if (Array.isArray(value)) { return value.map(sortKeys); }
    if (value === null || typeof (value) !== 'object') { return value; }
    const out = {};
    Object.keys(value).sort().forEach(k => {
        if (typeof (value[k]) === 'undefined') { return; }
        out[k] = sortKeys(value[k]);
    });
    return out;
};

const encode = (obj) => Buffer.from(JSON.stringify(obj), 'utf8');

// Byte-stable encoding, for anything that will be signed or verified.
const canonical = (obj) => Buffer.from(JSON.stringify(sortKeys(obj)), 'utf8');

const decode = (buf) => {
    try {
        return JSON.parse(Buffer.isBuffer(buf) ? buf.toString('utf8') : String(buf));
    } catch (err) {
        return undefined;
    }
};

module.exports = { encode, decode, canonical, name: 'json' };
