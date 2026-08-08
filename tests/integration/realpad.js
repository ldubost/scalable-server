// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors
//
// SPDX-License-Identifier: AGPL-3.0-or-later

/*  The real client libraries, for tests that drive an actual document.
 *
 *  `chainpad` and `chainpad-crypto` are the browser's, not the server's — the
 *  server has no idea either exists. They come from the client checkout that
 *  belongs to this server (`../cryptpad`, the same one `clientRoot` points at),
 *  so a test exercises the code a user would actually be running.
 */

const Path = require('node:path');

const CLIENT = process.env.CRYPTPAD_CLIENT ||
    Path.join(__dirname, '..', '..', '..', 'cryptpad');

const ChainPad = require(Path.join(CLIENT, 'node_modules', 'chainpad'));
const Crypto = require(Path.join(CLIENT, 'node_modules', 'chainpad-crypto'));

/*  A pad's key material, exactly as the client derives it from the URL fragment:
    one seed produces the signing keypair (whose public half the servers know as
    `validateKey`) and the symmetric key the servers never see. */
const mkCryptor = (seed) => {
    const keys = Crypto.createEditCryptor(undefined, seed);
    const enc = Crypto.createEncryptor(keys);
    return {
        signKey: keys.signKey,
        validateKey: keys.validateKey,
        cryptKey: keys.cryptKey,
        editKeyStr: keys.editKeyStr,
        encrypt: enc.encrypt,
        decrypt: enc.decrypt
    };
};

module.exports = { ChainPad, Crypto, mkCryptor, CLIENT };
