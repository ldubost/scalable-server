// SPDX-License-Identifier: AGPL-3.0-or-later
// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors

/*  Instance federation identity (spec R-23).

    Every instance that federates has one persistent ed25519 keypair. The public
    half is published at /api/federation and is how peers name this instance; the
    private half signs the handshake (R-24) and, later, ordering metadata on
    envelopes (R-16).

    The key lives on disk, outside the object backend, because it identifies the
    *instance* rather than any channel: it must not move when storage is
    resharded, and it must not end up in S3 with the pad data.

    The keypair is generated on first use. Generating it is the only way an
    operator can accidentally change their instance's identity, so it is written
    with an exclusive create and never overwritten.
*/

const Fs = require('node:fs');
const Path = require('node:path');
const Crypto = require('../crypto.js')('sodiumnative');

const FILE = 'identity.json';

// The origin id: how peers name this instance on the wire. The public key in
// base64 is the identity; the URL is a hint for dialling, never an authority.
const originId = (publicKey) => Crypto.encodeBase64(publicKey);

const parse = (raw, path) => {
    let obj;
    try {
        obj = JSON.parse(raw);
    } catch (err) {
        throw new Error(`E_FEDERATION_IDENTITY_CORRUPT: ${path}`);
    }
    if (obj?.version !== 1 || typeof (obj.secretKey) !== 'string') {
        throw new Error(`E_FEDERATION_IDENTITY_UNSUPPORTED: ${path}`);
    }
    const secretKey = Crypto.decodeBase64(obj.secretKey);
    const publicKey = Buffer.from(Crypto.publicKeyFromSecretKey(secretKey));

    /*  A stored public key that disagrees with the one derived from the secret
        means the file was hand-edited or half-written. Refuse rather than
        federate under an identity peers will not recognise. */
    if (typeof (obj.publicKey) === 'string' &&
        obj.publicKey !== Crypto.encodeBase64(publicKey)) {
        throw new Error(`E_FEDERATION_IDENTITY_MISMATCH: ${path}`);
    }
    return { publicKey, secretKey };
};

const mk = (pair, path, generated) => {
    const { publicKey, secretKey } = pair;
    return {
        path,
        generated: Boolean(generated),
        publicKey,
        originId: originId(publicKey),
        sign: (messageBuffer) => Crypto.detachedSign(messageBuffer, secretKey),
        verify: (messageBuffer, signature, theirPublicKey) => {
            try {
                return Crypto.detachedVerify(messageBuffer, signature, theirPublicKey);
            } catch (err) {
                return false;
            }
        }
    };
};

/*  Load the instance keypair, generating it if this instance has never
    federated before. Synchronous on purpose: nothing may serve federation
    traffic before the identity exists, and this runs once at startup. */
const load = (dir) => {
    const path = Path.join(dir, FILE);
    let raw;
    try {
        raw = Fs.readFileSync(path, 'utf8');
    } catch (err) {
        if (err.code !== 'ENOENT') { throw err; }
    }

    if (typeof (raw) === 'string') { return mk(parse(raw, path), path); }

    const pair = Crypto.signKeyPair();
    const body = JSON.stringify({
        version: 1,
        publicKey: Crypto.encodeBase64(pair.publicKey),
        secretKey: Crypto.encodeBase64(pair.secretKey)
    }, null, 2) + '\n';

    Fs.mkdirSync(dir, { recursive: true });
    try {
        // wx: never clobber an identity that appeared while we were looking
        Fs.writeFileSync(path, body, { mode: 0o600, flag: 'wx' });
    } catch (err) {
        if (err.code !== 'EEXIST') { throw err; }
        return mk(parse(Fs.readFileSync(path, 'utf8'), path), path);
    }
    return mk(pair, path, true);
};

module.exports = { load, originId, FILE };
