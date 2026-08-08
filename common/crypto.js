// SPDX-License-Identifier: AGPL-3.0-or-later
// SPDX-FileCopyrightText: 2024 XWiki CryptPad Team <contact@cryptpad.org> and contributors

module.exports = cryptoLib => {
    let exports = {};
    exports.decodeBase64 =  msg => Buffer.from(msg, 'base64');
    exports.encodeBase64 = msg => Buffer.from(msg).toString('base64');
    exports.decodeUTF8 = msg => Buffer.from(msg, 'utf8');
    exports.encodeUTF8 = msg => Buffer.from(msg).toString('utf8');
    let SodiumNative, NaCl;
    switch (cryptoLib) {
        case 'sodiumnative':
            SodiumNative = require("sodium-native");
            exports.sigVerify = (signedMessage, validateKey) => {
                let msg = signedMessage.subarray(64);
                let ok = SodiumNative.crypto_sign_open(msg, signedMessage, validateKey);
                if (!ok) { return false; }
                return msg;
            };
            exports.detachedVerify = (signedBuffer, signatureBuffer, validateKey) => SodiumNative.crypto_sign_verify_detached(signatureBuffer, signedBuffer, validateKey);
            exports.secretbox = (message, nonce, secretKey) => {
                let secretBox = Buffer.alloc(message.length + SodiumNative.crypto_box_MACBYTES);
                SodiumNative.crypto_secretbox_easy(secretBox, message, nonce, secretKey);
                return secretBox;
            };
            exports.secretboxOpen = (secretBox, nonce, secretKey) => {
                let msg = Buffer.alloc(secretBox.length - SodiumNative.crypto_secretbox_MACBYTES);
                if (SodiumNative.crypto_secretbox_open_easy(msg, secretBox, nonce, secretKey)) {
                    return msg;
                } else {
                    return void 0;
                }
            };
            exports.publicKeyFromSecretKey = (secretKey) => {
                let pk = new Uint8Array(SodiumNative.crypto_sign_PUBLICKEYBYTES);
                SodiumNative.crypto_sign_ed25519_sk_to_pk(pk, secretKey);
                return pk;
            };
            exports.signKeyPair = () => {
                const publicKey = Buffer.alloc(SodiumNative.crypto_sign_PUBLICKEYBYTES);
                const secretKey = Buffer.alloc(SodiumNative.crypto_sign_SECRETKEYBYTES);
                SodiumNative.crypto_sign_keypair(publicKey, secretKey);
                return { publicKey, secretKey };
            };
            exports.detachedSign = (messageBuffer, secretKey) => {
                const sig = Buffer.alloc(SodiumNative.crypto_sign_BYTES);
                SodiumNative.crypto_sign_detached(sig, messageBuffer, secretKey);
                return sig;
            };
            break;
        default: // tweetNaCl
            NaCl = require("tweetnacl/nacl-fast");
            exports.sigVerify = NaCl.sign.open;
            exports.detachedVerify = NaCl.sign.detached.verify;
            exports.secretbox = NaCl.secretbox;
            exports.secretboxOpen = NaCl.secretbox.open;
            exports.publicKeyFromSecretKey = (secretKey) => {
                return NaCl.sign?.keyPair?.fromSecretKey(secretKey)?.publicKey;
            };
            exports.signKeyPair = () => {
                const pair = NaCl.sign.keyPair();
                return {
                    publicKey: Buffer.from(pair.publicKey),
                    secretKey: Buffer.from(pair.secretKey)
                };
            };
            exports.detachedSign = (messageBuffer, secretKey) => {
                return Buffer.from(NaCl.sign.detached(
                    new Uint8Array(messageBuffer), new Uint8Array(secretKey)));
            };
            break;
    }
    return exports;
};
