// SPDX-FileCopyrightText: 2023 XWiki CryptPad Team <contact@cryptpad.org> and contributors
//
// SPDX-License-Identifier: AGPL-3.0-or-later

/*  Login blocks.

    A block is a small (<= 256 byte) encrypted object holding the credentials a
    registered user needs to log in. There is exactly one per account, it is
    replaced wholesale on a password change, and the previous version is archived
    rather than deleted so that a mistaken change can be undone.

    There is no append here and nothing to cache: every operation is a single
    read, write or move of a tiny object. So this goes straight through the
    storage backend via the Basic store rather than through the cached
    append-log machinery that channels need.
*/

const Block = module.exports;
const Util = require("../common-util");
const Core = require("../../common/core");
const Basic = require("../../common/storage/basic.js");
const Path = require("node:path");
const nThen = require("nthen");

Block.mkPath = function (Env, publicKey) {
    // prepare publicKey to be used as a file name
    const safeKey = Util.escapeKeyCharacters(publicKey);

    // validate safeKey
    if (typeof(safeKey) !== 'string') { return; }

    // derive the full path
    // /home/cryptpad/cryptpad/block/fg/fg32kefksjdgjkewrjksdfksjdfsdfskdjfsfd
    return Path.join(Env.paths.block, safeKey.slice(0, 2), safeKey);
};

Block.mkArchivePath = function (Env, publicKey) {
    // prepare publicKey to be used as a file name
    const safeKey = Util.escapeKeyCharacters(publicKey);

    // validate safeKey
    if (typeof(safeKey) !== 'string') {
        return;
    }

    // derive the full path
    // /home/cryptpad/cryptpad/block/fg/fg32kefksjdgjkewrjksdfksjdfsdfskdjfsfd
    return Path.join(Env.paths.archive, 'block', safeKey.slice(0, 2), safeKey);
};

const mkPlaceholderPath = function (Env, publicKey) {
    return Block.mkPath(Env, publicKey) + '.placeholder';
};
const addPlaceholder = function (Env, publicKey, reason, cb) {
    if (!reason) { return cb(); }
    const path = mkPlaceholderPath(Env, publicKey);
    const s_data = typeof(reason) === "string" ? reason : `${reason.code}:${reason.txt}`;
    // a placeholder may already exist from an earlier archival; replace it
    Basic.delete(Env, path, () => {
        Basic.write(Env, path, s_data, cb);
    });
};
const clearPlaceholder = function (Env, publicKey, cb) {
    const path = mkPlaceholderPath(Env, publicKey);
    Basic.delete(Env, path, cb);
};
Block.readPlaceholder = function (Env, publicKey, cb) {
    const path = mkPlaceholderPath(Env, publicKey);
    Basic.read(Env, path, function (err, content) {
        if (err) { return void cb(); }
        cb(content);
    });
};

Block.archive = function (Env, publicKey, reason, _cb) {
    const cb = Util.once(Util.mkAsync(_cb));

    // derive the filepath
    const currentPath = Block.mkPath(Env, publicKey);

    // make sure the path is valid
    if (typeof(currentPath) !== 'string') {
        return void cb('E_INVALID_BLOCK_PATH');
    }

    const archivePath = Block.mkArchivePath(Env, publicKey);
    // make sure the path is valid
    if (typeof(archivePath) !== 'string') {
        return void cb('E_INVALID_BLOCK_ARCHIVAL_PATH');
    }

    // TODO Env.incrementBytesWritten
    Basic.archive(Env, currentPath, archivePath, (err) => {
        cb(err);
        if (!err && reason) { addPlaceholder(Env, publicKey, reason, () => {}); }
    });
};

Block.restore = function (Env, publicKey, _cb) {
    const cb = Util.once(Util.mkAsync(_cb));

    // derive the filepath
    const livePath = Block.mkPath(Env, publicKey);

    // make sure the path is valid
    if (typeof(livePath) !== 'string') {
        return void cb('E_INVALID_BLOCK_PATH');
    }

    const archivePath = Block.mkArchivePath(Env, publicKey);
    // make sure the path is valid
    if (typeof(archivePath) !== 'string') {
        return void cb('E_INVALID_BLOCK_ARCHIVAL_PATH');
    }

    // TODO Env.incrementBytesWritten
    Basic.restore(Env, archivePath, livePath, (err) => {
        cb(err);
        if (!err) { clearPlaceholder(Env, publicKey, () => {}); }
    });
};

const isValidKey = Block.isValidKey = function (publicKey) {
    return typeof(publicKey) === 'string' && publicKey.length === 44;
};

const checkPath = function (Env, publicKey, pathFunction, _cb) {
    const cb = Util.once(Util.mkAsync(_cb));
    if (!isValidKey(publicKey)) { return void cb("INVALID_ARGS"); }
    const path = pathFunction(Env, publicKey);
    Basic.exists(Env, path, cb);
};

Block.isAvailable = function (Env, publicKey, _cb) {
    checkPath(Env, publicKey, Block.mkPath, _cb);
};

Block.isArchived = function (Env, publicKey, _cb) {
    checkPath(Env, publicKey, Block.mkArchivePath, _cb);
};

Block.check = function (Env, publicKey, _cb, noRedirect) { // 'check' because 'exists' implies boolean
    const cb = Util.once(Util.mkAsync(_cb));

    if (!noRedirect && !Core.checkStorage(Env, publicKey, 'BLOCK_CHECK', {
        blockId: publicKey
    }, cb)) { return; }

    const path = Block.mkPath(Env, publicKey);
    // callers expect an error when the block is absent, not a boolean
    Basic.exists(Env, path, (err, exists) => {
        if (err) { return void cb(err); }
        if (!exists) {
            const e = new Error('ENOENT');
            e.code = 'ENOENT';
            return void cb(e);
        }
        cb();
    });
};

Block.MAX_SIZE = 256;

Block.write = function (Env, publicKey, buffer, _cb) {
    const cb = Util.once(Util.mkAsync(_cb));
    const path = Block.mkPath(Env, publicKey);
    if (typeof(path) !== 'string') { return void cb('INVALID_PATH'); }

    nThen(function (w) {
        Block.archive(Env, publicKey, 'PASSWORD_CHANGE', w(function (/* err */) {
    /*
        we proceed even if there are errors.
        it might be ENOENT (there is no file to archive)
        or EACCES (bad filesystem permissions for the existing archived block?)
        or lots of other things, none of which justify preventing the write
    */
        }));
    }).nThen(function () {
        /*  The archive above moved any previous block out of the way, so this is
            normally a create. Delete first regardless: an archival that failed for
            some reason other than "nothing to archive" must not leave the account
            stuck with an un-replaceable block.  */
        Basic.delete(Env, path, () => {
            Basic.write(Env, path, buffer, cb);
        });
        //Env.incrementBytesWritten(buffer && buffer.length);
    });
};
