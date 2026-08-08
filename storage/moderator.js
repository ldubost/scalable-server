// SPDX-FileCopyrightText: 2023 XWiki CryptPad Team <contact@cryptpad.org> and contributors
//
// SPDX-License-Identifier: AGPL-3.0-or-later

const Basic = require("./storage/basic.js");
const Path = require("node:path");
const nThen = require('nthen');
const Util = require('../common/common-util');

const Moderator = module.exports;
/*  This module manages storage used to implement user management. "Known users" can
    be added here in order to store their public key, their block ID and an alias
    used to recognize them.
*/

const pathFromId = function (Env, id) {
    if (!id || typeof(id) !== 'string') { return; }
    id = Util.escapeKeyCharacters(id);
    if (!Basic.isValidId(id)) { return; }
    return Path.join(Env.paths.base, "support", id.slice(0, 2), id);
};

Moderator.read = function (Env, id, cb) {
    var path = pathFromId(Env, id);
    Basic.read(Env, path, (err, data) => {
        if (err) { return void cb(err.code); }
        cb(void 0, Util.tryParse(data));
    });
};

/*  Formerly synchronous, using fs.readdirSync. Object stores have no synchronous
    equivalent, so this is async now; a missing directory yields an empty list, as
    the synchronous version's try/catch did.  */
Moderator.getAllKeys = function (Env, cb) {
    let keys = [];
    nThen((waitFor) => {
        let dirPath = Path.join(Env.paths.base, "support");
        Basic.readDir(Env, dirPath, waitFor((err, prefixes) => {
            if (err) {
                // no support directory yet: there are no moderators
                if (err.code === 'ENOENT') { return; }
                waitFor.abort();
                return void cb(err.code || err);
            }
            prefixes.forEach((prefix) => {
                let dirPath2 = Path.join(Env.paths.base, "support", prefix);
                Basic.readDir(Env, dirPath2, waitFor((err, newKeys) => {
                    // an unreadable prefix shouldn't hide the moderators in the others
                    if (err) { return; }
                    keys.push(...newKeys);
                }));
            });
        }));
    }).nThen(() => {
        cb(void 0, keys);
    });
};
Moderator.getAll = function (Env, cb) {
    let users = {};
    nThen((waitFor) => {
        let dirPath = Path.join(Env.paths.base, "support");
        Basic.readDir(Env, dirPath, waitFor((err, prefixes) => {
            if (err && err.code === 'ENOENT') { return void cb(void 0, {}); }
            if (err) { waitFor.abort(); return void cb(err.code); }
            prefixes.forEach((prefix) => {
                var dirPath2 = Path.join(Env.paths.base, "support", prefix);
                Basic.readDir(Env, dirPath2, waitFor((err, files) => {
                    if (err) { waitFor.abort(); return void cb(err.code); }
                    files.forEach((id) => {
                        Moderator.read(Env, id, waitFor((err, data) => {
                            users[id] = data || { error: err };
                        }));
                    });
                }));
            });
        }));
    }).nThen(() => {
        cb(null, users);
    });
};

Moderator.write = function (Env, id, data, cb) {
    var path = pathFromId(Env, id);
    Basic.write(Env, path, JSON.stringify(data), (err) => {
        if (err) { return void cb(err.code); }
        cb();
    });
};

Moderator.delete = function (Env, id, cb) {
    var path = pathFromId(Env, id);
    Basic.delete(Env, path, (err) => {
        if (err) { return void cb(err.code); }
        cb();
    });
};



