// SPDX-FileCopyrightText: 2023 XWiki CryptPad Team <contact@cryptpad.org> and contributors
//
// SPDX-License-Identifier: AGPL-3.0-or-later

/*  Mulfi-factor auth requires some rudimentary storage methods
    for a number of data types:

* "challenges" (described in challenge.js)
* account settings for MFA (described in mfa.js)
* session tokens (described in sessions.js)

Each data type requires the same three simple methods:

* read
* write
* delete

These could be implemented as tables in a relational database, but committing to a relational DB
is a big decision, so these methods are instead implemented using the filesystem, with each
file's path and naming convention implemented outside of this module.

Feel free to migrate all of these to a relational DB at some point in the future if you like.

*/

/*  Implementation note (2026): these methods no longer talk to the filesystem
    directly. They delegate to an ObjectBackend (common/storage/backend/), which is
    either the local filesystem or an object store, depending on config.storage.type.

    Callers still pass absolute paths, because that is the vocabulary of every module
    that builds on this one (users, invitations, sessions, mfa, support, challenges).
    Those paths are resolved into backend keys relative to the backend's root. A path
    that escapes the root cannot be expressed as an object key, so it is reported as a
    configuration error rather than silently written somewhere else.
*/

const Basic = module.exports;
const Path = require("node:path");
const FsBackend = require("./backend/fs.js");

const mkError = (code, message) => {
    const err = new Error(message || code);
    err.code = code;
    return err;
};

const pathError = (cb) => {
    setTimeout(function () {
        cb(mkError("INVALID_PATH"));
    });
};

/*  The backend is normally installed on Env during node startup. If it is absent we
    build the default filesystem backend on demand, which is exactly the behaviour
    this module had before the backend seam existed.

    The one case we refuse is a node configured for remote storage whose backend was
    never installed: falling back to local disk there would look healthy while
    stranding data on a single machine.  */
const getBackend = (Env) => {
    if (Env && Env.storageBackend) { return Env.storageBackend; }

    const type = Env?.config?.storage?.type;
    if (type && type !== 'fs') {
        throw mkError('E_NO_STORAGE_BACKEND',
            `config.storage.type is '${type}' but no storage backend was installed on this node`);
    }

    const root = Env?.paths?.base;
    if (!root) { return; }

    // create() returns the backend synchronously and prepares its root in the
    // background; every operation below mkdirp's its own parent directory anyway
    const backend = FsBackend.create({ root });
    if (Env) { Env.storageBackend = backend; }
    return backend;
};

/*  Resolve an absolute filesystem path into a key relative to the backend root. */
const keyFromPath = (Env, path) => {
    const backend = getBackend(Env);
    if (!backend) { throw mkError('INVALID_PATH', 'NO_BACKEND'); }

    const root = backend.root || Env?.paths?.base;
    if (!root) { throw mkError('INVALID_PATH', 'NO_ROOT'); }

    const relative = Path.relative(Path.resolve(root), Path.resolve(path));
    if (!relative || relative.startsWith('..') || Path.isAbsolute(relative)) {
        throw mkError('E_PATH_OUTSIDE_ROOT',
            `'${path}' is outside the storage root '${root}'. ` +
            `Paths configured outside the base path cannot be used with object storage.`);
    }
    return { backend, key: relative.split(Path.sep).join('/') };
};

// run `body` with a resolved backend/key, funnelling resolution errors to cb
const withKey = (Env, path, cb, body) => {
    if (!path) { return void pathError(cb); }
    let resolved;
    try {
        resolved = keyFromPath(Env, path);
    } catch (err) {
        return void setTimeout(() => { cb(err); });
    }
    body(resolved.backend, resolved.key);
};

Basic.read = function (Env, path, cb) {
    withKey(Env, path, cb, (backend, key) => {
        backend.get(key, (err, body) => {
            if (err) { return void cb(err); }
            cb(void 0, body.toString('utf8'));
        });
    });
};

/*  Lists the names directly under a path, files and directories alike, matching
    what fs.readdir used to return.

    Note that a missing directory now yields an empty list rather than ENOENT: object
    stores have no directories to be missing. Every caller in the tree treats the two
    cases identically (they either check for ENOENT and return empty, or iterate over
    the result), so this is not a visible change in behaviour.  */
Basic.readDir = function (Env, path, cb) {
    withKey(Env, path, cb, (backend, key) => {
        const prefix = key.endsWith('/') ? key : key + '/';
        backend.list(prefix, { delimiter: '/' }, (err, result) => {
            if (err) { return void cb(err); }
            const names = result.keys.map(entry => entry.key.slice(prefix.length));
            result.prefixes.forEach(dir => {
                names.push(dir.slice(prefix.length).replace(/\/$/, ''));
            });
            cb(void 0, names);
        });
    });
};

/*  Existence without transferring the content. On an object store this is a HEAD
    rather than a GET, which matters for anything checked on a hot path. */
Basic.exists = function (Env, path, cb) {
    withKey(Env, path, cb, (backend, key) => {
        backend.exists(key, cb);
    });
};

Basic.write = function (Env, path, data, cb) {
    withKey(Env, path, cb, (backend, key) => {
        // exclusive create: writes fail with EEXIST if something is already there.
        // This could be made optional in the future if a caller needs to overwrite.
        backend.put(key, data, { ifNoneMatch: true }, err => { cb(err); });
    });
};

/*  Append a line to a small log.

    Intended for the low-frequency logs this module serves — decrees, task
    records — not for channel logs, which append constantly and need the caching
    in common/storage/cache/. On an object store an append below the multipart
    threshold is a read-modify-write, which is fine at this volume and would not
    be at a channel's.  */
Basic.append = function (Env, path, data, cb) {
    withKey(Env, path, cb, (backend, key) => {
        const buffer = Buffer.isBuffer(data) ? data : Buffer.from(String(data), 'utf8');
        backend.append(key, buffer, {}, err => { cb(err); });
    });
};

// TODO I didn't bother implementing the usual "archive/restore/delete-from-archives" methods
// because they didn't seem particularly important for the data implemented with this module.
// They're still worth considering, though, so don't let my ommission stop you.
// Login blocks could probably be implemented with this module if these methods were supported.
// --Aaron
Basic.delete = function (Env, path, cb) {
    withKey(Env, path, cb, (backend, key) => {
        backend.remove(key, cb);
    });
};

Basic.deleteDir = function (Env, path, cb) {
    withKey(Env, path, cb, (backend, key) => {
        const prefix = key.endsWith('/') ? key : key + '/';
        backend.removePrefix(prefix, cb);
    });
};

Basic.archive = function (Env, path, archivePath, cb) {
    withKey(Env, path, cb, (backend, key) => {
        withKey(Env, archivePath, cb, (_backend, archiveKey) => {
            backend.move(key, archiveKey, { overwrite: true }, err => { cb(err); });
        });
    });
};

Basic.restore = function (Env, archivePath, path, cb) {
    withKey(Env, archivePath, cb, (backend, archiveKey) => {
        withKey(Env, path, cb, (_backend, key) => {
            backend.move(archiveKey, key, { overwrite: false }, err => { cb(err); });
        });
    });
};

Basic.isValidId = id => {
    return id && typeof(id) === "string" && /^[a-zA-Z0-9-_+=]+$/.test(id);
};
