// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors
//
// SPDX-License-Identifier: AGPL-3.0-or-later

/*  Local filesystem implementation of the ObjectBackend interface
    (see ./types.d.ts).

    This is the default backend, and it is also the reference against which remote
    backends are tested: the same conformance suite runs against this and against S3,
    so the two cannot drift.

    Keys are '/'-separated and relative to `root`. They map onto paths under `root`
    verbatim, which means an instance migrated from local disk to S3 (or back) has the
    same layout on both sides.
*/

const Fs = require("node:fs");
const Path = require("node:path");
const Fse = require("fs-extra");

const { isValidKey, mkError } = require("./key.js");

// 511 -> octal 777, matching the permissions used elsewhere in the tree
const PERMISSIVE = 511;

/*  A cheap stand-in for an S3 entity tag. It must change whenever the content
    changes; size plus high-resolution mtime satisfies that for our access patterns
    (every write we make either changes the length or goes through a fresh file). */
const mkEtag = stats => {
    return `"${stats.size}-${Math.round(stats.mtimeMs * 1000)}"`;
};

const mkStat = stats => {
    return {
        size: stats.size,
        etag: mkEtag(stats),
        mtime: stats.mtimeMs
    };
};

module.exports.create = (conf, _cb) => {
    const cb = typeof (_cb) === 'function' ? _cb : () => {};
    const root = Path.resolve(conf.root);

    const toPath = key => Path.join(root, key);

    // temp files live beside their target so the rename/link below stays on one device
    const mkTempPath = path => {
        return `${path}.tmp.${process.pid}.${Math.random().toString(36).slice(2, 10)}`;
    };

    const head = (key, cb) => {
        if (!isValidKey(key)) { return void cb(mkError('EINVAL', 'INVALID_KEY')); }
        Fs.stat(toPath(key), (err, stats) => {
            if (err) { return void cb(err); }
            if (!stats.isFile()) { return void cb(mkError('ENOENT', 'NOT_A_FILE')); }
            cb(void 0, mkStat(stats));
        });
    };

    const exists = (key, cb) => {
        head(key, (err, stat) => {
            if (err) {
                if (err.code === 'ENOENT') { return void cb(void 0, false); }
                return void cb(err);
            }
            cb(void 0, Boolean(stat));
        });
    };

    const get = (key, cb) => {
        if (!isValidKey(key)) { return void cb(mkError('EINVAL', 'INVALID_KEY')); }
        Fs.readFile(toPath(key), (err, body) => {
            if (err) { return void cb(err); }
            cb(void 0, body);
        });
    };

    const getStream = (key, opts, cb) => {
        if (!isValidKey(key)) { return void cb(mkError('EINVAL', 'INVALID_KEY')); }
        opts = opts || {};
        const path = toPath(key);

        // createReadStream defers ENOENT to an 'error' event; callers of this
        // interface expect the error on the callback, so stat first.
        Fs.stat(path, err => {
            if (err) { return void cb(err); }
            const streamOpts = {};
            if (typeof (opts.start) === 'number') { streamOpts.start = opts.start; }
            if (typeof (opts.end) === 'number') { streamOpts.end = opts.end; }
            cb(void 0, Fs.createReadStream(path, streamOpts));
        });
    };

    /*  Write to a temp file first so a reader never observes a half-written object.
        The final step differs by mode:
          - exclusive (ifNoneMatch): link(), which fails with EEXIST and never clobbers
          - otherwise: rename(), which is atomic and replaces any existing object    */
    const put = (key, body, opts, cb) => {
        if (!isValidKey(key)) { return void cb(mkError('EINVAL', 'INVALID_KEY')); }
        opts = opts || {};
        const path = toPath(key);
        const temp = mkTempPath(path);

        const cleanup = (err) => {
            Fs.unlink(temp, () => { cb(err); });
        };

        const commit = () => {
            const finish = () => {
                Fs.stat(path, (err, stats) => {
                    if (err) { return void cb(err); }
                    cb(void 0, mkStat(stats));
                });
            };

            if (opts.ifNoneMatch) {
                return void Fs.link(temp, path, err => {
                    if (err) {
                        return void cleanup(err.code === 'EEXIST' ?
                            mkError('EEXIST', 'KEY_EXISTS') : err);
                    }
                    Fs.unlink(temp, () => { finish(); });
                });
            }

            const rename = () => {
                Fs.rename(temp, path, err => {
                    if (err) { return void cleanup(err); }
                    finish();
                });
            };

            if (!opts.ifMatch) { return void rename(); }

            // best-effort compare-and-swap: the fs backend has no atomic equivalent,
            // so this narrows the window rather than closing it
            head(key, (err, stat) => {
                if (err && err.code !== 'ENOENT') { return void cleanup(err); }
                if (!stat || stat.etag !== opts.ifMatch) {
                    return void cleanup(mkError('EPRECONDITION', 'ETAG_MISMATCH'));
                }
                rename();
            });
        };

        Fse.mkdirp(Path.dirname(path), PERMISSIVE, err => {
            if (err && err.code !== 'EEXIST') { return void cb(err); }

            if (typeof (body) === 'string' || Buffer.isBuffer(body)) {
                return void Fs.writeFile(temp, body, err => {
                    if (err) { return void cleanup(err); }
                    commit();
                });
            }

            if (!body || typeof (body.pipe) !== 'function') {
                return void cb(mkError('EINVAL', 'INVALID_BODY'));
            }

            const stream = Fs.createWriteStream(temp);
            let failed = false;
            const onError = err => {
                if (failed) { return; }
                failed = true;
                cleanup(err);
            };
            body.on('error', onError);
            stream.on('error', onError);
            stream.on('close', () => {
                if (failed) { return; }
                commit();
            });
            body.pipe(stream);
        });
    };

    /*  The local filesystem can genuinely append, so this is the cheap path.
        Remote backends have to work harder (see the S3 backend's multipart copy). */
    const append = (key, tail, opts, cb) => {
        if (!isValidKey(key)) { return void cb(mkError('EINVAL', 'INVALID_KEY')); }
        if (!Buffer.isBuffer(tail)) { return void cb(mkError('EINVAL', 'INVALID_TAIL')); }
        opts = opts || {};
        const path = toPath(key);

        const doAppend = () => {
            Fse.mkdirp(Path.dirname(path), PERMISSIVE, err => {
                if (err && err.code !== 'EEXIST') { return void cb(err); }
                Fs.appendFile(path, tail, err => {
                    if (err) { return void cb(err); }
                    Fs.stat(path, (err, stats) => {
                        if (err) { return void cb(err); }
                        cb(void 0, mkStat(stats));
                    });
                });
            });
        };

        if (typeof (opts.expectedSize) !== 'number' && !opts.expectedEtag) {
            return void doAppend();
        }

        head(key, (err, stat) => {
            if (err && err.code !== 'ENOENT') { return void cb(err); }
            const current = stat || { size: 0, etag: undefined };
            if (typeof (opts.expectedSize) === 'number' && current.size !== opts.expectedSize) {
                return void cb(mkError('EPRECONDITION', 'SIZE_MISMATCH'));
            }
            if (opts.expectedEtag && current.etag !== opts.expectedEtag) {
                return void cb(mkError('EPRECONDITION', 'ETAG_MISMATCH'));
            }
            doAppend();
        });
    };

    const copy = (srcKey, dstKey, opts, cb) => {
        if (!isValidKey(srcKey) || !isValidKey(dstKey)) {
            return void cb(mkError('EINVAL', 'INVALID_KEY'));
        }
        opts = opts || {};
        const dst = toPath(dstKey);

        const run = () => {
            Fse.copy(toPath(srcKey), dst, { overwrite: Boolean(opts.overwrite) }, cb);
        };

        if (opts.overwrite) { return void run(); }
        Fs.stat(dst, err => {
            if (!err) { return void cb(mkError('EEXIST', 'KEY_EXISTS')); }
            if (err.code !== 'ENOENT') { return void cb(err); }
            run();
        });
    };

    const move = (srcKey, dstKey, opts, cb) => {
        if (!isValidKey(srcKey) || !isValidKey(dstKey)) {
            return void cb(mkError('EINVAL', 'INVALID_KEY'));
        }
        opts = opts || {};
        const dst = toPath(dstKey);

        const run = () => {
            Fse.move(toPath(srcKey), dst, { overwrite: Boolean(opts.overwrite) }, cb);
        };

        if (opts.overwrite) { return void run(); }
        Fs.stat(dst, err => {
            if (!err) { return void cb(mkError('EEXIST', 'KEY_EXISTS')); }
            if (err.code !== 'ENOENT') { return void cb(err); }
            run();
        });
    };

    const remove = (key, cb) => {
        if (!isValidKey(key)) { return void cb(mkError('EINVAL', 'INVALID_KEY')); }
        Fs.unlink(toPath(key), cb);
    };

    const removePrefix = (prefix, cb) => {
        if (typeof (prefix) !== 'string') {
            return void cb(mkError('EINVAL', 'INVALID_PREFIX'));
        }
        // an empty prefix would mean "delete the entire store"; refuse it outright
        if (!prefix.length) { return void cb(mkError('EINVAL', 'EMPTY_PREFIX')); }
        if (!isValidKey(prefix.replace(/\/+$/, ''))) {
            return void cb(mkError('EINVAL', 'INVALID_PREFIX'));
        }

        // a prefix ending in '/' is a whole subtree; otherwise it selects by name
        if (prefix.endsWith('/')) {
            return void Fs.rm(toPath(prefix), { recursive: true, force: true }, cb);
        }

        list(prefix, {}, (err, result) => {
            if (err) { return void cb(err); }
            let i = 0;
            const next = () => {
                if (i >= result.keys.length) { return void cb(); }
                const entry = result.keys[i++];
                Fs.unlink(toPath(entry.key), err => {
                    if (err && err.code !== 'ENOENT') { return void cb(err); }
                    setImmediate(next);
                });
            };
            next();
        });
    };

    const readdirSorted = (dir, cb) => {
        Fs.readdir(dir, { withFileTypes: true }, (err, entries) => {
            if (err) {
                // a missing directory is an empty listing, matching S3's behaviour
                // where "directories" do not exist as objects at all
                if (err.code === 'ENOENT' || err.code === 'ENOTDIR') { return void cb(void 0, []); }
                return void cb(err);
            }
            entries.sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
            cb(void 0, entries);
        });
    };

    /*  Depth-first walk in lexicographic key order, so that `limit`/`cursor`
        pagination is stable across calls the way it is on S3. */
    const walk = (dir, keyDir, state, cb) => {
        if (state.done) { return void cb(); }
        readdirSorted(dir, (err, entries) => {
            if (err) { return void cb(err); }

            let i = 0;
            const next = () => {
                if (state.done || i >= entries.length) { return void cb(); }
                const entry = entries[i++];
                const key = keyDir + entry.name;

                if (entry.isDirectory()) {
                    const dirKey = key + '/';
                    // only descend where the subtree could contain matching keys
                    if (!dirKey.startsWith(state.prefix) && !state.prefix.startsWith(dirKey)) {
                        return void setImmediate(next);
                    }
                    return void walk(Path.join(dir, entry.name), dirKey, state, err => {
                        if (err) { return void cb(err); }
                        setImmediate(next);
                    });
                }

                if (!entry.isFile()) { return void setImmediate(next); }
                if (!key.startsWith(state.prefix)) { return void setImmediate(next); }
                if (state.cursor && key <= state.cursor) { return void setImmediate(next); }

                Fs.stat(Path.join(dir, entry.name), (err, stats) => {
                    // the file may have been removed since readdir; skip it
                    if (err) { return void setImmediate(next); }
                    state.keys.push({
                        key,
                        size: stats.size,
                        etag: mkEtag(stats),
                        mtime: stats.mtimeMs
                    });
                    if (state.limit && state.keys.length >= state.limit) {
                        state.done = true;
                        state.nextCursor = key;
                        return void cb();
                    }
                    setImmediate(next);
                });
            };
            next();
        });
    };

    const list = (prefix, opts, cb) => {
        prefix = prefix || '';
        opts = opts || {};
        if (typeof (prefix) !== 'string' || prefix.includes('..')) {
            return void cb(mkError('EINVAL', 'INVALID_PREFIX'));
        }

        // split the prefix into the deepest directory it implies plus a name filter,
        // mirroring how S3 treats a prefix as a string match rather than a directory
        const cut = prefix.lastIndexOf('/');
        const baseKeyDir = cut === -1 ? '' : prefix.slice(0, cut + 1);
        const baseDir = baseKeyDir ? Path.join(root, baseKeyDir) : root;

        if (opts.delimiter !== '/') {
            const state = {
                prefix,
                cursor: opts.cursor,
                limit: opts.limit,
                keys: [],
                done: false
            };
            return void walk(baseDir, baseKeyDir, state, err => {
                if (err) { return void cb(err); }
                cb(void 0, {
                    keys: state.keys,
                    prefixes: [],
                    cursor: state.nextCursor
                });
            });
        }

        // delimited listing: one level only, directories reported as common prefixes
        readdirSorted(baseDir, (err, entries) => {
            if (err) { return void cb(err); }

            const keys = [];
            const prefixes = [];
            let i = 0;
            const next = () => {
                if (i >= entries.length) {
                    return void cb(void 0, { keys, prefixes, cursor: undefined });
                }
                const entry = entries[i++];
                const key = baseKeyDir + entry.name;
                if (!key.startsWith(prefix)) { return void setImmediate(next); }

                if (entry.isDirectory()) {
                    prefixes.push(key + '/');
                    return void setImmediate(next);
                }
                if (!entry.isFile()) { return void setImmediate(next); }

                Fs.stat(Path.join(baseDir, entry.name), (err, stats) => {
                    if (err) { return void setImmediate(next); }
                    keys.push({
                        key,
                        size: stats.size,
                        etag: mkEtag(stats),
                        mtime: stats.mtimeMs
                    });
                    setImmediate(next);
                });
            };
            next();
        });
    };

    const backend = {
        name: 'fs',
        capabilities: {
            // 'wx' and link() give real exclusive-create; If-Match is only best-effort
            conditionalPut: true,
            serverSideAppend: true,
            presign: false,
            atomicMove: true
        },
        root,

        head, exists, get, getStream, list,
        put, append, copy, move, remove, removePrefix,

        close: cb => { cb(); }
    };

    Fse.mkdirp(root, PERMISSIVE, err => {
        if (err && err.code !== 'EEXIST') { return void cb(err); }
        cb(void 0, backend);
    });

    return backend;
};

module.exports.isValidKey = isValidKey;
