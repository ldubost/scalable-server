// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors
//
// SPDX-License-Identifier: AGPL-3.0-or-later

/*  A blob store backed by an object store.

    Blobs are the easy case, and for one reason: they are immutable. A blob is
    uploaded once, in chunks, and never modified again. So none of the cached
    append-log machinery that channels need applies here.

    What that leaves is a much simpler shape:

      * uploads stage on **local disk**, exactly as before. A partial upload is
        worthless, and streaming every chunk to the object store as it arrives
        would mean paying for an upload that is usually abandoned halfway;
      * on completion the staged file is streamed to the store in one multipart
        upload and the local copy is dropped;
      * everything afterwards — size, existence, archive, restore, remove, list —
        is metadata or a server-side copy. A blob is never downloaded to the
        server. Browsers fetch them straight from the store (see http-data.js).

    Two details deserve their own note: metadata logs and activity timestamps.
    Both are handled below where they are implemented.
*/

const Path = require("node:path");
const Fs = require("node:fs");
const nThen = require("nthen");

const Blob = require("./blob.js");
const Util = require("../common-util");

const CachedBlob = module.exports;

const BLOB_LENGTH = 48;

const isValidId = function (id) {
    return typeof (id) === 'string' && id.length === BLOB_LENGTH &&
        /^[a-f0-9]+$/.test(id);
};
const isValidSafeKey = function (safeKey) {
    return typeof (safeKey) === 'string' && !/\//.test(safeKey) && safeKey.length === 44;
};

/*  Keys mirror the layout blob.js uses on disk, so a bucket is a copy of the
    blob directory and migration is a plain file copy in either direction. */
const relBlob = id => `${id.slice(0, 2)}/${id}`;
const relMetadata = id => `${relBlob(id)}.metadata.ndjson`;
const relActivity = id => `${relBlob(id)}.activity`;
const relPlaceholder = id => `${relBlob(id)}.placeholder`;
const relProof = (safeKey, id) => `${safeKey.slice(0, 3)}/${safeKey}/${relBlob(id)}`;

/*  An activity timestamp records roughly when a blob was last fetched, and is
    used to decide what is inactive enough to evict. updateActivity() is called on
    every HEAD of every blob, so writing an object each time would mean a request
    to the store per request from a browser, to maintain a value whose useful
    resolution is days. It is throttled to one write per blob per interval. */
const ACTIVITY_THROTTLE = 60 * 60 * 1000; // one hour

CachedBlob.create = function (conf, _cb) {
    const cb = Util.once(Util.mkAsync(_cb));

    const backend = conf.backend;
    if (!backend) { return void cb(new Error('E_NO_BACKEND')); }

    const keyPrefix = (conf.keyPrefix || 'blob/').replace(/\/*$/, '/');
    const archiveKeyPrefix = (conf.archiveKeyPrefix || 'archive/blob/').replace(/\/*$/, '/');
    const stagingPath = conf.blobStagingPath;

    const toKey = rel => keyPrefix + rel;
    const toArchiveKey = rel => archiveKeyPrefix + rel;

    // when a blob was last known to be active, by id
    const activityWrites = new Map();

    /*  blob.js still owns the staging half: chunked writes, upload sessions,
        cookies and cancellation all work against local disk and are unchanged.
        Its `blobPath` is pointed at a directory that will stay empty, because
        completed blobs go to the object store instead.  */
    let staging;

    // the staged file for a given uploader, using blob.js's own layout
    const stagePath = safeKey => {
        return Path.join(stagingPath, safeKey.slice(0, 2), safeKey);
    };

    /*  Close the staging stream and wait for it to actually finish.

        `closeBlobstage` returns immediately: a write stream flushes and closes
        asynchronously, so the staged file on disk may still be short of the bytes
        the uploader has sent. Reading it at that moment yields a truncated blob —
        blob.js had the same exposure, and moved whatever was on disk, so the
        truncation was silent. Waiting for 'close' removes the window.  */
    const finishStaging = (safeKey, _cb) => {
        const cb = Util.once(Util.mkAsync(_cb));

        let stream;
        try {
            const session = conf.getSession && conf.getSession(safeKey);
            stream = session && session.blobstage;
        } catch (err) { /* no session: nothing to wait for */ }

        if (!stream || typeof (stream.once) !== 'function') {
            return void staging.closeBlobstage(safeKey, cb);
        }
        staging.closeBlobstage(safeKey, cb);
    };

    const headOf = (rel, cb) => {
        backend.head(toKey(rel), (err, stat) => {
            if (err) {
                if (err.code === 'ENOENT') { return void cb(void 0, null); }
                return void cb(err);
            }
            cb(void 0, stat);
        });
    };

    /*  Stream a staged upload into the store and drop the local copy.

        The destination is checked first rather than using a conditional write:
        a multipart upload cannot carry an If-None-Match, and this reproduces
        exactly the check blob.js made before its rename.  */
    const uploadStaged = (safeKey, id, cb) => {
        const from = stagePath(safeKey);
        const key = toKey(relBlob(id));

        nThen(w => {
            backend.exists(key, w((err, exists) => {
                if (err) {
                    w.abort();
                    return void cb(err.code || err);
                }
                if (exists) {
                    // an id collision: the caller should generate another
                    w.abort();
                    return void cb('RENAME_ERR');
                }
            }));
        }).nThen(w => {
            Fs.stat(from, w((err) => {
                if (err) {
                    w.abort();
                    return void cb('RENAME_ERR');
                }
            }));
        }).nThen(() => {
            const stream = Fs.createReadStream(from);
            let failed = false;
            stream.on('error', () => {
                if (failed) { return; }
                failed = true;
                cb('RENAME_ERR');
            });

            backend.put(key, stream, {}, err => {
                if (failed) { return; }
                if (err) { return void cb(err.code || 'RENAME_ERR'); }

                /*  The blob is durable now, so the staged copy can go. Remove it
                    *before* calling back rather than in the background: the same
                    uploader's next upload reuses this exact path, and a late
                    unlink would delete the data they had just staged. blob.js was
                    safe here only because a rename removes its source atomically. */
                Fs.unlink(from, () => {
                    Fs.unlink(from + '.cookie', () => {
                        cb(void 0, id);
                    });
                });
            });
        });
    };

    const api = {
        BLOB_LENGTH,
        isFileId: isValidId,

        // ---------------------------------------------------- staging (local)
        status: (safeKey, cb) => { staging.status(safeKey, cb); },
        upload: (safeKey, content, cb) => { staging.upload(safeKey, content, cb); },
        uploadCookie: (safeKey, cb) => { staging.uploadCookie(safeKey, cb); },
        checkUploadCookie: (safeKey, cb) => { staging.checkUploadCookie(safeKey, cb); },
        cancel: (safeKey, fileSize, cb) => { staging.cancel(safeKey, fileSize, cb); },
        closeBlobstage: (safeKey, cb) => { staging.closeBlobstage(safeKey, cb); },

        // ------------------------------------------------------- completion
        complete: (safeKey, id, _cb) => {
            const cb = Util.once(Util.mkAsync(_cb));
            if (!isValidSafeKey(safeKey)) { return void cb('INVALID_SAFEKEY'); }
            if (!isValidId(id)) { return void cb('INVALID_ID'); }

            finishStaging(safeKey, () => { uploadStaged(safeKey, id, cb); });
        },

        completeOwned: (safeKey, id, _cb) => {
            const cb = Util.once(Util.mkAsync(_cb));
            if (!isValidSafeKey(safeKey)) { return void cb('INVALID_SAFEKEY'); }
            if (!isValidId(id)) { return void cb('EINVAL_ID'); }

            const unsafeKey = safeKey.replace(/\-/g, '/');

            nThen(w => {
                finishStaging(safeKey, w());
            }).nThen(w => {
                // the ownership record has to exist before the blob does, or a
                // crash in between would leave an unowned and unremovable blob
                const md = JSON.stringify({ owners: [unsafeKey] });
                api.writeMetadata(id, md, w(err => {
                    if (err) {
                        w.abort();
                        return void cb(err.code || err);
                    }
                }));
            }).nThen(() => {
                uploadStaged(safeKey, id, cb);
            });
        },

        // --------------------------------------------------------- metadata
        /*  Blob metadata is a tiny ndjson log, written once at upload and
            occasionally amended. It is small enough to read whole and append to
            directly, so it needs none of the caching that channel logs do. */
        readMetadata: (blobId, handler, _cb) => {
            const cb = Util.once(Util.mkAsync(_cb));
            if (!isValidId(blobId)) { return void cb('INVALID_ID'); }

            backend.get(toKey(relMetadata(blobId)), (err, body) => {
                // no metadata log is a normal state, not an error
                if (err) { return void cb(err.code === 'ENOENT' ? undefined : err); }

                body.toString('utf8').split('\n').forEach(line => {
                    if (!line) { return; }
                    try {
                        handler(null, JSON.parse(line));
                    } catch (err) {
                        handler(err, line);
                    }
                });
                cb();
            });
        },

        writeMetadata: (blobId, data, _cb) => {
            const cb = Util.once(Util.mkAsync(_cb));
            if (!isValidId(blobId)) { return void cb('INVALID_ID'); }
            backend.append(toKey(relMetadata(blobId)),
                Buffer.from(data + '\n', 'utf8'), {}, err => { cb(err); });
        },

        hasMetadata: (blobId, _cb) => {
            const cb = Util.once(Util.mkAsync(_cb));
            if (!isValidId(blobId)) { return void cb('INVALID_ID'); }
            backend.exists(toKey(relMetadata(blobId)), cb);
        },

        // ------------------------------------------------------- existence
        isBlobAvailable: (blobId, _cb) => {
            const cb = Util.once(Util.mkAsync(_cb));
            if (!isValidId(blobId)) { return void cb('INVALID_ID'); }
            backend.exists(toKey(relBlob(blobId)), cb);
        },

        isBlobArchived: (blobId, _cb) => {
            const cb = Util.once(Util.mkAsync(_cb));
            if (!isValidId(blobId)) { return void cb('INVALID_ID'); }
            backend.exists(toArchiveKey(relBlob(blobId)), cb);
        },

        isOwnedBy: (safeKey, blobId, _cb) => {
            const cb = Util.once(Util.mkAsync(_cb));
            if (!isValidSafeKey(safeKey)) { return void cb('INVALID_SAFEKEY'); }
            // a deprecated ownership scheme, kept for compatibility
            backend.exists(toKey(relProof(safeKey, blobId)), cb);
        },

        size: (blobId, _cb) => {
            const cb = Util.once(Util.mkAsync(_cb));
            if (!isValidId(blobId)) { return void cb('INVALID_ID'); }
            headOf(relBlob(blobId), (err, stat) => {
                if (err) { return void cb(err); }
                // callers treat a missing blob as size zero
                cb(void 0, stat ? stat.size : 0);
            });
        },

        getPlaceholder: (blobId, cb) => {
            backend.get(toKey(relPlaceholder(blobId)), (err, body) => {
                if (err) { return void cb(); }
                cb(body.toString('utf8'));
            });
        },

        // -------------------------------------------------------- activity
        updateActivity: (blobId, _cb) => {
            const cb = Util.once(Util.mkAsync(_cb));
            if (!isValidId(blobId)) { return void cb('INVALID_ID'); }

            // throttled: this fires on every browser HEAD of every blob
            const now = +new Date();
            const last = activityWrites.get(blobId) || 0;
            if (now - last < ACTIVITY_THROTTLE) { return void cb(); }
            activityWrites.set(blobId, now);

            backend.put(toKey(relActivity(blobId)), String(now), {}, err => { cb(err); });
        },

        getActivity: (blobId, _cb) => {
            const cb = Util.once(Util.mkAsync(_cb));
            if (!isValidId(blobId)) { return void cb('INVALID_ID'); }
            backend.get(toKey(relActivity(blobId)), (err, body) => {
                if (err) { return void cb(err); }
                const date = new Date(Number(body.toString('utf8')));
                if (isNaN(date.getTime())) { return void cb('INVALID_ACTIVITY'); }
                cb(void 0, date);
            });
        },

        /*  Falls back to the blob's own modification time when no activity has
            ever been recorded, matching what blob.js did with a missing
            .activity file. */
        getStats: (blobId, _cb) => {
            const cb = Util.once(Util.mkAsync(_cb));
            if (!isValidId(blobId)) { return void cb('INVALID_ID'); }

            headOf(relActivity(blobId), (err, stat) => {
                if (err) { return void cb(err); }
                if (stat) {
                    return void cb(void 0, { mtime: new Date(stat.mtime), size: stat.size });
                }
                headOf(relBlob(blobId), (err, stat) => {
                    if (err) { return void cb(err); }
                    if (!stat) { return void cb('ENOENT'); }
                    cb(void 0, { mtime: new Date(stat.mtime), size: stat.size });
                });
            });
        },

        // ------------------------------------------- archive / restore / remove
        archive: {
            blob: (blobId, reason, _cb) => {
                const cb = Util.once(Util.mkAsync(_cb));
                if (!isValidId(blobId)) { return void cb('INVALID_ID'); }

                let moved = false;
                nThen(w => {
                    backend.move(toKey(relBlob(blobId)), toArchiveKey(relBlob(blobId)),
                        { overwrite: true }, w(err => { if (!err) { moved = true; } }));
                }).nThen(w => {
                    // the metadata and activity records follow the blob, but
                    // their absence must not fail the archival
                    backend.move(toKey(relMetadata(blobId)),
                        toArchiveKey(relMetadata(blobId)), { overwrite: true }, w(() => {}));
                    backend.move(toKey(relActivity(blobId)),
                        toArchiveKey(relActivity(blobId)), { overwrite: true }, w(() => {}));
                }).nThen(w => {
                    if (!reason) { return; }
                    const s_data = typeof (reason) === 'string' ?
                        reason : `${reason.code}:${reason.txt}`;
                    backend.put(toKey(relPlaceholder(blobId)), s_data, {}, w(() => {}));
                }).nThen(() => {
                    activityWrites.delete(blobId);
                    if (!moved) { return void cb('ENOENT'); }
                    cb();
                });
            }
        },

        restore: {
            blob: (blobId, _cb) => {
                const cb = Util.once(Util.mkAsync(_cb));
                if (!isValidId(blobId)) { return void cb('INVALID_ID'); }

                let restored = false;
                nThen(w => {
                    backend.move(toArchiveKey(relBlob(blobId)), toKey(relBlob(blobId)),
                        {}, w(err => { if (!err) { restored = true; } }));
                }).nThen(w => {
                    backend.move(toArchiveKey(relMetadata(blobId)),
                        toKey(relMetadata(blobId)), {}, w(() => {}));
                    backend.move(toArchiveKey(relActivity(blobId)),
                        toKey(relActivity(blobId)), {}, w(() => {}));
                    backend.remove(toKey(relPlaceholder(blobId)), w(() => {}));
                }).nThen(() => {
                    if (!restored) { return void cb('ENOENT'); }
                    cb();
                });
            }
        },

        remove: {
            blob: (blobId, _cb) => {
                const cb = Util.once(Util.mkAsync(_cb));
                if (!isValidId(blobId)) { return void cb('INVALID_ID'); }
                backend.remove(toKey(relBlob(blobId)), err => {
                    backend.remove(toKey(relActivity(blobId)), () => {
                        activityWrites.delete(blobId);
                        cb(err);
                    });
                });
            },
            archived: {
                blob: (blobId, _cb) => {
                    const cb = Util.once(Util.mkAsync(_cb));
                    if (!isValidId(blobId)) { return void cb('INVALID_ID'); }

                    let failure;
                    nThen(w => {
                        backend.remove(toArchiveKey(relBlob(blobId)), w(err => {
                            if (err && err.code !== 'ENOENT') {
                                failure = 'E_ARCHIVED_BLOB_REMOVAL_' + err.code;
                            }
                        }));
                        backend.remove(toArchiveKey(relMetadata(blobId)), w(err => {
                            if (err && err.code !== 'ENOENT') {
                                failure = 'E_ARCHIVED_BLOBMD_REMOVAL_' + err.code;
                            }
                        }));
                        backend.remove(toArchiveKey(relActivity(blobId)), w(() => {}));
                    }).nThen(() => {
                        cb(failure);
                    });
                }
            },
            /*  Activity records for blobs that no longer exist. A bug once
                created these; listing is cheap enough to sweep them here. */
            loneActivity: (_cb) => {
                const cb = Util.once(Util.mkAsync(_cb));
                const lone = [];

                const page = cursor => {
                    backend.list(keyPrefix, { cursor, limit: 1000 }, (err, result) => {
                        if (err) { return void cb(err); }

                        const names = new Set();
                        const activity = [];
                        result.keys.forEach(item => {
                            const rel = item.key.slice(keyPrefix.length);
                            if (rel.endsWith('.activity')) {
                                activity.push(rel);
                                return;
                            }
                            names.add(rel);
                        });
                        activity.forEach(rel => {
                            if (!names.has(rel.replace(/\.activity$/, ''))) {
                                lone.push(rel);
                            }
                        });

                        if (result.cursor) { return void page(result.cursor); }

                        let i = 0;
                        const next = () => {
                            if (i >= lone.length) { return void cb(void 0, lone.length); }
                            backend.remove(toKey(lone[i++]), () => { setImmediate(next); });
                        };
                        next();
                    });
                };
                page();
            }
        },

        // ------------------------------------------------------------ listing
        list: {
            blobs: (handler, cb, fast) => { listFrom(keyPrefix, handler, cb, fast); },
            archived: {
                blobs: (handler, cb, fast) => {
                    listFrom(archiveKeyPrefix, handler, cb, fast);
                }
            }
        }
    };

    /*  Enumerate blobs from a listing rather than a directory walk plus a stat
        per entry. Metadata and activity records are folded into their blob. */
    const listFrom = (prefix, handler, _cb, fast) => {
        const cb = Util.once(Util.mkAsync(_cb));
        const blobs = new Map();

        const page = cursor => {
            backend.list(prefix, { cursor, limit: 1000 }, (err, result) => {
                if (err) { return void cb(err); }

                result.keys.forEach(item => {
                    const rel = item.key.slice(prefix.length);
                    const match = /^..\/([0-9a-fA-F]{48})(\.metadata\.ndjson|\.activity)?$/
                        .exec(rel);
                    if (!match) { return; }

                    const blobId = match[1];
                    const entry = blobs.get(blobId) ||
                        { blobId, size: 0, mtime: 0, hasBlob: false };
                    if (!match[2]) {
                        entry.hasBlob = true;
                        entry.size = item.size;
                    }
                    entry.mtime = Math.max(entry.mtime, item.mtime);
                    blobs.set(blobId, entry);
                });

                if (result.cursor) { return void page(result.cursor); }

                const list = Array.from(blobs.values());
                let i = 0;
                const next = () => {
                    if (i >= list.length) { return void cb(); }
                    const item = list[i++];
                    if (fast) {
                        return void handler(void 0, { blobId: item.blobId },
                            () => { setImmediate(next); });
                    }
                    handler(void 0, {
                        blobId: item.blobId,
                        size: item.size,
                        // metadata with no blob is orphaned; blob.js reported
                        // these with a zero time so that eviction removes them
                        atime: item.hasBlob ? item.mtime : 0,
                        mtime: item.hasBlob ? item.mtime : 0
                    }, () => { setImmediate(next); });
                };
                next();
            });
        };
        page();
    };

    // build the staging half last: everything above is ready by the time it exists
    Blob.create({
        blobPath: conf.localBlobPath,
        blobStagingPath: stagingPath,
        archivePath: conf.archivePath,
        getSession: conf.getSession,
        sendCommand: conf.sendCommand
    }, (err, store) => {
        if (err) { return void cb(err); }
        staging = store;
        cb(void 0, api);
    });
};
