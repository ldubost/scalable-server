// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors
//
// SPDX-License-Identifier: AGPL-3.0-or-later

/*  A channel store backed by an object store, with a local write-through cache.

    This exposes exactly the API that file.js does, so nothing above it changes:
    history-manager, channel-manager and the RPC commands cannot tell the
    difference. Underneath, each method falls into one of three groups.

    **Needs local bytes** — appends, ranged reads, metadata parsing, log rewrites.
    These hydrate the object into the cache and then run the *unmodified* file.js
    engine against it. That engine's byte offsets, cached write streams and
    scheduler all keep working because the cached file is the object, byte for byte.

    **Needs only metadata** — sizes, existence checks. Answered from a HEAD against
    the store (or a local stat when the file is already cached and possibly ahead of
    it), so they never drag a multi-megabyte log across the network.

    **Server-side** — archive, restore, remove, listing. These are copies and
    deletes within the store; downloading anything to perform them would be absurd.

    Writes are acknowledged before they reach the store; the cache manager owns the
    flush policy that bounds that window.
*/

const Path = require("node:path");
const Fs = require("node:fs");
const nThen = require("nthen");

const File = require("./file.js");
const Manager = require("../../common/storage/cache/manager.js");
const Util = require("../common-util");

const CachedFile = module.exports;

const isValidChannelId = function (id) {
    return typeof (id) === 'string' &&
        id.length >= 32 && id.length < 50 &&
        /^[a-zA-Z0-9=+-]*$/.test(id);
};

/*  file.js lays a channel out as `<xx>/<id>.ndjson` with its metadata beside it.
    Those relative paths are also the object keys (under the store's prefix), so a
    bucket is a mirror of the datastore directory and migration is a plain copy. */
const relData = id => Path.join(id.slice(0, 2), id) + '.ndjson';
const relMetadata = id => Path.join(id.slice(0, 2), id) + '.metadata.ndjson';
const relPlaceholder = id => relData(id) + '.placeholder';

CachedFile.create = function (conf, _cb) {
    const cb = Util.once(Util.mkAsync(_cb));

    const backend = conf.backend;
    if (!backend) { return void cb(new Error('E_NO_BACKEND')); }

    const Log = conf.Log || {
        info: () => {}, warn: () => {}, error: () => {}, verbose: () => {}, silly: () => {}
    };

    // where cached copies live, and how keys are namespaced in the store
    const cacheRoot = Path.resolve(conf.cachePath);
    const keyPrefix = (conf.keyPrefix || 'channel/').replace(/\/*$/, '/');
    const archiveKeyPrefix = (conf.archiveKeyPrefix ||
        `archive/${conf.volumeId || 'datastore'}/`).replace(/\/*$/, '/');

    let inner;      // the unmodified file.js engine, rooted at the cache
    let manager;    // hydration, dirty tracking, flushing

    const toKey = rel => keyPrefix + rel.split(Path.sep).join('/');
    const toArchiveKey = rel => archiveKeyPrefix + rel.split(Path.sep).join('/');

    // ------------------------------------------------------------- hydration

    /*  Ensure the channel's log is in the cache. When there is no object to fetch
        we also pull the placeholder, so that file.js can report *why* a channel is
        gone rather than a bare ENOENT.  */
    const hydrateData = (id, cb) => {
        manager.hydrate(relData(id), (err, result) => {
            if (err) { return void cb(err); }
            if (!result || !result.created) { return void cb(); }
            manager.hydrate(relPlaceholder(id), () => { cb(); });
        });
    };

    const hydrateMetadata = (id, cb) => {
        manager.hydrate(relMetadata(id), err => { cb(err); });
    };

    const hydrateBoth = (id, cb) => {
        nThen(w => {
            hydrateData(id, w(err => { if (err) { w.abort(); cb(err); } }));
            hydrateMetadata(id, w(err => { if (err) { w.abort(); cb(err); } }));
        }).nThen(() => { cb(); });
    };

    /*  Note that the file has grown.

        The dirty mark has to be synchronous: a caller that has just appended must
        not be able to return before the manager knows there is something to flush,
        or a flush racing that return — the drain at shutdown, say — would skip the
        write entirely. The exact size follows from a stat a moment later and only
        refines the byte-threshold accounting.  */
    const touch = (rel) => {
        manager.touch(rel);
        Fs.stat(Path.join(cacheRoot, rel), (err, stats) => {
            if (!err) { manager.setSize(rel, stats.size); }
        });
    };

    // flush both halves of a channel and wait for them
    const flushChannel = (id, cb) => {
        nThen(w => {
            manager.flush(relData(id), w());
            manager.flush(relMetadata(id), w());
        }).nThen(() => { cb(); });
    };

    // drop local copies after the object has been removed or moved server-side
    const discardChannel = (id, cb) => {
        nThen(w => {
            manager.discard(relData(id), w());
            manager.discard(relMetadata(id), w());
            manager.discard(relPlaceholder(id), w());
        }).nThen(() => { cb(); });
    };

    /*  Size of a channel component. Prefer the local file when it exists: it is
        both cheaper than a round trip and more accurate, since it includes bytes
        that have not been flushed yet.  */
    const sizeOf = (rel, cb) => {
        Fs.stat(Path.join(cacheRoot, rel), (err, stats) => {
            if (!err) { return void cb(void 0, stats.size); }
            backend.head(toKey(rel), (err, stat) => {
                if (err) {
                    if (err.code === 'ENOENT') { return void cb(void 0, 0); }
                    return void cb(err);
                }
                cb(void 0, stat.size);
            });
        });
    };

    const existsAnywhere = (rel, cb) => {
        Fs.stat(Path.join(cacheRoot, rel), err => {
            if (!err) { return void cb(void 0, true); }
            backend.exists(toKey(rel), cb);
        });
    };

    // wrap a callback so invalid ids fail the way file.js makes them fail
    const guard = (id, cb, body) => {
        if (!isValidChannelId(id)) { return void cb(new Error('EINVAL')); }
        body();
    };

    // ------------------------------------------------------------------ setup

    nThen(w => {
        File.create({
            filePath: cacheRoot,
            archivePath: conf.archivePath,
            volumeId: conf.volumeId
        }, w((err, store) => {
            if (err) { w.abort(); return void cb(err); }
            inner = store;
        }));
    }).nThen(w => {
        Manager.create({
            backend,
            cacheRoot,
            prefix: keyPrefix,
            Log,
            monitoring: conf.monitoring,
            metricsPrefix: conf.metricsPrefix,
            flushDebounceMs: conf.flushDebounceMs,
            flushMaxDelayMs: conf.flushMaxDelayMs,
            flushMaxBytes: conf.flushMaxBytes,
            flushConcurrency: conf.flushConcurrency,
            appendThresholdBytes: conf.appendThresholdBytes,
            maxBytes: conf.maxBytes,
            maxIdleMs: conf.maxIdleMs,
            isPinned: conf.isPinned,
            // offsets and rewrite buffers are local optimisations, never objects
            isCacheable: rel => !/\.(offset|temp)$/.test(rel)
        }, w((err, m) => {
            if (err) { w.abort(); return void cb(err); }
            manager = m;
        }));
    }).nThen(() => {
        cb(void 0, api);
    });

    // -------------------------------------------------------------------- API

    const api = {
        // --- appends -------------------------------------------------------
        message: (channelName, content, cb) => {
            guard(channelName, cb, () => {
                hydrateData(channelName, err => {
                    if (err) { return void cb(err); }
                    inner.message(channelName, content, err => {
                        if (!err) { touch(relData(channelName)); }
                        cb(err);
                    });
                });
            });
        },
        messageBin: (channelName, content, cb) => {
            guard(channelName, cb, () => {
                hydrateData(channelName, err => {
                    if (err) { return void cb(err); }
                    inner.messageBin(channelName, content, err => {
                        if (!err) { touch(relData(channelName)); }
                        cb(err);
                    });
                });
            });
        },
        log: (channelName, content, cb) => {
            hydrateData(channelName, err => {
                if (err) { return void cb(err); }
                inner.log(channelName, content, err => {
                    if (!err) { touch(relData(channelName)); }
                    cb(err);
                });
            });
        },

        // --- reads ---------------------------------------------------------
        getMessages: (channelName, msgHandler, cb) => {
            guard(channelName, cb, () => {
                hydrateData(channelName, err => {
                    if (err) { return void cb(err); }
                    inner.getMessages(channelName, msgHandler, cb);
                });
            });
        },
        readMessagesBin: (channelName, start, asyncMsgHandler, cb) => {
            guard(channelName, cb, () => {
                hydrateData(channelName, err => {
                    if (err) { return void cb(err); }
                    inner.readMessagesBin(channelName, start, asyncMsgHandler, cb);
                });
            });
        },

        /*  The weak lock is where every worker job that reads a channel passes
            through, which makes it the natural place to guarantee the bytes are
            local before the job is dispatched.  */
        getWeakLock: (channelName, cb) => {
            hydrateData(channelName, () => {
                inner.getWeakLock(channelName, cb);
            });
        },

        // --- metadata ------------------------------------------------------
        getChannelMetadata: (channelName, cb) => {
            guard(channelName, cb, () => {
                hydrateData(channelName, err => {
                    if (err) { return void cb(err); }
                    inner.getChannelMetadata(channelName, cb);
                });
            });
        },
        readDedicatedMetadata: (channelName, handler, cb) => {
            guard(channelName, cb, () => {
                hydrateMetadata(channelName, err => {
                    if (err) { return void cb(err); }
                    inner.readDedicatedMetadata(channelName, handler, cb);
                });
            });
        },
        readChannelMetadata: (channelName, handler, cb) => {
            guard(channelName, cb, () => {
                hydrateBoth(channelName, err => {
                    if (err) { return void cb(err); }
                    inner.readChannelMetadata(channelName, handler, cb);
                });
            });
        },
        /*  Metadata changes are rare and small, and losing one loses a permission
            change, so this is write-through rather than deferred (§5.4).  */
        writeMetadata: (channelName, data, cb) => {
            guard(channelName, cb, () => {
                hydrateMetadata(channelName, err => {
                    if (err) { return void cb(err); }
                    inner.writeMetadata(channelName, data, err => {
                        if (err) { return void cb(err); }
                        Fs.stat(Path.join(cacheRoot, relMetadata(channelName)), (e, stats) => {
                            manager.touch(relMetadata(channelName), stats && stats.size);
                            manager.flush(relMetadata(channelName), cb);
                        });
                    });
                });
            });
        },

        // --- rewrites ------------------------------------------------------
        /*  These rewrite the log in place. Flush first so the object matches what
            we are about to rewrite, then push the result as a whole new version. */
        clearChannel: (channelName, cb) => {
            guard(channelName, cb, () => {
                hydrateBoth(channelName, err => {
                    if (err) { return void cb(err); }
                    flushChannel(channelName, () => {
                        inner.clearChannel(channelName, err => {
                            if (err) { return void cb(err); }
                            touch(relData(channelName));
                            manager.flush(relData(channelName), cb);
                        });
                    });
                });
            });
        },
        trimChannel: (channelName, hash, cb) => {
            guard(channelName, cb, () => {
                hydrateBoth(channelName, err => {
                    if (err) { return void cb(err); }
                    flushChannel(channelName, () => {
                        inner.trimChannel(channelName, hash, err => {
                            if (err) { return void cb(err); }
                            touch(relData(channelName));
                            touch(relMetadata(channelName));
                            flushChannel(channelName, cb);
                        });
                    });
                });
            });
        },
        /*  Federation repair (R-6): the cached store has to hydrate, flush and
            re-flush around the rewrite exactly as a single-line delete does, or
            the cache would keep serving the lines that were just removed. */
        deleteChannelLines: (channelName, hashes, cb) => {
            guard(channelName, cb, () => {
                hydrateBoth(channelName, err => {
                    if (err) { return void cb(err); }
                    flushChannel(channelName, () => {
                        inner.deleteChannelLines(channelName, hashes, (err, res) => {
                            if (err) { return void cb(err); }
                            touch(relData(channelName));
                            touch(relMetadata(channelName));
                            flushChannel(channelName, (e) => cb(e, res));
                        });
                    });
                });
            });
        },
        deleteChannelLine: (channelName, hash, checkRights, cb) => {
            guard(channelName, cb, () => {
                hydrateBoth(channelName, err => {
                    if (err) { return void cb(err); }
                    flushChannel(channelName, () => {
                        inner.deleteChannelLine(channelName, hash, checkRights, err => {
                            if (err) { return void cb(err); }
                            touch(relData(channelName));
                            touch(relMetadata(channelName));
                            flushChannel(channelName, cb);
                        });
                    });
                });
            });
        },

        // --- existence and size (no hydration) ------------------------------
        isChannelAvailable: (channelName, cb) => {
            guard(channelName, cb, () => {
                let exists = false;
                nThen(w => {
                    existsAnywhere(relData(channelName), w((err, e) => { exists ||= e; }));
                    existsAnywhere(relMetadata(channelName), w((err, e) => { exists ||= e; }));
                }).nThen(() => { cb(void 0, exists); });
            });
        },
        isChannelArchived: (channelName, cb) => {
            guard(channelName, cb, () => {
                let exists = false;
                nThen(w => {
                    backend.exists(toArchiveKey(relData(channelName)),
                        w((err, e) => { exists ||= Boolean(e); }));
                    backend.exists(toArchiveKey(relMetadata(channelName)),
                        w((err, e) => { exists ||= Boolean(e); }));
                }).nThen(() => { cb(void 0, exists); });
            });
        },
        getChannelSize: (channelName, cb) => {
            guard(channelName, cb, () => {
                let total = 0;
                nThen(w => {
                    sizeOf(relData(channelName), w((err, size) => { total += size || 0; }));
                    sizeOf(relMetadata(channelName), w((err, size) => { total += size || 0; }));
                }).nThen(() => { cb(void 0, total); });
            });
        },
        getChannelStats: (channelName, cb) => {
            guard(channelName, cb, () => {
                let size = 0;
                let mtime = 0;
                nThen(w => {
                    [relData(channelName), relMetadata(channelName)].forEach(rel => {
                        backend.head(toKey(rel), w((err, stat) => {
                            if (err || !stat) { return; }
                            size += stat.size;
                            mtime = Math.max(mtime, stat.mtime);
                        }));
                    });
                }).nThen(() => {
                    if (!size && !mtime) { return void cb('NO_DATA'); }
                    cb(void 0, {
                        channel: channelName,
                        size,
                        // object stores track modification only
                        atime: mtime, mtime, ctime: mtime
                    });
                });
            });
        },
        getPlaceholder: (channelName, cb) => {
            backend.get(toKey(relPlaceholder(channelName)), (err, body) => {
                if (err) { return void cb(); }
                cb(body.toString('utf8'));
            });
        },

        // --- server-side operations ----------------------------------------
        removeChannel: (channelName, cb) => {
            guard(channelName, cb, () => {
                let errors = 0;
                nThen(w => {
                    backend.remove(toKey(relData(channelName)), w(err => {
                        if (err && err.code === 'ENOENT') { errors++; }
                    }));
                    backend.remove(toKey(relMetadata(channelName)), w(err => {
                        if (err && err.code === 'ENOENT') { errors++; }
                    }));
                }).nThen(w => {
                    discardChannel(channelName, w());
                }).nThen(() => {
                    if (errors === 2) { return void cb('E_REMOVE_CHANNEL_ENOENT'); }
                    cb();
                });
            });
        },
        removeArchivedChannel: (channelName, cb) => {
            guard(channelName, cb, () => {
                nThen(w => {
                    backend.remove(toArchiveKey(relData(channelName)), w(() => {}));
                    backend.remove(toArchiveKey(relMetadata(channelName)), w(() => {}));
                }).nThen(() => { cb(); });
            });
        },
        archiveChannel: (channelName, reason, cb) => {
            guard(channelName, cb, () => {
                // flush first: the object has to be current before it is moved
                flushChannel(channelName, () => {
                    let moved = false;
                    nThen(w => {
                        backend.move(toKey(relData(channelName)),
                            toArchiveKey(relData(channelName)),
                            { overwrite: true }, w(err => { if (!err) { moved = true; } }));
                        backend.move(toKey(relMetadata(channelName)),
                            toArchiveKey(relMetadata(channelName)),
                            { overwrite: true }, w(err => { if (!err) { moved = true; } }));
                    }).nThen(w => {
                        if (!reason) { return; }
                        const s_data = typeof (reason) === 'string' ?
                            reason : `${reason.code}:${reason.txt}`;
                        backend.put(toKey(relPlaceholder(channelName)), s_data, {}, w(() => {}));
                    }).nThen(w => {
                        discardChannel(channelName, w());
                    }).nThen(() => {
                        if (!moved) { return void cb('E_ARCHIVE_ENOENT'); }
                        cb();
                    });
                });
            });
        },
        restoreArchivedChannel: (channelName, cb) => {
            guard(channelName, cb, () => {
                let conflict = false;
                let restored = false;
                nThen(w => {
                    // never clobber a live channel with an archived one
                    backend.exists(toKey(relData(channelName)),
                        w((err, e) => { conflict ||= Boolean(e); }));
                    backend.exists(toKey(relMetadata(channelName)),
                        w((err, e) => { conflict ||= Boolean(e); }));
                }).nThen(w => {
                    if (conflict) {
                        w.abort();
                        return void cb('UNARCHIVE_CHANNEL_CONFLICT');
                    }
                    backend.move(toArchiveKey(relData(channelName)),
                        toKey(relData(channelName)), {},
                        w(err => { if (!err) { restored = true; } }));
                    backend.move(toArchiveKey(relMetadata(channelName)),
                        toKey(relMetadata(channelName)), {},
                        w(err => { if (!err) { restored = true; } }));
                }).nThen(w => {
                    backend.remove(toKey(relPlaceholder(channelName)), w(() => {}));
                    discardChannel(channelName, w());
                }).nThen(() => {
                    if (!restored) { return void cb('ENOENT'); }
                    cb();
                });
            });
        },

        // --- listing --------------------------------------------------------
        listChannels: (handler, cb, fast) => {
            listFrom(keyPrefix, handler, cb, fast);
        },
        listArchivedChannels: (handler, cb, fast) => {
            listFrom(archiveKeyPrefix, handler, cb, fast);
        },

        // --- offsets: local only, never uploaded ----------------------------
        clearOffset: (channelName, cb) => { inner.clearOffset(channelName, cb); },
        writeOffset: (channelName, data, cb) => { inner.writeOffset(channelName, data, cb); },
        getOffset: (channelName, cb) => { inner.getOffset(channelName, cb); },

        // --- lifecycle -------------------------------------------------------
        closeChannel: (channelName, cb) => {
            // the write stream has to be closed before the file is read for upload
            inner.closeChannel(channelName, () => {
                flushChannel(channelName, cb);
            });
        },
        closeInactiveChannels: (activeSet) => {
            inner.closeInactiveChannels(activeSet);
            manager._entries.forEach(entry => {
                if (!entry.dirty) { return; }
                const id = Path.basename(entry.relPath).replace(/\.(metadata\.)?ndjson$/, '');
                if (activeSet && activeSet.has(id)) { return; }
                manager.flush(entry.relPath, () => {});
            });
        },

        /*  The drain step at shutdown, and the reconciliation at boot. */
        flushAll: cb => { manager.flushAll(cb); },
        recover: cb => { manager.recover(cb); },
        evict: cb => { manager.evict(cb); },
        cacheStats: () => manager.stats(),

        shutdown: () => {
            if (inner && typeof (inner.shutdown) === 'function') { inner.shutdown(); }
        }
    };

    /*  Reconstruct the shape file.js's listChannels produces, from a listing rather
        than a directory walk plus a stat per entry.  */
    const listFrom = (prefix, handler, cb, fast) => {
        const entries = new Map();

        const page = (cursor) => {
            backend.list(prefix, { cursor, limit: 1000 }, (err, result) => {
                if (err) { return void cb(err); }

                result.keys.forEach(item => {
                    const name = item.key.slice(prefix.length);
                    const match = /^..\/([a-zA-Z0-9=+-]+)(\.metadata)?\.ndjson$/.exec(name);
                    if (!match) { return; }
                    const channel = match[1];
                    const existing = entries.get(channel) ||
                        { channel, size: 0, mtime: 0 };
                    existing.size += item.size;
                    existing.mtime = Math.max(existing.mtime, item.mtime);
                    entries.set(channel, existing);
                });

                if (result.cursor) { return void page(result.cursor); }

                const list = Array.from(entries.values());
                let i = 0;
                const next = () => {
                    if (i >= list.length) { return void cb(); }
                    const item = list[i++];
                    const data = fast ? { channel: item.channel } : {
                        channel: item.channel,
                        size: item.size,
                        atime: item.mtime, mtime: item.mtime, ctime: item.mtime
                    };
                    handler(void 0, data, () => { setImmediate(next); });
                };
                next();
            });
        };
        page();
    };

    return api;
};
