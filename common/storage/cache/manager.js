// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors
//
// SPDX-License-Identifier: AGPL-3.0-or-later

/*  The cached append-log.

    CryptPad's history keeper is built on byte offsets into a channel's log: the
    index records `offsetByHash[hash] = byteOffset`, and GET_HISTORY re-reads the log
    from an offset. None of that maps onto object-store primitives — but a
    byte-identical local copy of the object makes all of it work unmodified. That is
    what this module maintains.

        ABSENT ──hydrate()──► CLEAN ──touch()──► DIRTY ──flush()──► CLEAN
           ▲                    │                  │
           └───── evict() ──────┘                  └── flush() is the only way out
                                                       (DIRTY is never evicted)

    Two invariants carry the design:

    1. **Byte fidelity.** The local file is exactly the object, byte for byte. No
       compression, no re-encoding, no trailing-newline normalisation — any of which
       would silently invalidate every stored offset.

    2. **The local copy is never clobbered.** Hydration refuses to overwrite an
       existing local file, because that file may hold appends the store has not
       seen yet. Concurrent hydration by several processes is therefore safe.

    Writes are acknowledged to clients before they reach the store, so the flush
    policy defines a real exposure window. It is bounded three ways — an idle
    debounce, a hard ceiling, and an unflushed-byte limit — and reported through
    `stats()` so the window is observable rather than assumed.
*/

const Fs = require("node:fs");
const Path = require("node:path");
const Semaphore = require("saferphore");

const Util = require("../../common-util");
const Journal = require("./journal.js");

const Manager = module.exports;

const DEFAULTS = {
    flushDebounceMs: 5000,
    flushMaxDelayMs: 30000,
    flushMaxBytes: 1024 * 1024,
    flushConcurrency: 8,
    // below this, re-uploading the whole object beats a server-side append,
    // and S3 will not copy a part smaller than 5 MiB anyway
    appendThresholdBytes: 5 * 1024 * 1024,
    maxBytes: 0,        // 0 disables size-based eviction
    maxIdleMs: 0        // 0 disables age-based eviction
};

const noopLog = {
    info: () => {}, warn: () => {}, error: () => {}, verbose: () => {}, silly: () => {}
};

Manager.create = (conf, _cb) => {
    const cb = typeof (_cb) === 'function' ? _cb : () => {};

    const backend = conf.backend;
    if (!backend) { return void cb(new Error('E_NO_BACKEND')); }

    const root = Path.resolve(conf.cacheRoot);
    const prefix = conf.prefix ? conf.prefix.replace(/\/*$/, '/') : '';
    const Log = conf.Log || noopLog;
    const monitoring = conf.monitoring;

    const opts = Object.assign({}, DEFAULTS, conf);

    // relPath -> entry. The durable mirror of this lives in the journal sidecars.
    const entries = new Map();
    const sema = Semaphore.create(opts.flushConcurrency);
    let closed = false;

    const counters = {
        hydrations: 0, hydrateBytes: 0, hydrateFailures: 0,
        flushes: 0, flushBytes: 0, flushFailures: 0,
        conflicts: 0, evictions: 0, recovered: 0
    };

    const toPath = relPath => Path.join(root, relPath);
    const toKey = relPath => prefix + relPath.split(Path.sep).join('/');

    const getEntry = relPath => {
        let entry = entries.get(relPath);
        if (!entry) {
            entry = {
                relPath,
                key: toKey(relPath),
                remoteSize: 0,
                etag: undefined,
                dirty: false,
                firstDirtyAt: 0,
                lastWriteAt: 0,
                lastAccessAt: +new Date(),
                hydrated: false,
                flushing: false,
                pendingFlush: [],
                timers: {}
            };
            entries.set(relPath, entry);
        }
        return entry;
    };

    const clearTimers = entry => {
        if (entry.timers.debounce) { clearTimeout(entry.timers.debounce); }
        if (entry.timers.ceiling) { clearTimeout(entry.timers.ceiling); }
        entry.timers = {};
    };

    const statLocal = (relPath, cb) => {
        Fs.stat(toPath(relPath), (err, stats) => {
            if (err) {
                if (err.code === 'ENOENT') { return void cb(void 0, null); }
                return void cb(err);
            }
            cb(void 0, stats);
        });
    };

    // ---------------------------------------------------------------- hydrate

    /*  Copy the object into the cache if it is not already there.

        Downloads land on a temp file and are then `link`ed into place: link fails
        with EEXIST rather than overwriting, so if two processes hydrate the same
        object concurrently — the storage primary and one of its workers, say — the
        loser discards its copy instead of clobbering a file the winner may already
        have appended to.  */
    const hydrate = (relPath, _cb) => {
        const cb = Util.once(Util.mkAsync(_cb));
        const entry = getEntry(relPath);
        entry.lastAccessAt = +new Date();

        statLocal(relPath, (err, stats) => {
            if (err) { return void cb(err); }

            if (stats) {
                // already present. If we have never looked at it in this process,
                // adopt whatever the journal recorded so that the unflushed tail
                // is computed from the right offset.
                if (entry.hydrated) { return void cb(void 0, { cached: true }); }
                return void Journal.read(toPath(relPath), (err, state) => {
                    if (state && typeof (state.remoteSize) === 'number') {
                        entry.remoteSize = state.remoteSize;
                        entry.etag = state.etag;
                        if (stats.size > state.remoteSize) {
                            // unflushed tail from a previous life
                            markDirty(entry, stats.size);
                        }
                    }
                    entry.hydrated = true;
                    cb(void 0, { cached: true });
                });
            }

            // not present locally: fetch it
            backend.head(entry.key, (err, remote) => {
                if (err && err.code !== 'ENOENT') {
                    counters.hydrateFailures++;
                    return void cb(err);
                }
                if (err) {
                    // no such object: this is a new channel. Nothing to download,
                    // and nothing is dirty until something writes to it.
                    entry.hydrated = true;
                    entry.remoteSize = 0;
                    entry.etag = undefined;
                    return void cb(void 0, { created: true });
                }

                const path = toPath(relPath);
                const temp = `${path}.hydrating.${process.pid}.` +
                    `${Math.random().toString(36).slice(2, 10)}`;

                Fs.mkdir(Path.dirname(path), { recursive: true }, err => {
                    if (err && err.code !== 'EEXIST') { return void cb(err); }

                    backend.getStream(entry.key, {}, (err, stream) => {
                        if (err) {
                            counters.hydrateFailures++;
                            return void cb(err);
                        }
                        const out = Fs.createWriteStream(temp);
                        const fail = Util.once(err => {
                            Fs.unlink(temp, () => { counters.hydrateFailures++; cb(err); });
                        });
                        stream.on('error', fail);
                        out.on('error', fail);
                        out.on('close', () => {
                            Fs.link(temp, path, err => {
                                Fs.unlink(temp, () => {});
                                if (err && err.code !== 'EEXIST') { return void fail(err); }
                                // EEXIST: another process won the race with identical
                                // bytes, or with bytes that are ahead of ours. Either
                                // way theirs is the one to keep.
                                entry.hydrated = true;
                                entry.remoteSize = remote.size;
                                entry.etag = remote.etag;
                                counters.hydrations++;
                                counters.hydrateBytes += remote.size;
                                Journal.write(path,
                                    Journal.clean(entry.key, remote, remote.size), () => {
                                        cb(void 0, { downloaded: !err, bytes: remote.size });
                                    });
                            });
                        });
                        stream.pipe(out);
                    });
                });
            });
        });
    };

    // ------------------------------------------------------------------ dirty

    const markDirty = (entry, localSize) => {
        const now = +new Date();
        entry.lastWriteAt = now;
        entry.lastAccessAt = now;
        if (!entry.dirty) {
            entry.dirty = true;
            entry.firstDirtyAt = now;
        }
        if (typeof (localSize) === 'number') { entry.localSize = localSize; }
    };

    /*  Pending flush timers must not keep the process alive on their own. The
        drain at shutdown is what guarantees outstanding writes reach the store; a
        timer that held the event loop open would only delay an exit that has
        already flushed everything.  */
    const unref = timer => {
        if (timer && typeof (timer.unref) === 'function') { timer.unref(); }
        return timer;
    };

    const scheduleFlush = (entry) => {
        if (closed) { return; }

        // a full debounce restart on every write, so a burst of typing produces
        // one upload rather than one per message
        if (entry.timers.debounce) { clearTimeout(entry.timers.debounce); }
        entry.timers.debounce = unref(setTimeout(() => {
            entry.timers.debounce = null;
            flush(entry.relPath, () => {});
        }, opts.flushDebounceMs));

        // ...bounded by a ceiling, so continuous editing still reaches the store
        if (!entry.timers.ceiling && opts.flushMaxDelayMs > 0) {
            entry.timers.ceiling = unref(setTimeout(() => {
                entry.timers.ceiling = null;
                flush(entry.relPath, () => {});
            }, opts.flushMaxDelayMs));
        }
    };

    /*  Record that a file has been appended to locally.

        `localSize` is the file's new size. Callers that do not know it can omit it;
        the size is read from disk at flush time regardless.  */
    const touch = (relPath, localSize) => {
        const entry = getEntry(relPath);
        markDirty(entry, localSize);

        const unflushed = (typeof (localSize) === 'number' ? localSize : 0) - entry.remoteSize;
        if (opts.flushMaxBytes > 0 && unflushed >= opts.flushMaxBytes) {
            // a bulk write: don't sit on it for the full debounce
            clearTimers(entry);
            return void flush(relPath, () => {});
        }
        scheduleFlush(entry);
    };

    /*  Refine an entry's known size after the fact.

        `touch` marks an object dirty synchronously, because a caller that has just
        appended must not be able to return before the manager knows there is
        something to flush — otherwise a flush racing that return would skip it.
        The size often arrives a moment later, from a stat; this applies it and
        re-checks the byte threshold.  */
    const setSize = (relPath, size) => {
        const entry = entries.get(relPath);
        if (!entry || typeof (size) !== 'number') { return; }
        entry.localSize = size;
        if (!entry.dirty) { return; }

        const unflushed = size - entry.remoteSize;
        if (opts.flushMaxBytes > 0 && unflushed >= opts.flushMaxBytes) {
            clearTimers(entry);
            flush(relPath, () => {});
        }
    };

    /*  Flush immediately, regardless of the timers. Used for checkpoints, for a
        channel whose last editor just left, and before any destructive operation. */
    const flushNow = (relPath, cb) => {
        const entry = getEntry(relPath);
        clearTimers(entry);
        flush(relPath, cb);
    };

    // ------------------------------------------------------------------ flush

    const quarantine = (entry, err, cb) => {
        counters.conflicts++;
        const from = toPath(entry.relPath);
        const to = Path.join(root, '.conflict',
            `${entry.relPath}.${+new Date()}`);

        Log.error('CACHE_OWNERSHIP_CONFLICT', {
            key: entry.key,
            error: err && err.code,
            quarantined: to
        });

        /*  Do not retry and do not overwrite: a conditional-write failure means
            somebody else owns this object now, and clobbering their data would turn
            a detectable problem into a silent one. Preserve our copy for an operator
            and drop it from the cache so the next read re-hydrates from the store. */
        Fs.mkdir(Path.dirname(to), { recursive: true }, () => {
            Fs.rename(from, to, () => {
                Journal.remove(from, () => {
                    entries.delete(entry.relPath);
                    cb(err);
                });
            });
        });
    };

    const readTail = (relPath, start, end, cb) => {
        const length = end - start;
        if (length <= 0) { return void cb(void 0, Buffer.alloc(0)); }
        Fs.open(toPath(relPath), 'r', (err, fd) => {
            if (err) { return void cb(err); }
            const buffer = Buffer.alloc(length);
            Fs.read(fd, buffer, 0, length, start, (err, bytesRead) => {
                Fs.close(fd, () => {});
                if (err) { return void cb(err); }
                cb(void 0, bytesRead === length ? buffer : buffer.subarray(0, bytesRead));
            });
        });
    };

    const doFlush = (entry, cb) => {
        const relPath = entry.relPath;

        statLocal(relPath, (err, stats) => {
            if (err) { return void cb(err); }
            if (!stats) {
                // the file went away (removed or archived while we were queued)
                entry.dirty = false;
                return void cb();
            }

            const localSize = stats.size;
            if (localSize === entry.remoteSize && entry.etag) {
                entry.dirty = false;
                return void cb();
            }

            const finish = (err, stat) => {
                if (err) {
                    if (err.code === 'EPRECONDITION' || err.code === 'EEXIST') {
                        return void quarantine(entry, err, cb);
                    }
                    counters.flushFailures++;
                    Log.error('CACHE_FLUSH_ERROR', {
                        key: entry.key, error: err.message || err.code
                    });
                    // stay dirty: the data is still on local disk and the next
                    // trigger (or the drain at shutdown) will try again
                    return void cb(err);
                }

                entry.remoteSize = stat.size;
                entry.etag = stat.etag;
                counters.flushes++;

                /*  Writes that landed while the upload was in flight must not be
                    marked clean. Clearing the flag unconditionally here would hide
                    them from flushAll, so a *graceful* shutdown would drop messages
                    that had already been acknowledged — the failure this whole
                    layer exists to prevent. Re-read the size and compare.  */
                statLocal(relPath, (err, current) => {
                    const stillAhead = Boolean(current && current.size > entry.remoteSize);
                    entry.dirty = stillAhead;
                    if (!stillAhead) { entry.firstDirtyAt = 0; }

                    const state = Journal.clean(entry.key, stat,
                        current ? current.size : localSize);
                    state.dirty = stillAhead;
                    Journal.write(toPath(relPath), state, () => { cb(); });
                });
            };

            /*  Append server-side where it pays off: the object must already be
                large enough for S3 to copy it as a multipart part, and the backend
                must actually support it (measured at startup, not assumed).
                Otherwise re-upload the file we already hold in full — which is
                cheaper than the backend's own read-modify-write fallback.  */
            const canAppend = backend.capabilities.serverSideAppend &&
                entry.remoteSize >= opts.appendThresholdBytes &&
                localSize > entry.remoteSize;

            if (canAppend) {
                return void readTail(relPath, entry.remoteSize, localSize, (err, tail) => {
                    if (err) { return void cb(err); }
                    counters.flushBytes += tail.length;
                    backend.append(entry.key, tail, {
                        expectedSize: entry.remoteSize
                    }, finish);
                });
            }

            Fs.readFile(toPath(relPath), (err, body) => {
                if (err) { return void cb(err); }
                counters.flushBytes += body.length;

                const putOpts = {};
                if (backend.capabilities.conditionalPut) {
                    /*  Guard against a second writer. We expect either to be
                        creating the object, or to be replacing exactly the version
                        we last saw; anything else is an ownership conflict. */
                    if (entry.etag) {
                        putOpts.ifMatch = entry.etag;
                    } else if (entry.remoteSize === 0) {
                        putOpts.ifNoneMatch = true;
                    }
                }
                backend.put(entry.key, body, putOpts, finish);
            });
        });
    };

    /*  Flush one object.

        The contract is that when this calls back, everything written before the
        call is in the store. Because appends continue during an upload, that means
        looping until the local file and the object agree — otherwise flushAll()
        could report success while the newest messages were still only on local
        disk.

        The loop is bounded: a channel being written faster than it can be uploaded
        would otherwise spin here. Hitting the bound hands the object back to the
        scheduler, which is the right outcome — the shutdown drain has its own
        timeout for exactly this case.  */
    const MAX_FLUSH_ROUNDS = 10;

    const flush = (relPath, _cb) => {
        const cb = Util.once(Util.mkAsync(_cb));
        const entry = getEntry(relPath);

        // collapse concurrent requests onto the in-flight upload
        if (entry.flushing) {
            entry.pendingFlush.push(cb);
            return;
        }
        if (!entry.dirty) { return void cb(); }

        entry.flushing = true;
        clearTimers(entry);

        let rounds = 0;
        const round = () => {
            sema.take(give => {
                const done = give();
                doFlush(entry, err => {
                    done();

                    // more arrived while we were uploading: keep going
                    if (!err && entry.dirty && !closed && ++rounds < MAX_FLUSH_ROUNDS) {
                        return void round();
                    }

                    entry.flushing = false;
                    if (entry.dirty && !closed) { scheduleFlush(entry); }

                    const waiting = entry.pendingFlush.splice(0, entry.pendingFlush.length);
                    cb(err);
                    waiting.forEach(f => { f(err); });
                });
            });
        };
        round();
    };

    /*  Flush everything outstanding. This is the drain step at shutdown: whatever
        it does not manage is left on local disk and recovered at the next boot. */
    const flushAll = (_cb) => {
        const cb = Util.once(Util.mkAsync(_cb));
        const dirty = [];
        entries.forEach(entry => { if (entry.dirty) { dirty.push(entry.relPath); } });
        if (!dirty.length) { return void cb(void 0, { flushed: 0 }); }

        let pending = dirty.length;
        let failed = 0;
        dirty.forEach(relPath => {
            flushNow(relPath, err => {
                if (err) { failed++; }
                pending--;
                if (!pending) { cb(void 0, { flushed: dirty.length - failed, failed }); }
            });
        });
    };

    // --------------------------------------------------------------- recovery

    /*  Walk the cache at boot and reconcile every file against the store.

        The journal's `dirty` flag is not trusted here: sizes are compared directly,
        so a crash between an append and a state write cannot lose the appended
        bytes. Four outcomes:

          local > remote   the normal "killed with unflushed writes" case: flush
          local == remote  clean, adopt the recorded etag
          local < remote   our copy is stale (ownership moved away and back);
                           discard it so the next read re-hydrates
          diverged         same size, different content: quarantine and alarm
    */
    const recover = (_cb) => {
        const cb = Util.once(Util.mkAsync(_cb));
        const found = [];

        const walk = (dir, relDir, next) => {
            Fs.readdir(dir, { withFileTypes: true }, (err, list) => {
                if (err) { return void next(); }
                let i = 0;
                const step = () => {
                    if (i >= list.length) { return void next(); }
                    const item = list[i++];
                    const relPath = relDir ? Path.join(relDir, item.name) : item.name;
                    // internal directories are not cached objects
                    if (item.isDirectory()) {
                        if (item.name === '.conflict') { return void setImmediate(step); }
                        return void walk(Path.join(dir, item.name), relPath, () => {
                            setImmediate(step);
                        });
                    }
                    if (!item.isFile()) { return void setImmediate(step); }
                    if (Journal.isJournalPath(item.name)) { return void setImmediate(step); }
                    // transient files are not objects either
                    if (/\.(tmp|temp|hydrating)\b/.test(item.name)) {
                        return void setImmediate(step);
                    }
                    if (typeof (conf.isCacheable) === 'function' &&
                        !conf.isCacheable(relPath)) {
                        return void setImmediate(step);
                    }
                    found.push(relPath);
                    setImmediate(step);
                };
                step();
            });
        };

        walk(root, '', () => {
            if (!found.length) { return void cb(void 0, { scanned: 0, flushed: 0 }); }

            const report = { scanned: found.length, flushed: 0, stale: 0, clean: 0, conflicts: 0 };
            let pending = found.length;
            const finish = () => {
                pending--;
                if (pending) { return; }
                Log.info('CACHE_RECOVERY', report);
                cb(void 0, report);
            };

            found.forEach(relPath => {
                sema.take(give => {
                    const release = Util.once(() => { give(); finish(); });
                    const entry = getEntry(relPath);

                    statLocal(relPath, (err, stats) => {
                        if (err || !stats) { return void release(); }

                        Journal.read(toPath(relPath), (err, state) => {
                            backend.head(entry.key, (err, remote) => {
                                if (err && err.code !== 'ENOENT') {
                                    Log.error('CACHE_RECOVERY_ERROR', {
                                        key: entry.key, error: err.message
                                    });
                                    return void release();
                                }

                                const remoteSize = remote ? remote.size : 0;
                                entry.remoteSize = remoteSize;
                                entry.etag = remote && remote.etag;
                                entry.hydrated = true;

                                if (stats.size > remoteSize) {
                                    // local is ahead: flush the difference
                                    report.flushed++;
                                    counters.recovered++;
                                    markDirty(entry, stats.size);
                                    Log.info('CACHE_RECOVERY_FLUSH', {
                                        key: entry.key,
                                        localSize: stats.size,
                                        remoteSize
                                    });
                                    return void flush(relPath, () => { release(); });
                                }

                                if (stats.size < remoteSize) {
                                    // local is behind: it cannot be trusted
                                    report.stale++;
                                    Log.warn('CACHE_RECOVERY_STALE', {
                                        key: entry.key,
                                        localSize: stats.size,
                                        remoteSize
                                    });
                                    return void forget(relPath, () => { release(); });
                                }

                                // same size: only a recorded etag can tell us whether
                                // this is the same content
                                if (state && state.etag && remote &&
                                    state.etag !== remote.etag) {
                                    report.conflicts++;
                                    return void quarantine(entry,
                                        { code: 'EDIVERGED' }, () => { release(); });
                                }

                                report.clean++;
                                entry.dirty = false;
                                release();
                            });
                        });
                    });
                });
            });
        });
    };

    // -------------------------------------------------------------- lifecycle

    /*  Drop an object from the cache. Refuses while it is dirty, because the local
        copy is the only place those bytes exist. */
    const forget = (relPath, _cb) => {
        const cb = Util.once(Util.mkAsync(_cb));
        const entry = entries.get(relPath);
        if (entry && entry.dirty) { return void cb(new Error('E_DIRTY')); }
        if (entry) { clearTimers(entry); }

        const path = toPath(relPath);
        Fs.unlink(path, err => {
            if (err && err.code !== 'ENOENT') { return void cb(err); }
            Journal.remove(path, () => {
                entries.delete(relPath);
                cb();
            });
        });
    };

    /*  Forget an object *and* its state without the dirty check, for use after the
        object has been removed or archived in the store itself. */
    const discard = (relPath, _cb) => {
        const cb = Util.once(Util.mkAsync(_cb));
        const entry = entries.get(relPath);
        if (entry) { clearTimers(entry); entry.dirty = false; }
        forget(relPath, cb);
    };

    /*  Evict clean, idle objects until the cache is back under its limit.

        Dirty objects are never candidates, and neither is anything the caller
        reports as in use — evicting a channel with connected editors would just
        force an immediate re-download.  */
    const evict = (_cb) => {
        const cb = Util.once(Util.mkAsync(_cb));
        if (!opts.maxBytes && !opts.maxIdleMs) { return void cb(void 0, { evicted: 0 }); }

        const now = +new Date();
        const candidates = [];
        let total = 0;

        const consider = (relPath, size, atime) => {
            total += size;
            const entry = entries.get(relPath);
            if (entry && entry.dirty) { return; }
            if (typeof (conf.isPinned) === 'function' && conf.isPinned(relPath)) { return; }
            candidates.push({ relPath, size, atime });
        };

        const walk = (dir, relDir, next) => {
            Fs.readdir(dir, { withFileTypes: true }, (err, list) => {
                if (err) { return void next(); }
                let i = 0;
                const step = () => {
                    if (i >= list.length) { return void next(); }
                    const item = list[i++];
                    const relPath = relDir ? Path.join(relDir, item.name) : item.name;
                    if (item.isDirectory()) {
                        if (item.name === '.conflict') { return void setImmediate(step); }
                        return void walk(Path.join(dir, item.name), relPath,
                            () => setImmediate(step));
                    }
                    if (!item.isFile() || Journal.isJournalPath(item.name)) {
                        return void setImmediate(step);
                    }
                    Fs.stat(Path.join(dir, item.name), (err, stats) => {
                        if (!err) {
                            consider(relPath, stats.size,
                                Math.max(stats.atimeMs || 0, stats.mtimeMs || 0));
                        }
                        setImmediate(step);
                    });
                };
                step();
            });
        };

        walk(root, '', () => {
            const doomed = [];

            if (opts.maxIdleMs > 0) {
                candidates.forEach(item => {
                    if (now - item.atime > opts.maxIdleMs) { doomed.push(item); }
                });
            }

            if (opts.maxBytes > 0 && total > opts.maxBytes) {
                // oldest first, until we are under the limit
                const remaining = candidates
                    .filter(item => doomed.indexOf(item) === -1)
                    .sort((a, b) => a.atime - b.atime);
                let freed = doomed.reduce((sum, item) => sum + item.size, 0);
                for (const item of remaining) {
                    if (total - freed <= opts.maxBytes) { break; }
                    doomed.push(item);
                    freed += item.size;
                }
            }

            if (!doomed.length) { return void cb(void 0, { evicted: 0, bytes: total }); }

            let pending = doomed.length;
            let evicted = 0;
            doomed.forEach(item => {
                forget(item.relPath, err => {
                    if (!err) { evicted++; counters.evictions++; }
                    pending--;
                    if (!pending) { cb(void 0, { evicted, bytes: total }); }
                });
            });
        });
    };

    // ---------------------------------------------------------------- metrics

    /*  Point-in-time state. `oldestDirtyAgeMs` is the live exposure window: it is
        directly comparable with the configured flushMaxDelayMs, and is the number
        worth alerting on.  */
    const stats = () => {
        const now = +new Date();
        let dirtyCount = 0;
        let dirtyBytes = 0;
        let oldest = 0;
        let inflight = 0;

        entries.forEach(entry => {
            if (entry.flushing) { inflight++; }
            if (!entry.dirty) { return; }
            dirtyCount++;
            if (typeof (entry.localSize) === 'number') {
                dirtyBytes += Math.max(0, entry.localSize - entry.remoteSize);
            }
            if (entry.firstDirtyAt && (!oldest || entry.firstDirtyAt < oldest)) {
                oldest = entry.firstDirtyAt;
            }
        });

        return {
            dirtyCount,
            dirtyBytes,
            oldestDirtyAgeMs: oldest ? now - oldest : 0,
            flushInflight: inflight,
            cachedObjects: entries.size,
            counters: Object.assign({}, counters)
        };
    };

    if (monitoring && typeof (monitoring.registerGauge) === 'function') {
        const label = conf.metricsPrefix || 's3';
        // pull-based: the dirty set changes on every stored message, so pushing a
        // value per mutation would be pure noise
        monitoring.registerGauge(`${label}_dirty_channels`, () => stats().dirtyCount);
        monitoring.registerGauge(`${label}_dirty_bytes`, () => stats().dirtyBytes);
        monitoring.registerGauge(`${label}_oldest_dirty_age_ms`, () => stats().oldestDirtyAgeMs);
        monitoring.registerGauge(`${label}_flush_inflight`, () => stats().flushInflight);
        monitoring.registerGauge(`${label}_cached_objects`, () => stats().cachedObjects);
    }

    const close = (_cb) => {
        const cb = Util.once(Util.mkAsync(_cb));
        flushAll(() => {
            closed = true;
            entries.forEach(clearTimers);
            cb();
        });
    };

    const manager = {
        root, prefix,
        hydrate, touch, setSize, flush: flushNow, flushAll,
        recover, forget, discard, evict,
        stats, close,
        keyFor: toKey,
        pathFor: toPath,
        // exposed for tests and for callers that need to reason about state
        _entries: entries
    };

    Fs.mkdir(root, { recursive: true }, err => {
        if (err && err.code !== 'EEXIST') { return void cb(err); }
        cb(void 0, manager);
    });

    return manager;
};
