// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors
//
// SPDX-License-Identifier: AGPL-3.0-or-later

/*  Per-object cache state, persisted beside each cached file.

    When a node dies with writes that were acknowledged to clients but not yet
    pushed to the object store, the only thing standing between that and data loss
    is knowing, at the next boot, that the local copy is ahead of the remote one.
    That is what this file records.

    The state lives in a sidecar named `<file>.s3state`:

        {
          "key": "channel/ab/abcd….ndjson",
          "etag": "\"9f8…\"",       // the remote object as of the last flush
          "remoteSize": 148213,     // how many bytes of this file are in the store
          "localSize": 149004,      // how many there were locally when last recorded
          "dirty": true,
          "flushedAt": 1765432100000
        }

    Two properties matter:

    * `remoteSize` is authoritative for what has been flushed. Everything from that
      offset to the end of the local file is the unflushed tail.
    * the `dirty` flag is an *optimisation*, not a correctness requirement. Recovery
      compares the real local size against `remoteSize` rather than trusting the
      flag, so a crash between an append and a state update cannot lose data. This
      is deliberate: writing a sidecar on every appended message would double the
      write load for no benefit.

    Sidecars never leave the cache. They are not objects in the store.
*/

const Fs = require("node:fs");
const Path = require("node:path");

const Journal = module.exports;

const SUFFIX = '.s3state';
Journal.SUFFIX = SUFFIX;

Journal.pathFor = filePath => filePath + SUFFIX;

/*  True for the sidecars themselves, so that scans and evictions can skip them
    rather than treating them as cached objects. */
Journal.isJournalPath = filePath => filePath.endsWith(SUFFIX);

/*  Written via a temp file and rename so that a crash mid-write leaves either the
    previous state or the new one, never a truncated JSON document that would fail
    to parse and strand the object it describes. */
Journal.write = (filePath, state, _cb) => {
    const cb = typeof (_cb) === 'function' ? _cb : () => {};
    const path = Journal.pathFor(filePath);
    const temp = `${path}.tmp.${process.pid}.${Math.random().toString(36).slice(2, 8)}`;

    let serialized;
    try {
        serialized = JSON.stringify(state);
    } catch (err) {
        return void cb(err);
    }

    Fs.mkdir(Path.dirname(path), { recursive: true }, err => {
        if (err && err.code !== 'EEXIST') { return void cb(err); }
        Fs.writeFile(temp, serialized, err => {
            if (err) { return void cb(err); }
            Fs.rename(temp, path, err => {
                if (err) {
                    return void Fs.unlink(temp, () => { cb(err); });
                }
                cb();
            });
        });
    });
};

/*  Calls back with the recorded state, or undefined when there is none.

    A sidecar that cannot be parsed is treated as absent rather than as an error:
    the object it describes is still recoverable by comparing its size against the
    store, which is what recovery does anyway.  */
Journal.read = (filePath, cb) => {
    Fs.readFile(Journal.pathFor(filePath), 'utf8', (err, content) => {
        if (err) {
            if (err.code === 'ENOENT') { return void cb(void 0, undefined); }
            return void cb(err);
        }
        let state;
        try {
            state = JSON.parse(content);
        } catch (err) {
            return void cb(void 0, undefined);
        }
        if (!state || typeof (state) !== 'object') { return void cb(void 0, undefined); }
        cb(void 0, state);
    });
};

Journal.remove = (filePath, _cb) => {
    const cb = typeof (_cb) === 'function' ? _cb : () => {};
    Fs.unlink(Journal.pathFor(filePath), err => {
        if (err && err.code !== 'ENOENT') { return void cb(err); }
        cb();
    });
};

/*  Build the state to record after a successful flush. */
Journal.clean = (key, stat, localSize) => {
    return {
        key,
        etag: stat && stat.etag,
        remoteSize: stat ? stat.size : 0,
        localSize: typeof (localSize) === 'number' ? localSize : (stat ? stat.size : 0),
        dirty: false,
        flushedAt: +new Date()
    };
};

/*  Build the state to record when an object first diverges from the store. */
Journal.dirty = (previous, key, localSize) => {
    return {
        key,
        etag: previous && previous.etag,
        remoteSize: previous && typeof (previous.remoteSize) === 'number' ?
            previous.remoteSize : 0,
        localSize: typeof (localSize) === 'number' ? localSize : 0,
        dirty: true,
        flushedAt: previous && previous.flushedAt
    };
};
