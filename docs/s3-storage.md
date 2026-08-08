<!-- SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors

SPDX-License-Identifier: AGPL-3.0-or-later
-->
# S3 Object Storage for the Scalable Server

**Status:** implemented for channels, pins, blobs, blocks, key/value stores, tasks and
decrees; verified against Scaleway Object Storage. Remaining work is listed in §11.
**Scope:** allow CryptPad's scalable server to keep its durable data in an S3-compatible
object store, while preserving the local append-log semantics that the history keeper
depends on. The S3 backend must be optional: an instance that does not enable it must
behave exactly as it does today, and must not need any S3 client library installed.

---

## 1. Analysis of the current storage layer

### 1.1 Node topology and the single-writer property

The server is split into `front`, `core` and `storage` nodes (`readme.md`). Only
`storage` nodes touch the database. Which storage node owns a given piece of data is
decided by a jump consistent hash over the first 8 bytes of the channel id / public key
(`common/env.js:70-79`, `Env.getStorageId`). Every read and write for a channel is
therefore routed to exactly one storage node.

Within a storage node:

* the **primary process** (`storage/index.js`) holds `Env.store` (channels),
  `Env.pinStore` (pin logs) and `Env.blobStore`, and serializes all operations per
  channel id through `storage/schedule.js` (ordered / unordered / blocking queues);
* **fork workers** (`storage/worker.js`, `./build/storage.worker.js`) open their *own*
  `File.create` / `Blob.create` handles on the *same local paths* for CPU-heavy read
  jobs (index computation, metadata parsing, hash offsets, size queries, tasks);
* **cluster workers** (`storage/cluster.js`) run the storage HTTP server and open their
  own `Blob.create` handle for uploads and for serving `/blob`, `/datastore`, `/block`.

This gives a very useful invariant for an object-store design:

> For any given channel, at any given time, exactly one storage node is entitled to
> write it — and inside that node, writes are serialized by the per-channel scheduler.

Everything below leans on this. The failure cases (topology change, split brain,
restart with unflushed data) are addressed in §7.

### 1.2 Data families

| Family | Path (per storage index `idx`) | Module | Semantics |
|---|---|---|---|
| Channel logs | `data/<idx>/channel/<xx>/<id>.ndjson` | `storage/storage/file.js` | **append-only log**, byte offsets are meaningful |
| Channel metadata | `…/<id>.metadata.ndjson` | same | append-only log of metadata amendments |
| Channel offset hint | `…/<id>.ndjson.offset` | same | *pure local optimisation*, disposable |
| Channel placeholder | `…/<id>.ndjson.placeholder` | same | tiny file, explains why a pad is gone |
| Channel rewrite temp | `…/<id>.ndjson.temp` | same | transient, used by `filterMessages` |
| Pin logs | `data/<idx>/pins/<xx>/<safeKey>.ndjson` | `file.js` (`volumeId: 'pins'`) | append-only log |
| Blobs | `data/<idx>/blob/<xx>/<blobId>` | `storage/storage/blob.js` | **immutable** once completed |
| Blob metadata | `…/<blobId>.metadata.ndjson` | same | small append-only log |
| Blob activity | `…/<blobId>.activity` | same | single mtime-ish value, eviction heuristic |
| Blob staging | `data/<idx>/blobstage/<xx>/<safeKey>` | same | transient, per-upload |
| Login blocks | `data/<idx>/block/<xx>/<safeKey>` | `storage/storage/block.js` | ≤256 B, overwrite-by-archive |
| Users / invitations / sessions / MFA / support | `data/<idx>/{users,invitations,sessions,mfa,support}/…` | `common/storage/basic.js` | small JSON blobs, read/write/delete |
| Challenges | `data/<idx>/challenges/…` | `core/storage/challenge.js` → `basic.js` | short-lived JSON blobs (on **core** nodes) |
| Tasks | `data/<idx>/tasks/<dayBucket>/<hash>.ndjson` | `storage/storage/tasks.js` | small files, listed & deleted in bulk |
| Decrees | `data/0/decrees/decree.ndjson` | `common/decrees-core.js` | append-only, single writer (`storage:0`) |
| Archive | `data/<idx>/archive/{datastore,pins,blob,block,accounts}/…` | all of the above | move target, cold |
| Logo | `data/0/logo/` | `storage/commands/admin.js` | one small file |

### 1.3 Where the filesystem is touched directly

```
storage/storage/file.js       channels + pins (streams, append, truncate, move, stat, readdir)
storage/storage/blob.js       blobs, blob metadata, staging, activity
storage/storage/block.js      login blocks
storage/storage/tasks.js      scheduled tasks
common/storage/basic.js       users / invitations / sessions / mfa / support / challenges
common/decrees-core.js        decrees
storage/pin-manager.js        pin log reads
storage/commands/pin.js       pin directory listing
storage/commands/admin.js     account archival reports, logo
storage/cluster.js            Express.static for /blob, /datastore, /block, /api/logo
storage/worker.js             account archival reports (Fse.readJson)
http-server/worker.js         static client assets only (not user data — out of scope)
```

### 1.4 Which operations genuinely need a local file

The history keeper is built around byte offsets into the channel log:

* `computeIndex` (`storage/worker.js:108`) records `cpIndex[].offset` and
  `offsetByHash[hash] = byteOffset`, and persists a `start` offset hint;
* `GET_HISTORY` re-reads the log from a byte offset
  (`storage/history-manager.js:137`, `Env.store.readMessagesBin(channel, start, …)`);
* `readFileBin` (`storage/stream-file.js:63`) counts offsets as it splits the stream on
  newlines, and the caller may `abort()` mid-stream;
* `messageBin` (`file.js:957`) keeps a cached `fs.WriteStream` in append mode per
  channel for up to `CHANNEL_WRITE_WINDOW` (5 min);
* `filterMessages` (`file.js:1003`) rewrites a log in place (trim / delete-line);
* `clearChannel` truncates to the end of the metadata line.

None of this maps onto S3 primitives directly. Conversely, a *byte-identical* local
copy of the object makes all of it work unmodified — which is the basis of the design.

### 1.5 Operations that map cleanly onto S3 and should never hydrate

* archive / restore (`Fse.move`) → server-side `CopyObject` + `DeleteObject`
* channel existence checks → `HeadObject`
* size / stats (`getChannelSize`, `blobStore.size`, `getStats`) → `HeadObject`
* remove → `DeleteObject`
* listing (`listChannels`, `list.blobs`, task buckets, pin dir) → `ListObjectsV2`
* blob download over HTTP → presigned `GET` or a streaming proxy

---

## 2. Design goals

1. **Optional.** `storage.type: 'fs'` (default) must behave byte-for-byte as today, with
   zero new dependencies. The S3 code ships as a plugin and is only loaded when enabled.
2. **Two clean seams, not one.** A narrow *object backend* interface (the only S3-aware
   code) plus a *cached log* layer that turns object storage into the append-log
   filesystem that `file.js` already knows how to drive.
3. **No change to callers.** `Env.store`, `Env.pinStore`, `Env.blobStore` keep their
   exact current method signatures. `storage/history-manager.js`,
   `storage/channel-manager.js`, `storage/commands/*` are untouched.
4. **S3 is the source of truth.** The local cache is a working copy that can be deleted
   at any time (when clean) without data loss.
5. **Bounded, explicit RPO.** Messages are acknowledged to clients before they reach S3;
   the flush policy defines the exposure window and must be configurable and observable.
6. **Cheap.** No read-modify-write of an entire multi-megabyte log on every keystroke.

---

## 3. Proposed architecture

```
        ┌──────────────────────────────────────────────────────────────┐
        │ callers (unchanged)                                          │
        │ history-manager · channel-manager · commands/* · worker.js   │
        └───────────────┬──────────────────────────────────────────────┘
                        │  Env.store / Env.pinStore / Env.blobStore
        ┌───────────────▼──────────────────────────────────────────────┐
   L3   │ Store factory  storage/storage/index.js                      │
        │   type 'fs'  → file.js / blob.js / block.js / basic.js       │
        │   type 's3'  → cached-file.js / cached-blob.js / …           │
        └───────────────┬──────────────────────────────────────────────┘
                        │
        ┌───────────────▼──────────────────────────────────────────────┐
   L2   │ Cached-log layer  common/storage/cache/*.js                  │
        │   hydrate · dirty tracking · flush scheduler · eviction      │
        │   crash journal · generation/ETag guard · local FS engine    │
        └───────────────┬──────────────────────────────────────────────┘
                        │  ObjectBackend (§4)
        ┌───────────────▼──────────────────────────────────────────────┐
   L1   │ common/storage/backend/fs.js      (in tree, no deps)         │
        │ plugins/S3/backend.js             (optional, @aws-sdk)       │
        └──────────────────────────────────────────────────────────────┘
```

### 3.1 New / changed files

```
common/storage/backend/index.js      backend registry + selection from config
common/storage/backend/fs.js         reference implementation over the local FS
common/storage/backend/types.d.ts    the interface, documented once

common/storage/cache/manager.js      hydration, dirty set, flush scheduler, eviction
common/storage/cache/journal.js      crash-safe sidecar state per cached object
common/storage/cache/append.js       "upload the tail" strategies (§5.3)

storage/storage/index.js             NEW  store factory (fs | s3)
storage/storage/cached-file.js       NEW  channel/pin store on top of file.js + cache
storage/storage/cached-blob.js       NEW  blob store on top of blob.js + backend
storage/storage/cached-block.js      NEW  login blocks
common/storage/basic.js              CHANGED to route through a backend
storage/storage/tasks.js             CHANGED to route through a backend
common/decrees-core.js               CHANGED to route through a backend

storage/http-data.js                 NEW  backend-aware handlers for /blob /datastore /block
storage/cluster.js                   CHANGED to use the above instead of Express.static

plugins/S3/index.js                  plugin manifest: registers the 's3' backend
plugins/S3/backend.js                the S3 ObjectBackend implementation
plugins/S3/package.json              @aws-sdk/client-s3, @aws-sdk/s3-request-presigner
```

---

## 4. Layer 1 — the `ObjectBackend` interface

One flat namespace of keys, callback style to match the codebase. Every method takes a
key relative to the configured bucket prefix, e.g. `channel/ab/abcd….ndjson`.

```js
Backend = {
    // --- reads -----------------------------------------------------
    head:        (key, cb) => cb(err, { size, etag, mtime } | null),
    get:         (key, cb) => cb(err, Buffer),
    getStream:   (key, opts /* {start, end} */, cb) => cb(err, ReadableStream),
    list:        (prefix, opts /* {delimiter, cursor, limit} */, cb) =>
                     cb(err, { keys: [{key, size, mtime, etag}], prefixes, cursor }),

    // --- writes ----------------------------------------------------
    put:         (key, buffer|Stream, opts /* {ifNoneMatch, ifMatch, contentType} */, cb)
                     => cb(err, { etag, size }),
    append:      (key, tailBuffer, opts /* {expectedSize, expectedEtag} */, cb)
                     => cb(err, { etag, size }),   // §5.3
    copy:        (srcKey, dstKey, opts /* {overwrite} */, cb) => cb(err),
    move:        (srcKey, dstKey, opts, cb) => cb(err),
    remove:      (key, cb) => cb(err),
    removePrefix:(prefix, cb) => cb(err),

    // --- optional --------------------------------------------------
    presignGet:  (key, ttlSeconds, cb) => cb(err, url),   // undefined ⇒ proxy mode
    capabilities: {
        conditionalPut: Boolean,   // If-None-Match: * / If-Match: <etag>
        serverSideAppend: Boolean, // UploadPartCopy-based append
        presign: Boolean,
        atomicMove: Boolean
    }
};
```

Notes:

* `fs.js` implements all of it with `fs` / `fs-extra`, `capabilities` all true except
  `presign`. It exists so the cache layer has exactly one code path, and so the whole
  stack can be unit-tested without a bucket.
* `append` is the only non-obvious method; see §5.3.
* `capabilities` is **probed at startup, not hard-coded** (§4.1).
* Error normalisation: backends must surface a `ENOENT`-shaped error (`err.code ===
  'ENOENT'`) for missing keys, because callers all over the tree special-case it
  (`file.js:219`, `file.js:406`, `blob.js`, `worker.js:483`).

### 4.1 Provider differences and the startup capability probe

The target is **S3-compatible** storage, not AWS specifically — Scaleway Object Storage,
MinIO, Ceph RGW, Garage, OVH, Cloudflare R2 and Backblaze B2 all speak the same core API
but diverge at exactly the three edges this design leans on:

| Capability | Used for | Fallback if absent |
|---|---|---|
| `UploadPartCopy` (server-side multipart copy) | O(tail) flushes of large logs (§5.3b) | full `PUT` of the whole object — correct, just more bandwidth |
| Conditional `PUT` (`If-None-Match: *`, `If-Match: <etag>`) | ownership-conflict detection (§7.2), `wx` semantics for `basic.js` (§6.4) | `head`-then-`put`, with a documented race window and a warning at boot |
| Presigned `GET` | serving `/blob` by redirect (§8) | proxy-stream mode |

Rather than maintaining a per-provider matrix that goes stale, the S3 backend **probes
the configured bucket at startup** against a throwaway key under `<prefix>_probe/`:

1. `PUT` with `If-None-Match: *`, then the same `PUT` again — the second must fail with
   `412`; if it succeeds, conditional writes are not honoured;
2. `PUT` a 6 MiB object, then a `CreateMultipartUpload` + `UploadPartCopy` + `UploadPart`
   + `CompleteMultipartUpload` cycle;
3. generate a presigned URL and `GET` it;
4. delete the probe objects.

The result is logged once at boot (`S3_CAPABILITIES`), stored in `backend.capabilities`,
and each degraded capability emits an explicit warning naming what changes as a result.
This turns "does Scaleway support X?" from a documentation question into an
observable fact about the actual bucket, and it catches misconfiguration (wrong region,
insufficient IAM permissions, read-only credentials) at startup rather than at the first
write. Individual capabilities can also be forced on or off in config to skip a probe
step.

**Measured on Scaleway** (`s3.fr-par.scw.cloud`, bucket in `fr-par`, 2026-08-07):

```
{ conditionalPut: true, serverSideAppend: true, presign: true, atomicMove: false }
```

Nothing is degraded, which settles three open points in this design's favour:

* conditional writes are **enforced**, not merely accepted — a repeated
  `If-None-Match: *` returns 412 — so the ownership-conflict detection in §7.2 is a real
  guarantee on this provider rather than best-effort;
* `UploadPartCopy` works, so appending to a large channel log is server-side and costs
  no download (§5.3b);
* presigned GETs work, so blobs can be served by redirect (§8) rather than proxied.

`atomicMove: false` is inherent to object storage — a move is a copy followed by a
delete — and is why archive/restore check the destination before moving rather than
relying on the store to refuse.

The value of the probe was demonstrated during development: run against a key that could
read but not write, it reported every capability as unavailable with the reason
`AccessDenied`, instead of assuming AWS semantics and failing at the first flush. Note
that those results described the *credentials*, not the provider — which is exactly why
the probe runs against the live bucket at startup rather than being recorded once.

---

## 5. Layer 2 — the cached append-log

### 5.1 Cache layout and states

The cache mirrors the current on-disk layout under a dedicated root
(`Env.paths.cache`, default `data/<idx>/cache/`), so `file.js` needs no path changes —
it is simply constructed with `root` pointing at the cache instead of the datastore.

Per cached object the manager keeps a sidecar `<path>.s3json`:

```json
{ "key": "channel/ab/abcd….ndjson",
  "etag": "\"9f8…\"", "remoteSize": 148213, "flushedAt": 1765432100000,
  "localSize": 149004, "dirty": true, "generation": 7 }
```

State machine per channel:

```
   ABSENT ──hydrate()──► CLEAN ──append()──► DIRTY ──flush()──► CLEAN
      ▲                    │                   │
      └───── evict() ──────┘                   └── flush() is the only path out
                                                   (DIRTY is never evicted)
```

### 5.2 Hydration

Triggered lazily, before any operation that needs local bytes (append, ranged read,
metadata read, rewrite). Never triggered for head/size/exists/archive/remove/list.

```
hydrate(channel):
  if local file exists:
      if sidecar.dirty        -> use local (it is ahead of S3)
      else if remote.size != sidecar.remoteSize or remote.etag != sidecar.etag
                              -> STALE: another owner wrote it (§7.3). discard + refetch
      else                    -> use local
  else:
      GET object -> <path>.hydrating.<pid>.<rand>
      link() into place, then unlink temp        # link() fails EEXIST: never clobbers
      write sidecar {etag, remoteSize, dirty:false}
```

`link()` rather than `rename()` is deliberate: two processes (primary + a fork worker)
may hydrate concurrently, and the loser must not overwrite a copy that the winner may
already have appended to. Both download the same immutable bytes, so the loser simply
discards its temp file.

**Hydration must be byte-exact.** Every persisted offset (`.offset` hint, the in-memory
`index.offsetByHash`, `cpIndex[].offset`) is a byte offset into this file. Any
transformation — compression, re-encoding, trailing-newline normalisation — breaks
`GET_HISTORY`. Compression, if wanted, belongs at the transport layer (`Content-Encoding`)
and must be transparent to the caller, or applied only to archived objects.

### 5.3 Flushing (uploading the tail)

Three strategies, selected per object by size and by `capabilities.serverSideAppend`:

**(a) Full PUT** — `put(key, wholeFile)`. Correct everywhere, O(size) per flush.
Used when `localSize < partThreshold` (default 5 MiB, the S3 minimum part size) or when
the backend cannot do server-side append.

**(b) Server-side append via multipart copy** — the recommended path for large logs:

```
CreateMultipartUpload(key)
UploadPartCopy(part 1 ← existing object, full range)     # server-side, no download
UploadPart     (part 2 ← the unflushed tail bytes)
CompleteMultipartUpload
```

Requires the existing object to be ≥5 MiB (S3 part-size minimum applies to every part
but the last), which is exactly the threshold at which (a) becomes expensive. Supported
by AWS S3, MinIO, Ceph RGW; verify on other providers before enabling
(`capabilities.serverSideAppend`).

**(c) Segmented logs** *(evolution, not v1)* — never mutate an object. Write immutable
segments `channel/ab/<id>/<seq>.<startOffset>.ndjson`; hydration concatenates them in
offset order; a compaction job periodically merges segments into a new generation.
Flushes become O(new bytes) with no read-modify-write at all, at the cost of a `list`
per hydration and a compactor. Worth adopting once the single-object layout proves to be
the bottleneck; the layout version should be recorded in a bucket-level marker object so
both can coexist during a migration.

### 5.4 Flush policy — when a document is written back to S3

Only **one** data family is written back lazily: the channel log, because it is the only
one that receives high-frequency appends. Everything else is written through to S3
synchronously, before the operation is acknowledged.

| Family | Write frequency | Policy |
|---|---|---|
| **Channel log** (`channel/`) | every message | **deferred**, see triggers below |
| Channel metadata log | on share/permission change | write-through (tiny, rare) |
| Pin log (`pins/`) | on pin/unpin | write-through |
| Blob | once, on upload completion | write-through (multipart PUT) |
| Blob metadata | once | write-through |
| Login blocks, users, invitations, sessions, MFA, support | on account operations | write-through |
| Decrees | on admin action | write-through |
| Tasks | on schedule/expiry | write-through |

So a crash can never lose a pin, a registration, a login block, an uploaded file, a
decree or a scheduled task. The only thing at risk is the tail of a document that was
being actively edited at that instant.

**Deferred flush triggers for channel logs** — the first of these to fire wins:

| Trigger | Config key | Default | Why |
|---|---|---|---|
| No append for a debounce period | `flushDebounceMs` | 5 s | the common case: a burst of typing, then a pause |
| Elapsed since the oldest unflushed byte | `flushMaxDelayMs` | 30 s | bounds the exposure window under continuous editing |
| Unflushed tail size | `flushMaxBytes` | 1 MiB | bounds re-upload cost for bulk writes and large pastes |
| A checkpoint message (`cp\|…`) is stored | `flushOnCheckpoint` | on | a checkpoint is a self-contained recovery point; cheap and meaningful |
| Last user leaves the channel (`onDropChannel`, `storage/index.js:89`) | — | immediate | **a closed document is always fully in S3** |
| `closeChannel` / `closeInactiveChannels` (the existing 5-minute write window) | — | immediate | |
| Before any blocking operation: trim, delete-line, clear, archive, remove | — | immediate | these rewrite or move the object server-side |
| Cache eviction selects the object | — | immediate | dirty objects are never evicted |
| `SIGTERM` / `SIGINT` | `shutdownFlushTimeoutMs` | flush all, then exit | §7.5 |

The three thresholds are the primary tuning knobs and must be settable per instance.
They are validated at boot: `flushDebounceMs ≤ flushMaxDelayMs`, all non-negative,
`flushMaxBytes ≥ 64 KiB`, and `flushDebounceMs: 0` is accepted as the strict
flush-before-acknowledge mode. Out-of-range values are rejected with a clear error rather
than silently clamped, since a misconfigured flush window is a silent durability
regression. The event-driven triggers below the thresholds are not configurable — they
are correctness requirements, not tuning.

In practice a document therefore reaches S3: **≤5 s after the user stops typing, ≤30 s
while they keep typing, at every checkpoint, and immediately when the last editor
disconnects.**

Flushes are asynchronous — they never block the client acknowledgement — and are
serialized per channel, so a channel never has two uploads in flight. A flush uploads
the tail as it exists when the upload starts; appends that arrive during the upload are
picked up by the next one.

**Why not flush on every message.** Each flush is a round trip to the S3 endpoint
(typically 20–100 ms to a regional endpoint such as `s3.fr-par.scw.cloud`), and with the
single-object layout a flush below the server-side-append threshold re-uploads the whole
log. Per-message flushing on a 2 MB pad with three active editors would mean re-uploading
several MB per second per pad, and would put an S3 round trip on the critical path of
every keystroke batch. The segmented layout (§5.3c) is what makes near-synchronous
flushing affordable, because a flush then uploads only the new bytes.

**Durability contract.** With the defaults, an ungraceful loss of a storage node exposes
up to ~30 s of *edits to actively-edited documents* — messages that were acknowledged to
clients but not yet in S3. Note that this is a difference of degree rather than of kind:
today `messageBin` acknowledges once the data is handed to the OS write stream
(`storage/storage/file.js:957`), without an `fsync`, so a local-disk deployment already
acknowledges before the data is durable.

Two mitigations, in order of preference:

* the local cache disk survives a process restart, so a `kill -9` or a crash loses
  nothing — the journal replays the unflushed tail at boot (§7.4). The window is only
  truly lost if the *machine or volume* is lost. On ephemeral storage (a pod with no
  persistent volume) this assumption fails and the strict mode below becomes necessary;
* a strict mode (`flushDebounceMs: 0`, flush before acknowledging) for deployments that
  cannot tolerate the window. Correct, but it puts an S3 round trip in the message path
  and is only really sensible with the segmented layout.

### 5.5 Eviction

An LRU over the cache root, run on an interval and on a high-water mark:

* never evict a `dirty` object;
* never evict a channel present in `Env.channel_cache` (has connected users);
* close the cached `fs.WriteStream` first (`closeChannel`, `file.js:274`) — the existing
  `CHANNEL_WRITE_WINDOW` machinery already does this after 5 minutes of inactivity;
* delete the log, its metadata log, the `.offset` hint and the sidecar together;
* configurable by `maxBytes` and `maxIdleMs`.

Because the offset hint is explicitly disposable (`file.js:1386-1388`), losing it on
eviction costs one full re-index, not correctness.

### 5.6 What never goes to S3

* `.offset` hints — pure local optimisation, regenerated on demand;
* `.temp` rewrite buffers — transient, `filterMessages` cleans them up;
* `blobstage/` — per-upload staging, already local to the cluster worker that owns the
  session; only the completed blob is uploaded;
* logs (`Env.Log` output).

### 5.7 Multi-process coordination inside a storage node

The fork workers open their own store handles on the same cache root. Two rules keep
this safe:

1. **Hydration is idempotent and no-clobber** (§5.2), so any process may hydrate.
2. **Only the primary flushes.** Workers are read-only against channel logs (index,
   metadata, hash offsets, older history, sizes); the dirty set and the flush scheduler
   live in the primary only. The one worker path that mutates channel data is task
   execution and account archival, which use archive/remove — server-side operations that
   the manager routes to the backend directly and that invalidate the local copy.

The primary already funnels every worker job that reads a channel through
`Env.store.getWeakLock(channel, …)` (`storage/index.js:506-541`), which is the natural
place to hook "ensure hydrated" before dispatching. The worker-side jobs that are *not*
wrapped today — `GET_FILE_SIZE`, `GET_MULTIPLE_FILE_SIZE`, `GET_TOTAL_SIZE`,
`GET_DELETED_PADS`, `GET_LAST_CHANNEL_TIME` — must either be answered from
`backend.head()` (preferred: no hydration at all for sizes) or gain the same wrapper.

---

## 6. Per-family mapping

### 6.1 Channels and pins (`file.js`)

`cached-file.js` exposes the same 30-odd methods as `File.create` and delegates:

| Method | Handling |
|---|---|
| `message`, `messageBin`, `writeMetadata` | hydrate → local append → mark dirty → schedule flush |
| `readMessagesBin`, `getMessages`, `readChannelMetadata`, `readDedicatedMetadata`, `getChannelMetadata` | hydrate → local read (unchanged code) |
| `getChannelSize`, `getChannelStats` | `backend.head()` on log + metadata, no hydration |
| `isChannelAvailable`, `isChannelArchived` | `backend.head()` |
| `removeChannel`, `removeArchivedChannel` | flush-cancel, drop local, `backend.remove()` |
| `archiveChannel`, `restoreArchivedChannel` | flush, drop local, `backend.move()` (server-side) |
| `clearChannel`, `trimChannel`, `deleteChannelLine` | flush → local rewrite (existing `filterMessages`) → archive old object → full `put()` of the rewritten log → bump generation, clear offset |
| `writeOffset`, `getOffset`, `clearOffset` | local only, never uploaded |
| `getPlaceholder` | `backend.get()` (tiny) |
| `listChannels`, `listArchivedChannels` | `backend.list()` with `prefix`/`delimiter`; `fast` mode returns keys only, slow mode uses the `size`/`mtime` already present in the listing — cheaper than today's per-file `stat` |
| `closeChannel`, `closeInactiveChannels` | flush then existing close |
| `getWeakLock` | unchanged scheduler behaviour + hydration hook |

The pin store is the same module with a different prefix (`pins/`) and archive volume,
exactly as today (`storage/index.js:823-830`).

### 6.2 Blobs (`blob.js`)

Blobs are immutable once `complete`, which makes them the easy case:

* upload stages to the **local** `blobstage/` as today (`blob.js:275`, chunked writes
  from `/upload-blob`);
* `complete` / `completeOwned` move the staged file into a multipart `put()` to
  `blob/<xx>/<id>`, then write the ownership proof and the metadata log;
* `readMetadata` / `writeMetadata` use the same cached-log mechanism as channels (these
  logs are small — full PUT on flush is fine);
* `.activity` is a single timestamp used only as an eviction heuristic: keep it in the
  local cache and flush it lazily (or fold it into object metadata on the blob itself);
  losing it costs an inaccurate last-access date, not data;
* `size` → `backend.head()`; `archive`/`restore`/`remove` → server-side copy/delete;
* `list.blobs` → `backend.list()`.

### 6.3 Login blocks (`block.js`)

≤256 B, no append. `write` = archive-then-`put`; `check`/`isAvailable`/`isArchived` =
`head`; `archive`/`restore` = server-side `move`. Served over HTTP (§8).

### 6.4 Basic key/value (`common/storage/basic.js`)

Users, invitations, sessions, MFA, support tickets, challenges. Direct mapping:

* `read` → `get`, `write` → `put` **with `If-None-Match: *`** to preserve the current
  `wx` (fail-if-exists) semantics (`basic.js:64`); if the backend lacks conditional PUT,
  fall back to `head`-then-`put` and log the race window;
* `delete` → `remove`, `deleteDir` → `removePrefix`, `readDir` → `list` with a delimiter;
* **`readDirSync` must go.** It is used once, at startup, for moderator keys
  (`storage/moderator.js:35`, via `Moderators.getKeysSync`, called from
  `storage/index.js:909`). This becomes async — a small, self-contained change that
  should land in the prep phase.

Challenges live on **core** nodes (`core/storage/challenge.js`) and were previously
node-local; with more than one core node that is already a latent issue, and routing
them through the shared backend fixes it as a side effect.

**Core nodes therefore need a storage backend of their own** — this is easy to miss,
because the Basic store is otherwise used only by storage nodes. Core builds one in
`core/index.js` before it begins serving, since challenges gate file uploads: a backend
that arrived late would fail the first upload after every restart. It skips the
capability probe, which uploads several MiB to test multipart copy and is pointless for
a node that only writes small objects.

### 6.5 Tasks, decrees, logo, archival reports

* Tasks (`tasks.js`): per-day prefixes map onto `list(prefix)`; `write` → `put`,
  `remove` → `remove`. Chatty but low volume (a 5-minute interval, `storage/index.js:844`).
* Decrees (`common/decrees-core.js`): append-only, single writer (`storage:0`), small.
  Read-all at boot, full `put` on append. No cache layer needed.
* Logo and account archival reports: `put` / `get`.

---

## 7. Consistency, ownership and failure modes

### 7.1 The guarantee we rely on

One owner per key (consistent hash) + one serialized queue per key inside that owner
(`schedule.js`) ⇒ no concurrent writers to any object. S3's read-after-write consistency
(strong since Dec 2020) then gives us everything else.

### 7.2 Guarding it anyway

Every flush carries a **conditional write**:

* first flush of a new object: `put(key, …, { ifNoneMatch: '*' })`;
* subsequent flushes: `put`/`append` with `ifMatch: sidecar.etag`.

A `412 PreconditionFailed` means somebody else wrote the object — i.e. the ownership
invariant broke. The response must be loud and non-destructive:

1. do **not** retry blindly (that would clobber the other writer);
2. move the local copy aside to `cache/conflict/<key>.<timestamp>`;
3. log `S3_OWNERSHIP_CONFLICT` at error level and increment a monitoring counter;
4. drop the channel from cache and re-hydrate from S3 (the other writer wins), kicking
   connected clients so they reload rather than continue from a stale index.

If the probe (§4.1) finds that the provider does not honour conditional writes, this
becomes best-effort: the flush compares `head().etag` against the sidecar immediately
before writing, which narrows but does not close the window. The backend warns loudly at
boot in that case, because it is the one place where a provider limitation weakens a
safety property rather than just costing bandwidth.

Optionally, an explicit **lease object** (`leases/<key>` written with `If-None-Match: *`,
refreshed, released on clean shutdown) turns this from detection into prevention. Worth
adding only if operators actually reconfigure topology while running.

### 7.3 Topology changes

Changing `infra.storage.length` re-maps a fraction of all channels to different nodes.
Today that "works" only because the data would be on the wrong local disk and appear
empty — with S3 the data is visible everywhere, so the design must handle it explicitly:

* documented procedure: **drain → flush all dirty → stop → reconfigure → start**;
* on hydration, a local copy whose `remoteSize`/`etag` no longer matches S3 is treated as
  **stale** and discarded (§5.2) — this is the recovery path when ownership moved away
  and back;
* a local copy that is *dirty* and whose remote has moved on is a conflict (§7.2).

### 7.4 Crash recovery

On boot, the cache manager scans the cache root for sidecars with `dirty: true` and, for
each, compares `localSize` against `backend.head().size`:

* local ahead of remote → flush the tail before serving that channel (this is the normal
  "killed with unflushed writes" case, and it recovers with no data loss);
* local behind remote → stale, discard;
* local diverged (same size, different content — only possible after a rewrite) →
  quarantine and alarm.

Serving of a channel is blocked until its recovery completes; recovery is per-channel, so
boot is not serialized on the whole cache.

### 7.5 Graceful shutdown

**This does not exist today and has to be built.** There are no `SIGTERM` / `SIGINT`
handlers anywhere in the tree; `File.create` returns a `shutdown()` method
(`storage/storage/file.js:1486`) that is never called by anything; and the supervisor in
`index.js` treats *any* child exit as fatal (`nodeProcess.on('exit')` → `Log.error` →
`process.exit(1)`), which would tear down sibling nodes before they could finish
flushing. With local disks this is merely untidy; with S3 it is the difference between a
clean deploy and losing the last flush window on every restart.

The shutdown sequence for a storage node, in order:

1. **Stop accepting new work.** Mark the node draining and stop serving new joins, so the
   dirty set stops growing while it is being drained. Flushing before closing the door
   just means flushing again.
2. **Cancel the flush timers and flush everything dirty**, with bounded parallelism
   (a semaphore, as used elsewhere in `file.js`) so a large dirty set does not open
   hundreds of simultaneous uploads.
3. **Close cached write streams** (`closeChannel` for each open channel) and call the
   store's existing `shutdown()` to clear intervals — finally giving that dead method a
   caller.
4. **Shut down the fork workers and cluster workers.** They are read-only against channel
   logs, so ordering only matters in that they must not be killed while the primary still
   needs them. Blob staging in the cluster workers is transient and is safe to drop.
5. **Exit 0.**

Bounded by `shutdownFlushTimeoutMs` (default 30 s). On timeout the node logs
`S3_SHUTDOWN_INCOMPLETE` with the list of still-dirty channels and exits non-zero. That
is not data loss as long as the cache volume persists — those channels are in the journal
and replay on next boot (§7.4) — but it is a signal that the drain budget is too small
for the workload.

Two supporting changes outside the storage node:

* **The supervisor** (`index.js`) installs `SIGTERM` / `SIGINT` handlers that forward the
  signal to every child and wait for them, with its own timeout before force-killing; and
  it must distinguish an expected exit during shutdown from a crash, instead of the
  current unconditional `process.exit(1)`.
* **`uncaughtException`** — the storage worker already exits on it
  (`storage/worker.js:856`). In the primary, attempt a best-effort flush before exiting,
  but do not depend on it: the journal is the real guarantee, and an emergency flush from
  a process in an unknown state is a courtesy, not a mechanism.

This is worth landing as a standalone piece of work regardless of S3 — a server that
cannot be stopped cleanly is a problem for rolling deploys either way — which is why it
sits in Phase 0 of the plan rather than in the S3 phases.

### 7.6 Failure-mode summary

| Failure | Effect | Mitigation |
|---|---|---|
| S3 unavailable on flush | writes accumulate locally, cache grows | bounded retry with backoff; alarm; refuse new channels past a high-water mark; never drop dirty data |
| S3 unavailable on hydrate | channel cannot be opened | fail the join with a retryable error; clients retry |
| Node killed with dirty data | up to `flushMaxDelayMs` of messages replayed from local disk at boot | §7.4; local cache disk must therefore survive a restart |
| Node destroyed with dirty data | that window is lost | documented RPO; shorten the window, or strict mode |
| Two nodes own the same key | detected on flush | §7.2 conditional writes + conflict quarantine |
| Cache disk full | writes fail | eviction watermarks + monitoring; dirty data is never evicted, so alarm early |
| Partial multipart upload | orphan parts cost money | bucket lifecycle rule `AbortIncompleteMultipartUpload` (documented in ops) |

---

## 8. HTTP serving

`storage/cluster.js` currently serves user data straight off the disk:

* `/blob` → `Express.static(Env.paths.blob)` (lines 68, 101), plus a HEAD branch that
  bumps blob activity;
* `/datastore` → `Express.static(Env.paths.channel)`, **HEAD only** (lines 104-115);
* `/block/…` → access-control middleware then `Express.static(Env.paths.block)` (line 301);
* `/api/logo` → `res.sendFile` (line 375).

These become backend-aware handlers in a new `storage/http-data.js`:

| Mode | Behaviour | When |
|---|---|---|
| `static` | today's `Express.static` | `type: 'fs'` |
| `redirect` | `302` to `backend.presignGet(key, ttl)` | S3 + `serve.blobs: 'redirect'` (default) |
| `proxy` | `backend.getStream()` piped to the response, `Range` forwarded | S3 + `serve.blobs: 'proxy'`, or when the backend cannot presign |

Constraints to respect:

* `/datastore` must stay HEAD-only; it answers from `backend.head()` with no body and no
  hydration;
* the block access-control middleware (`cluster.js:119-300`) stays in front of the data
  handler in every mode;
* `setHeaders` (`http-server/headers.js`) applies CSP/CORP/cache headers. A `302` hands
  the response to S3, so its headers are whatever the bucket sends — the bucket needs a
  CORS policy allowing the sandbox origin, and `Cache-Control` should be set on the
  objects at upload time (blobs are immutable, so `max-age=31536000` matches the current
  `maxAge: '365d'`). Where header control matters more than bandwidth, use `proxy` mode;
* presigned URLs leak into browser history/referrers — keep the TTL short (minutes) and
  remember the payload is end-to-end encrypted anyway;
* blob HEAD requests must keep bumping `updateActivity` (`cluster.js:58-66`).

---

## 9. Configuration and packaging

### 9.1 Config

The endpoint is fully configurable so any S3-compatible provider can be pointed at.
Nothing in the backend assumes AWS.

```js
// config/config.js
storage: {
    type: 'fs',            // 'fs' (default) | 's3'

    s3: {
        // --- connection (provider-specific) ------------------------
        endpoint: 'https://s3.fr-par.scw.cloud',  // omit only for AWS itself
        region: 'fr-par',
        bucket: 'cryptpad-prod',
        prefix: '',                     // optional, lets several instances share a bucket
        forcePathStyle: false,          // true for MinIO / Ceph / older gateways
        credentials: {                  // omit to use the ambient provider chain
            accessKeyId: process.env.S3_ACCESS_KEY,
            secretAccessKey: process.env.S3_SECRET_KEY
        },
        sse: undefined,                 // e.g. 'AES256' where supported

        // --- capabilities: undefined ⇒ probe at startup (§4.1) -----
        capabilities: {
            conditionalPut: undefined,
            serverSideAppend: undefined,
            presign: undefined
        },

        // --- local working cache -----------------------------------
        cache: {
            path: './data/0/cache',
            maxBytes: 20 * 1024 * 1024 * 1024,
            maxIdleMs: 60 * 60 * 1000,

            // flush timing (§5.4) — validated at boot, not silently clamped.
            // 0 debounce = strict flush-before-acknowledge mode.
            flushDebounceMs: 5000,      // idle after last write
            flushMaxDelayMs: 30000,     // ceiling under continuous editing
            flushMaxBytes: 1024 * 1024, // unflushed tail size
            flushOnCheckpoint: true,
            flushConcurrency: 8         // parallel uploads, incl. during drain
        },

        // --- graceful shutdown (§7.5) ------------------------------
        shutdownFlushTimeoutMs: 30000,

        upload: {
            partSizeMB: 8,
            appendThresholdMB: 5        // ≥ this ⇒ server-side append when available
        },
        serve: { blobs: 'redirect', presignTtl: 300 },
        requestTimeoutMs: 10000,
        maxRetries: 3
    }
}
```

Provider notes to carry in `config.example.js`:

* **Scaleway** — endpoint `https://s3.<region>.scw.cloud`, region one of `fr-par`,
  `nl-ams`, `pl-waw`; virtual-hosted style works, so `forcePathStyle: false`. The region
  string must match the bucket's region or requests are rejected. Credentials are an API
  key pair from the Scaleway console, ideally scoped to a single bucket.
* **MinIO / Ceph / Garage** (useful for local development and CI) — `forcePathStyle: true`
  and a `http://…` endpoint.
* **AWS** — omit `endpoint`, set `region`, prefer the ambient credential chain (IAM roles)
  over static keys.

Credentials should come from environment variables rather than being committed to
`config.js`; the config file is read by `common/load-config.js` as plain JS, so
`process.env` interpolation works today with no changes.

`Core.getPaths` (`common/core.js:168`) gains `cachePath`, and `Env.paths` gains `cache`.

### 9.2 Two-level optionality

The user asked for either a plugin or a clean interface with several implementations.
Both are worth having, at different layers:

* **the interface is in-tree** (`common/storage/backend/`), with `fs` as the default
  implementation and a store factory that selects by `storage.type`. This is what keeps
  the codebase honest — the abstraction is exercised by the default path, not only by the
  optional one;
* **the S3 implementation is a plugin** (`plugins/S3/`), so `@aws-sdk/client-s3` is not a
  dependency of a stock install. The plugin manager already discovers
  `plugins/<name>/index.js` at runtime and is excluded from the rollup bundles
  (`rollup.config.mjs`, `dynamicRequireTargets`), and `__dirname` from `build/*.js`
  resolves to `<repo>/plugins` — so this works for the primary, the fork workers and the
  cluster workers alike, all of which load `common/env.js` → `plugin-manager.js`.

A new plugin hook is needed. `plugin-manager.js` currently offers `call`, `get`,
`addHttpEndpoints`, `getHttpProxy`; add:

```js
// common/plugin-manager.js
plugins.getStorageBackend = name => {
    let found;
    Object.values(plugins).forEach(plugin => {
        const b = plugin?.storageBackends?.[name];
        if (b) { found = b; }
    });
    return found;
};
```

```js
// plugins/S3/index.js
module.exports = {
    name: 'S3',
    modules: {
        storageBackends: { s3: require('./backend.js') }
    }
};
```

Selection fails fast and clearly: if `storage.type === 's3'` and no backend is
registered, the node refuses to start with "S3 storage requested but the S3 plugin is
not installed", rather than silently falling back to local disk.

---

## 10. Migration and operations

**fs → s3.** A `scripts/storage-migrate.js` that walks the local datastore and uploads
every family, with `--dry-run`, `--verify` (compare sizes and hashes) and resumability.
Because objects are byte-identical to the local files, the migration is a plain copy —
no transformation, and the same script run in reverse gives an exit path back to local
disk, which is important for adoption.

Recommended sequence: stop writers → migrate → flip `storage.type` → start → verify a
sample of channels with `--verify`. A dual-write / shadow-read mode is possible but the
added complexity is hard to justify given the single-writer property makes a stop-copy
migration short.

**Bucket setup to document:** versioning (cheap insurance against the conflict case),
an `AbortIncompleteMultipartUpload` lifecycle rule so failed flushes do not leave paid-for
orphan parts, a lifecycle rule moving `archive/` to a colder class after
`archiveRetentionTime`, CORS allowing the sandbox origin (needed for `redirect` blob
delivery), and least-privilege credentials scoped to the bucket and prefix. Lifecycle and
versioning support varies between providers — the runbook should state which of these the
chosen provider actually offers rather than assuming AWS semantics.

### 10.1 Metrics

The `Env.plugins.MONITORING` hook already exists and is used around `computeIndex`,
`storeMessage`, `rpc_*` and friends, but it currently exposes only two shapes:

```js
Env.plugins?.MONITORING?.increment(key, value);   // counter
Env.plugins?.MONITORING?.average(key).time();     // timing
```

Reporting *how many files are currently unflushed* needs a third shape — a **gauge**, a
point-in-time value rather than an accumulation. The dirty set changes on every stored
message, so pushing a value on each mutation would be needlessly chatty; a pull-based
registration is both cheaper and always accurate at scrape time:

```js
// common/plugin-manager.js — new hook, alongside increment/average
Env.plugins?.MONITORING?.registerGauge('s3_dirty_channels', () => cache.dirtyCount);
```

The cache manager maintains these incrementally (a counter, a running byte total and the
oldest dirty timestamp) so a scrape never walks the cache directory.

**Gauges**

| Metric | Meaning |
|---|---|
| `s3_dirty_channels` | **files with unflushed local writes right now** — the headline number |
| `s3_dirty_bytes` | unflushed byte volume |
| `s3_oldest_dirty_age_ms` | age of the oldest unflushed byte — *the live RPO indicator, alarm on this* |
| `s3_flush_inflight` | uploads currently in progress |
| `s3_cache_bytes` / `s3_cache_objects` | local cache footprint |
| `s3_backend_degraded` | 1 if a capability probe (§4.1) came back degraded |

**Counters:** `s3_flush_ok`, `s3_flush_failed`, `s3_flush_bytes`, `s3_hydrate_ok`,
`s3_hydrate_failed`, `s3_hydrate_bytes`, `s3_evictions`, `s3_conflicts`,
`s3_errors_<operation>`.

**Timings** (via the existing `average()`): `s3_flush`, `s3_hydrate`, `s3_head`,
`s3_presign`.

**Shutdown:** `s3_shutdown_flush_ms` and `s3_shutdown_unflushed` (how many channels were
still dirty when the drain timeout expired — should always be 0).

`s3_dirty_channels` and `s3_oldest_dirty_age_ms` are the two worth alerting on: the first
rising steadily means flushes are failing or the backend is unreachable, and the second
*is* the current exposure window in milliseconds, directly comparable to the configured
`flushMaxDelayMs`.

---

## 11. Implementation plan

Phases are ordered so that each one lands independently, keeps the test suite green, and
is useful on its own.

**Phase 0 — prep, no behaviour change**
* ✅ **graceful shutdown (§7.5)** — `common/shutdown.js` runs an ordered, timeout-bounded
  drain; `storage/index.js` registers its steps and installs `SIGTERM`/`SIGINT` handlers;
  `common/worker-module.js` gained a `shutdown()` that stops workers without the respawn
  logic undoing it; the supervisor in `index.js` forwards signals and waits for children
  instead of `process.exit(1)`-ing on the first exit. `store.shutdown()` finally has a
  caller;
* ✅ **`ObjectBackend` interface and `backend/fs.js`** — plus `backend/index.js`, a
  registry that resolves built-in and plugin-provided backends and fails loudly on an
  unavailable one;
* ✅ **`common/storage/basic.js` routed through a backend** — users, invitations,
  sessions, MFA, support and challenges are now backend-agnostic;
* ✅ **`Moderators.getKeysSync` is async** (`storage/moderator.js`,
  `storage/commands/moderators.js`, `storage/index.js`), removing the last
  `readdirSync` — object stores have no synchronous listing;
* ✅ `storage/storage/block.js`, `storage/storage/tasks.js` and
  `common/decrees-core.js` routed through a backend instance;
* ✅ the `Express.static` mounts replaced with `storage/http-data.js` handlers that keep
  using static in `fs` mode;
* ✅ `cachePath` added to `Core.getPaths` / `Env.paths`.

*Exit criteria:* `npm run tests` and `npm test` green; no diff in behaviour.
A `npm run test:unit` suite (`tests/unit/`) covers the new modules without needing a
running server; `tests/unit/backend-conformance.js` is the shared spec that the S3
backend will be held to in Phase 2.

**Phase 1 — store factory and config plumbing**
* `storage/storage/index.js` returning `{ store, pinStore, blobStore }` from
  `config.storage`; wire it into `storage/index.js:816-841`, `storage/worker.js:40-90`
  and `storage/cluster.js:434-452` (three places construct stores today);
* config schema + `config.example.js` documentation.

**Phase 2 — S3 backend** ✅
* ✅ `common/storage/backend/s3.js` implementing every method, including multipart-copy
  append and conditional writes. Shipped **in-tree with a lazy `require`** rather than as
  a plugin: `.gitignore` excludes `plugins/`, so a plugin there would be neither
  version-controlled nor testable. The AWS SDK is loaded only when `storage.type` is
  `'s3'`, so a stock install neither needs nor loads it;
* ✅ the startup capability probe (§4.1) and its `S3_CAPABILITIES` boot log;
* ✅ the conformance suite runs green against Scaleway (33/33), under a unique prefix per
  run that is deleted afterwards, so it is safe against a real bucket;
* ✅ `scripts/s3-check.js`, which verifies a credential set has every permission the
  storage layer needs (list/put/get/copy/multipart/delete) and names the missing ones —
  run it before pointing an instance at a bucket;
* ⬜ a MinIO service in CI so the suite runs on every commit without credentials.

**Phase 3 — the cached append-log (the substantial one)** — *core landed*
* ✅ `common/storage/cache/journal.js` — crash-safe per-object state. The `dirty` flag is
  deliberately an optimisation, not a correctness requirement: recovery compares real
  sizes, so a crash between an append and a state write cannot lose the appended bytes;
* ✅ `common/storage/cache/manager.js` — hydration (no-clobber, safe under concurrent
  processes), dirty tracking, the flush scheduler (debounce / ceiling / byte threshold),
  conflict quarantine, boot recovery, LRU eviction that never touches dirty objects, and
  `stats()`;
* ✅ the dirty-set gauges, pull-based via `registerGauge` (§10.1);
* ✅ verified end-to-end against Scaleway, including the multipart-copy append path on a
  6 MiB object, byte-for-byte;
* ✅ `storage/storage/cached-file.js` — wraps the unmodified `file.js` engine over the
  cache root and maps its API onto hydrate / touch / flush / backend calls (§6.1),
  including the hydration hook in `getWeakLock`, sizes answered without hydrating, and
  archive/restore/remove/list performed server-side;
* ✅ `storage/storage/index.js` — the store factory, wired into the storage primary
  (`storage/index.js`) and the fork workers (`storage/worker.js`);
* ✅ `manager.flushAll` reached by the drain step, `manager.recover` run at startup before
  the node serves anything;
* ✅ boot-time validation of the flush thresholds, and `cachePath` in `Core.getPaths` /
  `Env.paths`;
* ✅ blobs moved across in Phase 4.

*Note on flush semantics:* `flush()` loops until the local file and the object agree,
rather than returning after one upload. Appends continue during an upload, and a flush
that returned early would let `flushAll` report a clean drain while the newest messages
were still only on local disk — losing acknowledged data on a *graceful* shutdown. The
loop is bounded, and hitting the bound hands the object back to the scheduler.

**Phase 4 — blobs and blocks on S3** ✅
* ✅ `storage/storage/block.js` routed through the backend. Blocks are tiny, immutable
  and replaced wholesale, so they go straight through the Basic store rather than
  through the cached append-log machinery;
* ✅ `storage/http-data.js` — backend-aware handlers replacing every `Express.static`
  mount of user data in `storage/cluster.js`, in three modes (`static` / `redirect` /
  `proxy`) with full `Range` support. `/datastore` stays HEAD-only and is answered from
  object metadata, so a pad is never transferred there;
* ✅ `storage/storage/cached-blob.js` — uploads stage on local disk and are streamed to
  the store on completion; size, existence, archive, restore, remove and listing are all
  metadata or server-side copies, so a completed blob is never downloaded to the server.
  Verified against Scaleway with a 12 MiB chunked upload and presigned delivery;
* ✅ the store factory takes an `only` subset, so HTTP workers build the blob store
  without the channel caches and flush timers they would never use.

*Two details in the blob store worth knowing:* blob metadata is a tiny ndjson log read
whole and appended to directly, needing none of the channel caching; and activity
timestamps are throttled to one write per blob per hour, because `updateActivity` fires
on every browser HEAD of every blob and the value's useful resolution is days.

*On completing an upload:* three processes are involved — an HTTP worker writes the
staged file, the primary orchestrates, and a fork worker reads that file to upload it.
Closing a write stream is asynchronous, so completion must wait for the stage to be
closed **in every process** before reading it. It previously did not, and reading a file
whose stream still held buffered bytes produced a truncated blob with no error at all.
This only bites on files large enough that the flush has not finished, which is why it
survived so long.

*On serving modes:* a configured `redirect` degrades to `proxy` when the backend cannot
presign, and a presigning failure at request time falls back to streaming rather than
erroring — failing to serve a blob is a worse outcome than serving it the slower way.
Ranged requests are always proxied, because a redirect would drop the `Range` header on
the way to the store. Blocks are always proxied: their access-control middleware decides
who may read one, and a presigned URL would outlive that check.

**Phase 5 — operations** — *partly landed*
* ✅ `scripts/storage-migrate.js` — moves an instance's data in either direction, with
  `--dry-run`, `--verify`, `--reverse`, `--prefix` and `--overwrite`. Because keys map
  onto paths verbatim, migration is a plain copy; verified as a byte-for-byte lossless
  round trip through Scaleway, including binary blobs. Offsets, staged uploads and cache
  working files are excluded. Re-running is safe, so an interrupted migration resumes;
* ✅ `scripts/s3-check.js` — confirms a credential set has every permission the storage
  layer needs, naming the ones it lacks;
* ⬜ alarms and runbook (bucket policy, lifecycle rules, RPO, conflict procedure);
* ⬜ load test: N concurrent editors on M pads, measuring flush latency, request rate and
  cost per active pad-hour.

*The reverse direction is not an afterthought:* an operator is far more willing to try
object storage knowing they can undo it, so `--reverse` is tested as a first-class path
rather than assumed to work.

**Phase 6 — optional evolution**
* segmented log layout (§5.3c) with a compactor, behind a layout version marker;
* lease objects if operators need online topology changes.

### Testing strategy

* **Backend conformance suite** — one spec run against `fs`, MinIO, and (in a nightly
  job) the real provider, so the implementations cannot drift and so a provider that
  silently ignores a conditional header is caught by CI rather than in production.
* **Store-level equivalence** — the existing `tests/` suite parameterised over
  `storage.type`, asserting identical results for both backends.
* **Offset fidelity** — property test: append random messages, flush at random points,
  evict, re-hydrate, and assert that `computeIndex` and `GET_HISTORY` from every recorded
  offset return byte-identical results to the pure-local run. This is the test that
  protects the history keeper's core assumption.
* **Crash injection** — kill `-9` a storage node at random points during a write storm;
  assert no message acknowledged before the kill is missing after recovery, and that no
  channel ends up quarantined.
* **Graceful-stop assertion** — under the same write storm, send `SIGTERM`; assert the
  process exits 0, that `s3_dirty_channels` is 0 at exit, and that every acknowledged
  message is in S3 *without* relying on journal replay. This is the test that keeps
  rolling deploys honest.
* **Conflict injection** — deliberately point two nodes at the same channel; assert the
  conditional write fails, the conflict is quarantined, and no data is clobbered.

---

## 12. Open questions

1. **Durability policy.** Everything except the channel log is written through to S3
   synchronously (§5.4), so the exposure is limited to the last few seconds of edits to a
   document that is being actively edited. Are the proposed defaults (≤5 s after typing
   stops, ≤30 s under continuous editing, immediate on checkpoint and on last-editor
   disconnect) the right trade-off, or should the window be tightened?
2. ~~**Layout.**~~ **Decided: option A**, a single object per channel. The bucket mirrors
   the datastore layout, so migration is a plain copy in either direction. Flushes use a
   full `PUT` below the append threshold and multipart `UploadPartCopy` above it (§5.3a/b).
   The consequence to keep in view is that flush cost is proportional to document size
   rather than to the number of new bytes, which is why the flush window is measured in
   seconds (§5.4). Segmented logs (§5.3c) remain the documented evolution if that
   becomes the bottleneck.
3. **Blob delivery.** Presigned redirect (cheap, offloads bandwidth, needs bucket CORS)
   versus proxy streaming (full header control, costs egress twice). Default proposed:
   redirect.
4. **Cache durability.** The design assumes the cache disk survives a process restart. In
   a container-per-pod deployment with ephemeral local storage, that assumption fails and
   the strict mode becomes mandatory — which deployment shape is being targeted?
5. **Topology changes.** Is online re-sharding a requirement? If yes, lease objects and a
   handoff protocol move from "optional hardening" into Phase 3.
