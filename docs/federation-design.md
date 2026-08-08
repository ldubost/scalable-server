<!-- SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors

SPDX-License-Identifier: AGPL-3.0-or-later
-->
# CryptPad ↔ CryptPad Federation — Design

**Status:** M0, M1 (L0), M2 (L1), most of M3 (L2, multi-master), M4 (real pads) and M5
(blobs) are implemented and tested. See [`federation-status.md`](federation-status.md) for exactly what is and is not
built. Milestones and what each delivered are in §9; §2 marks the
modules that exist with ✅.
**Reads with:** [`federation-spec.md`](federation-spec.md). Requirement ids (`R-n`),
constraint ids (`C-n`) and conformance levels (`L0`–`L2`) refer to that document.
**Scope:** how to build federation of a standard `/pad/` inside *this* scalable server —
node topology, module layout, on-disk format, algorithms, config, failure handling,
milestones and tests.

---

## 1. Placement in the topology

### 1.1 The constraint

The node graph is fixed by `common/interface.js`: `core` nodes call `Interface.init` and
listen; `front` and `storage` nodes call `Interface.connect` and dial every core.
**`front` and `storage` cannot talk to each other**, and `storage` nodes cannot talk to
each other except through the storage cluster. Every cross-node path goes through `core`.

Federation needs three capabilities:

| Need | Lives on |
| --- | --- |
| Read/append the channel log, compute the index | `storage` (`Env.getStorageId(channel)`, `common/env.js:70-79`) |
| Push live messages to connected members | `front`, reachable only via `core` |
| Hold long-lived outbound TLS sessions to remote instances, and terminate inbound ones | — |

The third has no home today.

### 1.2 Decision: a new `federation` node type

Add `federation:N`, an `Interface.connect` client of `core`, exactly like `front` and
`storage`.

```
                    ┌──────────────┐
   remote instance  │              │
   ══════TLS═══════▶│ federation:N │──┐
                    │              │  │
                    └──────────────┘  │   ┌────────┐    ┌───────────┐
                                      ├──▶│ core:M │───▶│ storage:K │
                    ┌──────────────┐  │   └────────┘    └───────────┘
                    │   front:J    │──┘        │
                    └──────────────┘           │
                          ▲                    │
                     browsers ─────────────────┘  (live fan-out)
```

Why not the alternatives:

* **On `storage`** — the log is there, so it is tempting. But peer sessions would then be
  per-shard: with *n* storage nodes and *m* peers you get *n·m* TLS sessions, each
  needing the peering policy, the instance key and the rate limiters. Trust configuration
  fragments across shards. Rejected.
* **On `front`** — `front` already terminates public WebSockets and could host
  `/federation`. But `front` is the untrusted-input tier, sized for browser fan-out, and
  it has no path to `storage` except through `core` anyway. Rejected for blast radius.
* **On `core`** — `core` is the routing brain and reaches everything, but it holds
  session/auth state and is deliberately free of outbound network I/O. Rejected.

`federation:N` keeps every peer-trust decision in one process type, scales independently,
and can be omitted entirely (`infra.federation: []`) by instances that do not want the
feature — which satisfies the same "must be optional" property the S3 backend has.

Channel→node ownership uses the existing hash, so a federation node routes a channel to
`Env.getStorageId(channel)` via core. Peer→node ownership uses a second jump hash over
the peer's `originId`, so each peer has exactly one owning federation node and sessions
are not duplicated.

### 1.3 New message flows

Outbound (local write must reach peers):

```
storage:K  --SEND_EVENT('FED_PUBLISH')-->  core:M  -->  federation:N  --PUBLISH-->  peer
```

Inbound (remote write must reach the log and the users):

```
peer --PUBLISH--> federation:N --QUERY('FED_INGEST')--> core:M --> storage:K
                                                                      |
                                          storage:K --SEND_CHANNEL_MESSAGE--> core --> front --> browsers
```

The second half already exists verbatim: `Env.interface.sendEvent(coreId,
'SEND_CHANNEL_MESSAGE', res)` in `storage/channel-manager.js:379` and
`core/index.js:309-322`. Remote ingestion reuses it unchanged, which is the main reason
G6 (no client change) is cheap.

---

## 2. Module layout

Files marked ✅ exist as of M1.

```
federation/
  index.js         ✅ node entry: Interface.connect, command table, heartbeat
  federation.ts    ✅ rollup entry (mirrors front.ts / core.ts / storage.ts)
  peer-manager.js  ✅ outbound dialling, reconnect/backoff, session registry
  peer-session.js  ✅ one authenticated session: handshake, framing, dispatch
  server.js        ✅ inbound WebSocket endpoint (/federation)
  policy.js        ✅ allowlist and admission decisions (per-peer quotas: M4)
  sync.js          ✅ SUBSCRIBE/SYNC/PUBLISH; head-based backfill (DAG walk: M3)
  blob-transfer.js ✅ BLOB_REQ/BLOB_CHUNK: on-demand blob fetch          (R-44)
  worker.js           CPU-bound: envelope signature batches, Bloom filters

common/
  federation/
    envelope.js    ✅ build / canonicalise / sign / verify envelopes  (R-14..R-18)
    order.js       ✅ the (l, o, id) total order                       (R-2, R-36)
    codec.js       ✅ Codec seam — JSON today, BARE later               (C-4)
    handshake.js   ✅ mutual instance-key authentication               (R-24)
    ids.js         ✅ IdCodec seam — getHash today, BLAKE3 later        (C-4)
    identity.js    ✅ instance keypair: load, generate, publish         (R-23)
    capability.js  ✅ mint/verify pad-key replication capabilities      (R-25)

storage/
  federation/
    manager.js     ✅ Env.FM: enable, ingest (R-15), serve history, head
    merge.js       ✅ Lamport clocks, watermark, commit rule, trim precondition
    blobs.js       ✅ blob read/write through the ObjectBackend            (R-44)
    fedlog.js      ✅ per-origin pending logs + committed log I/O
    control.js     ~  control commits: META and DELETE live in manager.js;
                      TRIM / EXPIRE / RECONCILE are M3
```

Additions to existing files are enumerated in §6.

`common/federation/*` is deliberately dependency-light: it must be loadable from the
`federation` node, the `storage` node and their workers.

`handshake.js` was not in the original layout. It lives in `common/federation/` rather
than `federation/` because both ends of a session run the same state machine — the
dialling and listening halves differ only in which frame they send first — and keeping
one implementation is what makes "the listener's proof cannot be replayed as the
dialler's" a property of one file rather than an agreement between two.

---

## 3. On-disk layout

### 3.1 New keys in the object backend

Everything goes through the existing `ObjectBackend` interface
(`common/storage/backend/types.d.ts`), so it works on `fs` and on S3 with no new
primitives. Only `append`, `get`, `put` with `ifMatch`, and `list` are used.

```
channel/ab/abcdef….ndjson          # unchanged: the committed merged log
fed/ab/abcdef….pending.<originId>  # per-origin durable arrival log (ndjson envelopes)
fed/ab/abcdef….state.json          # merge state: heads, watermarks, seq counters
fed/peers.json                     # peering policy + last-known peer state
fed/identity.json                  # instance keypair (0600; see §7.1)
```

`Constants.paths` (`common/constants.js:16-30`) gains `federation: 'fed/'`.

### 3.2 Why a separate pending log

R-9 requires durability at acknowledgement time; R-3 forbids committing before the
watermark. Those cannot both hold with a single file. So:

* **`…pending.<originId>`** — append-only, written the instant an envelope is accepted or
  received. One file per origin, so appends never interleave between origins and a gap in
  `s` is directly visible as a gap in the file.
* **`channel/….ndjson`** — the committed merged log. Written only by the merge loop, only
  in `(l, o, id)` order, only for `l ≤ W`. This is the file every existing reader already
  understands; `computeIndex`, `offsetByHash`, `cpIndex` and `readMessagesBin` keep
  working on it with **no changes**.

A pending record is dropped from its origin file (by rewriting the file's remaining tail,
or by advancing a `committedSeq` marker in `state.json` and compacting lazily) once it has
been committed. Compaction is a background task, not on the write path.

### 3.3 `state.json`

```jsonc
{
  "v": 1,
  "channel": "abcdef…",
  "validateKey": "…",              // R-21: immutable, cross-checked on every sync
  "members": ["<originId>", "…"],  // replica set
  "self": { "seq": 812, "lamport": 4711 },
  "peers": {
    "<originId>": { "seq": 344, "lamport": 4702, "atime": 1786000000000 }
  },
  "committed": { "lamport": 4699, "lastId": "…", "line": 5120 },
  "heads": ["<id>", "…"],
  "evicted": []                    // R-5
}
```

Written with `ifMatch` on its etag so that a concurrent writer is detected rather than
silently lost — the same conditional-put discipline the S3 backend already relies on.
The per-channel single-writer property (`Env.getStorageId` + `Env.queueStorage`) means
contention is a fault indicator, not a normal case.

### 3.4 Migration and reversibility

A non-federated channel has no `fed/` keys and behaves exactly as today — the merge loop
is never entered. Un-federating a channel means deleting its `fed/` keys and its
`state.json`; `channel/….ndjson` is left in place and remains a perfectly ordinary pad.
**There is no format migration.** That is the point of keeping the committed log
byte-compatible.

---

## 4. Core algorithms

### 4.1 Accepting a local message

Hooks into `CM.onChannelMessage` (`storage/channel-manager.js:141-246`), after validation
and checkpoint-dedup, replacing the direct `storeMessage` call for federated channels.

```js
// storage/federation/merge.js  (pseudocode)
const acceptLocal = (Env, channel, content, cb) => {
    const st = getState(Env, channel);

    const env = Envelope.create({
        c: channel,
        o: Env.federation.originId,
        s: ++st.self.seq,
        l: ++st.self.lamport,          // strictly increasing per origin
        a: st.heads.slice(),           // C-1: causal parents
        id: HKUtil.getHash(content),
        t: +new Date(),
        m: content
    });
    Envelope.sign(env, Env.federation.secretKey);          // R-16

    nThen(w => {
        FedLog.appendPending(Env, channel, env, w());      // R-9: durable before ack
    }).nThen(() => {
        st.heads = [env.id];                               // local heads collapse
        writeState(Env, channel, st);

        cb(void 0, env);                                   // → live broadcast (R-7)

        pushToPeers(Env, channel, env);                    // → FED_PUBLISH
        tryCommit(Env, channel);                           // → merge loop
    });
};
```

Note the ordering: the client is acknowledged and the message is broadcast *before*
commitment. Latency for a local edit is one `appendPending` — one `append` on the
backend, the same cost as today's `store.messageBin`.

### 4.2 Receiving a remote envelope

```js
const acceptRemote = (Env, channel, env, cb) => {
    // R-14
    if (env.id !== HKUtil.getHash(env.m)) { return cb('EBADID'); }
    // R-16 — binds (s, l, a) to the origin instance
    if (!Envelope.verify(env)) { return cb('EBADSIG'); }
    // R-27 — is this origin in the replica set for this channel?
    const st = getState(Env, channel);
    if (!st.members.includes(env.o)) { return cb('ENOTMEMBER'); }
    // R-18 — at-least-once delivery
    if (FedLog.has(Env, channel, env.id)) { return cb(); }

    // R-15 — revalidate the pad signature ourselves, never trust the peer
    validateAgainstValidateKey(Env, channel, env.m, err => {
        if (err) { return cb('FAILED_VALIDATION'); }

        const peer = st.peers[env.o] ||= { seq: 0, lamport: 0 };
        if (env.s !== peer.seq + 1) {          // R-17
            requestBackfill(Env, channel, env.o, peer.seq + 1, env.s - 1);
            FedLog.appendPending(Env, channel, env);   // hold it; do not advance seq
            return cb();
        }

        FedLog.appendPending(Env, channel, env);
        peer.seq = env.s;
        peer.lamport = env.l;                  // H[P] advances only on contiguity
        peer.atime = +new Date();
        st.self.lamport = Math.max(st.self.lamport, env.l);   // Lamport receive rule
        st.heads = recomputeHeads(st.heads, env);
        writeState(Env, channel, st);

        broadcastLocally(Env, channel, env);   // R-7
        tryCommit(Env, channel);
        cb();
    });
};
```

`validateAgainstValidateKey` reuses the existing round trip to core
(`Env.interface.sendQuery(coreId, 'VALIDATE_MESSAGE', …)`,
`storage/channel-manager.js:198-219`) and the same worker
(`core/worker.js:73-75`). No new crypto code is introduced for pad messages.

### 4.3 The merge loop

```js
const tryCommit = (Env, channel) => {
    const st = getState(Env, channel);

    // W = min over the (non-evicted) replica set          (R-3, R-5)
    const live = st.members.filter(o => !st.evicted.includes(o));
    const W = Math.min(
        st.self.lamport,
        ...live.filter(o => o !== Env.federation.originId)
               .map(o => (st.peers[o] || { lamport: 0 }).lamport)
    );

    const ready = FedLog.pendingWithLamportUpTo(Env, channel, W)
        .sort(byKey);                                       // (l, o, id)   R-2

    if (!ready.length) { return; }

    Env.queueStorage(channel, next => {                     // reuse the existing queue
        nThen(w => {
            ready.forEach(env => {
                const line = [0, null, 'MSG', channel, env.m, env.t];
                Env.store.messageBin(channel, Buffer.from(JSON.stringify(line) + '\n'), w());
                updateIndexIncremental(Env, channel, env);  // cpIndex, offsetByHash
            });
        }).nThen(() => {
            st.committed = { lamport: last(ready).l, lastId: last(ready).id };
            writeState(Env, channel, st);
            FedLog.markCommitted(Env, channel, ready);
            next();
        });
    });
};
```

Three things to notice:

1. **The committed line uses `env.t`, not local time.** R-1 does not require byte-identical
   files, but using the origin's timestamp keeps `msgStruct[5]` meaningful (it is the
   moment the author's instance accepted it, which is what a user would expect) and makes
   the files identical apart from `msgStruct[1]`.
2. **`msgStruct[1]` is `null` for every committed line on a federated channel**, including
   locally-originated ones. `null` is already a valid, understood value — private messages
   use it (`storage/channel-manager.js:346-352`). Storing a local netflux id would leak a
   session identifier into a log that gets replicated, violating R-33. Normalising to
   `null` on federated channels is a small, deliberate behaviour change; it must be listed
   in the release notes.
3. **`Env.queueStorage`** is the existing per-channel serialiser
   (`storage/schedule.js`). The merge loop is just another writer on it, so it cannot
   interleave with metadata writes, trims or archival.

### 4.4 Watermark liveness

`federation/index.js` runs a `T_hb = 5 s` interval that sends `HEARTBEAT` with `{c, s, l}`
for every channel whose `self.lamport` advanced, plus a keepalive for idle ones (R-4).
Received heartbeats update `peers[o].lamport` **only if** `s` matches `peers[o].seq` —
i.e. only if we have that origin's contiguous prefix. That single condition is what makes
`H[P]` sound.

A `T_evict = 60 s` sweep moves silent peers into `st.evicted` (R-5) and logs
`FED_PEER_EVICTED`. Recovery emits a `RECONCILE` control commit before splicing the
backlog (R-6).

### 4.5 Serving history

`HistoryManager.getHistoryAsync` (`storage/history-manager.js:116-146`) streams
`Env.store.readMessagesBin` from an offset. For a federated channel it gains a tail:

```js
// after the committed log is exhausted:
FedLog.pendingAbove(Env, channel, st.committed.lamport)
      .sort(byKey)
      .forEach(env => handler([0, null, 'MSG', channel, env.m, env.t], readMore));
```

`getHistoryOffset` is untouched: `lastKnownHash` resolution still happens against
`offsetByHash` over the committed log. A client whose `lastKnownHash` names an
*uncommitted* message — possible, because it was broadcast live — resolves to the end of
the committed log and then receives the sorted tail, which necessarily contains its hash.
The tail is small (R-4), so scanning it for the hash and slicing from there is cheap and
avoids re-sending.

### 4.6 Checkpoints

`channelData.lastSavedCp` (`storage/channel-manager.js:164-167, 227-230`) is a per-node
in-memory value and is wrong under federation (R-10). Replace, for federated channels,
with a lookup over the committed log's `cpIndex` plus the pending tail:

* a checkpoint whose `cpId` already appears in either is a duplicate → suppressed exactly
  as today;
* two *different* concurrent checkpoints both commit. `HK.sliceCpIndex`
  (`storage/hk-util.js:51-60`) already keeps at least the last two plus anything within
  100 lines, which is precisely the behaviour needed to stop clients forking on
  checkpoints — it needs no change.

### 4.7 Anti-entropy (`sync.js`)

```
requester                                        holder
    │  SYNC_REQ {c, knownHeads, knownIds?}          │
    │──────────────────────────────────────────────▶│
    │                                    walk DAG back from local heads,
    │                                    stop at knownHeads or knownIds hits,
    │                                    emit in reverse-topological order   (R-19, C-3)
    │  SYNC_RES {c, envelopes[], done:false}        │
    │◀──────────────────────────────────────────────│
    │  SYNC_RES {c, envelopes[], done:true}         │
    │◀──────────────────────────────────────────────│
```

The DAG walk uses `a` (acks), which is why C-1 pays for itself here as well as for
NextGraph: without acks, backfill would need a full per-origin sequence scan on both
sides. `knownIds` is an optional Bloom filter (`worker.js`) that lets the holder prune the
walk early on pads with long histories; it maps directly onto NextGraph's
`TopicSyncReq.known_commits`.

Bootstrapping a brand-new replica is the degenerate case: `knownHeads: []`, and the holder
streams the whole log. For a large pad this should be served from the committed file
sequentially rather than by DAG walk — the file *is* already in causal order — with
envelopes reconstructed from a side index. `fedlog.js` keeps `id → (s, l, a)` for exactly
this.

---

## 5. Peering and sessions

### 5.1 Handshake

```
A                                                     B
│  HELLO {v, originId_A, origin, nonce_A, caps}       │
│────────────────────────────────────────────────────▶│
│  HELLO {v, originId_B, origin, nonce_B, caps}       │
│◀────────────────────────────────────────────────────│
│  AUTH  {sig_A over ("cryptpad-fed-v1"‖nonce_B‖originId_B)}
│────────────────────────────────────────────────────▶│
│  AUTH  {sig_B over ("cryptpad-fed-v1"‖nonce_A‖originId_A)}
│◀────────────────────────────────────────────────────│
```

Domain-separated and bound to both peers' ids, so a signature cannot be replayed against a
third instance. Over TLS (R-24); the instance key authenticates the *instance*, TLS
authenticates the *endpoint* and hides traffic.

Transport is `ws`, already a dependency, reusing the `socketToClient` wrapper shape from
`common/ws-connector.js:6-54`. Inbound `/federation` is served by `federation/server.js` on
its own port so it is trivially firewallable and never shares a listener with browser
traffic.

### 5.2 Enabling federation on a pad

```
browser (has the pad's signing key, from the URL fragment)
   │
   │ 1. mint capability: sign (channel, originA, originB, nonce, expiry)   R-25
   │
   ├── 2. authenticated RPC to A: FED_ENABLE {channel, peer:B, cap}
   │        A: owner check (R-26) → policy check (R-27) → record replica set
   │
   └── 3. authenticated RPC to B: FED_ENABLE {channel, peer:A, cap, validateKey}
            B: policy check → federation:N dials A → SUBSCRIBE {c, validateKey, cap}
            A: verify cap against validateKey → SUBSCRIBE_OK {members, heads}
            B: SYNC_REQ {knownHeads: []} → backfill → live
```

Both RPCs go through the existing authenticated RPC path
(`core/index.js:351-426` → `core/rpc.js`), so signature checking, cookies and session
handling are inherited. `FED_ENABLE` is added to the authenticated command list.

Step 1 happens entirely in the browser. The server never sees the signing key — it only
ever verifies against `validateKey`, which it already stores.

### 5.3 The federation link

The user-facing artefact is a link the *other* instance's users open. Because the channel
id and all key material derive from the same secret, the pad hash is identical on both
instances; only the host differs:

```
https://b.example.org/pad/#/2/pad/edit/<same-hash-as-on-a.example.org>/
```

Nothing about the client's URL handling changes (G6). The server-visible descriptor —
what `FED_ENABLE` and any future UI pass around — is:

```jsonc
{ "v": 1, "origin": "https://a.example.org", "channel": "abcdef…", "validateKey": "…" }
```

This is C-5's "structured, resolvable locator". Its three fields are exactly the three
components of a Nuri that a server needs: repo id, read-capability public part, and
location hint. Keeping it as a typed object rather than a config string is what lets it
later *become* a `did:ng:o:…:l:…` without touching call sites.

### 5.4 Policy

`federation/policy.js`, driven by `fed/peers.json` and `config.federation`:

```js
{
  enabled: true,
  listen: { host: '0.0.0.0', port: 3050 },
  origin: 'https://a.example.org',       // advertised in HELLO and /api/federation
  peers: [
    { origin: 'https://b.example.org',
      originId: '<base64 ed25519>',       // pinned; mismatch = hard refuse
      maxChannels: 500,
      maxBytesPerChannel: 50 * 1024 * 1024,
      maxEnvelopesPerSecond: 200 }
  ],
  budget: { maxChannels: 2000, maxTotalBytes: 20 * 1024 * 1024 * 1024 },  // R-31
  heartbeatInterval: 5000,
  evictAfter: 60000
}
```

Pinning `originId` per peer means a hostile DNS/TLS takeover of the peer's domain still
cannot impersonate it. An empty `peers` array with `enabled: true` means "listen but
accept nothing" — a safe default for a first deployment.

Federation state is *not* an admin decree. Decrees are broadcast instance-wide
(`core/index.js:467-504`) and are the wrong shape for per-peer secrets and per-channel
replica sets. It is ordinary config plus `fed/` state.

---

## 6. Changes to existing files

| File | Change | Risk |
| --- | --- | --- |
| `common/constants.js` | add `paths.federation` | none — *not needed in M0; identity and policy sit at the base path. Deferred to M1 with the `fed/` keys* |
| `common/env.js` | `Env.numberFederations`, `Env.getFederationId(originId)` | low — *done in M0. The identity is not put on `Env` globally: it is loaded by the federation node only, so no other node type can accidentally read the private key* |
| `common/crypto.js` | `signKeyPair`, `detachedSign` for both backends | none — *added in M0; the module could verify but not sign* |
| `common/interface.js` | accept `federation` as a node type in the identity handshake (`:39-90`) | low — *done in M0: one entry in `ctx.others`. Federation dials cores through the same `else` branch as `front`/`http`, so no other change was needed* |
| `tsconfig.json` | add `federation/**/*` to `include` | none — *required, or rollup parses the `.ts` entry as JavaScript* |
| `core/index.js` | `isFederationCmd` guard; route `FED_INGEST`/`FED_PUBLISH`/`FED_SUBSCRIBE` between `federation` and `storage` via `Env.getStorageId`; add `FED_ENABLE` to the RPC surface | medium — this is the authorisation boundary; every new command needs its `extra.from` check, cf. `core/index.js:33-38, 49-90` |
| `core/rpc.js` | register `FED_ENABLE`, `FED_DISABLE`, `FED_STATUS` as authenticated calls | low |
| `storage/index.js` | add the federation commands to `COMMANDS` (`:428`); construct `Env.FM` next to `Env.CM` | low |
| `storage/channel-manager.js` | in `onChannelMessage` (`:141-246`), branch to `FM.acceptLocal` for federated channels instead of `storeMessage`; replace `lastSavedCp` dedup (`:164-167, 227-230`) per §4.6 | **high** — the hot path for every message on the instance. Must be a strict no-op for non-federated channels, guarded by a single cached boolean |
| `storage/history-manager.js` | append the pending tail in `getHistoryAsync` (`:116-146`); make `handleFirstMessage` (`:153-182`) refuse to create metadata for a channel awaiting its first sync | medium |
| `storage/metadata.js` + `storage/commands/metadata.js` | route mutations through `federation/control.js` when the channel is federated (R-20) | medium |
| `storage/storage/file.js` | no change to `messageBin`/`readMessagesBin`/`trimChannel`; new helpers live in `storage/federation/fedlog.js` on top of the backend | none |
| `front/network.js` | serve `GET /api/federation` (instance id, origin, versions) — a sibling of `/api/config` (`:73-151`) | low — *done in M0. It answers JSON rather than an AMD module, because the consumer is another server. Front cannot reach federation directly, so it goes through a new core command `FEDERATION_INFO`* |
| `index.js` (launcher) | spawn `infra.federation` nodes after cores, like `front`/`storage` (`:126-146`) | low — *done in M0* |
| `rollup.config.mjs` | `federation` build target | none — *done in M0* |
| `config/infra.example.js` | `federation: []` block | none — *done in M0, in `config/infra.js` too* |

The single highest-risk edit is `storage/channel-manager.js`. Mitigation: the federated
branch is entered only when `Env.channel_cache[channel].federated === true`, a flag set
once when the channel is first opened and never re-evaluated per message. A non-federated
instance executes one extra property read per message.

---

## 7. Security implementation notes

### 7.1 Instance key

Generated on first start into `data/identity.json`, mode `0600`, written with an
exclusive create so an existing identity is never clobbered. The public part is served at
`/api/federation`.

*Implemented as the parenthetical above preferred, and the reasoning is worth making
explicit rather than leaving as an aside:* the key goes to a **local file, not the object
backend**. It identifies the instance rather than any channel, so it must not move when
storage is resharded and must not end up in S3 next to the pad data. It is the one piece
of federation state that is deliberately outside `ObjectBackend`. Loss of the key means peers must re-pin; compromise means an
attacker can forge ordering metadata for channels where the instance is a member, but
**cannot** forge pad messages — those are protected by `validateKey` and revalidated
independently by every replica (R-15). That asymmetry is worth stating in operator docs:
the blast radius of a compromised instance key is ordering and availability, not content.

### 7.2 What a malicious peer can do

| Attack | Mitigated by |
| --- | --- |
| Inject forged pad content | R-15 — every replica revalidates against `validateKey` |
| Replay old envelopes | R-18 (no-op) + `s` monotonicity |
| Forge another origin's ordering | R-16 — `sig` binds `(s, l, a)` to the origin key |
| Stall the watermark to freeze history | R-5 eviction |
| Rush the Lamport clock to always sort last | Ordering nuisance only; content still converges. Detectable: log `FED_LAMPORT_JUMP` when a peer's `l` outpaces local by a wide margin |
| Flood storage | R-30 per-peer limits, R-31 budget |
| Subscribe to channels it was never given | R-25 capability verified against `validateKey` — cannot be minted without the pad's signing key |
| Learn pad content | Nothing to mitigate; it is ciphertext (§1.1 of the spec) |
| Learn *which* pads exist on this instance | Not mitigated by design. R-34: it only learns the ones it is a member of, and peering is allowlisted |

### 7.3 The trust boundary that remains

R-28: a replica cannot verify that a remote `META`/`TRIM`/`DELETE` really came from a pad
owner, because owners are account keys and accounts are instance-scoped. It trusts the
peer's assertion. This is the one place where federation genuinely extends trust, and it
is why §5.4 pins `originId` and why the first iteration should keep metadata
anchor-authoritative (spec §6).

---

## 8. Failure modes

| Failure | Behaviour | Recovery |
| --- | --- | --- |
| Peer unreachable < `T_evict` | `W` stalls; commits pause; live editing unaffected (R-7); pending tail grows | Automatic on reconnect |
| Peer unreachable > `T_evict` | Peer evicted; `W` resumes; local users get durable history again | `RECONCILE` + backlog splice (R-6) |
| Gap in a peer's `s` | That peer's `H[P]` freezes; backfill requested; other peers unaffected | Automatic |
| `validateKey` mismatch on sync | Hard refuse, `UNSUBSCRIBE`, alert. Never reconcile (R-21) | Operator |
| Both instances trim concurrently | Forbidden while any peer is evicted (spec §11.3); otherwise both `TRIM` commits apply in merged order, the later one being a no-op | Automatic |
| Crash between `appendPending` and commit | Pending log is the durable record; merge loop re-runs from `state.json` on start | Automatic |
| Crash between commit and `markCommitted` | Duplicate commit attempt; `offsetByHash` already holds the id, so it is skipped — the same idempotence `storeMessage` already relies on (`storage/channel-manager.js:91-94`) | Automatic |
| Storage node re-sharded (`infra.storage` resized) | `Env.getStorageId(channel)` changes; `fed/` keys must move with `channel/` keys | Same operator procedure as the existing storage migration (`scripts/storage-migrate.js`) — must be extended to `fed/` |
| Clock skew between instances | None. `t` is never used for ordering (R-2). Only `expire` handling is clock-sensitive, and it is centralised into a control commit (spec §8) | — |

---

## 9. Milestones

Each milestone is independently shippable and independently useful.

**M0 — Plumbing (no federation yet). — IMPLEMENTED**
`federation` node type, `Interface` support, launcher and rollup targets, instance
identity, `/api/federation`, handshake, policy file, `federation:N` ↔ `core` ↔
`storage:K` round trip proved with a `FED_PING`.
*Done when:* two instances authenticate and exchange a ping; a single instance with
`infra.federation: []` is byte-identical in behaviour to today.

*Both criteria are verified by `tests/integration/federation-m0.test.js`, which boots
two complete instances as real processes. Operator instructions are in
[`federation-operating.md`](federation-operating.md).*

Delivered beyond the letter of M0, because the session was useless without them:
a 2 s heartbeat that pings every live peer (the beat R-37 will later carry the
watermark on), reconnect with jittered backoff to a 5-minute ceiling, and a
deterministic tie-break when both instances dial each other at once.

**M1 — L0, read-only mirror. — IMPLEMENTED**
Envelope format, `common/federation/*`, capability minting and verification,
`SUBSCRIBE`/`SYNC_REQ`/`SYNC_RES`, pending log, and a committed log built by straight
append (single writer ⇒ no merge needed). The mirror refuses local writes.
*Done when:* a pad edited on A is fully readable and live on B, and B's committed id
sequence equals A's.

*Verified by `tests/integration/federation-mirror.test.js`, which drives real WebSocket
clients against two complete instances and asserts the id **sequence**, not just the set —
because §11.1 showed that replicas agreeing on the set but not the order can diverge
permanently.*

Two deviations from the plan above, both deliberate:

* **`storage/channel-manager.js` was not touched.** The live fan-out hangs off core's
  `onChannelMessage`, which already handles every write on its way to the front nodes.
  A non-federated channel costs one `Set` lookup there. This removes M1's share of the
  highest-risk edit in §6 entirely.
* **`(s, l)` are derived from the committed log**, not counted in memory, so a restart
  cannot desynchronise a replica's sequence from its log. In M1 they coincide with the
  log position; M3 separates them.

**M2 — L1, anchored write-through. — IMPLEMENTED**
B forwards local writes to A over the federation session and broadcasts only after A
assigns the position. Metadata anchor-authoritative. Control commits for `DELETE`/`EXPIRE`.
*Done when:* users on both instances edit the same pad concurrently and converge; killing
B loses nothing; killing A makes B read-only with a clear client-visible state.

**Done:** write-through itself, verified by `tests/integration/federation-writethrough.test.js`
— a mirror's write is committed by the anchor and comes back through `PUBLISH`; concurrent
editing from both instances converges on one order; and with the anchor's federation node
killed the mirror *refuses* writes rather than committing locally, which is the behaviour
anchoring exists to guarantee.

**Also done:** metadata and control commits, in `federation-metadata.test.js` — a
metadata change at the anchor replicates as a `META` control commit and is applied through
`Meta.handleCommand`, so replica metadata is a pure function of the commands (R-20); the
mirror refuses local metadata changes; self-destructing (R-22) and restricted (R-29) pads
are refused federation outright, because their semantics are instance-scoped and a replica
could not honour them.

Implementing this exposed a defect in M1 worth recording: **a mirror had no channel
metadata at all.** Its log is built by `FM.append`, which never goes through
`handleFirstMessage`, so it had no `validateKey`, no owners and no `created` — a silent
R-21 violation that the M1 tests missed because they only read message history. A replica
now seeds those fields from the anchor at subscribe time.

**Still outstanding:** the pad-key half of R-28. A control commit carries the origin
instance's signature but not a pad-key one, because that can only be minted in the
browser. Until it exists, applying a peer's control commit is an explicit trust-the-peer
decision — the boundary R-28 names, and the reason R-27's allowlist is not optional.

**`storage/channel-manager.js` is still untouched.** The write interception lives in core's
`onChannelMessage`, next to the M1 fan-out — a `Set` lookup per write on a channel this
instance does not mirror. §6 called this edit the highest-risk in the project; M1 and M2
have both avoided needing it.

**M3 — L2, multi-master. — MOSTLY IMPLEMENTED**
Lamport clocks, `a` (acks), watermark, merge loop, heartbeats, eviction, `RECONCILE`,
federated checkpoint dedup, `TRIM` control commits, full metadata replication.
*Done when:* the convergence and partition tests in §10 pass repeatedly.

**Done:** `storage/federation/merge.js` — Lamport clocks, watermark, commit rule,
eviction, and the trim precondition — plus the merge loop, per-origin pending logs and
the heartbeat clock exchange. `federation-concurrent.test.js` shows two instances
accepting concurrent writes with no anchor and converging on an identical id *sequence*.

**Not done:** `RECONCILE` (R-6), checkpoint dedup (R-10), `TRIM` itself (R-11), and R-8
in `GET_HISTORY`.

Three things were got wrong first and are worth recording:

* **The heartbeat must advertise a *promise*, not the sender's last clock.** Reporting
  `self.lamport` pinned the watermark wherever an idle member last wrote, so two
  instances that each wrote once converged only if they kept writing.
* **R-17 as originally written was too weak.** Blocking only the origin with a sequence
  gap let other origins commit past it — but a member's promise says nothing about
  whether we have *received* what it already sent, so a message in flight could arrive
  needing to be inserted behind something already committed. Now any live member with
  unreceived messages blocks the whole pass.
* **Federation state needs its own write queue.** Four paths read-modify-write
  `state.json` with a conditional put; without serialising them one silently loses the
  race, which showed up as tests passing and failing at random.

**M4 — The two-instance install, and real-pad testing. — IMPLEMENTED**
*Reordered ahead of hardening, deliberately.* Everything up to M3 is verified by tests
that write synthetic messages. Nothing had yet proved that a **real pad**, with real
ChainPad patches, checkpoints and encryption, federates correctly when a person opens it
in a browser — and that is the only evidence that matters before hardening anything.

Target: open a pad on one instance, run one command, open the same pad on the other and
edit it from both sides. `federation-realpad.test.js` proves the protocol half of that
with the real client libraries — real `chainpad`, real `chainpad-crypto`, real
checkpoints — asserting the two instances render **identical document text** rather than
merely holding the same message ids. What remains untested is a browser: the console
helper that mints the capability has not been run against a live session.

*Done:* R-7 (a remote edit is broadcast to local readers on arrival, not at commit — the
difference between federation feeling live and feeling a heartbeat late), R-8 (history
serves the committed prefix plus the sorted uncommitted tail, so a joining client sees the
same sequence as everyone else), and a browser helper that mints the capability and calls
both endpoints (`experiments/federation/federate-pad.js`).

*Outstanding, and each one is a plausible reason a real pad still misbehaves:*

* **Checkpoint dedup (R-10).** *Downgraded.* `federation-realpad.test.js` drives a real
  document past several checkpoint boundaries on both instances at once and it converges:
  a duplicate checkpoint is a message ChainPad already knows, and it ignores it. Still
  worth doing so a busy federated pad does not grow faster than it needs to, but an
  efficiency matter rather than the correctness risk it was billed as.
* **`RECONCILE` (R-6)** — an evicted peer's backlog is not spliced in with a marker.
* No UI: federating a pad is a console command.

**M4b — Making it work in reality. — MOSTLY IMPLEMENTED**
Three problems came out of real use, and analysing them was worth more than any of
the fixes:

* **Self-healing (R-47).** *Done.* Two pads diverged and neither recovered. The
  causes were bugs, but the lesson was the absence of a repair: nothing detected or
  corrected a missing message. Heartbeats now carry a per-origin `have`, a peer
  resends its own envelopes the other lacks, and envelopes are retained until every
  member has acknowledged them rather than being dropped at local commit.
* **Repair must not reorder (R-48).** *Half done — the half that prevents damage.*
  A resent envelope sorting before the committed head cannot just be appended:
  that is the §11.1 failure exactly. The merge now tracks the committed tip as a
  full `(l, o, id)` and **holds** anything below it, logging
  `FEDERATION_RECONCILE_REQUIRED`. The channel stalls visibly instead of diverging
  silently, and nothing is lost — the envelopes stay in the pending log.

  The *splice* still needs R-6's `RECONCILE`, which means rewriting a channel's
  committed history and telling clients to reload. That is the one part of the
  storage layer this work has deliberately never touched, so it is scheduled
  rather than rushed. **M6.**
* **Comments and annotations (R-49).** *No work needed, and that is the finding.*
  They live in `metadataMgr` metadata, which the app framework embeds into the
  ChainPad content — so they already travel in the pad's channel. Their apparent
  absence was a symptom of divergence. Building a separate comments channel would
  have been wasted effort against a misdiagnosis.
* **The chat is a separate channel (R-50).** *Done.* R-49's rule — inside the
  channel federates, outside needs a mechanism — put the pad **chat** on the wrong
  side of the line. It is neither: a *second document*, with its own random
  channel id kept in the pad's metadata as `chat2`, which no server can read.

  It shares the pad's cryptography, though, so it needs no new machinery: the same
  signing key mints its capability and the same validate key checks it. The client
  enumerates the pad's auxiliary channels and federates each one; the server treats
  each as ordinary. `cursor` and `integration` are excluded on purpose — they are
  ephemeral and hold no history.

  The one genuinely new piece is timing. `chat2` is minted the first time somebody
  opens the chat, usually well after the pad was federated, so the client federates
  it then, asking `GET /api/federation/channel/:channel` whether the pad is
  federated and to whom. A failure there is reported but never fails the pad's own
  federation.
* **Blobs.** Implemented in M5 and covered by tests, but not yet proved end to end
  through a real pad in a browser.

**M5 — Blob support. — IMPLEMENTED**
A pad embedding an image works on both instances. R-40 assumed the best a server could
manage was a warning; it can do better. A replica **fetches a blob on demand**, the first
time somebody asks for one it does not hold — which inverts the problem, because the
reference is resolved by the only party that can read it (the browser) and the resulting
miss is what triggers the transfer. Blobs added long after federation was enabled are
covered by the same mechanism, and a pad with no blobs costs nothing.

Blobs are immutable and content-addressed, so there is no ordering, no watermark and no
conflict — a fetch, not a replication, which is why it is a separate mechanism from the
channel log. Chunked at 64 KB to fit `MAX_FRAME`, capped at 20 MB, stored on arrival.
`storage/federation/blobs.js` addresses them through the ObjectBackend, so `fs` and S3
both work.

One implementation note worth keeping: the HTTP fallback also runs in the storage
**cluster** process, which serves blobs and has a proxied interface but none of the
federation state. Existence is therefore checked via `Env.blobStore`, not `Env.FB`.

*Not done:* quota accounting for federated blobs, and garbage collection of blobs whose
pad was later un-federated. Both M6.

**M6 — Hardening.**
Bloom-filter sync, rate limiting, budgets, admin UI/CLI (`FED_STATUS`), metrics
(`Env.plugins?.MONITORING`), operator documentation, storage-migration support for `fed/`.

**M7 — NextGraph adapter (separate project).**
Implement `Codec`/`IdCodec`/`Transport`/`Validator` against NextGraph and prove
`TopicSyncReq` ↔ `SYNC_REQ` equivalence on a toy repo. Not scheduled here; M0–M4 exist to
make it possible.

---

## 10. Test plan

### 10.0 The manual rig

Automated tests boot throwaway instances and assert. They cannot tell you whether a
federated pad *feels* right in a browser — whether the cursor jumps, whether a
reconnect duplicates a line, whether the userlist looks sane with editors on two
instances. So alongside them there is a persistent two-instance deployment at
`/home/ludovic/dev/cryptpad/experiments/federation`:

| | instance A | instance B |
| --- | --- | --- |
| browse | `http://localhost:3000` | `http://localhost:4000` |
| federation | `ws://127.0.0.1:3050/federation` | `ws://127.0.0.1:4050/federation` |

`node setup.js` builds it; `./start.sh`, `./status.sh`, `./stop.sh` run it. Three
decisions in it are worth stating because getting them wrong produces a rig that
appears to work while proving nothing:

* **Both instances run the same server code**, selected by `CRYPTPAD_CONFIG` and
  `CRYPTPAD_CONFIG_INFRA`. Nothing is copied, so there is no second checkout to drift.
* **Every path is pinned inside each instance's own data directory.** The defaults are
  relative to the server checkout, so two instances started naively share one datastore —
  which looks exactly like perfect federation until you notice the network was never used.
* **One shared client checkout.** The client changes that federation needs (capability
  minting, the R-40 blob warning) are then live on both instances at once.

TLS is off there; it is loopback-only and the node logs `FEDERATION_NO_TLS` on every
start. R-24 still applies to anything reachable from outside the machine.

### 10.1 Automated tests

The existing harness (`tests/`, `node --test`, `tests/runAll.js`) already spins a server
and drives real WebSocket clients (`tests/pad.test.js`, `tests/common/rpc.js`). Federation
tests extend it to **two full topologies in one process**, distinguished by config.

**Unit** (`tests/unit/`)
* `federation-envelope.test.js` — canonical encoding stability, sign/verify, R-14/16
  rejection cases, tamper detection on each field.
* `federation-order.test.js` — `byKey` is a strict total order; property test: any two
  permutations of the same envelope set produce the same sorted output.
* `federation-merge.test.js` — watermark arithmetic; commit rule never emits out of order;
  late envelope handling; gap blocking.
* `federation-capability.test.js` — R-25 tokens do not transfer across channel, peer pair,
  or expiry.

**Integration** (`tests/`)
* `federation-mirror.test.js` (M1) — write 500 messages on A; B's committed sequence
  matches exactly; a client on B replays identical history.
* `federation-concurrent.test.js` (M3) — two clients, one per instance, editing at
  ~10 msg/s for 30 s; assert (a) identical committed id sequences, (b) identical decrypted
  ChainPad document on both sides. **This is the test that settles spec §11.1** and it must
  exist before M3 is called done.
* `federation-partition.test.js` (M3) — drop the session mid-edit for `> T_evict`, keep
  editing both sides, heal; assert convergence and that a single `RECONCILE` marker appears
  at the same position on both.
* `federation-restart.test.js` — kill and restart mid-flight; assert no acknowledged
  message is lost and no duplicate is committed.
* `federation-abuse.test.js` — forged signature, wrong `validateKey`, replayed envelope,
  sequence gap, rate-limit breach; each must produce a typed `ERROR` and leave the channel
  usable.

**Regression**
The whole existing suite must pass unchanged with `infra.federation: []`, and again with a
federation node running but no channel federated. That is the guard on the
`channel-manager.js` hot-path edit.

---

## 11. NextGraph seams

The four interfaces from C-4, stated concretely so they are not quietly bypassed.

```js
// common/federation/codec.js
{ encode(obj) -> Buffer, decode(Buffer) -> obj, name: 'json' }        // → 'bare'

// common/federation/ids.js
{ fromContent(content) -> id, isValid(id), compare(a, b), name: 'sigprefix' }  // → 'blake3'

// federation/transport.js
{ listen(cfg, onSession), dial(peer, cb) }                            // → noise/broker

// common/federation/validator.js
{ verifyMessage(content, validateKey, cb), verifyEnvelope(env, originKey) }
```

Rules for keeping the seam real:

* No file outside `common/federation/ids.js` may call `HKUtil.getHash` on a federated
  path, or assume an id is 64 characters, or slice one.
* No file outside `codec.js` may call `JSON.stringify`/`JSON.parse` on wire data.
* `sync.js` must be written against heads and acks only — never against `s`, `l`, or file
  offsets. `s` and `l` exist for the *merge*, not for the *sync*; conflating them is what
  would make the NextGraph adapter a rewrite.

The mapping to `TopicSyncReq`/`TopicSyncRes`, and the six places CryptPad and NextGraph
genuinely differ, are tabulated in spec §10.1–10.2.

---

## 12. Effort and sequencing

Rough, for planning only, and assuming familiarity with this codebase:

| Milestone | Scope | Notes |
| --- | --- | --- |
| M0 | New node type, identity, handshake, policy | Mostly mechanical; the `Interface` and launcher changes are small and well-patterned |
| M1 | Envelopes, capabilities, sync, mirror | The largest *new-code* step, but low-risk: no changes to the write path |
| M2 | Write-through, control commits | Touches `channel-manager.js`; needs the regression guard from §10 |
| M3 | Lamport merge, watermark, partitions | The hard one. Budget most of the schedule here. Spec §11.1 is now **resolved** — the sort key will not change — but the resolution makes R-1 and R-8 load-bearing, so `federation-concurrent.test.js` is still front-loaded, now as a *regression* guard rather than a design probe |
| M4 | Install + real-pad testing | Promoted ahead of everything else: no further work is worth doing until a real pad is known to federate |
| M5 | Blobs | A federated pad with an image is broken today, silently |
| M6 | Limits, budgets, ops | Required before any public deployment |

**All of spec §11 is now answered** (`docs/experiments/chainpad-ordering/`), so no
scheduling dependency remains on an unresolved design question. Two results change how M3
must be built:

* The sort key does not change and `t` never enters it. What replaced that risk is a
  constraint: ChainPad is not order-independent, so R-1 and R-36 must hold *exactly*,
  including for the uncommitted tail.
* A premature `TRIM` does not merely pick the wrong branch, it **destroys the other
  branch's content silently** (§11.3). R-38's precondition must therefore be implemented
  as a hard assertion at trim time (R-41), and `control.js` should be written with that
  check first, before the trim path exists at all.
