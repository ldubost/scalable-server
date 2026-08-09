<!-- SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors

SPDX-License-Identifier: AGPL-3.0-or-later
-->
# CryptPad ↔ CryptPad Federation — Specification

**Status:** partially implemented. **M0**, **M1 (L0, read-only mirror)**, **M2 (L1,
anchored write-through)** and most of **M3 (L2, multi-master)** are built and tested: two
instances accept concurrent edits with no anchor and converge on an identical committed
sequence. **M4 (the two-instance install, for testing with real pads)** is in progress.
Outstanding within M3: `RECONCILE` (R-6), checkpoint dedup (R-10) and `TRIM` (R-11).
Blob support is M5; quotas, budgets and admin tooling are M6. **§12 records the status of
every requirement**, and [`federation-status.md`](federation-status.md) summarises it. All
open questions in §11 have been resolved, several by experiment
([`experiments/chainpad-ordering/`](experiments/chainpad-ordering/README.md)).
**Scope:** allow a single pad (`/pad/`, i.e. a standard 32-character channel carrying
ChainPad messages) to live on two or more independent CryptPad instances at once, such
that every instance converges on the same document state, and users of either instance
can edit it concurrently.
**Secondary goal:** structure the protocol so that a NextGraph transport/store can later
be substituted for the CryptPad-native one without redesigning the synchronisation
layer. §10 states precisely which decisions exist only to serve that goal.

Companion document: [`federation-design.md`](federation-design.md) describes *how* to
build this inside the scalable server. This document describes *what* must be true.

---

## 1. Analysis of the current pad model

Everything below is derived from the code in this repository. The relevant invariants
are not incidental — they are what federation must either preserve or explicitly break.

### 1.1 A pad is an append-only log of opaque, self-authenticating records

A pad is a **channel**: a 32-hex-character identifier
(`STANDARD_CHANNEL_LENGTH`, `common/constants.js:5`). Its history is a newline-delimited
file of JSON arrays, appended one line at a time
(`storage/channel-manager.js:54-139` → `store.messageBin`, `storage/storage/file.js:972`).

A stored line has the shape:

```js
[ 0, netfluxSenderId, "MSG", channelId, content, serverTimestamp ]
```

`content` (`msgStruct[4]`) is the only part the client produces. It is
base64 of `ed25519_signature(64 bytes) ‖ xsalsa20-poly1305 ciphertext`, optionally
prefixed by `cp|<hash>|` for checkpoints (`CHECKPOINT_PATTERN`,
`common/constants.js:3`).

The server **cannot read** `content`. The symmetric key lives in the URL fragment and
never reaches the server. The server can only verify the attached signature against the
channel's `validateKey` (`core/worker.js:11-30`, `crypto_sign_open`), which is a 32-byte
ed25519 public key stored in the channel metadata
(`storage/history-manager.js:308`, `storage/hk-util.js:71-78`).

**Consequence for federation:** a remote server can validate and store a foreign
message with exactly the same authority as the originating server, and gains no ability
to read it. Replication is a *pure data-plane* operation. This is the single most
important property that makes CryptPad federation tractable.

### 1.2 Message identifiers are already globally stable

Clients address history positions by "hash", which is not a hash at all:

```js
HK.getHash = function (msg) { return msg.slice(0, 64); };   // storage/hk-util.js:29-37
```

It is the first 64 base64 characters — 48 bytes — of the message's ed25519 signature.
Ed25519 signatures are deterministic, so this identifier is:

* derived purely from the content and the pad's signing key;
* identical on every server that ever sees the message;
* unforgeable without the pad's signing key.

The comment at `storage/hk-util.js:24-27` forbids changing this function. Good: it means
**federation needs no new message identifier scheme**. `getHash(content)` is already a
usable global content address.

### 1.3 What *is* server-local

Three things are local to one server and cannot be replicated verbatim:

| Thing | Where | Why it is local |
| --- | --- | --- |
| Byte offsets (`index.offsetByHash`, `index.cpIndex[].offset`, `index.size`) | `storage/channel-manager.js:82-136`, `storage/history-manager.js:39-114` | Positions in *this* server's file |
| `serverTimestamp` (`msgStruct[5]`) | `storage/channel-manager.js:234` | Wall clock of whichever server accepted the write |
| `netfluxSenderId` (`msgStruct[1]`) | `front/index.js:185`, `front/network.js:243-251, 414-429` | Random per-connection id, meaningful only on the accepting front node |

Clients never exchange offsets — they exchange `lastKnownHash`
(`storage/history-manager.js:39-114`). So offsets may legitimately differ per server.

### 1.4 The ordering authority is the accepting storage node

For a given channel, exactly one storage node may write it
(`Env.getStorageId`, `common/env.js:70-79`, jump consistent hash over the first 8 bytes
of the channel id), and inside that node writes are serialised per channel
(`Env.queueStorage`, `storage/schedule.js`). The *arrival order at that node* becomes the
pad's history order, and clients replay exactly that order.

This is the invariant federation breaks. Two instances accepting concurrent writes
produce two different arrival orders for the same set of messages. §4 defines what
replaces it.

### 1.5 Metadata is a second, command-oriented log

Channel metadata (`{channel, validateKey, owners[], allowed[], restricted, expire,
mailbox, selfdestruct, deleteLines}`) is created on first write
(`handleFirstMessage`, `storage/history-manager.js:153-182`) and subsequently mutated by
signed commands appended to a dedicated log (`ADD_OWNERS`, `RM_OWNERS`,
`RESTRICT_ACCESS`, `ADD_ALLOWED`, … — `storage/metadata.js:10-40`,
`storage/storage/file.js:499`).

Metadata commands are authenticated at the RPC layer by the *user's* account key, not by
the pad key (`core/index.js:351-426`), and the owner list holds user public keys. This
matters: metadata authority is tied to accounts, and accounts are instance-scoped, while
pad content authority is tied to the pad key, which is not.

### 1.6 Live delivery is already decoupled from persistence

A message is broadcast to connected members via a userlist held in memory on the storage
node (`Env.channel_cache[channel].users`, `storage/index.js:156-250`) and fanned out
through core to the right front nodes (`core/index.js:137-145`, `front/index.js:306-321`).
Reconnecting clients instead replay the file from `lastKnownHash`
(`HistoryManager.onGetHistory`, `storage/history-manager.js:277-411`).

**Consequence:** there are already two delivery tiers — a best-effort live tier and an
authoritative history tier. §4.4 exploits this: federation can broadcast immediately
while committing to history slightly later, without inventing a new mechanism.

### 1.7 Identity is already cross-domain

User signing keys are serialised as `[username@domain/pubkey]`
(`common/keys.js:20-42`). CryptPad's identity model therefore already names a home
instance alongside a key. Federation does not need to invent user identity.

---

## 2. Goals and non-goals

### 2.1 Goals

* **G1** — A standard pad may be replicated between two or more CryptPad instances.
* **G2** — Users on any participating instance may edit concurrently, with the same
  latency they have today for local edits.
* **G3** — All instances converge on the same document state and the same *history
  order*, permanently, without operator intervention.
* **G4** — Instances remain independently operable. A partition means both sides keep
  serving their own users; reconnection reconciles.
* **G5** — No plaintext, no symmetric key, and no pad signing key is ever exposed to a
  server, including the remote one. Federation must not weaken the current threat model.
* **G6** — Zero client-side protocol change for the *editing* path. An unmodified
  CryptPad client connected to either instance must edit a federated pad correctly.
* **G7** — The synchronisation layer must be expressible as a causal commit DAG with
  head-based anti-entropy, so a NextGraph backend can be substituted later (§10).

### 2.2 Non-goals (for this iteration)

* **N1** — Federating anything other than `/pad/`: no blobs, no teams, no drive, no
  calendar, no OnlyOffice/spreadsheet channels, no mailboxes, no user accounts.
* **N2** — Cross-instance user accounts, login, or quota transfer.
* **N3** — Cross-instance presence beyond a merged member count (see R-13).
* **N4** — Federating restricted (`metadata.restricted`) pads. Explicitly deferred; see
  §7.4.
* **N5** — More than a small, operator-configured set of peers. This is not an open
  gossip network.
* **N6** — Being a NextGraph implementation. The goal is a *seam*, not a bridge.

---

## 3. Terminology

| Term | Meaning |
| --- | --- |
| **Instance** | One CryptPad deployment (which may itself be many `front`/`core`/`storage` nodes). |
| **Origin** | The instance that first accepted a given message from a client. |
| **Instance key** | An ed25519 keypair identifying an instance to its peers. New; see §7.1. |
| **Origin id** | Base64 of the instance key's public part. Stable, 44 chars. |
| **Peer** | Another instance with which this one has an authenticated federation session. |
| **Replica set** | The set of instances replicating a given channel. |
| **Anchor** | For conformance level L1 only: the single instance owning write order. |
| **Envelope** | The federation record wrapping one pad message (§5.1). |
| **Merged log** | The deterministic total order over all envelopes for a channel (§4.2). |
| **Committed prefix** | The part of the merged log that is final and may be served as history (§4.3). |
| **Head** | An envelope with no known successor in the causal DAG (§5.2). |
| **Watermark** | Per-peer contiguity marker used to decide commitment (§4.3). |

---

## 4. The model: a replicated causal log

### 4.1 Why a deterministic global order is required

ChainPad reconciles concurrent patches into a single chain, and CryptPad's history
replay assumes that replay order is the order in which the chain was established. If two
instances serve *different* orders for the same message set, two things go wrong:

1. Clients on instance A and instance B may resolve forks differently and can, in the
   worst case, settle on different documents — the failure this whole feature exists to
   prevent (G3).
2. `lastKnownHash` resumption becomes instance-specific: a client that reconnects to the
   other instance may receive `EUNKNOWN` and be told to drop its cache
   (`storage/history-manager.js:54-88`), or replay messages it already has.

We could try to argue that ChainPad's fork resolution is order-insensitive. We
deliberately do not. **This specification requires that every replica serve the same
sequence of message ids for a channel**, which makes the question moot and removes an
entire class of hard-to-reproduce divergence bugs.

> **R-1 (Order agreement).** For any channel and any two instances in its replica set,
> the sequence of message ids in the committed prefix (§4.3) must be identical, and must
> be a prefix-stable extension: an id, once committed at position *n*, stays at position
> *n* forever.

Note what R-1 does **not** require: byte-identical files. `netfluxSenderId` and byte
offsets legitimately differ (§1.3). Only the ordered id sequence must match.

### 4.2 The order

Each envelope carries a **Lamport clock** `l` and its origin id `o`, and its id
`id = getHash(content)`. The merged log is the set of envelopes sorted by:

```
(l ASC, o ASC bytewise, id ASC bytewise)
```

* `l` is assigned by the origin as `1 + max(l)` over every envelope the origin had
  already merged for that channel at the moment of acceptance. Per origin, `l` is
  strictly increasing.
* `(o, id)` breaks ties between concurrent envelopes. Both components are content-fixed,
  so every replica breaks the tie identically.

Because `l` is a Lamport clock, if envelope *x* causally precedes *y* then `l(x) < l(y)`,
so the sort never places an effect before its cause. Concurrent envelopes are ordered
arbitrarily but *consistently*, which is all R-1 asks for.

> **R-2 (Deterministic sort).** The sort key must be `(l, o, id)` and must be computable
> from the envelope alone, with no reference to local state, local clocks, or arrival
> order.

### 4.3 Commitment and the watermark

The sort is only usable once no envelope can still arrive that would sort earlier than
something already written. Each instance therefore tracks, per peer *P* in the replica
set, a **contiguity watermark** `H[P]`: the Lamport value of the last envelope from *P*
received with no gap in *P*'s per-origin sequence numbers (§5.1, field `s`).

Let `W = min over all P in the replica set of H[P]` (including this instance itself).

> **R-3 (Commit rule).** An envelope with Lamport value `l` may be appended to the
> committed prefix only when `l ≤ W`. Envelopes with `l ≤ W` must be appended in
> `(l, o, id)` order.

This is sound: per-origin Lamport values strictly increase, so once *P*'s contiguous
prefix reaches Lamport `H[P]`, *P* can never emit an envelope with Lamport `≤ H[P]`.

> **R-4 (Liveness of the watermark).** Every instance must advance `H[P]` for idle peers.
> Peers must emit a periodic signed heartbeat carrying their current `(s, l)` per
> replicated channel (or an aggregate), at an interval `T_hb` with a default of 5 s.
> `W` must not lag live traffic by more than `T_hb + RTT` under healthy conditions.

> **R-5 (Partition behaviour).** If a peer is unreachable for longer than `T_evict`
> (default 60 s), the instance must *provisionally evict* it from the watermark
> computation so that `W` resumes advancing and local users keep getting durable
> history. On reconnection, the returning peer's envelopes will carry Lamport values
> below the local `W`. Such **late envelopes** must be handled per R-6.

> **R-6 (Late envelopes).** A received envelope whose Lamport value is `≤` the committed
> prefix's last Lamport value must not be discarded and must not be inserted into the
> committed prefix. It must be appended at the end of the committed prefix, and its
> effective sort key must be rewritten to `(l', o, id)` where `l'` is the Lamport value
> it *would* have received had it been merged at that point. The rewrite must be
> performed identically by every replica; it is therefore driven by the position of a
> **reconciliation marker** (§5.4), not by local time.

R-6 is the price of R-5. A partition that outlives `T_evict` degrades from "consistent
ordering" to "consistent ordering with an explicit, replicated reconciliation point".
Clients see the returning messages as a fork, which is exactly the situation ChainPad is
built to resolve, and every replica sees the same fork at the same position.

### 4.4 Two delivery tiers

> **R-7 (Live tier).** An instance must broadcast a message to its locally connected
> members as soon as it is accepted (locally) or received and validated (remotely),
> without waiting for commitment. Latency for a local edit must not regress.

> **R-8 (History tier).** `GET_HISTORY`, `GET_HISTORY_RANGE` and `GET_FULL_HISTORY` must
> serve the committed prefix, followed by the uncommitted envelopes in `(l, o, id)`
> order. The uncommitted tail is bounded by R-4 and is expected to hold a handful of
> messages.

> **R-9 (Durability).** A message accepted from a local client must be durably persisted
> before the client is acknowledged, even though it is not yet committed to the merged
> log. Losing an acknowledged message on restart is not acceptable.

### 4.5 Checkpoints

Checkpoints (`cp|<id>|…`) let history be trimmed and let clients resume from a snapshot
instead of the full log (`storage/channel-manager.js:159-231`, `HK.sliceCpIndex`,
`storage/hk-util.js:51-60`).

> **R-10 (Checkpoint dedup).** Duplicate-checkpoint suppression must be evaluated against
> the merged log, not against a per-node in-memory `lastSavedCp`
> (`storage/channel-manager.js:227-230`). Two *different* checkpoints produced
> concurrently on two instances must both be retained; suppressing either would remove
> history that the other instance's clients depend on.

> **R-11 (Trimming).** `trimChannel` (`storage/storage/file.js:1217`) removes history
> below a hash and is destructive and order-sensitive. It must be modelled as a control
> commit in the federation log (§5.4) with its own Lamport value, applied by every
> replica at the same position in the merged log. An instance must never trim a
> federated channel on local authority alone.

### 4.6 What convergence means here

> **R-12 (Convergence).** Given a replica set with no permanently failed member and a
> finite message set, every instance must, within a finite time after the last message
> and the last partition heal, hold the same committed prefix (R-1) and the same
> metadata state (§6).

---

## 5. Wire format

Encoding is JSON over an authenticated WebSocket session for the CryptPad-native
transport. §10 requires that the encoding be swappable.

### 5.1 Envelope

Created once, by the origin, for each pad message. Immutable and replicated verbatim.

```jsonc
{
  "v":   1,                 // envelope version
  "c":   "<channelId>",     // 32 hex chars
  "o":   "<originId>",      // base64 ed25519 instance public key, 44 chars
  "s":   1234,              // per (channel, origin) sequence number, starts at 1, no gaps
  "l":   4711,              // Lamport clock (§4.2)
  "a":   ["<id>", "..."],   // acks: merged-log heads known to the origin at accept time
  "id":  "<msgId>",         // getHash(m) — first 64 base64 chars of the signature
  "t":   1786000000000,     // origin wall-clock ms; informational, never used for ordering
  "m":   "<content>",       // msgStruct[4] verbatim, including any cp| prefix
  "sig": "<base64>"         // origin instance signature over the canonical encoding of
                            // all preceding fields
}
```

Rules:

* **R-14** `id` must equal `getHash(m)`. A receiver must recompute and reject on mismatch.
* **R-15** `m` must verify against the channel's `validateKey` using the same code path
  as a local message (`core/worker.js:11-30`). A receiver must not trust the origin's
  validation.
* **R-16** `sig` must verify against `o`. This binds `(s, l, a)` — the ordering
  metadata, which is *not* covered by the pad signature — to the origin instance, so a
  peer cannot fabricate ordering on another origin's behalf.
* **R-17** `s` must be gapless per `(c, o)`. A receiver detecting a gap must not commit
  past it (it is what makes `H[P]` meaningful) and must request the missing range.
* **R-18** An envelope for a message id already present must be treated as a no-op, not
  as an error. Replication is at-least-once.

The `a` (acks) field is redundant for *ordering* — `l` alone determines the sort. It is
carried because it makes the log an explicit causal DAG, which gives us gap detection,
efficient anti-entropy, and the NextGraph mapping in §10. It is small: heads are few.

### 5.2 Session and channel messages

| Message | Direction | Purpose |
| --- | --- | --- |
| `HELLO {v, originId, origin, nonce, caps[]}` | both | announce identity, public URL, supported features |
| `AUTH {sig}` | both | sign the peer's nonce with the instance key; mutual |
| `SUBSCRIBE {c, validateKey, cap}` | requester → holder | ask to join a channel's replica set; `cap` per §7.2 |
| `SUBSCRIBE_OK {c, members[], heads[]}` | holder → requester | accept; return current replica set and heads |
| `SYNC_REQ {c, knownHeads[], targetHeads[]?, knownIds?}` | both | anti-entropy for a channel |
| `SYNC_RES {c, envelopes[], done}` | both | missing envelopes, **in causal order**, streamed |
| `PUBLISH {envelope}` | both | live push of one new envelope |
| `HEARTBEAT {[{c, s, l}, ...]}` | both | advance `H[P]` when idle (R-4) |
| `CONTROL {envelope}` | both | control commit (§5.4) |
| `UNSUBSCRIBE {c, reason}` | both | leave the replica set |
| `ERROR {c?, code, detail?}` | both | typed failure |

`SYNC_REQ`/`SYNC_RES` deliberately mirror NextGraph's `TopicSyncReq` (§10).
`knownIds` is an optional Bloom filter of locally held message ids, allowing the
responder to skip most of the DAG walk on large pads.

> **R-19 (Causal order on the wire).** `SYNC_RES` must emit envelopes in an order where
> every envelope's `a` entries appear earlier in the same stream or are already held by
> the requester. This lets the receiver validate and apply incrementally.

### 5.3 Heads

The **heads** of a channel are the envelopes not referenced in any known envelope's `a`.
In steady state a channel has one head; during concurrent editing it has one per active
origin. Heads are the anti-entropy handle: two instances with equal head sets are
synchronised.

### 5.3b Enabling replication: the client never contacts the peer

> **R-46 (Server-to-server setup).** A client MUST NOT contact a peer instance
> directly in order to federate a pad. It mints the capability — only it holds the
> pad key — and gives it to **its own** server, which invites the peer over the
> authenticated session the two instances already hold (`INVITE`).

The first implementation had the browser POST to both instances. That was wrong on
three counts, and the browser's own CSP caught it:

* a browser has no relationship with another instance, so `connect-src` blocks the
  request — correctly;
* it bypassed the authenticated, allowlisted peer session that exists precisely for
  instance-to-instance work;
* it made the client responsible for knowing the peer's address and identity, which
  is operator configuration.

The client now asks its own server which instances it is peered with
(`GET /api/federation/peers`), scopes the capability to one of them, and makes a
single call home. Nothing about the trust model changes — the capability is still
the authorisation, still minted where the pad key is — only the route.

### 5.3c Anti-entropy: repairing a divergence

Real use produced two divergences, and neither was recoverable. The bugs that
caused them were fixed, but the important lesson was not about the bugs:

> **R-47 (Self-healing).** An instance MUST detect and repair messages missing
> from a peer without operator action, whatever caused the loss. A dropped
> `PUBLISH`, an outage, a restart mid-edit, or a bug must all be recoverable.

The mechanism, deliberately push-shaped and per-origin:

* Every heartbeat carries `have` — for each origin, the highest **contiguous**
  sequence this instance holds. Contiguous rather than a set, because a hole
  blocks everything above it anyway (R-17), so the first hole is where the
  useful information stops.
* On receiving it, an instance resends **its own** envelopes the peer lacks. Only
  its own: those are the ones it can be certain it holds in full, so the repair
  does not depend on a third party's completeness.
* An envelope is therefore retained in the pending log until **every member has
  acknowledged it**, not merely until it commits locally. Dropping it at local
  commit is what left nothing to resend. Retention is bounded by the slowest
  peer, i.e. by the same watermark that bounds everything else.

> **R-48 (Repair must not reorder).** A resent envelope that sorts *before* the
> committed head cannot simply be appended: that would give the two replicas the
> same set in different orders, which §11.1 showed produces permanently different
> documents. It MUST be spliced in via the replicated `RECONCILE` marker of R-6,
> so both replicas make the identical choice.

R-48 has been **superseded in practice by R-54**. Holding a late envelope keeps
it out of the peer's log entirely, so instead of one message in two positions the
two instances hold two different documents — strictly worse, and permanent. Late
arrivals are therefore appended and reported (`FEDERATION_LATE_APPENDED`), and
their placement is the client's to reconcile. The requirement is kept because its
*analysis* is correct and matters: a reader of this document must understand why
appending is not free.

What is **not** built is the repair itself: splicing the late envelope into the
committed log and telling clients to reload. That is R-6's `RECONCILE`. Until it
lands, a channel that reaches this state needs operator attention; before this
guard, it silently produced two permanently different documents.

### 5.4 Control commits

Operations that are not pad messages but must be applied in a replicated, ordered way:

| Type | Payload | Notes |
| --- | --- | --- |
| `META` | one metadata command (`storage/metadata.js`) | §6 |
| `TRIM` | `{hash}` | R-11 |
| `DELETE` | `{reason}` | owner deleted the pad; every replica archives |
| `EXPIRE` | `{}` | `metadata.expire` reached |
| `RECONCILE` | `{peer, fromLamport}` | marks the position at which a late peer's backlog is spliced in (R-6) |

Control commits use the same envelope shape with `m` replaced by a `ctrl` object and
authorisation per §7.3. They occupy positions in the merged log like any other envelope.

---

## 6. Metadata

> **R-20 (Metadata replication).** Metadata commands must be replicated as `META`
> control commits and applied by every replica in merged-log order. The resulting
> metadata state must be a pure function of the merged log.

R-20 gives convergence, not intent preservation. `ADD_OWNERS(X)` concurrent with
`RM_OWNERS(X)` resolves to whichever sorts later — the same way on every replica. That is
a deliberate trade: a surprising-but-identical outcome beats a divergent one.

> **R-21 (Immutable core).** `channel`, `validateKey` and `created` must be identical
> across the replica set. On first sync, a receiver must reject a channel whose
> replicated `validateKey` differs from the one it already holds, or from the one
> asserted in `SUBSCRIBE`. This is the anchor of all message authorisation and must never
> be reconciled — a mismatch means the two sides are talking about different pads.

> **R-22 (Local-only metadata).** `selfdestruct` is scoped to a single instance
> (`storage/history-manager.js:156`, `storage/index.js:189-193`) and must not be
> federated. Federating a self-destructing pad must be refused.

For the first iteration (§9, L1) metadata may be **anchor-authoritative**: only the
anchor emits `META` commits, replicas apply them one-way. This removes all metadata
conflict handling from v1 while remaining forward-compatible with R-20.

> **R-42.** Metadata must eventually converge under *concurrent* modification, not merely
> anchor-authoritatively. Until then two instances that change metadata at the same time
> resolve independently and their owner lists can diverge permanently. Scheduled for M6:
> concurrent owner changes are rare, and the anchor-authoritative form above is a
> deliberate, spec-sanctioned intermediate rather than an oversight.

---

## 7. Trust, authorisation and abuse

### 7.1 Instance identity

> **R-23** Each instance must own a persistent ed25519 keypair, distinct from the support
> mailbox curve keys (`Env.curveKeys`), used only for federation. Its public part is the
> instance's `originId` and must be published at a well-known endpoint
> (`/api/federation`) along with the instance's canonical origin URL and protocol
> versions.

> **R-24** Federation sessions must be mutually authenticated (`HELLO`/`AUTH`
> challenge–response over the instance keys) and must run over TLS. Instance-key
> authentication protects against a compromised or substituted TLS endpoint; TLS protects
> metadata and traffic patterns.

### 7.2 Who may cause a pad to be replicated

The elegant answer is available because §1.1 already gives us a per-pad keypair:

> **R-25 (Replication capability).** A `SUBSCRIBE` must carry a capability token signed
> by the **pad's signing key** — the key whose public part is `validateKey` — over
> `(channelId, requesterOriginId, holderOriginId, nonce, expiry)`. Both instances verify
> it against `validateKey`. Holding this key is exactly equivalent to holding edit rights
> on the pad, so no new authority is created and no new key material is distributed.

Properties: the token is produced *by the client*, in the browser, from key material the
server never sees; it cannot be replayed against a different pad or a different pair of
instances; and a read-only viewer (who has no signing key) cannot unilaterally spread a
pad to another instance.

> **R-26 (Owner consent).** Where the channel has owners (`metadata.owners`), enabling
> federation must additionally require an authenticated RPC from an owner on the holding
> instance. R-25 proves *capability*; R-26 proves *intent* of the responsible account.

> **R-27 (Peer policy).** An instance must apply an operator-configured peering policy —
> at minimum an allowlist of peer origins — before honouring any `SUBSCRIBE`. R-25 and
> R-26 are necessary, not sufficient.

### 7.3 Authorisation of control commits

> **R-28** `META`, `TRIM` and `DELETE` control commits must be signed by the pad key
> (proving capability) *and* carry the originating instance's assertion, signed by the
> instance key, that a legitimate owner account requested it. A replica cannot verify the
> remote account itself — accounts are instance-scoped — so this is an explicit
> *trust-the-peer* boundary. It must be documented as such, and it is a reason R-27
> requires an allowlist rather than open peering.

### 7.4 Restricted pads

`metadata.restricted` gates access on a list of user public keys checked against
per-session authenticated keys held in core memory
(`storage/index.js:199-231`, `core/index.js:523-532`). A remote instance can replicate
the allow list but cannot verify that a local session belongs to the named user any more
weakly or strongly than it does today — the keys are global, the sessions are not.

> **R-29** Federating a channel with `metadata.restricted === true` must be refused in
> this iteration (N4), and enabling `RESTRICT_ACCESS` on an already-federated channel
> must be refused. Lifting this requires a separate design for cross-instance session
> authentication.

### 7.5 Resource abuse

> **R-30** Per-peer limits must exist and be enforced: maximum replicated channels,
> maximum bytes per channel, maximum envelopes per second, maximum total inbound bytes
> per second. Exceeding them must produce `ERROR` and suspend the offending channel, not
> drop the session.

> **R-31** A replicated channel consumes storage on every replica. Each instance must
> account federated channels against a dedicated, operator-configured budget, separate
> from user pin quota (`storage/pin-manager.js`), because on the receiving instance the
> pad may be pinned by nobody.

> **R-32** An instance must be able to unilaterally stop replicating a channel
> (`UNSUBSCRIBE`) at any time, for any reason, without the peer's consent.

### 7.6 Privacy

> **R-33** Federation must not transmit `netfluxSenderId`, client IP addresses, user
> agents, or local user identifiers. The envelope carries no user-attributable field
> beyond what the pad's own ciphertext already contains.

> **R-34** Which channels an instance replicates, and their sizes, are visible to its
> peers. This is inherent and must be documented for operators. It is why peering is
> allowlisted.

---

## 8. Lifecycle

| Event | Required behaviour |
| --- | --- |
| Enable federation | Client obtains the pad key, mints the R-25 capability, calls an authenticated RPC on both instances. Requester `SUBSCRIBE`s, receives `SUBSCRIBE_OK` with heads, runs `SYNC_REQ` to backfill, then joins live. |
| New message, local | Accept → validate → assign `(s, l, a)` → persist (R-9) → broadcast locally (R-7) → `PUBLISH` to peers. |
| New message, remote | Verify R-14/15/16/17 → persist → broadcast locally (R-7) → merge per R-3. |
| Reconnect after partition | `SYNC_REQ` with local heads; both directions; late envelopes per R-6 preceded by a `RECONCILE` control commit. |
| Owner deletes the pad | `DELETE` control commit; every replica archives (`archiveChannel`) and sends `EDELETED` to its members, as `CM.disconnectChannelMembers` does today (`storage/channel-manager.js:485-537`). |
| Expiry | The instance that observes `metadata.expire` first emits `EXPIRE`; all replicas archive. Replicas must not expire independently on local clocks. |
| Trim | `TRIM` control commit (R-11). |
| Peer removed from policy | Local `UNSUBSCRIBE` for all shared channels; local copy is retained (it is a valid pad) but stops receiving updates. |
| Instance shutdown | Uncommitted envelopes must survive restart (R-9) and be re-merged from persisted per-origin logs. |

> **R-13 (Presence).** Userlists are per-instance today
> (`storage/index.js:156-250`). Federation must not attempt to merge netflux ids across
> instances. It may expose an aggregate remote member *count* per channel. Nothing in the
> editing path may depend on it.

---

## 9. Conformance levels

The levels are cumulative and are intended as shipping milestones.

### L0 — Mirror (read-only replica)

One instance is the **anchor** and accepts all writes. The other maintains a read-only
copy: it serves history and live updates to its users but rejects local writes with a
redirect hint to the anchor.

Satisfies: R-1 trivially (one writer), R-7, R-8, R-9, R-14…R-19, R-23…R-27, R-30…R-34.
Does not satisfy: G2.
Value: proves transport, capability model, validation and backfill against real pads,
with zero ordering risk. It is a genuinely useful feature on its own (resilience,
locality, archival).

### L1 — Anchored write-through

Both instances accept connections; the non-anchor instance **forwards** local writes to
the anchor and only broadcasts them locally once the anchor has assigned their position.

Satisfies: G1, G3, G4 (degraded — non-anchor is read-only during a partition), G5, G6.
Ordering is exact by construction; `l` is assigned solely by the anchor, so R-1 holds
trivially and R-3/R-5/R-6 are unused.
Cost: a round trip to the anchor per remote-instance write.
Metadata is anchor-authoritative (§6).

### L2 — Multi-master

Both instances accept and order writes locally per §4. All requirements apply.

Satisfies: G1–G7.
This is the level at which the NextGraph mapping (§10) is meaningful, because it is the
level at which the data structure is genuinely a causal DAG rather than a linear log with
a single writer.

### L3 — n-way (out of scope here)

Replica sets larger than two. Nothing in §4 or §5 assumes two members — the watermark and
sort are defined over a set — but the peering, policy and failure surface grows enough
that it needs its own document.

---

## 10. NextGraph alignment

The point is **not** to be NextGraph-compatible now. It is to avoid decisions that would
make a NextGraph backend a rewrite. NextGraph's model, from its
[repo format](https://docs.nextgraph.org/en/specs/format-repo/) and
[client protocol](https://docs.nextgraph.org/en/specs/protocol-client/) specs:

* a **Repo** holds **Branches**; each branch has a **topic** and a DAG of **Commits**;
* `CommitHeaderV0` carries `acks` (causal past) and `deps`; the root commit is the only
  one without acks;
* objects are addressed by `ObjectRef` = `ObjectId` (BLAKE3 digest) + `ObjectKey`
  (ChaCha20 symmetric key) — an id and a decryption key travelling together;
* sync is `TopicSyncReq {topic, known_heads, target_heads, known_commits (Bloom)}` →
  a stream of events/commits **in causal order**;
* commits are Ed25519-signed, blocks ChaCha20-encrypted, everything BARE-encoded;
* addressing is a **Nuri**, `did:ng:o:<repoid>:b:<branchid>:r:<readcap>:l:<peers>`, where
  the `:l:` component is a broker/location hint.

### 10.1 Mapping

| CryptPad federated pad | NextGraph |
| --- | --- |
| Channel id (32 hex) | Repo id / topic |
| Symmetric key in the URL fragment | `ObjectKey` half of an `ObjectRef` / ReadCap |
| `validateKey` | Branch write-capability public part |
| Pad message (`sig ‖ ciphertext`) | Commit body (encrypted object) |
| `id = getHash(content)` | `ObjectId` — **different algorithm** (§10.2) |
| Envelope `a` (acks) | `CommitHeaderV0.acks` |
| Channel heads (§5.3) | Branch heads |
| `SYNC_REQ {knownHeads, targetHeads, knownIds}` | `TopicSyncReq {known_heads, target_heads, known_commits}` |
| `SYNC_RES` in causal order | `TopicSyncRes` stream |
| `PUBLISH` | `PublishEvent` |
| Checkpoint (`cp|`) | Snapshot commit |
| Metadata command log | Root-branch / permission commits |
| Instance key (`originId`) | Broker `PeerId` |
| Federation link (origin URL + channel) | Nuri `:l:<peers>` locator |

The correspondence is close enough to be useful and far from accidental: both systems are
"encrypted append-structured document, server sees ciphertext, sync by exchanging heads".

### 10.2 Where they genuinely differ

| Concern | CryptPad | NextGraph | Handling |
| --- | --- | --- | --- |
| Content hash | 48-byte ed25519 signature prefix | BLAKE3 digest | Both are opaque byte strings used only for equality and ordering. Confine to an `IdCodec` seam. |
| Encoding | JSON | BARE | Confine to a `Codec` seam. |
| Encryption | xsalsa20-poly1305, key in fragment | ChaCha20, convergent | Client-side; outside the server's scope entirely. |
| Signatures | Per-pad ed25519, attached | Per-commit ed25519 + threshold/quorum | Confine to a `Validator` seam. |
| Transport | TLS WebSocket + instance-key auth | Noise over WebSocket, brokers/overlays | Confine to a `Transport` seam. |
| CRDT | ChainPad | Automerge / Yjs / RDF | Not addressed. A pad stays a pad. |
| Ordering | Lamport total order, R-1 | Causal DAG, order-insensitive CRDTs | Ours is stricter. A NextGraph backend satisfies R-1 as a special case. |

### 10.3 Constraints adopted solely for G7

These are the decisions that cost us something now and buy NextGraph compatibility later.
They are listed so a future maintainer can price them:

* **C1** — Envelopes carry `a` (acks) even though `l` alone suffices for the sort. Cost:
  a few dozen bytes per message. Buys: a real causal DAG, and a 1:1 map to
  `CommitHeaderV0.acks`.
* **C2** — Anti-entropy is head-based with an optional Bloom filter of known ids, rather
  than the simpler "send me everything after sequence N per origin". Cost: more complex
  backfill. Buys: `TopicSyncReq` is a drop-in.
* **C3** — `SYNC_RES` guarantees causal order (R-19) rather than any order. Cost: the
  responder must walk the DAG. Buys: matches `TopicSyncRes` semantics; enables
  incremental validation.
* **C4** — Message ids, encodings, signatures and transport are reached only through
  named interfaces, never inlined. Cost: indirection. Buys: the NextGraph backend is an
  alternative implementation rather than a fork.
* **C5** — The federation link is a structured, resolvable locator (origin URL + channel
  id + `validateKey`), not an ad-hoc config entry, so it can later carry or become a Nuri.

> **R-35.** Any future change to §4 or §5 must state its effect on C1–C5.

---

## 11. Open questions

### 11.1 ChainPad under a same-order fork — **RESOLVED**

*Was:* R-1 guarantees every replica sees the same sequence, which we believed sufficient
for identical convergence; if ChainPad also needed same-*server*-timestamp ordering, `t`
would have to join the sort key.

**Resolved by experiment.** See
[`experiments/chainpad-ordering/`](experiments/chainpad-ordering/README.md), which drives
the real `chainpad@5.2.7` through forced concurrent edits. Two findings:

**(a) `t` must not enter the sort key; the contingency is structurally impossible.**
`chainpad-netflux.js:349` hands ChainPad only element `[4]` of the history-keeper line —
the ciphertext. The server timestamp `[5]` reaches only the `config.onMessage` metadata
hook, never `realtime.message()`. ChainPad has no path by which a server timestamp could
influence chain selection, so adding `t` could never fix a convergence defect.
**`(l, o, id)` stands unchanged.**

**(b) R-1 is necessary, not merely sufficient.** The experiment found the *converse* of
what this question feared: ChainPad is **not** order-independent. The same message set
replayed in different orders can yield different documents whenever the longest chain is
tied at depth ≥ 2. `getBestChild` (`chainpad.dist.js:1223-1231`) selects among
equal-length siblings with a bare `>` and no tiebreak, over an array that `storeMessage`
(`:985`) fills in **arrival order** — unlike `handleMessage` (`:1343-1348`), which does
break ties on `strcmp(hashOf)`. Siblings that `getBestChild` skips are never reconsidered.

Crucially this does **not** reliably self-heal. If only one replica authors a new patch
its branch wins and both converge; but if both replicas keep editing their own branch —
exactly what two federated instances with active users do — the tie is preserved and the
divergence is **persistent**.

> **R-36.** The deterministic `(l, o, id)` sort of the *uncommitted tail* (R-8) is a
> correctness requirement of the same rank as R-1, not a tidiness measure. A replica that
> serves the tail in arrival order rather than sorted order may serve a permanently
> different document to a joining client.

*Consequence for planning:* `federation-design.md` §12 budgeted for this question forcing
a sort-key change in M3. It will not. But its assumption that R-1 was a conservative
over-specification was wrong — R-1 is the minimum.

### 11.2 Uncommitted-tail size under sustained editing — **RESOLVED (bounded)**

ChainPad's own send rate bounds this. `DEFAULT_AVERAGE_SYNC_MILLISECONDS = 300`
(`chainpad.dist.js:807`), and a client holds at most one unacknowledged message at a time
(`realtime.pending`, `:900`), so each *actively typing* client emits at most ~3.3 msg/s.

    tail ≈ (active editors) × (T_hb + RTT) / 0.3 s

With `T_hb = 2 s` and a 200 ms intercontinental RTT, that is ~7 messages per active
editor — ~37 for five simultaneous editors. Sorting a tail of that size on every history
request is negligible, and the tail is bounded by editor count rather than by pad size.

> **R-37.** `T_hb` MUST be ≤ 2 s, and a watermark advance SHOULD be piggy-backed on
> `PUBLISH` so that a busy pad advances its watermark at message rate rather than at
> heartbeat rate.

### 11.3 `RECONCILE` and `TRIM` after long partitions — **RESOLVED**

Measured by [`exp6-trim.js`](experiments/chainpad-ordering/README.md). Two replicas were
partitioned onto different branches of the same fork, each grew its branch past a
checkpoint, and each then trimmed to its own last checkpoint — exactly what `trimChannel`
leaves for `GET_HISTORY` to serve.

| Scenario | Result |
| --- | --- |
| Exchange **full** histories | Converge, in either order: `"AAA a0 a1 a2 a3 a4 a5 "` |
| Exchange **trimmed** histories | **Do not converge.** Each replica keeps its own branch |

The partition was healable right up until the trim. The mechanism is worse than
"the wrong branch wins": a ChainPad checkpoint carries the *entire document* as its
content, and a message whose parent has been pruned can never be relinked
(`handleMessage` returns at the "not connected to root" branch, `chainpad.dist.js:1303`).
So the losing branch is not merely deprioritised — **its content is destroyed**, silently,
with no error anywhere. In the run above neither merged replica retained the other side's
edits at all.

> **R-38.** A `TRIM` control commit MUST NOT commit unless (i) its trim point is an
> ancestor of the committed prefix, and (ii) the *global* watermark — the minimum across
> every peer in the replica set, **including evicted ones** — is at or beyond that point.
> In practice: no `TRIM` while any peer is evicted.

> **R-41.** Because a premature `TRIM` destroys data rather than merely reordering it, an
> implementation MUST treat the R-38 precondition as a hard assertion checked immediately
> before the trim is applied, not as a scheduling policy that callers are trusted to
> respect. Failing the check MUST abort the trim and raise an operator-visible error.

*This closes the last open question in §11.* Everything in §11 is now either resolved by
experiment or explicitly out of scope (§11.5).

### 11.4 Quota and abuse economics — **RESOLVED (policy)**

> **R-39.** A federated replica is charged to the local account whose consent authorised
> it (R-26). Where an instance holds a replica with no local owning account — a pure
> mirror — it is charged to the operator's federation budget (R-31), and when that budget
> is exhausted the instance MUST evict replicas, least-recently-active first, by sending
> `UNSUBSCRIBE` (R-32). No instance is ever obliged to store a pad it does not want.

### 11.5 User attribution across instances — **out of scope, unchanged**

`[user@domain/pubkey]` (§1.7) already spans domains, but nothing verifies the domain
claim. Out of scope here; will matter for comments, forms and any future federated team.

### 11.6 Blobs — **RESOLVED, and then superseded by replicating them**

A `/pad/` may embed images stored as blobs; federating the pad without them gives a broken
document on the remote instance. The important correction: **the server cannot detect
this.** Blob references live inside the pad's encrypted content, which by §1.1 the server
can never read. Server-side detection is impossible in principle, not merely unimplemented.

> **R-40.** Blob detection and the resulting warning MUST be performed by the client that
> enables federation on a pad, at the point of enabling it, since only the client can read
> the content. The server MUST NOT be relied upon to detect embedded blobs.

**Superseded.** R-40 assumed the best a server could do was warn. It can do better: a
replica **fetches a blob on demand**, the first time somebody asks for one it does not
have. That inverts the problem — the reference is resolved by the only party that can read
it, the browser, and the resulting miss is what triggers the transfer. No enumeration, no
client cooperation, and blobs added long after federation was enabled are covered by the
same mechanism.

> **R-44 (Blob replication).** A replica MUST be able to serve a blob referenced by a pad
> it replicates, fetching it from a peer on first use and storing it locally thereafter.
> Blobs are immutable and content-addressed, so this requires no ordering, no watermark
> and no conflict resolution — it is a fetch, not a replication.

> **R-45 (Blob access model).** A peer may request any blob by id, and an instance serves
> it if it holds it. This is the bearer-capability model CryptPad already uses for blobs —
> the 48-character id is unguessable, and knowing it is what grants access. Combined with
> R-27 (only allowlisted instances hold a session at all) that is the trust boundary. It
> MUST be documented as such: a peer cannot enumerate blob ids, only redeem ones it
> already knows.

**The reference has to point at the reader's own instance.** The on-demand fetch
is triggered by a miss, and a miss only happens if the request arrives. CryptPad
stores a media-tag `src` as an *absolute* URL carrying the origin of whichever
instance the file was uploaded to, and that string is part of the document, so it
replicates verbatim. A federated pad opened on the other instance therefore asks
the *first* one for the image: a cross-origin request its CSP forbids, which
never reaches the instance the reader is on — so the blob is not merely
unrendered, it is never replicated at all. The mechanism cannot fire because
nothing asks it to.

> **R-51 (Blob references are instance-neutral).** A blob reference inside
> federated content MUST be resolved against the *reader's own* instance, not
> against whichever instance the blob was uploaded to. A blob id names bytes, not
> a server: it is content-addressed, so any instance may serve it, and a federated
> one fetches it from a peer if it does not hold it (R-44). Clients MUST therefore
> treat an origin embedded in a stored reference as advisory and redirect it,
> rather than requiring content to be rewritten — documents predating federation
> already contain absolute references.

N1's exclusion of blobs no longer applies at L0–L2.

---

## 11d. Surviving a restart

Which channels are federated, and with whom, is consulted on the write path —
once per message — so it is held in memory: a `Set` in core, and the
subscription maps in the federation node. Both are caches, and both are empty
after a restart.

Nothing else could put them back. A `SUBSCRIBE` carries a capability signed by
the pad's own key, deliberately short-lived and single-use, so a restarted
instance cannot ask its peers to remind it what it was replicating: only a
browser holding the pad can mint one, and there may be nobody with the pad open
for days. The per-channel state files are the only durable record, so they are
what a restart must rebuild from.

Left unrebuilt, the failure took the worst possible shape. Both instances came
back, both served the pad, both accepted edits — and they simply stopped
agreeing, with no error anywhere. An operator restarting a server had no reason
to suspect it.

The restart is only the visible case. The *peer that did not restart* is affected
too: it had registered its counterpart against a session object, and when that
session died it dropped the registration and then refused the reconnected peer's
messages as coming from a channel it was not subscribed to. Since the peer cannot
re-`SUBSCRIBE` without a capability, one instance restarting broke replication in
**both** directions. The general statement is that replica-set membership is a
property of the channel and must not be stored as a property of a connection —
which also covers an ordinary network blip, not just a restart.

**And live push is not enough.** A restart always leaves a gap: something is
written in the seconds a server is down, and by definition no live push can
deliver it. Only a sync on reconnect can. The first implementation re-synced the
channels it *mirrored from an anchor* — an L1 notion, and empty at L2, where
there is no anchor and every member is a peer. So a multi-master pad asked for
nothing when its peer came back: live messages resumed and looked healthy, while
everything written during the outage stayed on one side for good. Reconnection
must therefore sync every channel shared with that peer, however the sharing
arose.

> **R-52 (Replication survives a restart).** An instance MUST rebuild its
> replication routing from durable state at startup: for every channel with
> federation state, that it is federated, at what level, whether this instance
> anchors it, and which peers are members. Membership MUST be held per channel
> and per peer identity, never per connection, so that a session dropping and
> reconnecting neither loses it nor requires a fresh capability. It MUST NOT
> depend on a peer re-subscribing, because a capability cannot be minted
> without a client. On reconnection an instance MUST sync **every** channel it
> shares with that peer, not only those it mirrors from an anchor: a restart
> always leaves work that no live push can deliver. The
> rebuild MUST read every storage node's share of the state, since it is sharded
> by channel; a partial read MUST resume what it can and log the rest rather than
> failing to start. It MUST NOT federate anything that was not already federated.

---

## 11b. The test deployment

Federation cannot be judged from unit tests. Two instances must actually run, side
by side, serving the real client, so that a real pad — real ChainPad patches,
checkpoints and encryption — can be opened in a browser and edited from both.

> **R-43 (Verifiable install).** The two-instance deployment must be reproducible from
> a single command, and its health must be **checkable by a script that fails loudly**,
> not by reading logs. The check must cover, at minimum: no processes left from an
> earlier run; both HTTP origins **and both sandbox origins** answering; the client's
> components present; and a live peer session whose ping reaches the far storage tier.

Each clause is there because it failed in exactly that way:

* **Stale processes.** A stop that killed only the supervisor left its forked nodes
  holding the ports. The next start failed to bind and exited, while the old processes
  kept writing to the freshly truncated log — so the deployment looked healthy while
  serving a build three hours old. Instances are now started with `setsid` and stopped
  by process group.
* **The sandbox origin.** One instance was serving its main port but not its sandbox
  port. The client refuses to render documents without it, so the failure appears in
  the browser and nowhere in the server logs.
* **Client components.** `tweetnacl-util` was missing from the client checkout, so
  require.js died before anything loaded. The server was perfectly healthy and every
  server-side check passed.

The last two are the point of the requirement: **an instance can pass every check it
makes of itself and still be unusable.** The verification has to be done from outside,
against the URLs a browser actually fetches.

Implemented as `experiments/federation/verify.sh`.

---

## 11c. What belongs to a document

A pad is not only its channel, and it was not obvious which parts federate.

**Comments and annotations do.** They are held in `metadataMgr` metadata, which
`sframe-app-framework.js` embeds into the ChainPad content before
`chainpad.contentUpdate` — so they travel inside the pad's own channel like any
other edit. No separate mechanism is needed, and building one would have been
wasted work. Their apparent absence in testing was a *symptom of divergence*.

**Blobs do not.** An embedded image lives outside the channel and is referenced
from content no server can read, which is why it needs its own mechanism (R-44).

**The chat does not — because it is not the pad's channel at all.** This was the
sharpest correction to the model above. A pad's chat is a *separate channel*
with its own random id, minted by `Hash.createChannelId()` and stored in the
pad's own metadata as `chat2`. It is not derived from the pad key and no server
can discover it, because it lives in content they cannot read.

What makes it tractable is that it shares the pad's *cryptography*: the client's
`openPadChat` builds its encryptor from the pad's `secret.keys` and validates it
against the pad's `validateKey`. So one pad signKey mints capabilities for all of
them and one validateKey checks them all — the chat federates through exactly the
same path as the document, with nothing new on the wire and no new key material.
Only the enumeration is client-side.

Two channels are deliberately *not* federated: `cursor` and `integration` are
created with `Hash.createChannelId(true)`, i.e. **ephemeral** — they hold no
history, exist only while somebody is connected, and there is nothing to
replicate.

The awkward case is timing. `chat2` is created the first time somebody opens the
chat, which is usually long after the pad was federated, and no server can
notice. So the client federates it at the moment it mints one, having asked its
own server whether the pad is federated and to whom.

> **R-49 (Document completeness).** Anything stored *inside* a pad's channel
> federates with it and requires no extra mechanism. Anything stored *outside* it
> — today only blobs — MUST have one, and the spec must say which is which.
> A claim that some feature "does not federate" should first be checked against
> this distinction: it is more often a divergence symptom than a missing feature.

> **R-50 (Auxiliary channels).** Federating a pad MUST federate the auxiliary
> channels that belong to it and share its keys — today the pad chat (`chat2`).
> Because their ids live in content no server can read, the client MUST enumerate
> them and mint one capability per channel from the pad's signing key; the server
> MUST treat each as an ordinary channel. An auxiliary channel created *after*
> federation MUST be federated when it is created, for which an instance MUST
> expose whether a given channel is federated and to which members. Ephemeral
> channels (`cursor`, `integration`) MUST NOT be federated. Failure to federate an
> auxiliary channel MUST NOT fail the pad's own federation, and MUST be reported
> rather than swallowed.

---

## 11e. A message that was never federated

The failure that survived every other safeguard, found on a live pair whose
instances were serving visibly different documents while agreeing on everything
federation tracks.

Core decides whether to publish a write by looking the channel up in a memory
set. If the channel is missing from it — the window after a restart, before
anything has repopulated it — the message is committed to the local log and
never federated. Because a federation sequence is only allocated at *publish*
time, that message has **no sequence at all**. It leaves no gap; `have` matches
on both sides; the watermark is satisfied; the committed tips are identical.
Every repair mechanism in §4 and §5.3c works on sequences, so none of them can
see it. It is lost permanently, and both instances are correct to believe they
are in sync.

Measured on one such pad: 116 messages common and in identical order, plus 2 held
only by one instance and 7 only by the other.

Two conclusions follow. The window must not exist — which means the durable
record must reach core before any client can write, not merely eventually. And
agreement about federation state is *not* evidence of agreement about the
document: detecting this needs a number that does not come from the same
bookkeeping. The length of the committed log is that number.

> **R-53 (Divergence is detected, not assumed away).** An instance MUST announce
> its federated channels from durable state before it can accept writes on them,
> so that no window exists in which a write on a federated channel is committed
> without being federated. Members MUST additionally exchange a measure of the
> committed log itself — its length — and MUST report a mismatch: two instances
> agreeing on every federation counter while holding different documents is a
> reachable state, and one an operator cannot otherwise discover. Reporting is
> required; repair is R-6, because re-sending an orphaned message would give it a
> fresh clock and append it at the peer's tail while it sits mid-log here, which
> is the divergence rather than the cure.

**R-6, as built: append-only.** Each instance sends the peer the messages the
peer lacks, and the peer stores them. Both then hold everything, each keeping its
own order, and reconciling that is the client's business — it holds the keys, it
can read the content, and ChainPad already resolves a chain it receives out of
order.

    AUDIT_REQ   {c}            "which messages do you hold?"
    AUDIT_IDS   {c, ids}       the ids in my committed log, in order
    REPAIR_MSG  {c, messages}  the ones you were missing

> **R-54 (Repair never destroys).** A repair MUST NOT remove or relocate a
> committed message. It may only add ones an instance does not have. A stored
> patch is a user's work; a server cannot read it, cannot judge it, and MUST NOT
> delete it to make two logs agree.

That rule was learned the hard way. An earlier implementation excised its own
unfederated messages and re-federated them so the merge would give both
instances one identical order. It was wrong twice over. It deleted committed
history for tidiness. And it could not distinguish an orphan from a message
merely *in flight* — the only available evidence, "the peer does not have it", is
true of both — so on a busy pad it cut out and re-sent correct messages, the
divergence outlived the retry interval, and each round moved more. On a live
pair a gap of 8 messages grew to 117 in three minutes. Appending cannot fail that
way: the worst an in-flight message suffers is being sent twice and recognised
by id.

A related trap, in the detection rather than the repair: counting a committed log
means reading all of it, so it must never sit on the heartbeat path. Making the
beat wait for a count stopped the heartbeat on a slow read, and with it the
watermark that keeps an idle channel committing. The count is cached and
refreshed in the background; staleness is harmless, because a divergence
persists and acting on one is rate-limited far beyond the cache's life.

---

## 12. Requirement index

**Status** tracks what is implemented in this repository, against the milestones in
[`federation-design.md`](federation-design.md) §9.

| Mark | Meaning |
| --- | --- |
| **done** | Implemented and covered by tests |
| **partial** | Implemented in part; the entry names the milestone that closes the rest. A remainder too small to schedule is split into its own requirement instead, and the original marked done |
| **M4**…**M7** | Not started; scheduled for that milestone. M4 install, M5 blobs, M6 hardening, M7 NextGraph |
| **n/a** | Not a code requirement (out of scope, or a rule about the documents) |

Implemented so far: **M0**, **M1 (L0)**, **M2 (L1)** and most of **M3 (L2)**; **M4**, the
install for testing against real pads, is in progress. The merge now derives the order
rather than delegating it to an anchor, so the ordering requirements are implemented
rather than holding by construction.

Remaining milestones: **M4** the two-instance install and real-pad testing, **M5** blob
support, **M6** hardening (quotas, budgets, admin tooling), **M7** the NextGraph adapter.

| Id | Summary | Status |
| --- | --- | --- |
| R-1 | Identical committed id sequence on every replica; prefix-stable | **done** — The merge commits in `(l, o, id)` order from a watermark every replica computes identically; `federation-concurrent.test.js` asserts the two instances' id *sequences* match after concurrent editing |
| R-2 | Sort key is `(l, o, id)`, computable from the envelope alone | **done** — `common/federation/order.js`; pinned by `federation-order.test.js` |
| R-3 | Commit only up to the watermark `W`, in sort order | **done** — `Merge.partition` commits only `l <= W`, in sort order |
| R-4 | Heartbeats keep `W` advancing when idle | **done** — Heartbeats carry each member's *promise* clock, so an idle channel still commits |
| R-5 | Provisionally evict unreachable peers so `W` advances | **done** — A member silent for `EVICT_AFTER` stops holding the watermark back; a periodic sweep re-runs the merge so eviction alone can unblock it |
| R-6 | Reconcile: a divergence is repaired by making both instances hold everything | **done** — `AUDIT_REQ`/`AUDIT_IDS`/`REPAIR_MSG` compare committed logs and exchange what each lacks, append-only. `federation-reconcile.test.js` covers two-sided and one-sided splits, a repair racing live traffic from both instances, and pins that a healthy pad is untouched. Ordering across the repaired messages is left to the client (R-54). A *late envelope* sorting below the committed tip is still refused rather than spliced (R-48) |
| R-54 | A repair never removes or relocates a committed message (§11e) | **done** — the repair only ever appends; the line-deletion primitives it once used have been removed from the stores entirely |
| R-7 | Broadcast live without waiting for commitment | **done** — A remote edit is broadcast to local readers when it *arrives*, not when it commits; the live and committed tiers are deliberately different moments |
| R-8 | History = committed prefix + sorted uncommitted tail | **done** — `getHistoryAsync` serves the committed prefix and then the sorted uncommitted tail |
| R-9 | Persist before acknowledging a local client | **done** — A write is durable in the pending log before the client is acknowledged; the sequence is persisted before it is issued |
| R-10 | Checkpoint dedup against the merged log; keep concurrent checkpoints | M3 — **Not done.** Checkpoints replicate as ordinary messages; concurrent ones are not deduplicated |
| R-11 | Trim is a replicated control commit | M3 — **Not done.** No `TRIM` control commit exists. Its precondition (R-38/R-41) is implemented and unit-tested first, deliberately |
| R-12 | Converge on committed prefix and metadata | **done** — Both replicas converge on an identical committed sequence under concurrent editing (`federation-concurrent.test.js`). Metadata convergence under *concurrent* modification is split out as R-42 |
| R-13 | No cross-instance userlist merging; aggregate count only | M6 |
| R-14 | `id` must equal `getHash(m)` | **done** — `envelope.js` derives `id` rather than accepting one, so it holds by construction |
| R-15 | Revalidate `m` against `validateKey` locally | **done** — `FM.ingest` revalidates against `validateKey` via core before appending |
| R-16 | `sig` binds ordering metadata to the origin instance | **done** — `envelope.js` signs the ordering metadata; verified against the *origin* key, so relaying is safe |
| R-17 | `s` gapless per `(c, o)`; gaps block commitment | **done** — Gapless per `(c, o)`; and, corrected during M3, a member that has *announced* messages we have not received blocks the whole pass, not just its own origin |
| R-18 | Duplicate envelopes are no-ops | **done** — A duplicate is a no-op against both the committed log and the pending log, checked by id so an out-of-order arrival is not mistaken for one. Necessary for R-1: appending a duplicate here and not on the peer would make the two serve different sequences |
| R-19 | `SYNC_RES` streams in causal order | **done** — `SYNC_RES` streams in committed-log order, which for a single-writer log is causal order |
| R-20 | Metadata commands replicate as ordered control commits | **done** — Replicated as ordered `META` control commits and applied via `Meta.handleCommand`, so replica metadata is a pure function of the commands applied. Anchor-authoritative, which §6 explicitly sanctions; the concurrent case is R-42 |
| R-21 | `channel`/`validateKey`/`created` immutable and identical | **done** — `validateKey` is refused on re-init, cross-checked on `SUBSCRIBE`, and a new replica **seeds** `channel`/`validateKey`/`created` from the anchor rather than starting with none |
| R-22 | `selfdestruct` is never federated | **done** — `FM.enable` refuses a `selfdestruct` pad, and the flag is stripped when seeding a replica's metadata |
| R-23 | Persistent per-instance ed25519 federation keypair, published | **done** — `common/federation/identity.js`, published at `/api/federation` |
| R-24 | Mutual instance-key auth over TLS | **done** — `common/federation/handshake.js`; TLS is configured on the listener, and its absence is logged |
| R-25 | `SUBSCRIBE` carries a pad-key-signed replication capability | **done** — `common/federation/capability.js`, enforced in `SUBSCRIBE`. Minted in the browser by `experiments/federation/federate-pad.js` |
| R-26 | Owner account consent in addition to R-25 | M6 — **Not done.** The pad-key capability is currently the only authorisation; no owner *account* check is made |
| R-27 | Operator peering policy (allowlist) | **done** — `federation/policy.js`; enforced in `peer-session.js` after the key is proved |
| R-28 | Control commits carry pad-key + instance-key authorisation | **partial → M6** — The instance-key half is done: control commits carry the origin's signature, verified against the named origin. The pad-key half is **not** implemented; it can only be minted in the browser. Kept as one requirement rather than split, because the remainder is a real authorisation gap: until it lands, applying a peer's control commit is an explicit trust-the-peer decision, which is why R-27's allowlist is not optional. Closes with R-26 in M6 |
| R-29 | Restricted pads are not federated in this iteration | **done** — `FM.enable` refuses a restricted pad; its access list is account-scoped and a remote replica could not enforce it |
| R-30 | Per-peer rate and volume limits | **done** — Per-peer frame and byte token buckets in `peer-session.js`; an over-limit peer is throttled, not disconnected |
| R-31 | Federated storage has its own budget | M6 |
| R-32 | Unilateral `UNSUBSCRIBE` at any time | **done** — `UNSUBSCRIBE` is handled and a dropped session releases its subscriptions |
| R-33 | No user-attributable data on the wire | **done** — Nothing user-attributable is on the wire; envelopes carry only channel, content and ordering |
| R-34 | Replica set membership is visible to peers; document it | **done** — `SUBSCRIBE_OK` returns the replica set |
| R-35 | Changes to §4/§5 must state their effect on C1–C5 | **n/a** — A rule about changes to this document |
| R-36 | Deterministic `(l, o, id)` sort of the uncommitted tail is a correctness requirement (§11.1) | **done** — The sort is implemented and its order-independence is pinned over all 720 permutations of a six-entry tail |
| R-37 | `T_hb` ≤ 2 s; piggy-back watermark advances on `PUBLISH` (§11.2) | **done** — 2 s heartbeat carrying the promise clock |
| R-38 | `TRIM` only below the global watermark incl. evicted peers (§11.3) | M3 |
| R-39 | Replicas charged to the consenting account, else the operator budget; evict when exhausted (§11.4) | M6 |
| R-40 | Blob detection is client-side; the server cannot read content to detect it (§11.6) | **n/a** — superseded by R-44: blobs are now replicated on demand rather than merely warned about. A warning would still be a courtesy when a peer is unreachable, but is no longer required for correctness |
| R-41 | The R-38 precondition is a hard assertion at trim time — a premature `TRIM` destroys data (§11.3) | M3 — To be written *before* the trim path exists, so trimming is never reachable without the check |
| R-42 | Metadata converges under *concurrent* modification, not merely anchor-authoritatively (split from R-12/R-20) | M6 — **Not done.** Two instances changing metadata at once resolve independently, so their owner lists can diverge permanently. §6 sanctions anchor-authority for now and concurrent owner changes are rare, so this is scheduled rather than blocking |
| R-43 | The two-instance install is reproducible and verifiable by a script that fails loudly (§11b) | **done** — `experiments/federation/verify.sh`; 18 checks across both instances, covering stale processes, both origins, both sandbox origins, client components and the peer session |
| R-44 | A replica fetches a referenced blob from a peer on first use and keeps it (§11.6) | **done** — `storage/federation/blobs.js` + `federation/blob-transfer.js`; chunked transfer, verified with a 200 KB blob and after the peer disappears, and end to end from a **real upload** decrypted on the far instance (`federation-upload.test.js`). The fetched copy is bytes only: no pin, no owner, no quota accounting — M6 |
| R-45 | Blob access is the bearer-capability model; peers redeem ids, never enumerate them (§11.6) | **done** — documented in `blob-transfer.js` and enforced by R-27's allowlist |
| R-46 | Federation is set up server-to-server; the client never contacts the peer (§5.3b) | **done** — `INVITE` over the existing session; `GET /api/federation/peers` lets the client pick a peer without contacting one |
| R-47 | Missing messages are detected and repaired without operator action (§5.3c) | **done** — heartbeats carry per-origin `have`; a peer resends its own missing envelopes; envelopes are retained until every member has acknowledged them |
| R-48 | A repair must splice late arrivals via `RECONCILE`, never reorder (§5.3c) | **superseded by R-54** — late arrivals are appended and reported rather than held. Holding kept them out of the peer's log entirely, which is worse than a differing order and does not heal. The analysis behind the requirement stands and is retained |
| R-49 | In-channel content federates automatically; out-of-channel data needs its own mechanism (§11c) | **done** — comments/annotations ride in the ChainPad content; blobs are the only exception and are covered by R-44. Corrected by R-50: the pad *chat* is neither — it is a separate channel |
| R-53 | No window in which a federated write goes unfederated; log-length mismatch is detected and repaired (§11e) | **done** — Storage announces its federated channels to core at startup, closing the window; divergence is detected by exchanging the committed log length on the heartbeat, reported as `FEDERATION_DIVERGED`, and repaired by R-6 |
| R-52 | Replication is rebuilt from durable state at startup, and membership is per peer rather than per connection (§11d) | **done** — `FM.listFederated` enumerates the state files, `FED_LIST` gathers them across every storage node, and the federation node restores core's federated set and its replica-set membership before dialling out. Membership is also recorded on enable, subscribe and subscribe-ok, so a peer reconnecting is still a member; `federation-restart.test.js` |
| R-51 | A blob reference resolves against the reader's own instance, not the uploader's (§11.6) | **done** — `media-tag.js` redirects an absolute `/blob/<xx>/<id>` src to the instance the reader is on, configured by `sframe-common.js` from `fileHost`/`origin`. Without it the request never arrives and R-44 never fires |
| R-50 | A pad's auxiliary channels (the chat) federate with it; ephemeral ones do not (§11c) | **done** — the client enumerates `chat2` and mints a capability per channel from the pad key; `GET /api/federation/channel/:channel` reports membership so a chat opened after federation catches up; `federation-auxiliary.test.js` |
