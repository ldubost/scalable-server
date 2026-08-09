<!-- SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors

SPDX-License-Identifier: AGPL-3.0-or-later
-->
# Federation — implementation status

Where the work stands, as of the last change to this file. The authoritative
per-requirement detail is [`federation-spec.md`](federation-spec.md) §12; this
page is the summary and the list of what is knowingly missing.

| | |
| --- | --- |
| **Working today** | Two instances replicate a pad. Users on **both** can edit it concurrently and converge on an identical history, with no single instance acting as authority. Verified with a **real ChainPad document** — real encryption, real checkpoints — and **blobs are fetched from the peer on demand**, so a pad with an image works. |
| **Untested in reality** | Nobody has yet federated a pad **in a browser**. It is proved in tests with the real client libraries, but the console helper that mints the capability has not been run against a live browser session. |
| **Not working** | `TRIM`, `RECONCILE` (so repair after a *long* partition, R-48), and the operational layer (quotas, budgets, admin tooling). |
| **Not started** | The NextGraph adapter (M7). |

## Milestones

| Milestone | Level | Status |
| --- | --- | --- |
| **M0** — plumbing, identity, handshake, policy | — | **done** |
| **M1** — read-only mirror | L0 | **done** |
| **M2** — anchored write-through, metadata | L1 | **done** |
| **M3** — multi-master merge | L2 | **mostly done** — see below |
| **M4** — the install, real-pad testing, self-healing, auxiliary channels | — | **done** — R-43, R-47, R-50 and R-48's safety guard; R-48's repair moves to M6 |
| **M5** — blob support | — | **done** |
| **M6** — hardening: quotas, budgets, admin | — | barely started (only R-30) |
| **M7** — NextGraph adapter | — | not started |

### M3, precisely

**Done and tested:** Lamport clocks, the watermark and commit rule, the merge
loop, per-origin pending logs, heartbeat clock exchange, eviction of absent
members, and the `(l, o, id)` total order. Two instances accept concurrent writes
and converge on an identical id sequence; an idle channel still commits; a
surviving instance keeps committing when its peer disappears.

**Not done:**

* **`RECONCILE` (R-6)** — an evicted peer's return is *detected* and flagged, but
  its backlog is not spliced in with a replicated marker. **M4**, since an evicted
  peer returning is a normal event in real use.
* **Checkpoint dedup on federated pads (R-10)** — checkpoints replicate as
  ordinary messages. Concurrent checkpoints are not deduplicated. **M4**, because
  a real pad checkpoints every 50 patches and this is the likeliest first failure.
* **`TRIM` (R-11)** — the R-38/R-41 precondition is implemented and unit-tested,
  but no `TRIM` control commit exists to invoke it. **This is deliberate**: §11.3
  measured a premature trim silently *destroying* data, so the assertion was
  written before the path that could trigger it. **M6** — nothing needs trimming
  before a federated pad has been running for a long time.

### M4 — the install, and real-pad testing (current focus)

Reordered ahead of hardening on purpose. Everything up to M3 is verified by tests
that write *synthetic* messages. Nothing has yet shown that a **real pad** — real
ChainPad patches, checkpoints, encryption — federates correctly when a person
opens it in a browser, and that is the only evidence worth hardening for.

**Done:** R-7 (a remote edit reaches local readers on arrival, not at commit),
R-8 (history serves the committed prefix plus the sorted tail),
`federate-pad.js` (a browser-console helper that mints the capability and calls
both endpoints), and **R-43** — the install is now verifiable by
`experiments/federation/verify.sh` rather than by reading logs.

Three things were broken while the deployment *looked* healthy, and each is now a
check in `verify.sh`:

* **Stale processes.** Stopping killed only the supervisor, so its forked nodes
  kept the ports. The next start failed to bind and exited while the old
  processes carried on writing to the truncated log — the deployment served a
  three-hour-old build and looked fine. Instances now start with `setsid` and
  stop by process group.
* **A missing sandbox origin.** One instance served its main port but not its
  sandbox port. The client cannot render documents without it, so the failure
  shows up only in the browser.
* **Missing client components.** `tweetnacl-util` was absent from the client
  checkout, so require.js died before anything loaded. Every server-side check
  passed.

The last two are why R-43 exists: **an instance can pass every check it makes of
itself and still be unusable.** Verification has to come from outside, against
the URLs a browser actually fetches.

**A real document works.** `federation-realpad.test.js` drives actual `chainpad`
and `chainpad-crypto` from the client checkout, encrypting and signing exactly as
the browser does, and asserts the two instances render **identical document
text** — not merely the same message ids, which §11.1 showed can differ while the
documents do not agree. Three scenarios pass: replication to a fresh replica,
concurrent editing from both instances, and a document driven past several
checkpoint boundaries on both sides at once.

That last one settles R-10: **checkpoints do not need deduplicating for
correctness.** A duplicate checkpoint is a message ChainPad already knows, and it
ignores it. Deduplicating remains worth doing to stop a busy federated pad
growing faster than it needs to, but it is an efficiency matter, not the
correctness risk it was billed as.

**The chat federates too (R-50).** A pad turned out not to be one channel: its
chat is a separate channel whose id lives in the pad's own metadata, invisible to
every server. It shares the pad's keys, so it federates through the same path —
the client enumerates it and mints one capability per channel. See below.

**Outstanding:**

* **`RECONCILE` (R-6)** — a repair that has to insert a message *before* the
  committed head still cannot be *applied*. It is now **refused rather than
  misapplied** (R-48's guard), so the failure is a visible stall instead of a
  silent divergence, and nothing is lost. Completing it means rewriting a
  channel's committed history, which is why it is scheduled for M6 rather than
  done here.

### M5 — blob support — **done**

A pad embedding an image now works on both instances. R-40 assumed the best a
server could manage was a warning; it can do better. A replica **fetches a blob
on demand**, the first time somebody asks for one it does not have, which
inverts the problem: the reference is resolved by the only party that can read
it — the browser — and the resulting miss triggers the transfer. No enumeration,
no client cooperation, and blobs added long after federation was enabled are
covered by the same mechanism.

Blobs are immutable and content-addressed, so there is nothing to order or merge:
this is a fetch, not a replication. Transfers are chunked at 64 KB to fit the
frame limit, capped at 20 MB, and stored locally on arrival — verified with a
200 KB blob and after the peer that held it disappeared.

**R-45** states the access model plainly: a peer may request any blob by id and
gets it if we hold it. That is the bearer-capability model CryptPad already uses
— the 48-character id is unguessable, and knowing it is what grants access —
bounded by R-27, since only allowlisted instances have a session at all.

**Proved from a real upload, not just from bytes on disk.**
`federation-blobs.test.js` places blobs directly in a store, which tests the
transfer and skips everything an upload actually does — the RPC session, the
quota check, the pending-upload slot, chunked encryption, `UPLOAD_COMPLETE`.
That gap had already hidden the `sendCommand` bug above. So
`federation-upload.test.js` drives the **client's own upload code**
(`tests/common/upload.js`, what the browser runs) against instance A over both
transports, then reads the blob from instance B and **decrypts it with the key
that never left the client**. Byte-identical plaintext on B, including a 400 KB
file whose 128 KB encryption chunks do not line up with the 64 KB transfer
slices.

Worth recording: `/blob` is served by the **storage** node, not the front, and
the federated fallback lives in that middleware — a test reading through the
front would exercise neither.

**The reference has to point at the reader's own instance (R-51).** Found in a
browser, and it invalidated the claim above that blobs "work": the pad on the
second instance was requesting the image from the *first* one. CryptPad stores a
media-tag `src` as an absolute URL carrying the origin of whichever instance the
file was uploaded to, and that string is part of the document, so it replicates
verbatim. The reader's browser then makes a cross-origin request the CSP forbids.

The rendering failure is the lesser half. That request is what makes an instance
notice it does not hold a blob, so sending it to the wrong server means **the
blob is never replicated at all** — the on-demand fetch cannot fire because
nothing asks it to. Every server-side test passed throughout, because they issue
the request to the right instance by construction.

Fixed in the client, at the one point every media-tag download passes through:
an absolute `/blob/<xx>/<id>` src is redirected to the instance the reader is on.
Done at render time rather than at upload time so documents that already contain
an absolute reference are fixed too, with no content migration. The target is
configured explicitly (`blobOrigin`, from `fileHost` or the instance origin) and
never guessed — media-tags render inside the sandboxed frame, whose own origin is
not where blobs live.

**Not done, and the sharper statement of it:** a fetched blob is written as bytes
only. It gets no `.metadata.ndjson`, no owner and **no pin**, so B's quota
accounting cannot see it and CryptPad's blob eviction would treat it as
unreferenced. In practice a GC'd copy is re-fetched on the next read, so the
failure mode is churn rather than loss — but only while the peer still holds it.
Once the pad is un-federated, or the peer goes away, B's copy is unowned with no
way back. Pinning and quota accounting for federated blobs are **M6**.

### M6 — hardening

Only **R-30** (per-peer frame and byte rate limits) is implemented. Not done:
storage budgets and eviction (R-31, R-39), owner-account consent (R-26),
cross-instance userlists (R-13), Bloom-filter sync, the admin/CLI surface,
metrics, and `fed/` storage migration.

## Requirement bookkeeping

Every requirement is **done**, **n/a**, or scheduled against a named milestone.
Where a requirement was mostly built and the remainder was too small to schedule
on its own, the remainder is split into a new requirement and the original marked
done — so "partial" never becomes a place things go to be forgotten.

Current: **38 done**, **3 partial**, 10 scheduled, 2 n/a, of 53.

The partials are **R-48** (the guard against reordering is built; the splice that
repairs it is not — see above) and **R-28** (pad-key *and* instance-key authorisation on
control commits). It is kept whole rather than split because the missing half is a
real authorisation gap, not a detail: until it lands, applying a peer's control
commit is an explicit trust-the-peer decision. It closes with R-26 in M6.

Split out during this pass:

* **R-42** — metadata converging under *concurrent* modification. R-12 and R-20
  were both "partial" only because of this; both are now done, since what they
  ask for at the current conformance level is built and §6 explicitly sanctions
  anchor-authoritative metadata. Scheduled M6: two instances changing metadata at
  once can end up with permanently different owner lists, but concurrent owner
  changes are rare.

Closed outright during this pass:

* **R-18** — duplicate envelopes are now no-ops against the pending log as well
  as the committed log. This was worth fixing rather than scheduling: the live
  push and a backfill overlap by design, and appending a duplicate on one replica
  and not the other would make them serve different sequences, breaking R-1.

## Self-healing (R-47) — implemented

Heartbeats now carry, per origin, the highest contiguous sequence an instance
holds. A peer that sees another is behind resends **its own** envelopes the other
lacks — its own, because those are the ones it can be sure it holds in full, so
repair does not depend on a third party. Envelopes are retained in the pending log
until **every member has acknowledged them**, rather than being dropped at local
commit, which is what previously left nothing to resend.

**One caveat, and it matters (R-48):** a resent envelope that sorts *before* the
committed head cannot simply be appended — that produces the same set in different
orders, which §11.1 showed is exactly what diverges documents permanently.

That case is now **detected and refused**, which is half of R-48 and the half that
prevents damage. The merge tracks the committed tip as a full `(l, o, id)` — not
just the clock, since envelopes routinely share one and the origin tie-break is
part of the order — and holds anything sorting below it, logging
`FEDERATION_RECONCILE_REQUIRED`. The channel stalls for that origin where an
operator can see it, and every envelope stays in the pending log.

The **repair** itself is still missing: splicing the late envelope into the
committed log and telling clients to reload is R-6's `RECONCILE`, which would mean
rewriting a channel's committed history — the one part of the storage layer this
work has deliberately never touched. Scheduled **M6**. So repair works today for a
peer that has fallen behind; a long partition now stalls instead of diverging.

## Comments and annotations — no mechanism needed

Worth recording because the obvious conclusion was wrong. Comments are held in
`metadataMgr` metadata, and `sframe-app-framework.js` embeds that into the
ChainPad content before `contentUpdate` — so they ride inside the pad's own
channel and federate like any other edit. Reports that they "do not federate"
were a symptom of the divergence above. Building a separate comments channel
would have been effort spent against a misdiagnosis (R-49).

## The chat, however, is a different channel (R-50) — implemented

The correction to the paragraph above. A pad's **chat is not in the pad's
channel**: it is a separate channel with its own random id, minted by
`Hash.createChannelId()` and kept in the pad's own metadata as `chat2`. So the
rule in R-49 — "inside the channel federates, outside needs a mechanism" — put
the chat on the wrong side of the line, because it is neither the document's
content nor an out-of-band object like a blob. It is a *second document*.

What makes it cheap to fix is that it shares the pad's cryptography. The
client's `openPadChat` builds its encryptor from the pad's `secret.keys` and
validates against the pad's `validateKey`, so the same signing key mints a
capability for it and the same validate key checks it. Nothing new on the wire,
no new key material, no server change beyond treating it as an ordinary channel.
`federation-auxiliary.test.js` pins that a second channel sharing a pad's keys
replicates and converges in both directions exactly as the pad does.

Only the *enumeration* is client-side, and it has to be: the id lives in content
no server can read. Two consequences:

* `cursor` and `integration` are deliberately excluded — `createChannelId(true)`
  makes them **ephemeral**, with no history to replicate.
* `chat2` is created the first time somebody opens the chat, usually long after
  the pad was federated, and no server can notice. So the client federates it at
  the moment it mints one, having asked `GET /api/federation/channel/:channel`
  whether the pad is federated and to whom. Without that endpoint a chat opened
  after federation would never replicate, and would look like federation being
  broken rather than like a channel nobody federated.

Failing to federate the chat never fails the pad: a pad that federates without
its chat is worth having. The failure is reported in the Federate dialog rather
than swallowed.

## Federation did not survive a restart (R-52) — fixed

The most consequential bug found so far, and the one that best illustrates why
"the tests pass" was never sufficient evidence.

Everything the write path consults about federation is a memory cache: core's
set of federated channels, and the federation node's map of who replicates what.
Both are read per message, so caching them is right. Both are empty after a
restart, and nothing put them back.

Nothing *could*, either. A `SUBSCRIBE` carries a capability signed by the pad
key — short-lived and single-use by design — so a restarted instance cannot ask
its peers to remind it what it was replicating. Only a browser holding the pad
can mint one, and there may be nobody with that pad open for days. The state
files were the only durable record and nothing read them at startup.

The failure took the worst available shape: both instances came back, both
served the pad, both accepted edits, and they silently stopped agreeing. No
error, no log line, nothing for an operator to notice.

Fixed by rebuilding the routing from the state files before dialling out —
across *every* storage node, since federation state is sharded by channel and a
single node holds one slice of it. Restoring from one slice would have resumed
some pads and quietly dropped the rest, which is the same bug wearing a
disguise.

**And the restart was only the visible half.** With the rebuild in place, the
anchor restarting worked and both restarting together worked — but the *replica*
restarting alone still failed, which is the case that proved the model wrong. The
instance that did *not* restart had registered its counterpart against a session
object; when that session died it dropped the registration, and then refused the
reconnected peer's messages as coming from a channel it was not subscribed to.
Since the peer cannot re-subscribe without a capability, one instance restarting
broke replication in **both** directions.

So membership is not per-connection. It is a property of the channel and a peer
identity, recorded wherever it is learned — enable, subscribe, subscribe-ok and
the startup rebuild — and never dropped when a session goes. That also covers an
ordinary network blip, which had the same defect and would have looked like an
unrelated intermittent fault.

**Then it still did not work on the real rig**, twice over, and both misses are
worth recording because the tests were green throughout.

*First*, the rebuild ran before storage had connected to core — seconds into a
cold start — and `interface.sendQuery` answers with a bare string `'EINVALDEST'`
when a destination is not connected, but with an object otherwise. Checking
`.error` read the string as a **successful empty answer**, so federation restored
nothing and logged `{channels: 0}` cheerfully next to 26 state files on disk. The
integration rig starts its nodes in order, so the race does not exist there. The
restore now retries with backoff instead of depending on winning it, and reports
`incomplete` so "nothing is federated" is distinguishable from "I could not find
out". That string-vs-object trap is available to every caller of `sendQuery`.

*Second*, and worse: a restart always leaves a gap — something is written in the
seconds a server is down — and no live push can deliver it. Only the sync on
reconnect can, and it was asking for the wrong set. It re-synced the channels in
`following()`, the map of channels mirrored *from an anchor*: an L1 notion, empty
at L2 where there is no anchor and every member is a peer. So a multi-master pad
asked for nothing when its peer returned. Live messages resumed, everything
looked healthy, and the outage's work stayed on one side permanently.

All three original tests passed while that was live, because none of them wrote
anything while the peer was away. That is the lesson worth keeping: the tests
described the mechanism, not the situation.

`federation-restart.test.js` now covers the anchor restarting, the replica
restarting, both restarting together, **work done while a peer was away**, and —
the guard against this fix going too far — that a restart never federates a
channel that was not federated already. Verified on the live two-instance rig as
well, with a real ChainPad document edited from both sides across a restart.
It needed a new rig capability: stopping an instance and starting it again on
the same ports and directory. That reaches a whole class of bug the suite could
not previously see, namely anything the processes were holding in memory and
never wrote down.

## Pads that were split while both servers agreed (R-53)

The most instructive failure so far. Two instances were serving visibly
different documents while agreeing on every number federation tracks: identical
`have` maps, identical committed tips, matching sequence counts. On the pad that
was reported, 116 messages were common **and in identical order** — so the
ordering machinery, the watermark, the merge and the anti-entropy were all
working — plus 2 held only by one instance and 7 only by the other.

Those extras never received a federation envelope. Core publishes a write only if
the channel is in its in-memory federated set, and that set was empty in the
window after a restart, so anything typed then was committed locally and dropped
from federation. Because a sequence is allocated only at *publish* time, such a
message has no sequence: no gap to find, nothing to resend, and every repair
mechanism blind to it. Both servers were correct to believe they were in sync.

**Prevention.** Storage now announces its federated channels to core as it
starts. It holds the durable record and is up before the front accepts a client,
so this closes the window at the earliest point available. The federation node's
own restore still runs and is idempotent; two sources of truth for the same fact
cannot conflict here, and the belt-and-braces is worth it for a failure this
quiet.

**Detection.** Agreement about federation state is not evidence of agreement
about the document, so detection needs a number that does not come from the same
bookkeeping: the length of the committed log, exchanged on the heartbeat. A
mismatch is reported as `FEDERATION_DIVERGED`, rate-limited to once per channel
per five minutes because a split pad stays split. Pointed at a live pair it found
**8 split channels out of 30** immediately.

**Repair is deliberately not attempted.** Re-sending an orphaned message gives it
a fresh Lamport clock, so the peer appends it at its tail while this instance
holds it mid-log — the same messages in different orders, which is the divergence
rather than the cure. Converging properly means both sides rewriting their
committed logs into the agreed order and telling clients to reload, which is R-6
`RECONCILE` and rewrites live channel history. Scheduled, not improvised.

## Two bugs found by real use, and what they mean

**A pad diverged.** Comparing the two committed logs: 109 messages in common,
6 only on one instance, 4 only on the other — genuinely *lost*, not reordered,
with both sides believing they were in sync.

The cause: when a pad is promoted to L2 it already has history, authored by the
instance holding it, so that instance's sequence allocator must continue after
it. That was applied to **every** L2 instance, including the joining replica —
which then claimed authorship of messages it had merely backfilled. From that
point the two disagreed about what any `(origin, seq)` referred to, the gap
detection that should block committing past a missing message was comparing
meaningless numbers, and each side silently skipped the other's messages. Fixed:
only the origin adopts the existing log; a replica starts at `seq: -1`. Pinned by
two tests in `federation-fedlog.test.js`.

**The deeper problem it exposed is not the bug but the absence of a repair.**
At L2 there is no anti-entropy: `SYNC_REQ`/`SYNC_RES` run only on subscribe and
reconnect, and serve from the committed log. Nothing periodically notices "this
peer says it is at seq 57 and I only hold 50 of theirs" and asks for the rest.
So *any* dropped `PUBLISH` is permanent, whatever caused it. **This is the most
important thing outstanding** — it is the difference between "a bug caused
divergence" and "divergence is possible and unrecoverable". Spec §4.7 describes
it; it is not implemented for L2.

**Blob uploads over HTTP crashed the storage worker.** `Stores.create` forwards
`sendCommand` to the blob store on the object-storage path but not on the
filesystem one. In the storage *cluster*, an HTTP upload has no RPC session of
its own and must ask the primary for the caller's quota, so `blob.js:upload`
reached `Env.sendCommand(...)` on an Env that had none. The worker died with a
TypeError, which surfaced as `ECONNRESET` at the proxy and, in the browser, an
upload that stalled with no error anywhere useful. WebSocket uploads were
unaffected, since they carry their own session — which is why this went unnoticed.

Pre-existing, unrelated to federation, and a one-line omission from the
object-storage refactor.

**Retention for repair broke the gap detector.** Found by the M3 convergence
tests immediately after R-47 landed, and worth recording because the mechanism is
counter-intuitive. R-47 keeps an instance's own envelopes in the pending log
after they commit, so there is something to resend. But `partition` reads the
pending log to find *gaps* in each origin's sequence — and a retained envelope
sits below its origin's `committedSeq`, so the very first entry it examined broke
the contiguous run and marked that origin blocked from its own lowest sequence
upwards. Our own new messages then never committed at all.

That did not show up as a stall but as a **reordering**: with one side's commits
blocked and the other's not, the two instances batched the same messages
differently, and each batch is sorted only within itself. Both ended up with the
same eleven messages in different orders — precisely the failure §11.1 showed
ChainPad does not recover from. The fix is to filter already-committed envelopes
*before* grouping by origin rather than at the commit step, and to source the
retention set from the whole pending file rather than from what this pass
classified, so a retained envelope is not retired the moment it commits.

The lesson generalises: the pending log now serves two purposes — what to commit
next and what to resend — and any code reading it must say which it means.

**Federating without a capability became impossible.** R-46 made `enable` send an
`INVITE` carrying the client's capability, and treat "no peer session" as a
failure. But `enable` is also called without a capability, by operators and by
the tests, where the peer presents its own via `/replicate` instead. Those calls
started sending capability-less invites that the peer correctly refused with
`E_CAP_MALFORMED`, and then failing with `ENOSESSION`. `enable` now invites only
when it has something to carry, and only that flow depends on a live session.

## Known gaps that are not milestone-shaped

* **R-28's pad-key signature.** Control commits carry the *origin instance's*
  signature but not a pad-key one, because only a browser can mint that. Until it
  exists, applying a peer's control commit is an explicit trust-the-peer
  decision — which is exactly why R-27's allowlist is not optional.
* **Client-side work.** Capability minting (R-25) now happens in the browser via
  `federate-pad.js`, but as a console command rather than anything a user would
  find. There is no UI.
* **`selfdestruct` and `restricted` pads are refused** federation outright
  (R-22, R-29). That is correct, not a gap, but it is a real limitation.

## What has *not* been touched

`storage/channel-manager.js`. Design §6 called an edit there the highest-risk
change in the project, on the grounds that every message on the instance passes
through it. M0–M4 have all avoided needing it: the federation hooks live in core's
`onChannelMessage`, costing one `Set` lookup per write on a channel this
instance does not federate.

## Tests

| Suite | Command | Covers |
| --- | --- | --- |
| unit | `npm run test:unit` | 301 tests (2 skipped: they need object storage). Handshake, capabilities, envelopes, the total order, the federation log, the merge rule |
| integration | `npm run test:integration` | 38 tests across 10 files, each booting complete instances as real processes. Includes a **real ChainPad document** suite, a blob suite and an auxiliary-channel suite |
| experiments | `docs/experiments/chainpad-ordering/` | The ChainPad ordering findings that shaped §4 and §11 |

`federation-upload.test.js` boots three complete rigs and needs longer than a
900 s cap allows; run its tests individually or raise the timeout.

**The integration suite is unreliable when every file runs in one invocation.**
Each test boots 4–8 processes, and back-to-back suites contend on ports and
timeouts; files pass reliably when run individually. Fixing that — a port
allocator and teardown barrier in `tests/integration/rig.js` — is outstanding.

Two integration tests are deliberately slow: eviction takes 60s
(`Merge.EVICT_AFTER`), so anything testing an absent peer waits at least that.

## Seeing a real pad federated

`/home/ludovic/dev/cryptpad/experiments/federation` runs two instances on
`localhost:3000` and `localhost:4000`, already peered, on **filesystem storage**.

```
./start.sh                       # both instances
```

Then, in a browser:

1. open a pad on `http://localhost:3000`, give it some content;
2. paste `federate-pad.js` into the console and run `await federatePad()`;
3. open the URL it prints on `http://localhost:4000` — same pad hash, different
   host — and edit from both sides.

Step 2 has to happen in the browser because the capability is signed with the
**pad's** signing key, which lives in the URL fragment and never reaches a
server. That is the property that stops a server federating its users' pads
without them, and it is why no server-side script can do it.

## The finding that shaped all of this

`docs/experiments/chainpad-ordering/` established, by experiment against the real
ChainPad, that **ChainPad is not order-independent**: two replicas holding the
same messages in different orders can converge on permanently different
documents, and it does **not** self-heal once both sides keep editing. A related
run showed a premature trim *destroying* the losing branch's content silently.

That is why the merge commits in a deterministic order rather than merely
ensuring both sides have the same set, why the uncommitted tail must be sorted,
and why the trim precondition is a hard assertion rather than a policy.
