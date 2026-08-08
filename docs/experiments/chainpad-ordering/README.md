<!-- SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors

SPDX-License-Identifier: AGPL-3.0-or-later
-->
# Experiment: does ChainPad need more than same-order agreement?

This answers **open question §11.1** of [`../../federation-spec.md`](../../federation-spec.md),
which the design document ([`../../federation-design.md`](../../federation-design.md) §12)
flags as the one question that must be settled *before* M3, because the merge sort key
`(l, o, id)` depends on the answer.

> **R-1** guarantees every replica sees the same sequence, which we believe is sufficient
> for identical convergence. This must be *verified experimentally* against the real
> ChainPad implementation before L2 ships […] If it turns out ChainPad also needs
> same-*server*-timestamp ordering, `t` would have to become part of the sort key, which
> is a spec change.

## Running

The experiments drive the real ChainPad (`chainpad@5.2.7`), which is not a dependency of
this repository. Point `CHAINPAD_PATH` at an installed copy — for example the one in a
CryptPad checkout:

```sh
export CHAINPAD_PATH=/path/to/cryptpad/node_modules/chainpad
node exp1-order.js        # random orderings of a full message set
node exp2-exhaustive.js   # every permutation of a small set with real forks
node exp3-ties.js         # prefixes, dangling ties, width-4 forks, large sets
node exp4-mechanism.js    # isolates the mechanism; tests self-healing
node exp5-checkpoints.js  # can the divergence be made durable?
node exp6-trim.js         # §11.3: does a TRIM freeze it? (yes, and it loses data)
```

`harness.js` drives real `ChainPad` instances through *forced concurrent edits*: every
client edits and emits before any of them receives the others, so each round produces a
genuine fork of width *N* — not a simulation of one.

## Result

**Answers §11.1: `t` must NOT enter the sort key, and R-1 is not merely sufficient — it
is necessary.** The `(l, o, id)` sort key stands unchanged. No spec change is required,
but the *reason* R-1 exists is stronger than the spec assumed.

### 1. The server timestamp cannot possibly matter

This is settled by inspection, not measurement. In `chainpad-netflux.js`, the history
keeper's line is destructured and only element `[4]` — the ciphertext content — is
decrypted and handed to ChainPad:

```js
msg = parsed1[4];                       // chainpad-netflux.js:349
...
if (realtime && isString) { realtime.message(message); }   // :395
```

The server timestamp `parsed1[5]` is passed **only** to the `config.onMessage` metadata
hook as `obj.time` (`:398-400`), never to `realtime.message()`. ChainPad has no channel
through which a server timestamp could reach its chain-selection logic. Adding `t` to the
federation sort key could therefore never fix a convergence problem — the hypothetical
in §11.1 is structurally impossible.

### 2. ChainPad is *not* order-independent — so R-1 is load-bearing

The interesting result is the converse of what §11.1 worried about. Feeding the **same
set** of messages to fresh ChainPad instances in **different orders** can produce
**different documents**.

* `exp1`: 17 messages, 3 clients, 5 forked rounds, 302 random orderings → **1** document.
* `exp2`: 5 messages with 2 real fork points, all 120 permutations → **1** document.
* `exp3`: replaying every *prefix* of a 14-message log → prefixes of length
  1,2,3,4,7,10,13,14 give one document; prefixes of length **5, 6, 8, 9, 11, 12 give two
  or three**.

The passing lengths are exactly the boundaries where a concurrent batch is *complete*.
A prefix that cuts a batch in half leaves two branches tied for longest, and the winner
depends on arrival order.

### 3. The mechanism

`exp4` isolates it. Two code paths in `chainpad.dist.js` break ties differently:

| Path | Line | Tiebreak among equal-length branches |
| --- | --- | --- |
| `handleMessage` | 1343-1348 | `Common.strcmp(best.hashOf, msg.hashOf) > 0` — deterministic, content-derived |
| `getBestChild` | 1223-1231 | `parentCount(child) > parentCount(best)` — **strict `>`, no tiebreak** |

`storeMessage` (`:985`) appends to `messagesByParent[parent]` in **arrival order**, and
`getBestChild` scans that array taking the first strict improvement. So when several
equal-length sibling branches are already buffered and the message that links them to the
root arrives last, `getBestChild` returns whichever sibling was *stored first* — and the
hash tiebreak in `handleMessage` is then only applied against that one winner. The
siblings it skipped are never reconsidered.

This is why `exp4` finds ties at depth 1 are safe (each child is connected to the root on
arrival, so the `handleMessage` hash tiebreak decides) while ties at depth ≥ 2 are not.

### 4. It does not reliably self-heal

`exp4` Q-d: if **one** replica authors a new patch, its branch becomes strictly longer and
both replicas converge. That is the optimistic case.

`exp5`: if **both** replicas keep editing their own branch — which is precisely what
happens when two federated instances each have active users — both branches grow by one,
the tie is preserved, and the merged set is **still order-dependent** (4536 orderings give
one document, 504 give another). The divergence is *persistent*, not transient.

## What this means for the spec

1. **`t` stays out of the sort key.** §11.1's contingency is void. Any total order works
   provided it is the *same* total order everywhere, which is what `(l, o, id)` gives.
2. **R-1 must be enforced exactly, and that includes the uncommitted tail.** The spec
   already says history is "committed prefix + sorted uncommitted tail" (R-8). This
   experiment shows that the deterministic sort of the *tail* is not a tidiness measure —
   a replica that serves the tail in arrival order instead of `(l, o, id)` order can serve
   a permanently different document to a joining client. R-8 is a correctness requirement
   of the same rank as R-1.
3. **Risk assessment changes.** `federation-design.md` §12 budgeted for §11.1 possibly
   forcing a sort-key change in M3. It will not. But the same section's confidence that
   R-1 was a conservative over-specification was wrong: it is the minimum.

## 5. Trimming makes it permanent, and destroys data (`exp6`)

`exp5`'s trim scenario was **inconclusive as run** — it replayed a lone patch whose parent
had been trimmed, which yields an empty document on both sides for reasons unrelated to
the question. `exp6-trim.js` builds the real situation and answers spec §11.3.

Two replicas are partitioned onto different branches of one fork; each grows its branch
past a checkpoint; each then trims to its own last checkpoint — which is exactly what
`trimChannel` leaves behind for `GET_HISTORY` to serve.

| Scenario | Result |
| --- | --- |
| Exchange **full** histories | Converge, in either order: `"AAA a0 a1 a2 a3 a4 a5 "` |
| Exchange **trimmed** histories | **Do not converge.** Each replica keeps its own branch |

So the partition was recoverable right up until the trim, and the trim is what made it
permanent. The severity is higher than "the wrong branch wins": a ChainPad checkpoint
carries the **entire document** as its content, and a message whose parent has been pruned
can never be relinked (`handleMessage` returns at the "not connected to root" branch,
`chainpad.dist.js:1303`). The losing branch's edits are therefore *destroyed* — in the run
above, neither merged replica retained any of the other side's content, and nothing
anywhere reported an error.

This is why the spec now carries **R-41** alongside R-38: the "no trim below the global
watermark" rule has to be a hard assertion checked at trim time, not a scheduling policy
that callers are trusted to honour. The failure mode is silent data loss, so it must not
be reachable by a caller getting the order wrong.
