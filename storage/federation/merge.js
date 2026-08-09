// SPDX-License-Identifier: AGPL-3.0-or-later
// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors

/*  The merge: Lamport clocks, the watermark, and the commit rule
    (spec §4, R-1…R-6).

    This is what replaces the anchor. Under L1 one instance ordered everything,
    so every replica trivially agreed. At L2 both instances accept writes, and
    agreement has to be *derived* rather than delegated.

    How it works
    ------------
    Every accepted message gets a Lamport clock `l` and a per-origin sequence
    `s`. The federation order is `(l, o, id)` — see common/federation/order.js,
    which is the single definition of it.

    An envelope may be committed once no member can still produce a message that
    would sort before it. A Lamport clock only ever increases, so a member
    currently at clock L will next emit L+1 or higher; therefore everything with
    `l <= min(all members' clocks)` is already in hand. That minimum is the
    **watermark** W, and R-3 is: commit everything with `l <= W`, in sort order,
    and nothing else.

    Why this is the whole game
    --------------------------
    docs/experiments/chainpad-ordering established that ChainPad is *not*
    order-independent: two replicas holding the same messages in different
    orders can converge on permanently different documents, and it does not
    self-heal. So "both replicas end up with the same set" is not good enough —
    they must produce the same *sequence*. The commit rule is what guarantees
    that, and R-8's sorted uncommitted tail is what keeps it true for the
    messages that are not committed yet.

    Liveness
    --------
    A silent member would freeze W and with it the committed log. Two defences:
    heartbeats carry each member's clock even when it has nothing to say (R-4),
    and a member that stops answering is provisionally evicted from the
    watermark computation so the rest keep going (R-5). Eviction is the reason
    R-6's RECONCILE exists: an evicted member's backlog arrives late and has to
    be spliced in visibly rather than pretended away.
*/

const Order = require('../../common/federation/order.js');

// A member that has not been heard from in this long stops holding W back (R-5).
const EVICT_AFTER = 60 * 1000;

/*  How long a hole in one member's sequence holds that member's later messages
    back before it is given up on.

    Short, because it costs little: a hole delays only the origin that has it,
    and the messages behind it are not lost — they are stored either way, and it
    is their position in the order that is at stake, which the client resolves.
    A minute is long enough for an ordinary backfill and short enough that a pad
    with a permanent hole in its history keeps working. */
const GAP_TIMEOUT = 60 * 1000;

/*  Note that a member is holding the pass up, and give up on it once it has
    done so for too long. Returns true if anything was abandoned, so the caller
    knows the state needs saving. */
const noteStalled = (state, stalled, now) => {
    const since = state.stalledSince = state.stalledSince || {};
    const seen = new Set();
    let changed = false;

    (stalled || []).forEach(({ origin, expected }) => {
        seen.add(origin);
        const at = since[origin];
        if (!at || at.expected !== expected) {
            since[origin] = { at: now, expected };
            return;
        }
        if ((now - at.at) < GAP_TIMEOUT) { return; }
        state.abandoned = state.abandoned || [];
        if (!state.abandoned.some(x => x.o === origin && x.s === expected)) {
            state.abandoned.push({ o: origin, s: expected, t: now });
        }
        /*  Step over the hole so the run can continue past it. */
        const peer = state.peers?.[origin];
        if (peer) { peer.committedSeq = Math.max(peer.committedSeq ?? -1, expected); }
        delete since[origin];
        changed = true;
    });

    Object.keys(since).forEach(o => { if (!seen.has(o)) { delete since[o]; } });
    return changed;
};

/*  The next Lamport clock for a message accepted here.

    Standard Lamport rule: one more than the highest clock this replica has seen
    from anybody, so causality is respected across instances without any clock
    synchronisation. Nothing here reads a wall clock — §11.1 showed server
    timestamps never reach ChainPad and must not enter the order.
*/
const nextLamport = (state) => {
    let highest = state.self?.lamport || 0;
    Object.values(state.peers || {}).forEach(p => {
        if (typeof (p?.lamport) === 'number' && p.lamport > highest) {
            highest = p.lamport;
        }
    });
    return highest + 1;
};

/*  The clock a member can promise never to go below.

    A member's next message will carry `nextLamport`, i.e. one more than the
    highest clock it has seen from anybody. So it can safely promise that
    highest value: nothing it emits from now on will sort at or below it.

    This is what makes an idle member harmless. Reporting only its *own* last
    clock would pin the watermark there forever — a member that has written
    nothing since would never advance, and everyone else's history would stop
    with it. That was a real defect: two instances that each wrote once
    converged only if they kept writing.
*/
const promise = (state) => nextLamport(state) - 1;

/*  The watermark: the minimum clock across every member still counted.

    `now` is passed in rather than read, so the same state always yields the same
    watermark in a test.
*/
const watermark = (state, now) => {
    const evicted = new Set(state.evicted || []);
    // what we ourselves can promise, not merely what we last emitted
    let w = promise(state);

    (state.members || []).forEach(m => {
        if (m === state.me || evicted.has(m)) { return; }
        const peer = state.peers?.[m];

        /*  A member we have never heard from at all holds the watermark at 0:
            it may hold messages older than anything we have, and committing
            past it would mean reordering them in later. It is evicted on the
            usual timer like anyone else, at which point the rest proceed. */
        if (!peer) { return void (w = 0); }

        if (typeof (peer.atime) === 'number' && (now - peer.atime) > EVICT_AFTER) {
            return;   // provisionally evicted (R-5); does not hold W back
        }
        const l = peer.lamport || 0;
        if (l < w) { w = l; }
    });
    return w;
};

/*  Which members are currently evicted, so callers can report it and — more
    importantly — refuse to trim (R-38/R-41) while anybody is missing. */
const evictedMembers = (state, now) => {
    const evicted = new Set(state.evicted || []);
    (state.members || []).forEach(m => {
        if (m === state.me) { return; }
        const peer = state.peers?.[m];
        if (!peer) { return void evicted.add(m); }
        if (typeof (peer.atime) === 'number' && (now - peer.atime) > EVICT_AFTER) {
            evicted.add(m);
        }
    });
    return Array.from(evicted);
};

/*  Split pending envelopes into those that may commit now and those that may
    not, given the watermark.

    The committable half is returned in `(l, o, id)` order — R-3 says commit *in
    sort order*, and since every replica computes the same order over the same
    set, every replica writes the same lines in the same sequence (R-1).

    R-17: a gap in an origin's `s` blocks that origin. A gap means a message we
    have not seen yet whose Lamport clock may sort before ones we have, so
    committing past it would reorder history when it arrives.
*/
const partition = (state, pending, now) => {
    const W = watermark(state, now);
    const evicted = new Set(evictedMembers(state, now));

    /*  An envelope at or below its origin's committedSeq is already in the log.

        It is still in the pending file on purpose — retained so it can be
        resent to a peer that is missing it (R-47) — but as far as the merge is
        concerned it does not exist. Filtering it here rather than at the commit
        step matters: left in, it reads as an out-of-order sequence at the head
        of its origin's run and the gap rule below would block that origin's
        *future* messages forever. */
    const committedFor = (origin) => origin === state.me
        ? (state.self?.committedSeq ?? -1)
        : (state.peers?.[origin]?.committedSeq ?? -1);
    const fresh = pending.filter(e => e.s > committedFor(e.o));

    // group by origin and order by sequence
    const byOrigin = new Map();
    fresh.forEach(e => {
        if (!byOrigin.has(e.o)) { byOrigin.set(e.o, []); }
        byOrigin.get(e.o).push(e);
    });
    byOrigin.forEach(list => list.sort((a, b) => a.s - b.s));

    /*  The highest sequence we hold in an unbroken run from each origin, and the
        sequence at which its run breaks. */
    const haveThrough = new Map();
    const blockedAbove = new Map();
    const originsSeen = new Set([...byOrigin.keys(), ...(state.members || [])]);

    originsSeen.forEach(origin => {
        const committedSeq = committedFor(origin);
        const list = byOrigin.get(origin) || [];
        let through = committedSeq;
        let limit = Infinity;
        for (const e of list) {
            if (e.s !== through + 1) { limit = e.s; break; }   // gap
            through = e.s;
        }
        haveThrough.set(origin, through);
        blockedAbove.set(origin, limit);
    });

    /*  R-17, stated properly: a member's *promise* only says it will not emit
        anything lower in future. It says nothing about whether we have received
        what it already sent. So before committing anything we must also hold an
        unbroken run from every live member up to the sequence it has told us
        about — otherwise a message still in flight could arrive afterwards and
        have to be inserted *behind* something already committed, which is
        precisely the reordering §11.1 showed produces permanent divergence.

        When a member is incomplete we commit nothing this pass rather than
        guess. It costs latency and self-heals the moment the gap fills; the
        alternative risks a divergence that never heals.
    */
    /*  Who is holding the pass up, and since when.

        Waiting is right while the messages are still coming. Waiting *forever*
        is not: a sequence can contain a hole that no longer exists to be filled
        — messages lost in an outage before anything retained them for resend —
        and then the channel never commits again. Federation looks healthy the
        whole time, because it is: sessions up, heartbeats flowing, and every
        pass declining to commit for a reason that will never stop being true.

        So a hole that has not filled within `GAP_TIMEOUT` is given up on. That
        loses nothing: the messages either do not exist, or arrive later through
        the repair (R-6), which puts them in the log regardless of what the
        federation order thinks. It costs the guarantee that the two logs are in
        the same order across that hole, which is the client's to reconcile
        (R-54). A stalled channel helps nobody. */
    const stalled = [];
    (state.members || []).forEach(m => {
        if (m === state.me || evicted.has(m)) { return; }
        const reported = state.peers?.[m]?.seq;
        if (typeof (reported) !== 'number') { return; }
        if (reported > (haveThrough.get(m) ?? -1)) {
            stalled.push({ origin: m, expected: (haveThrough.get(m) ?? -1) + 1 });
        }
    });

    /*  A hole holds back the origin that has it, and nothing else.

        It used to stop the whole pass: if any member had announced a message we
        had not received, nothing at all committed, including our own writes and
        other members'. The reasoning was that a message still in flight could
        otherwise arrive after we had committed past it, and have to be inserted
        behind something already in the log.

        That is true, and it is not worth freezing a channel for. The messages
        either arrive — and the client reconciles their placement, which is its
        job (R-54) — or they never existed, in which case waiting for them stops
        the pad for good. One peer's gap must not be able to silence everyone.

        So the wait is scoped to the origin that is missing something, and even
        there it is bounded: `noteStalled` gives up after GAP_TIMEOUT. */
    stalled.forEach(({ origin, expected }) => {
        const at = blockedAbove.get(origin);
        if (!(at <= expected)) { blockedAbove.set(origin, expected); }
    });
    const incomplete = false;

    /*  An envelope that sorts *before* what is already committed (R-48).

        The commit rule is meant to make this impossible — nothing commits until
        no member can still produce something earlier — and it happens anyway
        when the rule was applied with incomplete information: an evicted peer's
        backlog, or a gap that was given up on.

        An earlier version **held these indefinitely**, on the grounds that
        appending one gives the two instances the same messages in different
        orders, which §11.1 showed ChainPad does not resolve by itself. The
        reasoning was sound and the conclusion was not. Holding is not a neutral
        act: the message stays out of the peer's log entirely, so instead of two
        orders of the same content we get two different *contents*, permanently,
        which is strictly worse. On a live pad this produced 394 refusals in two
        minutes and a channel that never caught up.

        So it is appended, and reported. Both instances end up holding
        everything, which is the property worth protecting (R-54); their
        placement is left to the client, which can read the content and already
        resolves a chain it receives out of order.
    */
    const tip = state.tip;
    const sortsBeforeTip = (e) => {
        if (!tip || typeof (tip.l) !== 'number') { return false; }
        return Order.compare({ l: e.l, o: e.o, id: e.id }, tip) < 0;
    };

    const ready = [];
    const held = [];
    const reordered = [];
    fresh.forEach(e => {
        if (sortsBeforeTip(e)) { reordered.push(e); }
        const gapAt = blockedAbove.get(e.o);
        if (!incomplete && e.l <= W && e.s < gapAt) { ready.push(e); }
        else { held.push(e); }
    });

    return {
        watermark: W, incomplete, held, reordered, stalled,
        ready: Order.sortTail(ready)
    };
};

/*  Note a message seen from a member, advancing what we know of its clock.

    Monotonic on purpose: an envelope that arrives out of order must not pull a
    peer's clock backwards, or the watermark could go backwards and we would
    commit something twice.
*/
const observe = (state, originId, info, now) => {
    if (originId === state.me) {
        state.self = state.self || { seq: 0, lamport: 0 };
        if (typeof (info.lamport) === 'number' && info.lamport > state.self.lamport) {
            state.self.lamport = info.lamport;
        }
        if (typeof (info.seq) === 'number' && info.seq > state.self.seq) {
            state.self.seq = info.seq;
        }
        return state;
    }
    const peers = state.peers = state.peers || {};
    const peer = peers[originId] = peers[originId] || { seq: -1, lamport: 0 };
    if (typeof (info.lamport) === 'number' && info.lamport > peer.lamport) {
        peer.lamport = info.lamport;
    }
    /*  A heartbeat is the peer describing its own log, so it is authoritative
        and may move this *down*; an envelope only ever raises it.

        The distinction matters because the value is a promise we then wait on:
        the merge refuses to commit anything while a member has announced
        messages we have not received. Kept monotonic, a single wrong high value
        — from a restore, a rollback, or a bug — freezes the channel for good,
        because the peer can never take it back. That is not hypothetical: a pad
        sat with 37 uncommitted envelopes waiting for sequences 72 to 126 from a
        peer whose log ended at 71.

        An envelope stays monotonic because it is evidence of one message, not a
        statement about the sender's log, and arrives out of order routinely. */
    if (typeof (info.seq) === 'number') {
        if (info.authoritative) { peer.seq = info.seq; }
        else if (info.seq > peer.seq) { peer.seq = info.seq; }
    }
    peer.atime = typeof (now) === 'number' ? now : Date.now();

    /*  Hearing from a peer un-evicts it. Its backlog is spliced in by the
        RECONCILE path rather than silently appearing mid-history (R-6). */
    if (Array.isArray(state.evicted) && state.evicted.includes(originId)) {
        state.evicted = state.evicted.filter(o => o !== originId);
        return Object.assign(state, { __reconcile: originId });
    }
    return state;
};

// Record that an origin's messages up to `seq` are in the committed log.
const noteCommitted = (state, originId, seq, lamport, lastId) => {
    if (originId === state.me) {
        state.self = state.self || { seq: 0, lamport: 0 };
        state.self.committedSeq = Math.max(state.self.committedSeq ?? -1, seq);
    } else {
        const peers = state.peers = state.peers || {};
        const peer = peers[originId] = peers[originId] || { seq: -1, lamport: 0 };
        peer.committedSeq = Math.max(peer.committedSeq ?? -1, seq);
    }
    const committed = state.committed = state.committed || { lamport: 0, lastId: null, line: 0 };
    if (lamport > committed.lamport) { committed.lamport = lamport; }
    committed.lastId = lastId;
    committed.line = (committed.line || 0) + 1;

    /*  The highest point in the federation order we have committed, kept as the
        full `(l, o, id)` and not merely the clock: R-48 has to recognise an
        envelope that sorts before it, and two envelopes routinely share a
        Lamport clock. Monotonic, so a commit can never move it backwards. */
    const here = { l: lamport, o: originId, id: lastId };
    if (!state.tip || typeof (state.tip.l) !== 'number' ||
        Order.compare(here, state.tip) > 0) {
        state.tip = here;
    }
    return state;
};

/*  R-38 / R-41: may history be trimmed to this point?

    Returned as a reason rather than a boolean because the caller must be able to
    log *why* it refused. A premature trim does not merely reorder — §11.3
    measured it destroying the other branch's content silently — so this is
    checked as a hard assertion immediately before trimming, never as a policy
    the caller is trusted to have consulted.
*/
const mayTrim = (state, toLamport, now) => {
    const evicted = evictedMembers(state, now);
    if (evicted.length) {
        return { ok: false, reason: 'EEVICTEDPEERS', evicted };
    }
    const W = watermark(state, now);
    if (typeof (toLamport) !== 'number' || !isFinite(toLamport)) {
        return { ok: false, reason: 'EBADTRIMPOINT' };
    }
    if (toLamport > W) {
        return { ok: false, reason: 'EABOVEWATERMARK', watermark: W };
    }
    if (toLamport > (state.committed?.lamport || 0)) {
        return { ok: false, reason: 'ENOTCOMMITTED' };
    }
    return { ok: true, watermark: W };
};

/*  What we hold from each origin, as an unbroken run: `origin -> highest seq`.

    This is what a peer needs in order to work out what to resend. It is stated
    as a contiguous run rather than a set because that is the only shape the
    commit rule can act on — a hole means everything above it is blocked anyway
    (R-17), so the first hole is exactly where the useful information stops.
*/
const have = (state, pending) => {
    const out = {};
    const byOrigin = new Map();
    (pending || []).forEach(e => {
        if (!byOrigin.has(e.o)) { byOrigin.set(e.o, []); }
        byOrigin.get(e.o).push(e);
    });

    const origins = new Set([
        ...(state.members || []),
        ...Object.keys(state.peers || {}),
        ...byOrigin.keys()
    ]);
    origins.forEach(origin => {
        const committed = origin === state.me
            ? (state.self?.committedSeq ?? -1)
            : (state.peers?.[origin]?.committedSeq ?? -1);
        const list = (byOrigin.get(origin) || []).slice().sort((a, b) => a.s - b.s);
        let through = committed;
        for (const e of list) {
            if (e.s !== through + 1) { break; }
            through = e.s;
        }
        out[origin] = through;
    });
    return out;
};

/*  Which of OUR OWN envelopes a peer is missing, given what it says it has.

    Anti-entropy is deliberately push-shaped and per-origin: an instance only
    ever resends messages it authored, because those are the ones it can be sure
    it holds in full. Asking a peer to send back messages it received from
    somebody else would work too, but it makes the repair depend on a third
    party's completeness.
*/
const missingFor = (state, peerHave, pending) => {
    const me = state.me;
    const theirs = (peerHave && typeof (peerHave[me]) === 'number') ? peerHave[me] : -1;
    const mine = have(state, pending)[me];
    if (typeof (mine) !== 'number' || mine <= theirs) { return { from: null, to: null }; }
    return { from: theirs + 1, to: mine };
};

module.exports = {
    nextLamport, promise, watermark, partition, observe, noteCommitted, have, missingFor,
    evictedMembers, mayTrim, noteStalled, EVICT_AFTER, GAP_TIMEOUT
};
