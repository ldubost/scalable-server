// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors
//
// SPDX-License-Identifier: AGPL-3.0-or-later

/*  The merge rule: Lamport clocks, the watermark, the commit condition and the
 *  trim precondition (spec R-1…R-6, R-17, R-38, R-41).
 *
 *  All pure functions over a state object, so the behaviour that decides whether
 *  two instances converge is pinned here rather than only observable through a
 *  two-instance integration test. The integration test proves it works; these
 *  prove *why*, and catch a regression in the rule itself.
 */

const { test } = require('node:test');
const assert = require('node:assert');

const Merge = require('../../storage/federation/merge.js');

const A = 'AAAA', B = 'BBBB', C = 'CCCC';
const NOW = 1_000_000;

const mkState = (over) => Object.assign({
    me: A,
    members: [A, B],
    self: { seq: 0, lamport: 0 },
    peers: {},
    committed: { lamport: 0, lastId: null, line: 0 },
    evicted: []
}, over || {});

const env = (o, s, l, id) => ({ v: 1, c: 'chan', m: 'x', id, o, s, l, t: 0, sig: 's' });

// --- Lamport ---------------------------------------------------------------

test('lamport: one above the highest clock seen anywhere', () => {
    const state = mkState({
        self: { seq: 3, lamport: 5 },
        peers: { [B]: { seq: 2, lamport: 9 } }
    });
    assert.strictEqual(Merge.nextLamport(state), 10,
        'must exceed the peer clock, not just our own');
});

test('lamport: starts at 1 on a fresh channel', () => {
    assert.strictEqual(Merge.nextLamport(mkState()), 1);
});

// --- watermark (R-3, R-4, R-5) ---------------------------------------------

test('watermark: the minimum across live members', () => {
    const state = mkState({
        self: { seq: 5, lamport: 8 },
        peers: { [B]: { seq: 4, lamport: 6, atime: NOW } }
    });
    assert.strictEqual(Merge.watermark(state, NOW), 6);
});

/*  A member we have never heard from may be holding messages older than
    anything we have. Committing past it would mean inserting them into history
    later, which is the divergence §11.1 measured. */
test('watermark: an unheard member pins it at zero', () => {
    const state = mkState({ self: { seq: 1, lamport: 4 } });
    assert.strictEqual(Merge.watermark(state, NOW), 0);
});

/*  R-5: otherwise one dead instance freezes every other instance's history. */
test('watermark: a silent member is evicted and stops holding it back', () => {
    const state = mkState({
        self: { seq: 5, lamport: 8 },
        peers: { [B]: { seq: 1, lamport: 2, atime: NOW - Merge.EVICT_AFTER - 1 } }
    });
    assert.strictEqual(Merge.watermark(state, NOW), 8,
        'an evicted member must not pin the watermark');
    assert.deepStrictEqual(Merge.evictedMembers(state, NOW), [B]);
});

test('watermark: a member still inside the eviction window does hold it back', () => {
    const state = mkState({
        self: { seq: 5, lamport: 8 },
        peers: { [B]: { seq: 1, lamport: 2, atime: NOW - 1000 } }
    });
    assert.strictEqual(Merge.watermark(state, NOW), 2);
    assert.deepStrictEqual(Merge.evictedMembers(state, NOW), []);
});

// --- the commit rule (R-1, R-3, R-17) --------------------------------------

test('commit: only up to the watermark, in (l, o, id) order', () => {
    const state = mkState({
        self: { seq: 2, lamport: 5, committedSeq: -1 },
        peers: { [B]: { seq: 1, lamport: 4, atime: NOW, committedSeq: -1 } }
    });
    const pending = [
        env(A, 0, 5, 'a5'),   // above W
        env(B, 0, 2, 'b2'),
        env(A, 1, 2, 'a2'),
        env(B, 1, 4, 'b4')
    ];
    const { watermark, ready, held } = Merge.partition(state, pending, NOW);
    assert.strictEqual(watermark, 4);
    assert.deepStrictEqual(ready.map(e => e.id), ['a2', 'b2', 'b4'],
        'lamport first, then origin — A sorts before B at equal lamport');
    assert.deepStrictEqual(held.map(e => e.id), ['a5']);
});

/*  R-17, and the limit of what it should cost.
 *
 *  A gap in a member's sequence means a message we have not received, whose
 *  clock may sort before ones we hold. Committing past it risks having to insert
 *  that message *behind* something already in the log — the reordering
 *  docs/experiments/chainpad-ordering showed produces divergence.
 *
 *  An earlier version therefore stopped the *whole pass*: while any member was
 *  incomplete, nothing committed at all. That is too much. It means one peer's
 *  hole silences every other member and this instance's own writes, and if the
 *  hole is permanent — messages lost in an outage before anything retained them
 *  — the channel never commits again. Measured on a live pad: 40 envelopes
 *  waiting on sequences that no longer existed, while federation looked healthy.
 *
 *  So a gap holds back the origin that has it, and nobody else. Placement of a
 *  late arrival is the client's to reconcile (R-54); a stalled pad helps no one.
 */
test('commit: a gap holds back its own origin, not the whole pass', () => {
    const state = mkState({
        members: [A, B, C],
        self: { seq: 0, lamport: 9, committedSeq: -1 },
        peers: {
            // B says it is at seq 5, but we only hold 0 and 2
            [B]: { seq: 5, lamport: 9, atime: NOW, committedSeq: -1 },
            [C]: { seq: 0, lamport: 9, atime: NOW, committedSeq: -1 }
        }
    });
    const pending = [
        env(B, 0, 1, 'b0'),
        env(B, 2, 2, 'b2'),   // b1 is missing
        env(C, 0, 1, 'c0')
    ];
    const { ready, held } = Merge.partition(state, pending, NOW);
    const ids = ready.map(e => e.id);

    assert.ok(ids.includes('c0'),
        'another member must not be silenced by B\'s gap');
    assert.ok(ids.includes('b0'),
        'and B\'s messages *below* its gap are fine');
    assert.ok(!ids.includes('b2'),
        'but nothing of B\'s past the gap, which is what the gap rule is for');
    assert.ok(held.some(h => h.id === 'b2'));
});

/*  Once the gap fills, the same state commits everything in order. */
test('commit: filling the gap unblocks the pass', () => {
    const state = mkState({
        members: [A, B, C],
        self: { seq: 0, lamport: 9, committedSeq: -1 },
        peers: {
            [B]: { seq: 2, lamport: 9, atime: NOW, committedSeq: -1 },
            [C]: { seq: 0, lamport: 9, atime: NOW, committedSeq: -1 }
        }
    });
    const pending = [
        env(B, 0, 1, 'b0'), env(B, 1, 2, 'b1'), env(B, 2, 3, 'b2'),
        env(C, 0, 1, 'c0')
    ];
    const { ready, incomplete } = Merge.partition(state, pending, NOW);
    assert.strictEqual(incomplete, false);
    assert.deepStrictEqual(ready.map(e => e.id), ['b0', 'c0', 'b1', 'b2'],
        'lamport first, then origin at equal lamport');
});

/*  An evicted member cannot block: that is the point of eviction (R-5). */
test('commit: an evicted member does not make the pass incomplete', () => {
    const state = mkState({
        self: { seq: 0, lamport: 5, committedSeq: 0 },
        peers: { [B]: { seq: 9, lamport: 3, atime: NOW - Merge.EVICT_AFTER - 1,
            committedSeq: -1 } }
    });
    const pending = [env(A, 1, 5, 'a1')];
    const { ready, incomplete } = Merge.partition(state, pending, NOW);
    assert.strictEqual(incomplete, false);
    assert.deepStrictEqual(ready.map(e => e.id), ['a1']);
});

/*  The property the whole design rests on: two replicas holding the same
    pending set and the same watermark must commit the same sequence. */
test('R-1: the committed sequence is independent of arrival order', () => {
    const state = () => mkState({
        self: { seq: 2, lamport: 6, committedSeq: -1 },
        peers: { [B]: { seq: 2, lamport: 6, atime: NOW, committedSeq: -1 } }
    });
    const pending = [
        env(A, 0, 3, 'aaa'), env(B, 0, 3, 'bbb'),
        env(A, 1, 4, 'ccc'), env(B, 1, 3, 'ddd'),
        env(A, 2, 5, 'eee'), env(B, 2, 6, 'fff')
    ];
    const expected = Merge.partition(state(), pending, NOW).ready.map(e => e.id);

    // every permutation of arrival must yield the same committed sequence
    const permute = function* (arr) {
        if (arr.length <= 1) { yield arr; return; }
        for (let i = 0; i < arr.length; i++) {
            const rest = arr.slice(0, i).concat(arr.slice(i + 1));
            for (const p of permute(rest)) { yield [arr[i], ...p]; }
        }
    };
    let n = 0;
    for (const perm of permute(pending)) {
        n++;
        assert.deepStrictEqual(
            Merge.partition(state(), perm, NOW).ready.map(e => e.id), expected);
    }
    assert.strictEqual(n, 720, 'all permutations checked');
});

// --- observe ---------------------------------------------------------------

test('observe: clocks only ever move forward', () => {
    const state = mkState();
    Merge.observe(state, B, { seq: 4, lamport: 7 }, NOW);
    Merge.observe(state, B, { seq: 2, lamport: 3 }, NOW);
    assert.strictEqual(state.peers[B].lamport, 7,
        'a late envelope must not pull a peer clock backwards');
    assert.strictEqual(state.peers[B].seq, 4);
});

/*  R-6: an evicted peer that comes back has a backlog, and the fact that it was
    away has to be visible so the backlog can be spliced in deliberately. */
test('observe: hearing from an evicted peer flags a reconcile', () => {
    const state = mkState({ evicted: [B] });
    Merge.observe(state, B, { seq: 0, lamport: 1 }, NOW);
    assert.deepStrictEqual(state.evicted, []);
    assert.strictEqual(state.__reconcile, B);
});

// --- trim (R-38, R-41) -----------------------------------------------------

/*  §11.3 measured a premature trim *destroying* the other branch's content
    silently — not merely reordering it. So these are the conditions that must
    hold, checked as an assertion at trim time rather than as a policy. */
test('trim: refused while any member is evicted', () => {
    const state = mkState({
        self: { seq: 9, lamport: 9 },
        peers: { [B]: { seq: 1, lamport: 1, atime: NOW - Merge.EVICT_AFTER - 1 } },
        committed: { lamport: 9, lastId: 'x', line: 5 }
    });
    const res = Merge.mayTrim(state, 5, NOW);
    assert.strictEqual(res.ok, false);
    assert.strictEqual(res.reason, 'EEVICTEDPEERS');
    assert.deepStrictEqual(res.evicted, [B]);
});

test('trim: refused above the watermark', () => {
    const state = mkState({
        self: { seq: 9, lamport: 9 },
        peers: { [B]: { seq: 4, lamport: 4, atime: NOW } },
        committed: { lamport: 9, lastId: 'x', line: 5 }
    });
    const res = Merge.mayTrim(state, 7, NOW);
    assert.strictEqual(res.ok, false);
    assert.strictEqual(res.reason, 'EABOVEWATERMARK');
    assert.strictEqual(res.watermark, 4);
});

test('trim: refused above what is actually committed', () => {
    const state = mkState({
        self: { seq: 9, lamport: 9 },
        peers: { [B]: { seq: 9, lamport: 9, atime: NOW } },
        committed: { lamport: 3, lastId: 'x', line: 2 }
    });
    const res = Merge.mayTrim(state, 6, NOW);
    assert.strictEqual(res.ok, false);
    assert.strictEqual(res.reason, 'ENOTCOMMITTED');
});

test('trim: allowed when every member is live and past the point', () => {
    const state = mkState({
        self: { seq: 9, lamport: 9 },
        peers: { [B]: { seq: 9, lamport: 9, atime: NOW } },
        committed: { lamport: 9, lastId: 'x', line: 9 }
    });
    const res = Merge.mayTrim(state, 5, NOW);
    assert.strictEqual(res.ok, true, JSON.stringify(res));
});

test('trim: a non-numeric point is refused rather than coerced', () => {
    const state = mkState({
        peers: { [B]: { seq: 9, lamport: 9, atime: NOW } },
        committed: { lamport: 9, lastId: 'x', line: 9 }
    });
    assert.strictEqual(Merge.mayTrim(state, undefined, NOW).reason, 'EBADTRIMPOINT');
    assert.strictEqual(Merge.mayTrim(state, '5', NOW).reason, 'EBADTRIMPOINT');
});

/*  R-48: an envelope that sorts before the committed tip.
 *
 *  This is the state a channel reaches after an eviction: the peer stopped
 *  answering, stopped holding the watermark back, we committed past it, and its
 *  backlog then turned up carrying clocks below our committed head. The peer
 *  itself committed those messages in their proper place, so appending them
 *  here would give the two instances the same messages in different orders —
 *  the §11.1 failure, which is permanent and silent.
 *
 *  Holding it was the first answer and the wrong one: the message then never
 *  reaches the peer at all, so instead of one message in two positions we get
 *  two different documents, permanently. It is appended and reported, and the
 *  client resolves the placement (R-54).
 */
test('R-48: an envelope below the committed tip is appended, and reported', () => {
    const state = mkState({
        me: 'A', members: ['A', 'B'],
        self: { seq: 5, lamport: 9, committedSeq: 5 },
        peers: { B: { seq: 3, lamport: 9, committedSeq: 3, atime: Date.now() } }
    });
    state.tip = { l: 9, o: 'A', id: 'zzz' };

    // arrives late, and sorts before everything already committed
    const late = { c: 'chan', o: 'B', s: 4, l: 2, id: 'aaa' };

    const res = Merge.partition(state, [late], Date.now());
    assert.deepStrictEqual(res.ready.map(r => r.id), ['aaa'],
        'a late envelope is stored rather than withheld from the peer');
    assert.deepStrictEqual(res.reordered, [late],
        'and reported, because these two logs now order that stretch differently');
});

/*  The guard must not fire on the normal case, or every channel would stall the
    moment it committed anything. */
test('R-48: an envelope above the committed tip commits as usual', () => {
    const state = mkState({
        me: 'A', members: ['A', 'B'],
        self: { seq: 5, lamport: 9, committedSeq: 5 },
        peers: { B: { seq: 3, lamport: 12, committedSeq: 3, atime: Date.now() } }
    });
    state.tip = { l: 9, o: 'A', id: 'zzz' };

    const next = { c: 'chan', o: 'B', s: 4, l: 10, id: 'bbb' };

    const res = Merge.partition(state, [next], Date.now());
    assert.deepStrictEqual(res.reordered, [],
        'nothing is out of order here');
    assert.deepStrictEqual(res.ready.map(r => r.id), ['bbb']);
});

/*  Same clock, different origin: the tie-break is part of the order, so the
    guard has to use the whole `(l, o, id)` and not just the clock. A tip that
    compared on `l` alone would let this through and diverge. */
test('R-48: the tip comparison uses the whole (l, o, id)', () => {
    const state = mkState({
        me: 'B', members: ['A', 'B'],
        self: { seq: 2, lamport: 7, committedSeq: 2 },
        peers: { A: { seq: 1, lamport: 7, committedSeq: 1, atime: Date.now() } }
    });
    state.tip = { l: 7, o: 'B', id: 'mmm' };

    // same clock, but origin 'A' sorts before 'B'
    const late = { c: 'chan', o: 'A', s: 2, l: 7, id: 'aaa' };

    const res = Merge.partition(state, [late], Date.now());
    assert.deepStrictEqual(res.reordered.map(r => r.id), ['aaa'],
        'an equal clock is not enough: the origin tie-break decides');
});

/*  A peer's announced sequence is a promise the merge then waits on: nothing
 *  commits while a member has announced messages we have not received. So a
 *  value that is too high, once recorded, freezes the channel for good — the
 *  peer has no way to take it back.
 *
 *  Found on a live pad: 37 envelopes sat uncommitted, waiting for sequences 72
 *  through 126 from an instance whose log ended at 71. Federation looked
 *  healthy — sessions up, heartbeats flowing — and simply never committed
 *  anything again.
 */
test('a heartbeat may correct a peer sequence downwards', () => {
    const state = mkState({ me: A, members: [A, B] });

    // an envelope, and then a wrong high value from anywhere
    Merge.observe(state, B, { seq: 5, lamport: 5 }, Date.now());
    Merge.observe(state, B, { seq: 126, lamport: 10 }, Date.now());
    assert.strictEqual(state.peers[B].seq, 126);

    // the peer itself says its log ends at 71: it knows, and it wins
    Merge.observe(state, B, { seq: 71, lamport: 10, authoritative: true }, Date.now());
    assert.strictEqual(state.peers[B].seq, 71,
        'a peer describing its own log must be able to correct it downwards');
});

/*  Envelopes stay monotonic: one arriving late is evidence of a single message,
    not a statement about the sender's log, and out-of-order arrival is routine. */
test('an envelope never pulls a peer sequence backwards', () => {
    const state = mkState({ me: A, members: [A, B] });
    Merge.observe(state, B, { seq: 9, lamport: 9 }, Date.now());
    Merge.observe(state, B, { seq: 4, lamport: 4 }, Date.now());
    assert.strictEqual(state.peers[B].seq, 9,
        'an out-of-order envelope must not look like a shorter log');
});

/*  The point of the fix: once corrected, the completeness barrier stops
    blocking and the pass commits again. */
test('correcting the sequence unblocks a frozen channel', () => {
    const now = Date.now();
    const state = mkState({
        me: A, members: [A, B],
        self: { seq: 3, lamport: 10, committedSeq: 3 },
        peers: { [B]: { seq: 126, lamport: 10, committedSeq: 1, atime: now } }
    });
    const pending = [env(B, 2, 11, 'bbb')];

    // waiting for sequences that will never arrive
    assert.strictEqual(Merge.partition(state, pending, now).ready.length, 0);

    Merge.observe(state, B, { seq: 2, lamport: 11, authoritative: true }, now);
    assert.deepStrictEqual(
        Merge.partition(state, pending, now).ready.map(e => e.id), ['bbb'],
        'once the peer corrects itself the pass must commit again');
});

/*  A hole in a member's sequence that no longer exists to be filled.
 *
 *  Messages lost in an outage, before anything retained them for resend, leave
 *  a gap the merge waits on for ever. Federation looks healthy throughout —
 *  sessions up, heartbeats flowing — and every pass declines to commit for a
 *  reason that will never stop being true. Found on a live pad sitting at 40
 *  uncommitted envelopes.
 */
test('a gap that never fills is given up on, and the pass continues', () => {
    const t0 = Date.now();
    const state = mkState({
        me: A, members: [A, B],
        self: { seq: 3, lamport: 20, committedSeq: 3 },
        // B says its log ends at 6 and its clock at 21; we hold 6 but not 5
        peers: { [B]: { seq: 6, lamport: 21, committedSeq: 4, atime: t0 } }
    });
    const pending = [env(B, 6, 21, 'bbb')];

    assert.strictEqual(Merge.partition(state, pending, t0).ready.length, 0,
        'it waits while the gap might still fill');

    /*  Noticed, then noticed again much later — with the peer still alive and
        heartbeating, which is the case that matters. A peer that has gone quiet
        is evicted and stops blocking anyway; this is the one where everything
        looks healthy and nothing commits. */
    Merge.noteStalled(state, Merge.partition(state, pending, t0).stalled, t0);
    const later = t0 + Merge.GAP_TIMEOUT + 1000;
    state.peers[B].atime = later;
    const changed = Merge.noteStalled(state,
        Merge.partition(state, pending, later).stalled, later);

    assert.ok(changed, 'the gap must eventually be abandoned');
    assert.strictEqual(state.peers[B].committedSeq, 5,
        'and stepped over, so the run can continue past it');
    assert.deepStrictEqual(Merge.partition(state, pending, later).ready.map(e => e.id),
        ['bbb'], 'the channel commits again');
});

/*  But not prematurely: a slow backfill must not be mistaken for a lost
    message, or a repair would be replaced by a permanent mis-ordering. */
test('a gap that is still filling is not abandoned', () => {
    const t0 = Date.now();
    const state = mkState({
        me: A, members: [A, B],
        self: { seq: 3, lamport: 20, committedSeq: 3 },
        peers: { [B]: { seq: 6, lamport: 21, committedSeq: 4, atime: t0 } }
    });
    const pending = [env(B, 6, 21, 'bbb')];

    Merge.noteStalled(state, Merge.partition(state, pending, t0).stalled, t0);
    const soon = t0 + 30 * 1000;
    state.peers[B].atime = soon;
    assert.strictEqual(
        Merge.noteStalled(state, Merge.partition(state, pending, soon).stalled, soon),
        false, 'half a minute is not long enough to give up on a message');
    assert.strictEqual(state.peers[B].committedSeq, 4, 'nothing stepped over');
});
