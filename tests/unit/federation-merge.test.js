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

/*  R-17, in its corrected form.
 *
 *  The first version of this blocked only the origin with the gap and let other
 *  origins commit past it. That is wrong: a member's Lamport *promise* says only
 *  that it will not emit anything lower in future — it says nothing about
 *  whether we have received what it already sent. A message still in flight
 *  could then arrive and need inserting *behind* something already committed,
 *  which is exactly the reordering docs/experiments/chainpad-ordering showed
 *  produces permanent, non-self-healing divergence.
 *
 *  So while any live member is incomplete, nothing commits. It costs latency
 *  and heals the instant the gap fills.
 */
test('commit: an incomplete member blocks the whole pass, not just itself', () => {
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
    const { ready, held, incomplete } = Merge.partition(state, pending, NOW);
    assert.strictEqual(incomplete, true, 'B is missing messages it has announced');
    assert.deepStrictEqual(ready, [], 'nothing may commit while a member is incomplete');
    assert.strictEqual(held.length, 3);
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
 *  So it is held. The channel stalls where an operator can see it, and the
 *  envelope stays in the pending log for a future RECONCILE to splice.
 */
test('R-48: an envelope below the committed tip is held, not committed', () => {
    const state = mkState({
        me: 'A', members: ['A', 'B'],
        self: { seq: 5, lamport: 9, committedSeq: 5 },
        peers: { B: { seq: 3, lamport: 9, committedSeq: 3, atime: Date.now() } }
    });
    state.tip = { l: 9, o: 'A', id: 'zzz' };

    // arrives late, and sorts before everything already committed
    const late = { c: 'chan', o: 'B', s: 4, l: 2, id: 'aaa' };

    const res = Merge.partition(state, [late], Date.now());
    assert.strictEqual(res.ready.length, 0,
        'a late envelope must never be appended after the tip it precedes');
    assert.deepStrictEqual(res.reordered, [late],
        'and it must be reported, so the stall is visible');
    assert.ok(res.held.some(h => h.id === 'aaa'),
        'it stays in the pending log for RECONCILE to splice');
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
    assert.strictEqual(res.ready.length, 0,
        'an equal clock is not enough: the origin tie-break decides');
    assert.deepStrictEqual(res.reordered.map(r => r.id), ['aaa']);
});
