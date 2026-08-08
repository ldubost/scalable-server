// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors
//
// SPDX-License-Identifier: AGPL-3.0-or-later

/*  Regression guard for spec R-1 and R-36.
 *
 *  docs/experiments/chainpad-ordering established that ChainPad is NOT
 *  order-independent: the same set of messages served in different orders can
 *  produce permanently different documents. Everything federation does to keep
 *  replicas identical rests on that, in particular:
 *
 *    R-1   every replica serves the same id sequence
 *    R-36  the uncommitted tail is sorted by (l, o, id) -- a correctness
 *          requirement, not tidiness, because a replica that serves the tail in
 *          arrival order can serve a permanently different document
 *
 *  The sort is a pure function, so it is pinned here rather than behind a running
 *  federation. If someone later "optimises" the ordering -- drops the id
 *  tiebreak, sorts by arrival, compares lamport clocks numerically-as-strings --
 *  these tests fail instead of two instances silently diverging in production.
 */

const { test } = require('node:test');
const assert = require('node:assert');

const Order = require('../../common/federation/order.js');

// A tail entry: lamport clock, origin id, message id.
const e = (l, o, id) => ({ l, o, id });

test('sorts by lamport clock first', () => {
    const sorted = Order.sortTail([e(3, 'B', 'x'), e(1, 'A', 'z'), e(2, 'C', 'y')]);
    assert.deepStrictEqual(sorted.map(m => m.l), [1, 2, 3]);
});

test('breaks lamport ties on origin, then on id', () => {
    const sorted = Order.sortTail([
        e(5, 'B', 'aaa'),
        e(5, 'A', 'zzz'),
        e(5, 'A', 'aaa')
    ]);
    assert.deepStrictEqual(
        sorted.map(m => `${m.o}/${m.id}`),
        ['A/aaa', 'A/zzz', 'B/aaa']);
});

test('is a total order: no two distinct entries compare equal', () => {
    const entries = [];
    ['A', 'B'].forEach(o => {
        [1, 2].forEach(l => {
            ['p', 'q'].forEach(id => entries.push(e(l, o, id)));
        });
    });
    for (let i = 0; i < entries.length; i++) {
        for (let j = 0; j < entries.length; j++) {
            const c = Order.compare(entries[i], entries[j]);
            if (i === j) { assert.strictEqual(c, 0); }
            else { assert.notStrictEqual(c, 0, `${i} vs ${j} compared equal`); }
        }
    }
});

test('compare is antisymmetric and transitive', () => {
    const entries = [e(1, 'A', 'a'), e(1, 'A', 'b'), e(1, 'B', 'a'), e(2, 'A', 'a')];
    // `|| 0` normalises -0, which strictEqual (Object.is) treats as distinct from 0
    const sign = (n) => Math.sign(n) || 0;
    entries.forEach(x => entries.forEach(y => {
        assert.strictEqual(
            sign(Order.compare(x, y)),
            sign(-Order.compare(y, x)),
            'compare must be antisymmetric');
    }));
    entries.forEach(x => entries.forEach(y => entries.forEach(z => {
        if (Order.compare(x, y) < 0 && Order.compare(y, z) < 0) {
            assert.ok(Order.compare(x, z) < 0, 'compare must be transitive');
        }
    })));
});

/*  The property that actually matters (R-36): the result must not depend on the
    order the entries arrived in. This is the one that catches a regression to
    "serve the tail as it arrived". */
test('R-36: the sorted tail is independent of arrival order', () => {
    const base = [
        e(4, 'B', 'm2'), e(4, 'A', 'm1'), e(7, 'A', 'm4'),
        e(4, 'A', 'm3'), e(5, 'C', 'm0'), e(7, 'A', 'm5')
    ];
    const expected = Order.sortTail(base).map(m => m.id).join(',');

    // every permutation of six entries
    const permute = function* (arr) {
        if (arr.length <= 1) { yield arr; return; }
        for (let i = 0; i < arr.length; i++) {
            const rest = arr.slice(0, i).concat(arr.slice(i + 1));
            for (const p of permute(rest)) { yield [arr[i], ...p]; }
        }
    };
    let n = 0;
    for (const perm of permute(base)) {
        n++;
        assert.strictEqual(Order.sortTail(perm).map(m => m.id).join(','), expected);
    }
    assert.strictEqual(n, 720, 'all permutations were checked');
});

test('sortTail does not mutate its input', () => {
    const input = [e(2, 'B', 'b'), e(1, 'A', 'a')];
    const copy = input.slice();
    Order.sortTail(input);
    assert.deepStrictEqual(input, copy);
});

/*  Lamport clocks are numbers and must be compared as numbers. A string compare
    puts 10 before 9, which would reorder any pad busy enough to reach two
    digits -- silently, and only under load. */
test('lamport clocks compare numerically, not lexically', () => {
    const sorted = Order.sortTail([e(10, 'A', 'x'), e(9, 'A', 'y'), e(100, 'A', 'z')]);
    assert.deepStrictEqual(sorted.map(m => m.l), [9, 10, 100]);
});

test('rejects entries that cannot be totally ordered', () => {
    assert.throws(() => Order.sortTail([e(1, 'A', 'a'), { l: 1, o: 'A' }]),
        /E_ORDER/, 'a missing id makes the order non-total; fail loudly');
    assert.throws(() => Order.sortTail([{ l: '1', o: 'A', id: 'a' }]),
        /E_ORDER/, 'a non-numeric lamport clock must not be silently coerced');
});
