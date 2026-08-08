// SPDX-License-Identifier: AGPL-3.0-or-later
// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors

/*  The federation total order: `(l, o, id)` (spec §4.2).

        l   Lamport clock
        o   originId of the instance that accepted the message
        id  message id -- getHash(content), globally stable (spec §1.2)

    This is the single definition of "the order", used both by the merge loop to
    build the committed log and by history serving to sort the uncommitted tail.
    They must be the same function; that is the point of the file.

    Why this is correctness rather than tidiness
    --------------------------------------------
    ChainPad is NOT order-independent. Measured in
    docs/experiments/chainpad-ordering: the same set of messages replayed in
    different orders can converge on permanently different documents whenever the
    longest chain is tied at depth >= 2, and the divergence does not heal if both
    replicas keep editing. So two replicas that agree on the *set* but not the
    *order* can serve different documents forever.

    That makes R-1 (same id sequence everywhere) and R-36 (the tail is sorted,
    never served in arrival order) load-bearing. The comparison is deliberately
    strict: anything that cannot be totally ordered throws rather than falling
    back on arrival order, because the failure mode of a silent tie is a
    divergence nobody observes until two instances disagree.
*/

const err = (why, entry) => {
    const e = new Error(`E_ORDER: ${why}`);
    e.entry = entry;
    return e;
};

/*  Validate eagerly. A malformed entry that merely sorts oddly would be a
    silent, permanent divergence; a throw is a bug report. */
const check = (m) => {
    if (!m || typeof (m) !== 'object') {
        throw err('entry is not an object', m);
    }
    if (typeof (m.l) !== 'number' || !Number.isFinite(m.l)) {
        throw err('lamport clock `l` must be a finite number', m);
    }
    if (typeof (m.o) !== 'string' || !m.o) {
        throw err('origin `o` must be a non-empty string', m);
    }
    if (typeof (m.id) !== 'string' || !m.id) {
        throw err('message id `id` must be a non-empty string', m);
    }
    return m;
};

/*  Strings are compared with < / > rather than localeCompare: this must be a
    byte-ish comparison that every instance agrees on, not one that depends on
    the node build's ICU data or the server's locale. */
const cmpStr = (a, b) => (a < b ? -1 : (a > b ? 1 : 0));

/*  Total order on (l, o, id). Returns 0 only for entries that are equal in all
    three components, which -- because `id` is content-derived -- means the same
    message. */
const compare = (a, b) => {
    check(a); check(b);
    if (a.l !== b.l) { return a.l < b.l ? -1 : 1; }   // numeric, never lexical
    const byOrigin = cmpStr(a.o, b.o);
    if (byOrigin) { return byOrigin; }
    return cmpStr(a.id, b.id);
};

// Sort a copy: callers hold on to their arrays, and an in-place sort of a
// caller's pending list has caused enough bugs elsewhere in this codebase.
const sortTail = (entries) => {
    if (!Array.isArray(entries)) { throw err('sortTail expects an array', entries); }
    entries.forEach(check);
    return entries.slice().sort(compare);
};

module.exports = { compare, sortTail, check };
