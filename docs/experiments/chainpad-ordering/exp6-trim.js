// SPDX-License-Identifier: AGPL-3.0-or-later
// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors

/*  §11.3: can a TRIM freeze a branch divergence permanently?
 *
 *  exp5 tried this and did not discriminate: it replayed a lone patch whose
 *  parent had been trimmed, which yields an empty document on both sides for a
 *  reason unrelated to the question. This version builds the real situation.
 *
 *  The scenario R-38 is meant to prevent:
 *
 *    - two replicas hold the same message set, tied at depth >= 2, so by §11.1
 *      they may pick different branches;
 *    - each keeps editing its own branch, so (per exp5) the tie persists;
 *    - each takes a CHECKPOINT on its own branch;
 *    - history is trimmed to that checkpoint, which is what CryptPad's
 *      GET_HISTORY actually serves.
 *
 *  Question: after the trim, can the two replicas still converge if they
 *  exchange everything they have? If not, TRIM must be gated on a watermark
 *  that includes every peer (R-38).
 *
 *  A trimmed history is modelled the way the server really serves one: the
 *  checkpoint first, then everything after it. Messages before the checkpoint
 *  are gone from the wire, exactly as trimChannel would leave them.
 */

const ChainPad = require(process.env.CHAINPAD_PATH || 'chainpad');



// checkpointInterval must be small or no checkpoint is ever emitted
const CP_INTERVAL = 2;

const mkClient = (name, extra) => {
    const cp = ChainPad.create(Object.assign({
        userName: name, logLevel: 0,
        checkpointInterval: CP_INTERVAL,
        avgSyncMilliseconds: 1e9,
        noPrune: true
    }, extra));
    const out = { name, cp, outbox: [], sent: [] };
    cp.onMessage((msg, cb) => { out.outbox.push({ msg, cb }); });
    cp.start();
    return out;
};

const defer = (f) => setTimeout(f, 0);

const emit = (client) => new Promise(resolve => {
    client.cp.sync();
    defer(() => {
        const item = client.outbox.shift();
        if (!item) { return resolve(null); }
        client.sent.push(item.msg);
        defer(() => { item.cb(); defer(() => resolve(item.msg)); });
    });
});

const replay = (msgs, extra) => {
    const cp = ChainPad.create(Object.assign({
        userName: 'r', logLevel: 0,
        checkpointInterval: CP_INTERVAL,
        avgSyncMilliseconds: 1e9,
        noPrune: true
    }, extra));
    cp.onMessage(() => {});
    cp.start(); cp.abort();
    msgs.forEach(m => cp.message(m));
    return cp;
};

const isCheckpoint = (msg) => {
    try {
        const parsed = JSON.parse(msg);
        // Message.CHECKPOINT === 4 in chainpad's Message module
        return parsed[0] === 4;
    } catch (err) { return false; }
};

const main = async () => {
    console.log('=== building two replicas that disagree about the best branch ===');

    /*  Force a fork: two clients edit and emit before either sees the other,
        so both patches name the same parent. */
    const A = mkClient('A');
    const B = mkClient('B');

    A.cp.contentUpdate('AAA ');
    B.cp.contentUpdate('BBB ');
    await emit(A);
    await emit(B);
    console.log(`fork created: 2 concurrent patches on the same parent`);

    /*  Each replica now keeps editing its OWN branch and never sees the other's
        work -- a partition. Push each side deep enough to cross a checkpoint. */
    const growth = 6;
    for (let i = 0; i < growth; i++) {
        A.cp.contentUpdate(A.cp.getUserDoc() + `a${i} `);
        await emit(A);
        B.cp.contentUpdate(B.cp.getUserDoc() + `b${i} `);
        await emit(B);
    }

    const cpA = A.sent.filter(isCheckpoint);
    const cpB = B.sent.filter(isCheckpoint);
    console.log(`A produced ${cpA.length} checkpoint(s), B produced ${cpB.length}`);
    if (!cpA.length || !cpB.length) {
        console.log('\n!! no checkpoints emitted -- experiment is vacuous');
        process.exit(2);
    }

    console.log(`A doc: ${JSON.stringify(A.cp.getAuthDoc())}`);
    console.log(`B doc: ${JSON.stringify(B.cp.getAuthDoc())}`);

    // ---------------------------------------------------------------- untrimmed
    /*  Control: the partition heals if BOTH full histories are exchanged.
        If this fails the experiment says nothing about trimming. */
    const full = A.sent.concat(B.sent);
    const bothFull = replay(full);
    const aThenB = replay(A.sent.concat(B.sent));
    const bThenA = replay(B.sent.concat(A.sent));
    console.log('\n=== control: exchange FULL histories ===');
    console.log(`  A-then-B: ${JSON.stringify(aThenB.getAuthDoc())}`);
    console.log(`  B-then-A: ${JSON.stringify(bThenA.getAuthDoc())}`);
    const fullConverges = aThenB.getAuthDoc() === bThenA.getAuthDoc();
    console.log(`  converges: ${fullConverges}`);
    void bothFull;

    // ---------------------------------------------------------------- trimmed
    /*  Now the real question. Each side trims to its own last checkpoint, which
        is what the server serves after trimChannel: the checkpoint, then
        everything after it. */
    const trimTo = (sent) => {
        let lastCp = -1;
        sent.forEach((m, i) => { if (isCheckpoint(m)) { lastCp = i; } });
        return sent.slice(lastCp);
    };
    const trimmedA = trimTo(A.sent);
    const trimmedB = trimTo(B.sent);
    console.log(`\n=== after TRIM: A serves ${trimmedA.length} msgs, B serves ${trimmedB.length} ===`);
    console.log(`  A alone: ${JSON.stringify(replay(trimmedA).getAuthDoc())}`);
    console.log(`  B alone: ${JSON.stringify(replay(trimmedB).getAuthDoc())}`);

    /*  The partition ends and the two replicas exchange their trimmed
        histories. Each order models one replica's view. */
    const mergedAB = replay(trimmedA.concat(trimmedB));
    const mergedBA = replay(trimmedB.concat(trimmedA));
    const docAB = mergedAB.getAuthDoc();
    const docBA = mergedBA.getAuthDoc();
    console.log(`\n=== exchange TRIMMED histories ===`);
    console.log(`  replica that had A's trim, then receives B's: ${JSON.stringify(docAB)}`);
    console.log(`  replica that had B's trim, then receives A's: ${JSON.stringify(docBA)}`);
    const trimConverges = docAB === docBA;
    console.log(`  converges: ${trimConverges}`);

    /*  And the decisive check: does either side's work survive at all? A
        message whose parent was trimmed away can never be relinked
        (chainpad.dist.js:1303), so the other branch's edits are simply lost. */
    const survivesAB = docAB.includes('a0') && docAB.includes('b0');
    const survivesBA = docBA.includes('a0') && docBA.includes('b0');
    console.log(`  both branches' content present: AB=${survivesAB} BA=${survivesBA}`);

    console.log('\n=== VERDICT (spec §11.3 / R-38) ===');
    if (!fullConverges) {
        console.log('INCONCLUSIVE: the untrimmed control did not converge either.');
        process.exit(2);
    }
    if (trimConverges && survivesAB && survivesBA) {
        console.log('TRIM IS SAFE in this scenario: the replicas still converged');
        console.log('and no branch was lost. R-38 may be weaker than stated.');
        process.exit(0);
    }
    console.log('TRIM FREEZES THE DIVERGENCE.');
    console.log('Untrimmed, the same two histories converge; trimmed, they do not');
    console.log(`(converged=${trimConverges}, both-branches-survive=${survivesAB && survivesBA}).`);
    console.log('R-38 is necessary: a TRIM must not commit unless every peer,');
    console.log('including evicted ones, is at or beyond the trim point.');
    process.exit(0);
};

main().catch(e => { console.error(e); process.exit(3); });
