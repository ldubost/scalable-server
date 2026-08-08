// §11.1 / §11.3 experiment 5: can the transient branch divergence be made DURABLE
// by a checkpoint, and what does that mean for TRIM (spec R-11, open question 3)?
//
// exp4 showed: two replicas holding the SAME message set can transiently pick
// different branches when the longest chain is tied at depth >= 2, and that any
// later patch heals it. A checkpoint is different: CryptPad trims history to the
// last checkpoint, so a checkpoint taken on the losing branch could freeze it.

const { generate } = require('./harness.js');
const ChainPad = require(process.env.CHAINPAD_PATH || 'chainpad');

const CFG = { logLevel: 0, avgSyncMilliseconds: 1e9 };

const permutations = function* (arr) {
    if (arr.length <= 1) { yield arr; return; }
    for (let i = 0; i < arr.length; i++) {
        const rest = arr.slice(0, i).concat(arr.slice(i + 1));
        for (const p of permutations(rest)) { yield [arr[i], ...p]; }
    }
};

const mkReplay = (msgs, extra) => {
    const cp = ChainPad.create(Object.assign({ userName: 'r', noPrune: true }, CFG, extra));
    cp.onMessage(() => {});
    cp.start(); cp.abort();
    msgs.forEach((m) => cp.message(m));
    return cp;
};

const spread = (msgs, extra) => {
    const s = new Map();
    for (const p of permutations(msgs)) {
        let d; try { d = mkReplay(p, extra).getAuthDoc(); } catch (e) { d = 'THREW ' + e.message; }
        s.set(d, (s.get(d) || 0) + 1);
    }
    return s;
};

// author one patch on top of a given message set, returning the emitted message
const authorOn = async (msgs, text, extra) => {
    const cp = ChainPad.create(Object.assign({ userName: 'w', noPrune: true }, CFG, extra));
    let out = null;
    cp.onMessage((m, cb) => { if (!out) { out = m; } setTimeout(cb, 0); });
    cp.start();
    msgs.forEach((m) => cp.message(m));
    cp.contentUpdate(cp.getUserDoc() + text);
    cp.sync();
    await new Promise((r) => setTimeout(r, 60));
    return out;
};

const main = async () => {
    const { log } = await generate({
        nClients: 3,
        rounds: 4,
        edit: (c, r) => { c.cp.contentUpdate(c.cp.getUserDoc() + `${c.name}${r} `); },
    });

    // find the smallest prefix whose replay is order-dependent
    let k = null;
    for (let i = 1; i <= 8; i++) {
        if (spread(log.slice(0, i)).size > 1) { k = i; break; }
    }
    if (k === null) { console.log('no order-dependent prefix'); process.exit(2); }
    const prefix = log.slice(0, k);
    console.log(`order-dependent prefix: ${k} messages`);

    // the distinct branch outcomes, and one ordering that produces each
    const byDoc = new Map();
    for (const p of permutations(prefix)) {
        const d = mkReplay(p).getAuthDoc();
        if (!byDoc.has(d)) { byDoc.set(d, p); }
    }
    const variants = [...byDoc.entries()];
    console.log(`branch outcomes (${variants.length}):`);
    variants.forEach(([d]) => console.log(`   ${JSON.stringify(d)}`));

    // ------------------------------------------------------------------
    // Replica A settled on branch 0, replica B on branch 1.
    // A client on each authors a patch. Both patches enter the shared log.
    console.log('\n=== both replicas keep editing their own branch, then exchange ===');
    const pA = await authorOn(variants[0][1], 'PA ');
    const pB = await authorOn(variants[1][1], 'PB ');
    const merged = prefix.concat([pA, pB]);
    const s = spread(merged);
    console.log(`  merged set (${merged.length} msgs), all orderings: ${s.size} distinct doc(s)`);
    for (const [d, n] of s) { console.log(`     [${n}] ${JSON.stringify(d)}`); }
    console.log(`  => ${s.size === 1 ? 'CONVERGES (transient healed)' : 'STILL SPLIT'}`);

    // ------------------------------------------------------------------
    // Now the dangerous variant: history is TRIMMED to a checkpoint that each
    // replica chose on its own branch. Simulate by replaying only the messages
    // at/after each replica's chosen tip -- i.e. the two replicas serve
    // different truncated histories.
    console.log('\n=== durable case: each replica trims to a checkpoint on ITS branch ===');
    console.log('  (simulated: replica serves only its own branch as truncated history)');
    const trimmedA = [pA];
    const trimmedB = [pB];
    const dA = (() => { try { return mkReplay(trimmedA).getAuthDoc(); } catch (e) { return 'THREW'; } })();
    const dB = (() => { try { return mkReplay(trimmedB).getAuthDoc(); } catch (e) { return 'THREW'; } })();
    console.log(`  replica A serves trimmed history -> ${JSON.stringify(dA)}`);
    console.log(`  replica B serves trimmed history -> ${JSON.stringify(dB)}`);
    console.log(`  => ${dA === dB ? 'same' : 'DURABLE DIVERGENCE: trimming freezes the branch choice'}`);
    console.log('\n  (a message whose parent was trimmed away can never be linked to the');
    console.log('   root again -- handleMessage returns at the "not connected to root" branch,');
    console.log('   chainpad.dist.js:1303. So TRIM must not run while replicas can still');
    console.log('   disagree about which branch is best.)');

    process.exit(0);
};

main().catch((e) => { console.error(e); process.exit(3); });
