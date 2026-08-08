// §11.1 experiment 4: pin down the mechanism of the prefix order-dependence
// found in exp3, and establish whether it is transient (self-healing) or durable.
//
// Hypothesis: chainpad.dist.js getBestChild() (l.1225) picks among equal-parentCount
// siblings by ARRIVAL order (messagesByParent insertion order, l.985) with no hash
// tiebreak, unlike handleMessage() (l.1345) which does use strcmp on hashOf.
// So a message set that leaves two branches tied resolves differently by order.
//
// Questions answered here:
//   Q-a  minimal reproduction
//   Q-b  is the divergence confined to sets with a TIED longest chain?
//   Q-c  does it self-heal once the tie is broken by any later message?
//   Q-d  does it self-heal if a client on the "wrong" branch authors a new patch?

const { generate } = require('./harness.js');
const ChainPad = require(process.env.CHAINPAD_PATH || 'chainpad');

const CFG = { logLevel: 0, noPrune: true, checkpointInterval: 1e9, avgSyncMilliseconds: 1e9 };

const permutations = function* (arr) {
    if (arr.length <= 1) { yield arr; return; }
    for (let i = 0; i < arr.length; i++) {
        const rest = arr.slice(0, i).concat(arr.slice(i + 1));
        for (const p of permutations(rest)) { yield [arr[i], ...p]; }
    }
};

const mkReplay = (msgs) => {
    const cp = ChainPad.create(Object.assign({ userName: 'r' }, CFG));
    cp.onMessage(() => {});
    cp.start(); cp.abort();
    msgs.forEach((m) => cp.message(m));
    return cp;
};

// how many distinct authDocs over all permutations of `msgs`
const spread = (msgs) => {
    const s = new Map();
    for (const p of permutations(msgs)) {
        let d; try { d = mkReplay(p).getAuthDoc(); } catch (e) { d = 'THREW ' + e.message; }
        s.set(d, (s.get(d) || 0) + 1);
    }
    return s;
};

// describe the chain state: longest-chain length and whether it is tied
const chainInfo = (cp) => {
    const rt = cp._;
    const depth = {};
    const pc = (h) => {
        if (h in depth) { return depth[h]; }
        const m = rt.messages[h];
        if (!m) { return -1; }
        if (m === rt.rootMessage) { return (depth[h] = 0); }
        const p = pc(m.lastMsgHash);
        return (depth[h] = p < 0 ? -1 : p + 1);
    };
    const all = Object.keys(rt.messages);
    all.forEach(pc);
    const linked = all.filter((h) => depth[h] >= 0);
    const max = Math.max(...linked.map((h) => depth[h]));
    const tips = linked.filter((h) => depth[h] === max);
    return { max, tips, best: rt.best.hashOf, tied: tips.length > 1 };
};

const main = async () => {
    // reproduce exp3's generation exactly
    const { log } = await generate({
        nClients: 3,
        rounds: 4,
        edit: (c, r) => { c.cp.contentUpdate(c.cp.getUserDoc() + `${c.name}${r} `); },
    });

    console.log('=== Q-a/Q-b: which prefixes diverge, and are they exactly the TIED ones? ===');
    console.log('k  = prefix length');
    console.log('nd = distinct authDocs over all permutations of that prefix');
    console.log('tied = does the prefix leave >1 longest-chain tip (in server order)?\n');
    console.log('  k  nd  tied  tips@maxdepth');
    let hypothesisHolds = true;
    for (let k = 1; k <= Math.min(log.length, 8); k++) {
        const prefix = log.slice(0, k);
        const nd = spread(prefix).size;
        const info = chainInfo(mkReplay(prefix));
        const agree = (nd > 1) === info.tied;
        if (!agree) { hypothesisHolds = false; }
        console.log(`  ${String(k).padStart(2)}  ${String(nd).padStart(2)}  ${String(info.tied).padStart(5)}  ${info.tips.length} @ depth ${info.max}   ${agree ? '' : '  <-- hypothesis violated'}`);
    }
    console.log(`\nhypothesis "diverges iff longest chain is tied": ${hypothesisHolds ? 'HOLDS' : 'VIOLATED'}`);

    // ---------------------------------------------------------------- Q-c
    console.log('\n=== Q-c: does adding the rest of the batch heal it? ===');
    for (let k = 4; k <= Math.min(log.length, 9); k++) {
        const s = spread(log.slice(0, k));
        console.log(`  prefix ${k}: ${s.size} distinct -> ${s.size === 1 ? 'healed/never split' : 'SPLIT'}`);
    }

    // ---------------------------------------------------------------- Q-d
    console.log('\n=== Q-d: does one new patch authored on a diverged replica heal it? ===');
    // find a prefix that splits
    let bad = null;
    for (let k = 1; k <= 9; k++) {
        const s = spread(log.slice(0, k));
        if (s.size > 1) { bad = k; break; }
    }
    if (bad === null) { console.log('  no splitting prefix found'); process.exit(0); }
    const prefix = log.slice(0, bad);
    console.log(`  using splitting prefix of ${bad} messages`);

    // two replicas: same SET, different order, landing on different branches
    const orders = [];
    for (const p of permutations(prefix)) { orders.push(p); }
    const byDoc = new Map();
    orders.forEach((p) => {
        const d = mkReplay(p).getAuthDoc();
        if (!byDoc.has(d)) { byDoc.set(d, p); }
    });
    const variants = [...byDoc.entries()];
    console.log(`  ${variants.length} distinct branch outcomes:`);
    variants.forEach(([d]) => console.log(`     ${JSON.stringify(d)}`));

    // Replica A takes variant 0, replica B takes variant 1.
    // A client on B authors one new patch; that patch is then shipped to A.
    const A = mkReplay(variants[0][1]);
    const B = mkReplay(variants[1][1]);

    // make B emit a real patch on top of ITS chosen branch
    const Bw = ChainPad.create(Object.assign({ userName: 'Bw' }, CFG));
    let emitted = null;
    Bw.onMessage((m, cb) => { emitted = m; setTimeout(cb, 0); });
    Bw.start();
    variants[1][1].forEach((m) => Bw.message(m));
    Bw.contentUpdate(Bw.getUserDoc() + 'X ');
    Bw.sync();
    await new Promise((r) => setTimeout(r, 50));

    if (!emitted) { console.log('  B produced no patch'); process.exit(0); }
    A.message(emitted);
    B.message(emitted);
    console.log(`\n  after B authors one patch and it reaches A:`);
    console.log(`     A: ${JSON.stringify(A.getAuthDoc())}`);
    console.log(`     B: ${JSON.stringify(B.getAuthDoc())}`);
    console.log(`     converged: ${A.getAuthDoc() === B.getAuthDoc()}`);

    // and a fresh replica replaying the whole augmented set in any order
    const aug = prefix.concat([emitted]);
    const s2 = spread(aug);
    console.log(`     fresh replay of the augmented set, all orders: ${s2.size} distinct doc(s)`);

    process.exit(0);
};

main().catch((e) => { console.error(e); process.exit(3); });
