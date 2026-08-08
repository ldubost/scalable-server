// §11.1 experiment 2: EXHAUSTIVE permutation of a small message set containing
// genuine equal-length forks.
//
// Targets the specific hazard in chainpad.dist.js:
//   storeMessage()   pushes into messagesByParent[parent] in ARRIVAL order (l.985)
//   getBestChild()   scans that array with a strict `>` and no hash tiebreak (l.1225)
// so among two equal-length sibling branches it returns whichever was stored first.
// handleMessage() does have a hash tiebreak (l.1345), but only against the CURRENT
// best -- a sibling discarded by getBestChild is never reconsidered.
//
// If that hazard is reachable, some permutation of a fixed message set yields a
// different authDoc.

const { generate, replay } = require('./harness.js');

// Rebuild the message DAG by inspecting ChainPad's own internal index after a replay.
const dagOf = (log) => {
    const ChainPad = require(process.env.CHAINPAD_PATH || 'chainpad');
    const cp = ChainPad.create({ userName: 'dag', logLevel: 0, noPrune: true,
        checkpointInterval: 1e9, avgSyncMilliseconds: 1e9 });
    cp.onMessage(() => {});
    cp.start(); cp.abort();
    log.forEach((m) => cp.message(m));
    const rt = cp._;
    const byParent = {};
    Object.keys(rt.messages).forEach((h) => {
        const m = rt.messages[h];
        (byParent[m.lastMsgHash] = byParent[m.lastMsgHash] || []).push(h);
    });
    return { rt, byParent };
};

const permutations = function* (arr) {
    if (arr.length <= 1) { yield arr; return; }
    for (let i = 0; i < arr.length; i++) {
        const rest = arr.slice(0, i).concat(arr.slice(i + 1));
        for (const p of permutations(rest)) { yield [arr[i], ...p]; }
    }
};

const factorial = (n) => n <= 1 ? 1 : n * factorial(n - 1);

const main = async () => {
    // deliberately small: 2 clients, 2 forked rounds -> few messages, real forks
    const { log, docs } = await generate({
        nClients: 2,
        rounds: 2,
        edit: (c, r) => { c.cp.contentUpdate(c.cp.getUserDoc() + `${c.name}${r} `); },
    });

    console.log(`message set: ${log.length} messages`);
    console.log(`live clients agree: ${docs.every((d) => d.doc === docs[0].doc)}`);
    console.log(`live authDoc: ${JSON.stringify(docs[0].doc)}`);

    const { byParent } = dagOf(log);
    const forks = Object.entries(byParent).filter(([, kids]) => kids.length > 1);
    console.log(`\nDAG fork points (a parent with >1 child): ${forks.length}`);
    forks.forEach(([p, kids]) => {
        console.log(`  parent ${p.slice(0, 12)}.. -> ${kids.length} children: ${kids.map((k) => k.slice(0, 12)).join(', ')}`);
    });

    if (!forks.length) {
        console.log('\n!! no forks generated -- experiment is vacuous, adjust generation');
        process.exit(2);
    }

    const total = factorial(log.length);
    console.log(`\nexhaustively replaying all ${total} permutations of ${log.length} messages...`);
    if (total > 4000000) {
        console.log('too many; aborting'); process.exit(2);
    }

    const results = new Map();
    let n = 0;
    for (const perm of permutations(log)) {
        n++;
        let doc;
        try { doc = replay(perm, {}); }
        catch (e) { doc = `THREW: ${e.message}`; }
        if (!results.has(doc)) { results.set(doc, { count: 0, example: perm }); }
        results.get(doc).count++;
    }

    console.log(`\n=== ${n} permutations replayed ===`);
    console.log(`distinct resulting documents: ${results.size}`);
    for (const [doc, info] of results) {
        console.log(`  [${info.count} / ${n}] ${JSON.stringify(doc)}`);
    }

    const converged = results.size === 1;
    console.log(`\nVERDICT: ${converged ? 'ORDER-INDEPENDENT over the full permutation space' : 'ORDER-DEPENDENT -- R-1 is load-bearing'}`);

    if (!converged) {
        // print a minimal divergent pair for the writeup
        const entries = [...results.entries()];
        console.log('\ndivergent example orderings (indices into the server-order log):');
        entries.forEach(([doc, info]) => {
            const idx = info.example.map((m) => log.indexOf(m));
            console.log(`  ${JSON.stringify(doc)}  <- order ${idx.join(',')}`);
        });
    }
    process.exit(converged ? 0 : 1);
};

main().catch((e) => { console.error(e); process.exit(3); });
