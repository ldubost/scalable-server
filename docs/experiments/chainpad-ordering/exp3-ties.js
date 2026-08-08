// §11.1 experiment 3: the adversarial cases.
//
//  (a) equal-length DANGLING siblings -- a fork that no later message resolves.
//      This is where getBestChild's missing hash tiebreak could bite, and it is
//      also exactly what a replica sees mid-partition.
//  (b) every PREFIX of the log (a replica that has only part of the history),
//      each under many orderings -- two replicas holding the same prefix in
//      different orders must still agree.
//  (c) larger sets, exhaustive where feasible.

const { generate, replay } = require('./harness.js');

const mkRnd = (seed) => () => {
    seed = (seed * 1664525 + 1013904223) >>> 0;
    return seed / 4294967296;
};
const shuffle = (arr, rnd) => {
    const a = arr.slice();
    for (let i = a.length - 1; i > 0; i--) {
        const j = Math.floor(rnd() * (i + 1));
        [a[i], a[j]] = [a[j], a[i]];
    }
    return a;
};
const permutations = function* (arr) {
    if (arr.length <= 1) { yield arr; return; }
    for (let i = 0; i < arr.length; i++) {
        const rest = arr.slice(0, i).concat(arr.slice(i + 1));
        for (const p of permutations(rest)) { yield [arr[i], ...p]; }
    }
};

const docsFor = (msgs, { exhaustive, samples = 400 }) => {
    const results = new Map();
    const add = (perm) => {
        let doc;
        try { doc = replay(perm, {}); } catch (e) { doc = `THREW: ${e.message}`; }
        if (!results.has(doc)) { results.set(doc, 0); }
        results.set(doc, results.get(doc) + 1);
    };
    if (exhaustive) {
        for (const p of permutations(msgs)) { add(p); }
    } else {
        add(msgs);
        add(msgs.slice().reverse());
        for (let s = 1; s <= samples; s++) { add(shuffle(msgs, mkRnd(s))); }
    }
    return results;
};

const report = (label, results) => {
    const ok = results.size === 1;
    console.log(`  ${ok ? 'OK  ' : 'FAIL'} ${label}: ${results.size} distinct doc(s)`);
    if (!ok) {
        for (const [doc, count] of results) {
            console.log(`         [${count}] ${JSON.stringify(doc)}`);
        }
    }
    return ok;
};

const main = async () => {
    let allOk = true;

    // ---------------------------------------------------------------- (a)
    // Build a set whose FINAL fork is left dangling with equal-length branches:
    // generate normally, then keep only up to the last forked round, dropping the
    // reconciling tail. Every truncation point is tested.
    console.log('=== (a)+(b) prefixes and dangling equal-length forks ===');
    const { log, docs } = await generate({
        nClients: 3,
        rounds: 4,
        edit: (c, r) => { c.cp.contentUpdate(c.cp.getUserDoc() + `${c.name}${r} `); },
    });
    console.log(`full log: ${log.length} messages; live clients agree: ${docs.every((d) => d.doc === docs[0].doc)}`);

    for (let k = 1; k <= log.length; k++) {
        const prefix = log.slice(0, k);
        const exhaustive = k <= 7;
        const res = docsFor(prefix, { exhaustive });
        const ok = report(`prefix of ${k} msg${k > 1 ? 's' : ''} (${exhaustive ? 'exhaustive' : 'sampled'})`, res);
        allOk = allOk && ok;
    }

    // ---------------------------------------------------------------- (c)
    // Wider fork: 4 concurrent authors on the SAME parent, nothing after it.
    console.log('\n=== (c) width-4 fork, nothing resolves it ===');
    const g2 = await generate({
        nClients: 4,
        rounds: 1,
        edit: (c) => { c.cp.contentUpdate(c.cp.getUserDoc() + `${c.name} `); },
    });
    console.log(`log: ${g2.log.length} messages; live agree: ${g2.docs.every((d) => d.doc === g2.docs[0].doc)}`);
    // the first 4 messages are the concurrent batch
    const batch = g2.log.slice(0, 4);
    allOk = report('width-4 concurrent batch alone (exhaustive)',
        docsFor(batch, { exhaustive: true })) && allOk;
    allOk = report('full width-4 log (exhaustive)',
        docsFor(g2.log, { exhaustive: g2.log.length <= 8 })) && allOk;

    // ---------------------------------------------------------------- (d)
    // Bigger, longer-running set, sampled hard.
    console.log('\n=== (d) larger set, sampled ===');
    const g3 = await generate({
        nClients: 5,
        rounds: 8,
        edit: (c, r) => { c.cp.contentUpdate(c.cp.getUserDoc() + `${c.name}${r} `); },
    });
    console.log(`log: ${g3.log.length} messages; live agree: ${g3.docs.every((d) => d.doc === g3.docs[0].doc)}`);
    allOk = report('full log, 2000 random orders',
        docsFor(g3.log, { exhaustive: false, samples: 2000 })) && allOk;

    console.log(`\nVERDICT: ${allOk ? 'ORDER-INDEPENDENT in every case tested' : 'ORDER-DEPENDENT in at least one case'}`);
    process.exit(allOk ? 0 : 1);
};

main().catch((e) => { console.error(e); process.exit(3); });
