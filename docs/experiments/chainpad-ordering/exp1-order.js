// §11.1 experiment 1: is ChainPad's converged authDoc a function of the message
// SET alone, or does the delivery ORDER change it?
//
// If order-independent  -> R-1 (same id sequence on every replica) is more than
//                          enough, and `t` never needs to enter the sort key.
// If order-dependent    -> R-1 is load-bearing and must be exact.

const { generate, replay } = require('./harness.js');

const shuffle = (arr, rnd) => {
    const a = arr.slice();
    for (let i = a.length - 1; i > 0; i--) {
        const j = Math.floor(rnd() * (i + 1));
        [a[i], a[j]] = [a[j], a[i]];
    }
    return a;
};

// deterministic PRNG so the run is reproducible
const mkRnd = (seed) => () => {
    seed = (seed * 1664525 + 1013904223) >>> 0;
    return seed / 4294967296;
};

const main = async () => {
    const nClients = 3;
    const rounds = 5;

    const { log, docs } = await generate({
        nClients,
        rounds,
        edit: (c, r) => {
            // each client appends its own marker at its own position
            const doc = c.cp.getUserDoc();
            c.cp.contentUpdate(doc + `${c.name}${r} `);
        },
    });

    console.log(`generated ${log.length} messages from ${nClients} clients over ${rounds} forked rounds`);
    console.log('live client authDocs after convergence:');
    docs.forEach((d) => console.log(`  ${d.name}: ${JSON.stringify(d.doc)}`));
    const liveAgree = docs.every((d) => d.doc === docs[0].doc);
    console.log(`live clients agree: ${liveAgree}`);

    // --- the actual question -------------------------------------------------
    const canonical = replay(log, {});
    console.log(`\nreplay in server order: ${JSON.stringify(canonical)}`);
    console.log(`matches live clients:   ${canonical === docs[0].doc}`);

    const results = new Map();
    results.set(canonical, ['server-order']);

    const N = 300;
    for (let s = 1; s <= N; s++) {
        const perm = shuffle(log, mkRnd(s));
        let doc;
        try {
            doc = replay(perm, {});
        } catch (e) {
            doc = `THREW: ${e.message}`;
        }
        if (!results.has(doc)) { results.set(doc, []); }
        results.get(doc).push(`seed-${s}`);
    }

    // also try strict reverse
    try {
        const rev = replay(log.slice().reverse(), {});
        if (!results.has(rev)) { results.set(rev, []); }
        results.get(rev).push('reversed');
    } catch (e) {
        const k = `THREW: ${e.message}`;
        if (!results.has(k)) { results.set(k, []); }
        results.get(k).push('reversed');
    }

    console.log(`\n=== ${N + 2} orderings of the SAME message set ===`);
    console.log(`distinct resulting documents: ${results.size}`);
    for (const [doc, who] of results) {
        console.log(`  [${who.length} orderings] ${JSON.stringify(doc)}`);
        if (who.length <= 5) { console.log(`      e.g. ${who.join(', ')}`); }
    }

    console.log(`\nVERDICT: authDoc is ${results.size === 1 ? 'ORDER-INDEPENDENT' : 'ORDER-DEPENDENT'}`);
};

main().catch((e) => { console.error(e); process.exit(1); });
