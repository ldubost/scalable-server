// Experiment harness for federation spec §11.1:
// "Does ChainPad converge given only same-order agreement, or does it also need
//  same-server-timestamp ordering?"
//
// Phase 1 (generate): drive real ChainPad instances through forced concurrent edits
//                     to produce a realistic message SET containing genuine forks.
// Phase 2 (replay):   feed that set, under many different orders, to fresh ChainPad
//                     instances and byte-compare the resulting authDoc.

const CHAINPAD = process.env.CHAINPAD_PATH || 'chainpad';
const ChainPad = require(CHAINPAD);

const defer = (f) => setTimeout(f, 0);

const mkConfig = (name, extra) => Object.assign({
    userName: name,
    logLevel: 0,
    noPrune: true,
    // large so checkpoints don't fire unless a test asks for them
    checkpointInterval: 1e9,
    avgSyncMilliseconds: 1e9,
}, extra || {});

// A client wrapping a real ChainPad, with manual control over when it emits
// and when it receives.
const mkClient = (name, cfg) => {
    const cp = ChainPad.create(mkConfig(name, cfg));
    const client = {
        name,
        cp,
        outbox: [],      // messages it wants to send, with their ack callbacks
        sent: [],        // message strings it has emitted
    };
    cp.onMessage((msg, cb) => {
        client.outbox.push({ msg, cb });
    });
    cp.start();
    return client;
};

// Ask a client to emit exactly one message for its pending local work.
// Resolves with the message string, or null if it had nothing to send.
const emit = (client) => new Promise((resolve) => {
    client.cp.sync();
    defer(() => {
        const item = client.outbox.shift();
        if (!item) { return resolve(null); }
        client.sent.push(item.msg);
        // ack it: this makes the client apply its own message and clears `pending`
        defer(() => {
            item.cb();
            defer(() => resolve(item.msg));
        });
    });
});

const deliver = (client, msg) => { client.cp.message(msg); };

// Generate a message set with `rounds` fork-points of width `nClients`.
// Returns { log, docs } where `log` is the ordered list of message strings the
// simulated server accepted, and `docs` the per-client final authDoc.
const generate = async (opts) => {
    const { nClients = 3, rounds = 4, edit, cfg } = opts;
    const clients = [];
    for (let i = 0; i < nClients; i++) {
        clients.push(mkClient(String.fromCharCode(65 + i), cfg));
    }
    const log = [];

    for (let r = 0; r < rounds; r++) {
        // 1. every client edits locally, in ignorance of the others
        clients.forEach((c, i) => edit(c, r, i));

        // 2. every client emits. Nothing is delivered yet, so all N messages
        //    name the same parent -> a genuine fork of width N.
        const batch = [];
        for (const c of clients) {
            const m = await emit(c);
            if (m) { batch.push({ from: c, msg: m }); }
        }

        // 3. the server appends the batch in this order, and broadcasts
        for (const { from, msg } of batch) {
            log.push(msg);
            for (const c of clients) {
                if (c !== from) { deliver(c, msg); }
            }
        }
    }

    // let everything settle: repeatedly emit until nobody has anything to say
    for (let i = 0; i < 10; i++) {
        let quiet = true;
        for (const c of clients) {
            const m = await emit(c);
            if (m) {
                quiet = false;
                log.push(m);
                for (const o of clients) { if (o !== c) { deliver(o, m); } }
            }
        }
        if (quiet) { break; }
    }

    return {
        log,
        docs: clients.map((c) => ({ name: c.name, doc: c.cp.getAuthDoc() })),
    };
};

// Replay a message list into a fresh ChainPad. Fully synchronous.
const replay = (log, cfg) => {
    const cp = ChainPad.create(mkConfig('replay', cfg));
    cp.onMessage(() => {});   // never emits: we never call sync()
    cp.start();
    cp.abort();               // kill the sync timer; we only feed it history
    log.forEach((m) => { cp.message(m); });
    return cp.getAuthDoc();
};

module.exports = { mkClient, emit, deliver, generate, replay, defer };
