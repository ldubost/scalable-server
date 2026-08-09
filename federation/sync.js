// SPDX-License-Identifier: AGPL-3.0-or-later
// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors

/*  The channel wire protocol (spec §5.2), milestone M1.

        SUBSCRIBE / SUBSCRIBE_OK   join a channel's replica set        R-25, R-34
        SYNC_REQ / SYNC_RES        anti-entropy backfill               R-19
        PUBLISH                    live push of one envelope
        UNSUBSCRIBE                leave, unilaterally                 R-32

    M1 is L0: one origin writes, every other member mirrors. So `SYNC_RES` is
    just the committed log from a known point, and there is no merge — the
    mirror appends in the order the origin committed, which by construction is
    the order every replica ends up with (R-1).

    Two properties worth keeping when M3 replaces the middle of this file:

      * **Sync is authoritative, PUBLISH is an optimisation.** A dropped or
        skipped live push costs latency, never consistency, because the next
        sync finds the gap. Nothing here may become correct only because
        PUBLISH arrived.
      * **The requester drives.** A mirror asks for what it is missing rather
        than the origin tracking what it has sent, so a restart on either side
        needs no shared state to recover.
*/

const Envelope = require('../common/federation/envelope.js');
const Ids = require('../common/federation/ids.js');

// how many envelopes one SYNC_RES may carry
// a split pad stays split, so say so occasionally rather than every heartbeat
const DIVERGENCE_LOG_EVERY = 5 * 60 * 1000;

/*  How long a cached log length may be trusted.

    Counting the committed log means reading all of it, so this must not sit on
    the heartbeat path: at a 2 s beat, a pad of a few thousand messages would be
    re-read thirty times a minute per peer, and — worse — an early version made
    the beat *wait* for the count, so a slow read stopped the heartbeat
    altogether and with it the watermark that keeps an idle channel committing.

    Staleness costs nothing here. The number exists to notice a divergence, a
    divergence persists, and acting on it is rate-limited well beyond this. */
const COUNT_TTL = 30 * 1000;

/*  The last known length of a channel's committed log, refreshed in the
    background. Returns undefined until the first read completes, which simply
    means the first heartbeat for a channel carries no count. */
const logCount = (Env, channel) => {
    const hit = Env.logCounts.get(channel);
    const fresh = hit && (Date.now() - hit.at) < COUNT_TTL;
    if (!fresh && !Env.logCountsInFlight.has(channel)) {
        Env.logCountsInFlight.add(channel);
        toStorage(Env, 'FED_HEAD', { channel }, (e, info) => {
            Env.logCountsInFlight.delete(channel);
            if (e || typeof (info?.count) !== 'number') { return; }
            Env.logCounts.set(channel, { at: Date.now(), n: info.count });
        });
    }
    return hit ? hit.n : undefined;
};

const SYNC_BATCH = 256;

/*  requestSync and the SYNC_RES handler call each other: a response that is not
    the last batch asks for the next one. ingest is likewise used by both SYNC_RES
    and PUBLISH before it is defined. Declared here so neither has to come first. */
let requestSync, ingest;

const err = (session, channel, code, detail) => {
    session.send({ type: 'ERROR', c: channel, code, detail });
};

/*  Ask core (which routes to the owning storage node) about a channel. Every
    storage call from here goes through this, so the routing lives in one place. */
const toStorage = (Env, command, args, cb) => {
    const coreId = Env.getCoreId(args.channel);
    Env.interface.sendQuery(coreId, command, args, (answer) => {
        cb(answer?.error, answer?.data);
    });
};

// --------------------------------------------------------------- SUBSCRIBE

/*  A peer asks to join a channel's replica set.

    Authorisation is R-25: the capability is signed by the *pad's* key and
    verified against the `validateKey` this server already stores. That is the
    whole point — proving the right to replicate needs exactly what proving the
    right to write needs, and the server learns nothing it did not already hold.
*/
const onSubscribe = (Env, session, frame) => {
    const channel = frame?.c;
    if (typeof (channel) !== 'string' || !channel) {
        return void err(session, channel, 'EBADCHANNEL');
    }

    toStorage(Env, 'FED_STATE', { channel }, (e, state) => {
        if (e) { return void err(session, channel, 'ESTATE', String(e)); }
        if (!state) { return void err(session, channel, 'ENOTFEDERATED'); }

        /*  R-21: the validateKey is what makes this the same pad. A peer asking
            under a different key is asking about a different document, however
            good its capability. */
        if (frame.validateKey && frame.validateKey !== state.validateKey) {
            return void err(session, channel, 'EVALIDATEKEY');
        }

        const check = Env.verifyCapability(frame.cap, state.validateKey, {
            channel,
            from: Env.identity.originId,
            to: session.originId
        });
        if (check.error) {
            Env.Log.warn('FEDERATION_SUBSCRIBE_REFUSED', {
                channel, peer: Env.policy.describe(session.originId), error: check.error
            });
            return void err(session, channel, check.error);
        }

        toStorage(Env, 'FED_HEAD', { channel }, (e2, info) => {
            if (e2) { return void err(session, channel, 'EHEAD', String(e2)); }

            toStorage(Env, 'FED_METADATA', { channel }, (e3, metadata) => {
            if (e3) { return void err(session, channel, 'EMETA', String(e3)); }

            /*  Register the subscriber before answering, so a write that lands
                between the answer and the registration is still pushed. The
                cost of being early is a duplicate, which R-18 makes a no-op;
                the cost of being late is a lost message. */
            Env.subscriptions.add(session, channel);
            /*  And durably, by originId: this session will not last forever and
                the peer cannot re-subscribe without a capability only a browser
                can mint (R-52). */
            Env.subscriptions.restore(channel, [session.originId]);
            // we hold the pad: at L1 we are the anchor, at L2 just a member
            Env.markFederated(channel, { mirror: false, level: state.level });

            Env.Log.info('FEDERATION_SUBSCRIBED', {
                channel, peer: Env.policy.describe(session.originId)
            });
            session.send({
                type: 'SUBSCRIBE_OK',
                c: channel,
                members: state.members,       // R-34
                origin: state.origin,
                validateKey: state.validateKey,
                level: state.level,
                // R-21: the replica seeds its own metadata from this
                metadata,
                heads: info.head ? [info.head] : []
            });
            });
        });
    });
};

/*  Our SUBSCRIBE was accepted. Record the replica set locally, then start
    backfilling from whatever we already hold. */
const onSubscribeOk = (Env, session, frame) => {
    const channel = frame?.c;
    const pending = Env.pendingSubscribes.get(channel);
    if (pending) {
        Env.pendingSubscribes.delete(channel);
        clearTimeout(pending.timer);
    }

    toStorage(Env, 'FED_ENABLE', {
        channel,
        validateKey: frame.validateKey,
        self: Env.identity.originId,
        origin: frame.origin || session.originId,
        members: frame.members || [session.originId],
        metadata: frame.metadata,
        level: frame.level
    }, (e) => {
        /*  Already federated is fine: this is a resubscribe after a restart,
            which must not reset the replica's state. */
        if (e && !/EEXIST|EPRECONDITION/.test(String(e))) {
            Env.Log.error('FEDERATION_ENABLE_FAILED', { channel, error: String(e) });
            pending?.cb?.(String(e));
            return;
        }
        /*  At L1 we are a mirror and must forward writes to the anchor. At L2
            there is no anchor: we accept writes here and the merge orders them. */
        const isL2 = frame.level === 'L2';
        Env.subscriptions.restore(channel,
            (frame.members || [session.originId])
                .filter(m => m && m !== Env.identity.originId));
        Env.markFederated(channel, { mirror: !isL2, level: frame.level });
        pending?.cb?.();
        requestSync(Env, session, channel);
    });
};

// --------------------------------------------------------------- SYNC

/*  Ask a peer for everything after the last message we hold. Sending our head
    rather than a count means the request is meaningful even if our logs differ
    in length, and it is what lets the responder detect a divergence. */
requestSync = (Env, session, channel) => {
    toStorage(Env, 'FED_HEAD', { channel }, (e, info) => {
        if (e) {
            return void Env.Log.error('FEDERATION_SYNC_HEAD_ERROR',
                { channel, error: String(e) });
        }
        session.send({ type: 'SYNC_REQ', c: channel, sinceId: info?.head });
    });
};

const onSyncReq = (Env, session, frame) => {
    const channel = frame?.c;
    if (!Env.isSubscribed(session, channel)) {
        return void err(session, channel, 'ENOTSUBSCRIBED');
    }

    toStorage(Env, 'FED_SINCE', {
        channel, sinceId: frame.sinceId, limit: SYNC_BATCH
    }, (e, res) => {
        if (e) {
            /*  The requester's head is not in our log: either it has history we
                have trimmed, or the two have genuinely diverged. Say so rather
                than replaying from the start, which would look like success
                while producing a different document. */
            if (String(e) === 'EUNKNOWNSINCE') {
                return void err(session, channel, 'EUNKNOWNSINCE');
            }
            return void err(session, channel, 'ESINCE', String(e));
        }

        const messages = res?.messages || [];
        const envelopes = messages.map(m => Envelope.create({
            channel,
            content: m.content,
            seq: m.seq,
            lamport: m.seq,   // M1: single writer, so position is the clock
            time: m.time
        }, Env.identity));

        /*  R-19: emitted in log order, which for a single-writer log is causal
            order. The receiver can therefore validate and apply incrementally
            rather than buffering the whole response. */
        session.send({
            type: 'SYNC_RES',
            c: channel,
            envelopes,
            done: envelopes.length < SYNC_BATCH
        });
    });
};

const onSyncRes = (Env, session, frame) => {
    const channel = frame?.c;
    const envelopes = Array.isArray(frame?.envelopes) ? frame.envelopes : [];
    if (!envelopes.length) {
        if (!frame?.done) { Env.Log.verbose('FEDERATION_SYNC_EMPTY', { channel }); }
        return;
    }

    // apply strictly in order; a failure stops the batch rather than skipping
    let i = 0;
    const next = () => {
        if (i >= envelopes.length) {
            /*  More to come: ask again from our new head. Driving the next
                request from what we just stored means a dropped response
                retries correctly instead of losing a batch. */
            if (!frame.done) { requestSync(Env, session, channel); }
            return;
        }
        const env = envelopes[i++];
        ingest(Env, session, channel, env, (e) => {
            if (e) {
                Env.Log.error('FEDERATION_SYNC_INGEST_ERROR',
                    { channel, error: String(e) });
                return;
            }
            next();
        });
    };
    next();
};

// --------------------------------------------------------------- PUBLISH

const onPublish = (Env, session, frame) => {
    const env = frame?.envelope;
    const channel = env?.c;
    if (!Env.isSubscribed(session, channel)) {
        return void err(session, channel, 'ENOTSUBSCRIBED');
    }
    ingest(Env, session, channel, env, (e) => {
        if (!e) { return; }
        /*  A live push that does not apply is not fatal: the next sync will
            fetch whatever is missing. Log it and let anti-entropy repair. */
        Env.Log.warn('FEDERATION_PUBLISH_REJECTED',
            { channel, peer: Env.policy.describe(session.originId), error: String(e) });
        if (String(e) === 'EUNKNOWNSINCE' || String(e) === 'EGAP') {
            requestSync(Env, session, channel);
        }
    });
};

/*  Verify an envelope end to end, then hand it to storage.

    The peer that relays an envelope is not necessarily the origin that signed
    it, so the signature is checked against the *origin's* key. In M1 the two
    always coincide; keeping them distinct now is what makes relaying work later
    without revisiting this.
*/
ingest = (Env, session, channel, env, cb) => {
    const structural = Envelope.check(env, { channel });
    if (structural.error) { return void cb(structural.error); }

    const originKey = Env.originKey(env.o);
    if (!originKey) { return void cb('EUNKNOWNORIGIN'); }

    const verified = Envelope.verify(env, originKey);
    if (verified.error) { return void cb(verified.error); }

    /*  L2 channels go through the merge: the envelope lands in the pending log
        and is committed when the watermark allows. L1 channels commit straight
        away, because the anchor already fixed the order. */
    const command = Env.multiMaster.has(channel) ? 'FED_ACCEPT_REMOTE' : 'FED_INGEST';

    // storage revalidates the content itself against validateKey (R-15)
    toStorage(Env, command, { channel, envelope: env }, (e, res) => {
        if (e) { return void cb(e); }
        if (res?.duplicate) {
            Env.Log.verbose('FEDERATION_DUPLICATE', { channel, id: env.id });
        }
        cb();
    });
};

// --------------------------------------------------------------- publish out

/*  A local client wrote to a federated channel. Core saw it on the way to the
    front nodes and told us. Build the envelope from the committed log and push
    it to every subscribed peer.

    Nothing waits on this: the client was acknowledged when storage committed,
    exactly as on an unfederated instance. Federation adds no latency to the
    local write path in M1.
*/
const onLocalMessage = (Env, args) => {
    const { channel, content, time } = args || {};
    if (typeof (content) !== 'string') { return; }

    let id;
    try {
        id = Ids.fromContent(content);
    } catch (e) { return; }

    const peers = Env.subscribersOf(channel);
    if (!peers.length) { return; }

    toStorage(Env, 'FED_HEAD', { channel }, (e, info) => {
        if (e) {
            return void Env.Log.error('FEDERATION_PUBLISH_HEAD_ERROR',
                { channel, error: String(e) });
        }
        /*  Position in the committed log supplies both `s` and `l`. Deriving
            them rather than counting in memory means a restart cannot
            desynchronise the sequence from the log. */
        const seq = Math.max(0, (info?.count || 1) - 1);
        const env = Envelope.create({
            channel, content, seq, lamport: seq, time
        }, Env.identity);

        if (env.id !== id) { return; }   // the log moved under us; sync will fix it

        peers.forEach(session => {
            session.send({ type: 'PUBLISH', envelope: env });
        });
    });
};

// --------------------------------------------------------------- WRITE (M2)

/*  Anchored write-through (conformance level L1).

    A mirror may not append to its own log — in M1 it could not write at all.
    In M2 it forwards the write to the origin, which is the single ordering
    authority, and the message comes back through the normal PUBLISH path.

    This keeps R-1 trivially true while there is still one writer: every replica
    sees the origin's order because the origin is the only thing that assigns
    one. M3 is what removes the anchor, and that is where the merge is needed.

    The client on the mirror is acknowledged only once the origin has committed
    (L1: "broadcasts only after A assigns the position"). That costs one RTT on
    the mirror's writes and nothing at all on the origin's — the asymmetry is
    the point of calling this *anchored*.
*/

// mirror side: hand a local write to the origin and wait for its verdict
const writeThrough = (Env, args, cb) => {
    const { channel, content } = args || {};
    const originId = Env.subscriptions.originOf(channel);
    if (!originId) { return void cb('ENOTMIRROR'); }

    const session = Env.peers.sessions.get(originId);
    /*  No session means the anchor is unreachable. Say so with a typed error
        rather than accepting the write locally: accepting it would fork the
        log, which is exactly what L1 exists to prevent. The client sees a
        failure and can retry when the anchor returns. */
    if (!session) { return void cb('EANCHORDOWN'); }

    const txid = require('node:crypto').randomBytes(8).toString('base64');
    const timer = setTimeout(() => {
        Env.pendingWrites.delete(txid);
        cb('ETIMEOUT');
    }, 20 * 1000);
    Env.pendingWrites.set(txid, { cb, timer });

    if (!session.send({ type: 'WRITE', c: channel, m: content, txid })) {
        clearTimeout(timer);
        Env.pendingWrites.delete(txid);
        return void cb('EUNSENDABLE');
    }
};

// origin side: a mirror is asking us to commit a message
const onWrite = (Env, session, frame) => {
    const channel = frame?.c;
    const txid = frame?.txid;
    const reply = (error, id) => {
        session.send({ type: 'WRITE_OK', c: channel, txid, error, id });
    };

    if (!Env.isSubscribed(session, channel)) { return void reply('ENOTSUBSCRIBED'); }
    if (typeof (frame.m) !== 'string' || !frame.m) { return void reply('EBADCONTENT'); }

    let id;
    try {
        id = Ids.fromContent(frame.m);
    } catch (e) { return void reply('EBADCONTENT'); }

    /*  Commit it exactly as a local write, through the same storage path — so
        the content is validated against validateKey (R-15), the checkpoint
        dedup applies, and the fan-out to our own users and to every other
        replica happens without a second mechanism.

        The peer's originId goes in as the sender, so the stored line records
        which instance the write came from. */
    toStorage(Env, 'FED_WRITE', {
        channel,
        msgStruct: [0, session.originId, 'MSG', channel, frame.m]
    }, (e) => {
        if (e) {
            Env.Log.warn('FEDERATION_WRITE_REFUSED', {
                channel, peer: Env.policy.describe(session.originId), error: String(e)
            });
            return void reply(String(e));
        }
        reply(undefined, id);
    });
};

// mirror side: the origin's verdict
const onWriteOk = (Env, session, frame) => {
    const pending = Env.pendingWrites.get(frame?.txid);
    if (!pending) { return; }
    Env.pendingWrites.delete(frame.txid);
    clearTimeout(pending.timer);
    pending.cb(frame.error ? String(frame.error) : undefined, { id: frame.id });
};

// --------------------------------------------------------------- L2 (M3)

/*  A local write on a multi-master channel.

    Storage assigns `(s, l)` because the merge state lives there; this node
    signs, because the instance key lives here. The envelope goes to the pending
    log first (R-9) and only then to the peers — a peer must never learn of a
    message this instance could lose.
*/
const acceptLocal = (Env, args, cb) => {
    const { channel, content, time } = args || {};

    // storage assigns the position; we sign, because the key is here
    toStorage(Env, 'FED_ALLOCATE', { channel }, (e, alloc) => {
        if (e) { return void cb(String(e)); }
        if (!alloc) { return void cb('EALLOC'); }

        const env = Envelope.create({
            channel, content,
            seq: alloc.seq, lamport: alloc.lamport,
            time: time || Date.now()
        }, Env.identity);

        /*  Durable before anybody hears about it (R-9). A peer must never learn
            of a message this instance could still lose. */
        toStorage(Env, 'FED_ACCEPT_REMOTE', { channel, envelope: env }, (e2) => {
            if (e2) { return void cb(String(e2)); }
            Env.subscribersOf(channel).forEach(session => {
                session.send({ type: 'PUBLISH', envelope: env });
            });
            cb(void 0, { users: alloc.users, id: env.id });
        });
    });
};

/*  Per-channel clocks, piggy-backed on the heartbeat (R-4, R-37).

    Without this a quiet pad never advances its watermark and never commits
    anything — the members are all waiting to hear that the others have moved on.
*/
const heartbeatState = (Env, session) => {
    const channels = Env.subscriptions.channelsFor(session);
    if (!channels.length) { return; }
    let pending = channels.length;
    const out = [];
    channels.forEach(channel => {
        toStorage(Env, 'FED_STATE', { channel }, (e, state) => {
            if (!e && state?.level === 'L2') {
                const entry = {
                    c: channel,
                    s: state.self?.seq ?? -1,
                    // the promise, not the last emitted clock — see merge.js
                    l: state.promise ?? (state.self?.lamport || 0)
                };
                /*  How many messages are actually in the committed log (R-53).

                    Everything else in this frame is federation bookkeeping, and
                    two instances can agree on all of it while holding different
                    documents: a message committed while core did not know the
                    channel was federated never gets a sequence at all, so no
                    gap exists, `have` matches on both sides, and each is
                    satisfied. The log length is the one number that does not
                    come from the same bookkeeping, which is exactly why it can
                    contradict it. */
                /*  What we hold, per origin, so the peer can resend anything we
                    are missing (R-47). */
                const known = logCount(Env, channel);
                if (typeof (known) === 'number') { entry.n = known; }
                toStorage(Env, 'FED_HAVE', { channel }, (e2, h) => {
                    if (!e2 && h?.have) { entry.have = h.have; }
                    out.push(entry);
                    if (--pending === 0 && out.length) {
                        session.send({ type: 'HEARTBEAT', channels: out });
                    }
                });
                return;
            }
            if (--pending === 0 && out.length) {
                session.send({ type: 'HEARTBEAT', channels: out });
            }
        });
    });
};

const onHeartbeat = (Env, session, frame) => {
    const channels = Array.isArray(frame?.channels) ? frame.channels : [];
    channels.forEach(entry => {
        if (!Env.isSubscribed(session, entry?.c)) { return; }
        toStorage(Env, 'FED_OBSERVE', {
            channel: entry.c,
            originId: session.originId,
            seq: entry.s,
            lamport: entry.l,
            /*  This is the peer describing its own log, so it may correct a
                sequence downwards — see `observe` in merge.js. */
            authoritative: true
        }, () => {});

        /*  Anti-entropy (R-47).

            The heartbeat carries what the peer holds, per origin. Anything of
            ours it is missing gets resent — which is what makes a dropped
            PUBLISH, or an outage, recoverable rather than permanent. Without
            this the two instances stay silently divergent forever, which is
            exactly what happened in practice.

            Cheap when there is nothing to do: the comparison is two integers
            per origin, and the common case sends nothing. */
        /*  Divergence detection (R-53).

            The two logs should hold the same messages, so they should be the
            same length. When they are not, these instances are serving
            different documents — and every other signal says they agree, which
            is why this needs saying out loud rather than inferring.

            Reported, not repaired. Re-sending an orphaned message would give it
            a fresh clock, so the peer would append it at the end while we hold
            it mid-log: the same messages in a different order, which is the
            divergence rather than the cure. Repair needs the log rewrite of R-6.

            Rate-limited per channel: this rides a 2 s heartbeat and a divergence
            persists, so without it one split pad would fill the log. */
        if (typeof (entry.n) === 'number') {
            const mine = logCount(Env, entry.c);
            if (typeof (mine) === 'number' && mine !== entry.n) {
                const last = Env.divergedAt.get(entry.c) || 0;
                if (Date.now() - last >= DIVERGENCE_LOG_EVERY) {
                    Env.divergedAt.set(entry.c, Date.now());
                    Env.Log.error('FEDERATION_DIVERGED', {
                        channel: entry.c,
                        peer: Env.policy.describe(session.originId),
                        mine, theirs: entry.n
                    });
                }
                // and fix it (R-6)
                requestRepair(Env, session, entry.c);
            } else if (mine === entry.n) {
                Env.divergedAt.delete(entry.c);
            }
        }

        if (!entry.have) { return; }
        toStorage(Env, 'FED_RECONCILE', {
            channel: entry.c,
            originId: session.originId,
            have: entry.have
        }, (e, res) => {
            if (e || !res?.envelopes?.length) { return; }
            res.envelopes.forEach(env => {
                session.send({ type: 'PUBLISH', envelope: env });
            });
        });
    });
};

// --------------------------------------------------------------- RECONCILE

/*  Repairing a split pad (spec R-6, R-53).

        AUDIT_REQ   {c}            "which messages do you hold?"
        AUDIT_IDS   {c, ids}       the ids in my committed log, in order
        REPAIR_MSG  {c, messages}  the ones you were missing

    Why this exists
    ---------------
    A message committed while core did not know the channel was federated never
    entered the federation order, so it has no sequence: no gap, nothing to
    resend, and every mechanism in §4 blind to it. Two instances end up serving
    different documents while agreeing on every counter they exchange.

    Append-only, on purpose
    -----------------------
    The repair makes both instances hold *everything*. It never removes a stored
    patch and never moves one, because a patch is somebody's work and no server
    is in a position to decide otherwise — it cannot read the content, and the
    party that can, the client, already resolves a chain it receives out of
    order.

    An earlier version did try to be cleverer: it lifted its own unfederated
    messages out of the log and re-federated them, so that the merge would give
    both instances one identical order. That was wrong twice. It deleted
    committed history to achieve tidiness, and it could not tell an orphan from a
    message that was merely *in flight* — so on a busy pad it excised and re-sent
    perfectly good messages, the divergence outlived the retry interval, and each
    round moved more of them. Measured on a live pair: a gap of 8 messages grew to
    117 in three minutes. Appending cannot do that, whatever it gets wrong.
*/

// one repair per channel at a time, and not more often than this
const REPAIR_COOLDOWN = 60 * 1000;
// a repair frame carries at most this many messages; the rest follow next round
const REPAIR_BATCH = 64;

const onAuditRequest = (Env, session, frame) => {
    const channel = frame?.c;
    if (!Env.isSubscribed(session, channel)) { return; }
    toStorage(Env, 'FED_LOG_IDS', { channel }, (e, res) => {
        if (e) {
            return void Env.Log.error('FEDERATION_AUDIT_ERROR',
                { channel, error: String(e) });
        }
        session.send({ type: 'AUDIT_IDS', c: channel, ids: res?.ids || [] });
    });
};

const onAuditIds = (Env, session, frame) => {
    const channel = frame?.c;
    if (!Env.isSubscribed(session, channel)) { return; }
    const pending = Env.repairs.get(channel);
    if (!pending || pending.originId !== session.originId) { return; }
    clearTimeout(pending.timer);

    const finish = () => {
        Env.repairs.delete(channel);
        Env.repairedAt.set(channel, Date.now());
    };

    const theirs = new Set(Array.isArray(frame.ids) ? frame.ids : []);
    toStorage(Env, 'FED_LOG_IDS', { channel }, (e, res) => {
        if (e) {
            Env.Log.error('FEDERATION_AUDIT_ERROR', { channel, error: String(e) });
            return void finish();
        }
        const missing = (res?.ids || []).filter(id => !theirs.has(id));
        if (!missing.length) {
            Env.Log.verbose('FEDERATION_REPAIR_NOTHING', { channel });
            return void finish();
        }

        /*  Bounded per round. A pad that has been split for a long time can be
            missing thousands of messages, and one frame holding all of them
            would exceed MAX_FRAME; the next audit picks up the remainder. */
        const batch = missing.slice(0, REPAIR_BATCH);
        toStorage(Env, 'FED_CONTENTS', { channel, ids: batch }, (e2, out) => {
            if (e2) {
                Env.Log.error('FEDERATION_REPAIR_ERROR', { channel, error: String(e2) });
                return void finish();
            }
            const messages = out?.messages || [];
            if (!messages.length) { return void finish(); }

            Env.Log.info('FEDERATION_REPAIR_SEND', {
                channel,
                peer: Env.policy.describe(session.originId),
                sending: messages.length,
                remaining: missing.length - batch.length
            });
            session.send({ type: 'REPAIR_MSG', c: channel, messages });
            finish();
        });
    });
};

/*  Messages a peer holds and we did not. Stored as they are: this is the point
    at which both instances come to hold everything. */
const onRepairMessages = (Env, session, frame) => {
    const channel = frame?.c;
    if (!Env.isSubscribed(session, channel)) { return; }
    const messages = Array.isArray(frame.messages) ? frame.messages : [];
    if (!messages.length) { return; }

    toStorage(Env, 'FED_ABSORB', { channel, messages }, (e, res) => {
        if (e) {
            return void Env.Log.error('FEDERATION_REPAIR_ERROR',
                { channel, error: String(e) });
        }
        Env.Log.info('FEDERATION_REPAIRED', {
            channel,
            peer: Env.policy.describe(session.originId),
            absorbed: res?.absorbed || 0
        });
    });
};

/*  Start a repair: ask the peer what it holds. Everything else follows from the
    answer. */
const requestRepair = (Env, session, channel) => {
    if (Env.repairs.has(channel)) { return; }
    const last = Env.repairedAt.get(channel) || 0;
    if (Date.now() - last < REPAIR_COOLDOWN) { return; }

    Env.repairs.set(channel, {
        originId: session.originId,
        timer: setTimeout(() => {
            Env.repairs.delete(channel);
            Env.Log.error('FEDERATION_REPAIR_TIMEOUT', { channel });
        }, 60 * 1000)
    });
    if (!session.send({ type: 'AUDIT_REQ', c: channel })) {
        clearTimeout(Env.repairs.get(channel)?.timer);
        Env.repairs.delete(channel);
    }
};

// --------------------------------------------------------------- INVITE

/*  "Replicate this pad" — sent by the instance that holds it, over the session
    the two already have (spec R-46).

    The client used to call the *other* instance's HTTP API directly. That was
    wrong twice over: the browser has no business talking to a server it has no
    relationship with (and the CSP rightly blocks it), and it bypassed the
    authenticated, allowlisted peer session that exists precisely for
    instance-to-instance work.

    So the browser now talks only to its own server, and the two servers settle
    it between themselves. The capability still comes from the client — only it
    holds the pad key — it just travels by a sensible route.
*/
const onInvite = (Env, session, frame) => {
    const channel = frame?.c;
    if (typeof (channel) !== 'string' || !channel) {
        return void err(session, channel, 'EBADCHANNEL');
    }
    /*  Handled exactly as if we had decided to replicate it ourselves: the
        capability is verified here against the pad's validateKey, and the
        peering policy applies. An invitation is a request, not an instruction. */
    Env.replicate({
        channel,
        peer: session.originId,
        cap: frame.cap,
        validateKey: frame.validateKey,
        level: frame.level
    }, (e) => {
        if (e) {
            Env.Log.warn('FEDERATION_INVITE_REFUSED', {
                channel, peer: Env.policy.describe(session.originId), error: String(e)
            });
            return void err(session, channel, String(e));
        }
        Env.Log.info('FEDERATION_INVITE_ACCEPTED', {
            channel, peer: Env.policy.describe(session.originId)
        });
    });
};

// --------------------------------------------------------------- CONTROL

/*  A control commit made locally, pushed to every replica (spec §5.4).

    Under L1 only the anchor emits these, and a replica refuses one from anybody
    else — see `FM.control`. The commit carries this instance's signature over
    the payload, which is the "originating instance asserts a legitimate owner
    asked for this" half of R-28.

    The other half — a pad-key signature proving capability — is **not** here:
    the pad's signing key lives only in the browser, so minting that assertion is
    client work, tracked with the other client-side items (R-40). Until it lands,
    a peer must trust that the anchor checked its own owner list, which is why
    R-27's allowlist is not optional.
*/
const publishControl = (Env, args) => {
    const { channel, ctrl } = args || {};
    const peers = Env.subscribersOf(channel);
    if (!peers.length) { return; }

    const payload = {
        type: 'CONTROL',
        c: channel,
        o: Env.identity.originId,
        ctrl,
        t: Date.now()
    };
    payload.sig = Env.signControl(payload);
    peers.forEach(session => session.send(payload));
};

const onControl = (Env, session, frame) => {
    const channel = frame?.c;
    if (!Env.isSubscribed(session, channel)) {
        return void err(session, channel, 'ENOTSUBSCRIBED');
    }
    /*  R-16-style binding: the instance that claims to have authorised this must
        have signed it. Verified against the *named* origin, not the relaying
        peer, so a control commit cannot be minted by whoever forwards it. */
    if (!Env.verifyControl(frame)) {
        Env.Log.warn('FEDERATION_CONTROL_BAD_SIG', {
            channel, peer: Env.policy.describe(session.originId)
        });
        return void err(session, channel, 'EBADSIG');
    }

    toStorage(Env, 'FED_CONTROL', {
        channel, ctrl: frame.ctrl, from: frame.o
    }, (e) => {
        if (!e) { return; }
        Env.Log.warn('FEDERATION_CONTROL_REFUSED',
            { channel, error: String(e), type: frame.ctrl?.t });
    });
};

// --------------------------------------------------------------- UNSUBSCRIBE

// R-32: either side may leave at any time, for any reason, without negotiation.
const onUnsubscribe = (Env, session, frame) => {
    const channel = frame?.c;
    Env.dropSubscription(session, channel);
    Env.Log.info('FEDERATION_UNSUBSCRIBED', {
        channel, peer: Env.policy.describe(session.originId), reason: frame?.reason
    });
};

module.exports = {
    onSubscribe, onSubscribeOk,
    onSyncReq, onSyncRes,
    onPublish, onUnsubscribe,
    onLocalMessage,
    onWrite, onWriteOk, writeThrough,
    onControl, publishControl, onInvite,
    acceptLocal, onHeartbeat, heartbeatState,
    requestSync,
    onAuditRequest, onAuditIds, onRepairMessages, requestRepair,
    SYNC_BATCH
};
