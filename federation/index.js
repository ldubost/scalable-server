// SPDX-License-Identifier: AGPL-3.0-or-later
// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors

/*  The federation node (design §1.2).

    An Interface.connect client of core, exactly like front and storage: it holds
    the long-lived sessions to remote instances, and reaches the channel log the
    way everything else does, by asking core.

    M0 carries no pad data. What it establishes is the shape: identity, policy,
    handshake, the inbound listener, outbound dialling, and one command in each
    direction so the federation -> core -> storage path is proved end to end.
*/

const Interface = require('../common/interface.js');
const WSConnector = require('../common/ws-connector.js');
const Logger = require('../common/logger.js');
const Util = require('../common/common-util.js');
const Environment = require('../common/env.js');
const nThen = require('nthen');

const Crypto = require('../common/crypto.js')('sodiumnative');
const Codec = require('../common/federation/codec.js');
const Identity = require('../common/federation/identity.js');
const Capability = require('../common/federation/capability.js');
const Policy = require('./policy.js');
const Server = require('./server.js');
const PeerManager = require('./peer-manager.js');
const Sync = require('./sync.js');
const BlobTransfer = require('./blob-transfer.js');

/*  Frames a peer may send us once authenticated.

    M0 has exactly one pair. FED_PING proves the whole path is alive: it crosses
    the peer session, then goes federation -> core -> storage and back, so a
    successful FED_PONG means every hop in design §1.3 works. M1 adds SUBSCRIBE,
    SYNC_REQ and PUBLISH here.
*/
const onPing = (Env, session, frame) => {
    const nonce = typeof (frame.nonce) === 'string' ? frame.nonce.slice(0, 64) : '';

    /*  Round-trip through core to storage. The point is not the number that
        comes back but that a federation node can reach the storage tier at all,
        which is the one topology claim M0 exists to prove. */
    const coreId = Env.getCoreId(Env.identity.originId);
    Env.interface.sendQuery(coreId, 'FED_PING', {
        originId: Env.identity.originId,
        nonce
    }, answer => {
        session.send({
            type: 'FED_PONG',
            nonce,
            originId: Env.identity.originId,
            time: Date.now(),
            // whether the local storage tier answered; peers use it as a health hint
            storage: !answer?.error && Boolean(answer?.data),
            error: answer?.error
        });
    });
};

const onPong = (Env, session, frame) => {
    const pending = Env.pendingPings.get(frame?.nonce);
    if (!pending) { return; }
    Env.pendingPings.delete(frame.nonce);
    clearTimeout(pending.timer);
    pending.cb(undefined, {
        peer: session.originId,
        rtt: Date.now() - pending.sent,
        storage: Boolean(frame.storage)
    });
};

const onPeerError = (Env, session, frame) => {
    Env.Log.warn('FEDERATION_PEER_ERROR', {
        peer: Env.policy.describe(session.originId),
        error: frame?.error,
        re: frame?.re
    });
};

const FEDERATION_COMMANDS = {
    'FED_PING': onPing,
    'FED_PONG': onPong,
    'ERROR': onPeerError,

    // channel protocol (M1)
    'SUBSCRIBE': Sync.onSubscribe,
    'SUBSCRIBE_OK': Sync.onSubscribeOk,
    'SYNC_REQ': Sync.onSyncReq,
    'SYNC_RES': Sync.onSyncRes,
    'PUBLISH': Sync.onPublish,
    'UNSUBSCRIBE': Sync.onUnsubscribe,

    // anchored write-through (M2)
    'WRITE': Sync.onWrite,
    'WRITE_OK': Sync.onWriteOk,

    // control commits: metadata and deletion (M2)
    'CONTROL': Sync.onControl,
    // "please replicate this pad" (R-46)
    'INVITE': Sync.onInvite,

    // multi-master (M3)
    'HEARTBEAT': Sync.onHeartbeat,

    // blobs (M5)
    'BLOB_REQ': BlobTransfer.onRequest,
    'BLOB_CHUNK': BlobTransfer.onChunk,
    'BLOB_ERR': BlobTransfer.onError
};

/*  Which peers replicate which channels.

    Two directions, deliberately kept apart:

      subscribers  channel -> sessions that asked US for it (we push to them)
      mine         channels WE asked a peer for (we pull, and refuse local writes)

      replicaSet   channels we replicate with named peers, rebuilt at startup
                   from the durable state (R-52)

    The first two are per-session and held in memory only: a session that drops
    loses them and re-subscribes on reconnect, which is also what re-runs the
    backfill.

    `replicaSet` exists because that is not enough across a *restart*. A
    SUBSCRIBE carries a capability signed by the pad key, which is short-lived
    and single-use — so a restarted instance cannot simply re-subscribe, because
    only a browser holding the pad can mint one and there may be nobody with the
    pad open for days. Membership therefore has to come from the state files,
    which are the only durable record of who replicates what.
*/
const mkSubscriptions = () => {
    const subscribers = new Map();   // channel -> Set(session)
    const mine = new Map();          // channel -> originId we pull from
    const replicaSet = new Map();    // channel -> Set(originId), durable

    return {
        add: (session, channel) => {
            if (!subscribers.has(channel)) { subscribers.set(channel, new Set()); }
            subscribers.get(channel).add(session);
        },
        drop: (session, channel) => {
            if (channel) {
                subscribers.get(channel)?.delete(session);
                if (!subscribers.get(channel)?.size) { subscribers.delete(channel); }
                return;
            }
            // whole session went away
            subscribers.forEach((set, c) => {
                set.delete(session);
                if (!set.size) { subscribers.delete(c); }
            });
        },
        of: (channel) => Array.from(subscribers.get(channel) || []),
        // channels this session replicates, for the heartbeat
        channelsFor: (session) => {
            const out = [];
            subscribers.forEach((set, c) => { if (set.has(session)) { out.push(c); } });
            mine.forEach((origin, c) => {
                if (origin === session.originId && !out.includes(c)) { out.push(c); }
            });
            /*  Restored membership counts too, or a channel that survived a
                restart would carry no heartbeat — and without heartbeats the
                watermark never advances and nothing commits. */
            replicaSet.forEach((set, c) => {
                if (set.has(session.originId) && !out.includes(c)) { out.push(c); }
            });
            return out;
        },
        has: (session, channel) =>
            Boolean(subscribers.get(channel)?.has(session)) || mine.has(channel) ||
            Boolean(replicaSet.get(channel)?.has(session.originId)),
        follow: (channel, originId) => mine.set(channel, originId),
        following: () => Array.from(mine.entries()),
        // which instance anchors this channel, if we are a mirror of it
        originOf: (channel) => mine.get(channel),
        isMirror: (channel) => mine.has(channel),

        /*  Durable membership, from the state files at startup. Additive and
            idempotent: a live SUBSCRIBE that arrives later adds a session on top
            and neither displaces the other. */
        restore: (channel, originIds) => {
            if (!replicaSet.has(channel)) { replicaSet.set(channel, new Set()); }
            const set = replicaSet.get(channel);
            (originIds || []).forEach(o => { if (o) { set.add(o); } });
        },
        membersOf: (channel) => Array.from(replicaSet.get(channel) || []),
        forget: (channel) => replicaSet.delete(channel),
        count: () => new Set([
            ...subscribers.keys(), ...mine.keys(), ...replicaSet.keys()
        ]).size
    };
};

/*  Mark a local channel as federated, so peers may subscribe to it
    (design §5.2, step 2). Run on the instance that holds the pad.

    This only records the replica set; it grants nothing by itself. A peer still
    has to present a capability signed by the pad's key to actually subscribe
    (R-25), and the operator's policy still decides which instances may hold it
    at all (R-27).
*/
const enableLocal = (Env, args, cb) => {
    cb = Util.once(cb || (() => {}));
    const { channel, validateKey, members, level } = args || {};
    if (typeof (channel) !== 'string' || !channel) { return void cb('EBADCHANNEL'); }
    if (typeof (validateKey) !== 'string' || !validateKey) {
        return void cb('ENOVALIDATEKEY');
    }

    /*  Refuse to name a member we do not peer with, rather than recording a
        replica set that can never form. */
    const bad = (members || []).find(m => m !== Env.identity.originId &&
        !Env.policy.admits(m));
    if (bad) { return void cb('ENOTPEERED'); }

    Env.interface.sendQuery(Env.getCoreId(channel), 'FED_ENABLE', {
        channel,
        validateKey,
        self: Env.identity.originId,
        origin: Env.identity.originId,   // we hold the pad, so we are the writer
        members,
        level
    }, answer => {
        if (answer?.error) { return void cb(String(answer.error)); }
        const already = Boolean(answer?.data?.already);
        Env.markFederated(channel, { mirror: false, level });
        /*  Record the replica set independently of any session (R-52). A peer's
            session can drop and come back — a restart, a network blip — and it
            cannot re-`SUBSCRIBE` without a fresh capability, which only a
            browser can mint. Membership is a property of the channel, not of a
            connection, so it is kept as one. */
        Env.subscriptions.restore(channel,
            (members || []).filter(m => m && m !== Env.identity.originId));

        /*  Invite the members over the session we already hold, rather than
            making the client call them. The capability came from the client and
            is scoped to this pair of instances; we are only carrying it. */
        const invited = [];
        /*  An INVITE carries the capability, and without one the far end has
            nothing to authorise itself with — it would answer E_CAP_MALFORMED.
            A capless enable is the operator/testing path, where the peer is
            expected to present its own capability via /replicate instead. */
        if (args.cap) {
            (members || []).forEach(originId => {
                if (originId === Env.identity.originId) { return; }
                const session = Env.peers.sessions.get(originId);
                if (!session) {
                    Env.Log.warn('FEDERATION_INVITE_NO_SESSION',
                        { channel, peer: Env.policy.describe(originId) });
                    return;
                }
                session.send({
                    type: 'INVITE',
                    c: channel,
                    cap: args.cap,
                    validateKey,
                    level
                });
                invited.push(originId);
            });
        }

        /*  Only the invite-carrying flow depends on a live session: there the
            caller has handed us a capability and gone away, so silently failing
            to deliver it would leave a pad that looks federated and is not. If
            the replica set was already complete there is nobody new to invite,
            and failing for want of a session would be wrong — the pad is
            federated, which is what the caller wanted. */
        if (args.cap && !already && (members || []).length && !invited.length) {
            return void cb('ENOSESSION');
        }
        cb(void 0, { ok: true, already: already, invited: invited.length });
    });
};

/*  Put replication back the way it was, from the durable state (R-52).

    Runs once at startup. For every channel with federation state, tell core it
    is federated — so local writes are published again — and record the other
    members, so they are pushed to and their frames accepted. A mirror also
    re-follows its anchor, which is what makes `onSessionUp` re-run the backfill.

    Failure is logged and not fatal. An instance that cannot reach storage yet
    should still come up and serve unfederated pads; the alternative is refusing
    to start, which helps nobody.
*/
const RESTORE_RETRY_MIN = 1000;
const RESTORE_RETRY_MAX = 30 * 1000;

const restoreFederation = (Env, done, delay) => {
    done = Util.once(done || (() => {}));
    const again = (why) => {
        /*  Retry rather than give up. The commonest reason to get here is that
            storage has not finished connecting to core yet — this runs seconds
            into a cold start — and an instance whose replication depends on
            winning that race is not fixed at all. Backs off to a slow poll so a
            storage node that is down for a while costs nothing.

            `done` fires regardless, so a peer session still comes up and serves
            what it can while the routing is still being rebuilt. */
        const next = Math.min((delay || RESTORE_RETRY_MIN) * 2, RESTORE_RETRY_MAX);
        /*  Quiet while this is the expected startup race, loud once it has been
            going long enough to mean something is actually wrong. */
        const log = next >= RESTORE_RETRY_MAX ? Env.Log.error : Env.Log.info;
        log('FEDERATION_RESTORE_RETRY', { in: next, why });
        done();
        Env.restoreTimer = setTimeout(() => {
            restoreFederation(Env, done, next);
        }, next);
    };

    Env.interface.sendQuery(Env.getCoreId('federation'), 'FED_LIST', {}, answer => {
        /*  A string answer means the destination was not reachable; only an
            object is a real reply. Reading `.error` alone treats the former as
            success. */
        if (!answer || typeof (answer) !== 'object' || answer.error) {
            return void again(String((answer && answer.error) || answer));
        }
        /*  Some storage node could not be asked, so this list is a subset of
            the truth. Applying it is right — those channels do replicate again —
            but we must come back for the rest. */
        if (answer.data?.incomplete) {
            applyRestored(Env, answer.data.channels || []);
            return void again('incomplete');
        }
        const channels = answer?.data?.channels || [];
        const restored = applyRestored(Env, channels);
        Env.Log.info('FEDERATION_RESTORED', { channels: restored });
        done();
    });
};

const applyRestored = (Env, channels) => {
        let restored = 0;
        channels.forEach(entry => {
            const channel = entry?.channel;
            if (typeof (channel) !== 'string' || !channel) { return; }

            const me = entry.me || Env.identity.originId;
            const others = (entry.members || []).filter(m => m && m !== me);
            const isL2 = entry.level === 'L2';
            const anchored = !isL2 && entry.origin && entry.origin !== me;

            Env.subscriptions.restore(channel, others);
            /*  At L1 we pull from the anchor and may not write locally; at L2
                there is no anchor and membership alone is enough. */
            if (anchored) { Env.subscriptions.follow(channel, entry.origin); }
            Env.markFederated(channel, { mirror: Boolean(anchored), level: entry.level });
            restored++;
        });
        return restored;
};

/*  Report whether a channel is federated here, and with whom (R-50).

    Answers from the replication state rather than from any local guess, so a
    replica and an anchor give the same answer. A channel nobody federated is
    reported as such rather than as an error: "no" is a legitimate answer to the
    question the client is asking.
*/
const channelStatus = (Env, args, cb) => {
    cb = Util.once(cb || (() => {}));
    const channel = args?.channel;
    if (typeof (channel) !== 'string' || !channel) { return void cb('EBADCHANNEL'); }

    Env.interface.sendQuery(Env.getCoreId(channel), 'FED_STATE', { channel },
        answer => {
            if (answer?.error) { return void cb(String(answer.error)); }
            const state = answer?.data;
            if (!state || !state.members) {
                return void cb(void 0, { federated: false, members: [] });
            }
            cb(void 0, {
                federated: true,
                level: state.level,
                origin: state.origin,
                self: Env.identity.originId,
                // everyone but us: what the caller has to mint capabilities for
                members: (state.members || [])
                    .filter(m => m !== Env.identity.originId)
            });
        });
};

/*  Start replicating a channel from a peer (design §5.2, step 3).

    Authorisation is the capability itself: it is signed by the pad's own key, so
    holding one is proof of the right to make this pad replicate (R-25). The
    peering policy (R-27) still applies on top — an operator decides which
    instances may hold their users' pads at all, and no capability overrides that.
*/
const replicate = (Env, args, cb) => {
    cb = Util.once(cb || (() => {}));
    const { channel, peer, cap, validateKey } = args || {};

    if (typeof (channel) !== 'string' || !channel) { return void cb('EBADCHANNEL'); }
    if (!Env.policy.admits(peer)) { return void cb('ENOTPEERED'); }

    const session = Env.peers.sessions.get(peer);
    if (!session) { return void cb('ENOSESSION'); }

    /*  Check the capability locally before spending a round trip on it. The
        holder verifies it again for itself — this end cannot be trusted to have
        checked, and does not need to be. */
    if (validateKey) {
        const check = Capability.verify(cap, validateKey, {
            channel, from: peer, to: Env.identity.originId
        });
        if (check.error) { return void cb(check.error); }
    }

    Env.subscriptions.follow(channel, peer);
    Env.pendingSubscribes.set(channel, {
        cb,
        timer: setTimeout(() => {
            Env.pendingSubscribes.delete(channel);
            cb('ETIMEOUT');
        }, 30 * 1000)
    });

    session.send({ type: 'SUBSCRIBE', c: channel, validateKey, cap });
};

// --------------------------------------------------------------- from core

/*  Ask a peer for a FED_PING and report the round trip. Exposed over the node
    bus so an admin command (and the M0 test) can drive it. */
const pingPeer = (Env, args, cb) => {
    cb = Util.once(cb || (() => {}));
    const { originId } = args || {};
    const session = Env.peers.sessions.get(originId);
    if (!session) { return void cb('ENOSESSION'); }

    const nonce = require('node:crypto').randomBytes(16).toString('base64');
    const timer = setTimeout(() => {
        Env.pendingPings.delete(nonce);
        cb('ETIMEOUT');
    }, 10 * 1000);
    Env.pendingPings.set(nonce, { cb: (err, data) => cb(err, data), timer, sent: Date.now() });

    if (!session.send({ type: 'FED_PING', nonce })) {
        clearTimeout(timer);
        Env.pendingPings.delete(nonce);
        return void cb('EUNSENDABLE');
    }
};

/*  Liveness (spec R-37 sets T_hb <= 2s for the watermark; this is the session
    heartbeat that will carry it).

    Ping each peer as soon as the session comes up and at every beat after. It
    proves the far end can still reach its own storage tier, and it is what turns
    a socket that is merely open into a session we can rely on. From M3 the same
    beat carries the watermark advance.
*/
const HEARTBEAT = 2 * 1000;

const beat = (Env, session) => {
    pingPeer(Env, { originId: session.originId }, (err, data) => {
        const peer = Env.policy.describe(session.originId);
        if (err) {
            session.healthy = false;
            return void Env.Log.warn('FEDERATION_PING_FAILED', { peer, error: err });
        }
        session.healthy = data.storage;
        session.rtt = data.rtt;
        // piggy-back per-channel clocks so idle pads still advance (R-4, R-37)
        Sync.heartbeatState(Env, session);
        Env.Log.info('FEDERATION_PING_OK', {
            peer, rtt: data.rtt, storage: data.storage
        });
    });
};

// What this node knows about federation. Feeds /api/federation and admin tools.
/*  The peers this instance may federate with, for the client to choose from.

    The browser needs a peer's originId to scope a capability to it, and it must
    not go and ask that peer directly — so its own server tells it. Only what an
    operator has already configured is exposed: a name, the id, and the address,
    all of which the operator chose to peer with in the first place.
*/
const listPeers = (Env, args, cb) => {
    cb ||= () => {};
    cb(void 0, {
        peers: Env.policy.peers.map(p => ({
            originId: p.originId,
            name: p.name,
            // where a browser reaches them; `url` is our dialling endpoint
            origin: p.origin,
            // only a peer with a live session can actually be invited
            connected: Env.peers.sessions.has(p.originId)
        }))
    });
};

const status = (Env, args, cb) => {
    cb ||= () => {};
    cb(undefined, {
        myId: Env.myId,
        originId: Env.identity.originId,
        policy: {
            path: Env.policy.path,
            present: Env.policy.present,
            peers: Env.policy.peers.length
        },
        sessions: Env.peers.sessions.list().map(s => ({
            originId: s.originId,
            name: Env.policy.describe(s.originId),
            role: s.role,
            healthy: Boolean(s.healthy),
            rtt: s.rtt
        }))
    });
};

const onNewDecrees = (Env, args, cb) => {
    const { type, decrees, curveKeys, freshKey } = args;
    Env.cacheDecrees(type, decrees);
    Env.FRESH_KEY = freshKey;
    Env.curveKeys ||= curveKeys;
    Env.getDecree(type).loadRemote(Env, decrees);
    cb();
};

const shutdown = (Env, args, cb) => {
    Object.values(Env.intervals || {}).forEach(clearInterval);
    /*  The restore retries on a timer, so a shutdown between attempts would
        otherwise fire it into a node that has already let go of its interface. */
    if (Env.restoreTimer) { clearTimeout(Env.restoreTimer); Env.restoreTimer = null; }
    Env.peers?.shutdown();
    Env.server?.shutdown();
    cb?.();
};

// ---------------------------------------------------------------- start

const start = (mainConfig) => {
    const { myId, index, config, infra } = mainConfig;

    const Env = {
        active: true,
        Log: Logger(config, myId),
        public: infra?.federation?.[index],
        pendingPings: new Map(),
        // channel -> when we last said it had diverged (R-53)
        divergedAt: new Map(),
        federationCommands: FEDERATION_COMMANDS
    };
    Environment.init(Env, mainConfig);

    /*  The instance identity is shared by every federation node of this
        instance: they are one peer as far as the outside world is concerned.
        It therefore lives at the shared base path, not a per-index one. */
    Env.identity = Identity.load(Env.paths.base);
    if (Env.identity.generated) {
        Env.Log.info('FEDERATION_IDENTITY_CREATED', {
            originId: Env.identity.originId, path: Env.identity.path
        });
    }
    Env.policy = Policy.load(Env.paths.base, Env.Log);
    Env.Log.info('FEDERATION_POLICY', {
        peers: Env.policy.peers.length,
        file: Env.policy.present ? Env.policy.path : 'none'
    });

    Env.peers = PeerManager.create(Env);
    Env.sessions = Env.peers.sessions;
    Env.subscriptions = mkSubscriptions();
    Env.pendingSubscribes = new Map();
    Env.pendingWrites = new Map();
    Env.pendingBlobs = new Map();

    /*  Blob transfer talks to storage without a channel to route on, so it gets
        a small helper rather than reaching into the interface itself. */
    Env.toStorage = (command, args, cb) => {
        Env.interface.sendQuery(Env.getCoreId(args.id || ''), command, args,
            (answer) => cb(answer?.error, answer?.data));
    };

    /*  The helpers sync.js is written against. Keeping them here rather than
        importing state into sync.js is what lets the wire protocol be tested
        against a stub Env. */
    Env.verifyCapability = (cap, validateKey, expect) => {
        const res = Capability.verify(cap, validateKey, expect);
        if (res.error) { return res; }
        // R-18 for the setup path: a capability is single-use
        if (!Env.capabilityNonces.claim(res.cap)) { return { error: 'E_CAP_REPLAY' }; }
        return res;
    };
    Env.capabilityNonces = Capability.mkNonceCache();

    /*  Which channels run multi-master here, so the ingest path can tell
        "commit now" from "merge later" without a round trip. */
    Env.multiMaster = new Set();

    Env.isSubscribed = (session, channel) =>
        Boolean(channel) && Env.subscriptions.has(session, channel);
    /*  Everyone we must push this channel's messages to.

        Two directions, and at L2 both matter: peers that subscribed to us, and
        the peer we ourselves subscribe to. Under L1 only the first mattered,
        because a mirror forwarded its writes instead of publishing them — at L2
        the relationship is symmetric and leaving out the second means our own
        writes never reach the instance we replicate from. */
    Env.subscribersOf = (channel) => {
        const sessions = Env.subscriptions.of(channel);
        const add = (originId) => {
            if (!originId) { return; }
            const session = Env.peers.sessions.get(originId);
            if (session && !sessions.includes(session)) { sessions.push(session); }
        };
        add(Env.subscriptions.originOf(channel));
        /*  Members restored from durable state (R-52). Without this a restarted
            instance keeps accepting local writes and pushes them to nobody,
            which looks exactly like federation having been switched off. */
        Env.subscriptions.membersOf(channel).forEach(add);
        return sessions;
    };
    Env.dropSubscription = (session, channel) => Env.subscriptions.drop(session, channel);

    /*  Control commits are signed over their canonical form, so the two ends
        agree byte-for-byte regardless of key order (see codec.js). */
    const controlBytes = (frame) => Codec.canonical({
        v: 1, c: frame.c, o: frame.o, ctrl: frame.ctrl, t: frame.t
    });
    Env.signControl = (frame) =>
        Crypto.encodeBase64(Env.identity.sign(controlBytes(frame)));
    Env.verifyControl = (frame) => {
        const key = Env.originKey(frame?.o);
        if (!key || typeof (frame.sig) !== 'string') { return false; }
        try {
            return Crypto.detachedVerify(controlBytes(frame),
                Crypto.decodeBase64(frame.sig), key);
        } catch (e) { return false; }
    };

    // an originId IS its public key, base64 — no lookup table needed
    Env.originKey = (originId) => {
        try {
            const key = Buffer.from(originId, 'base64');
            return key.length === 32 ? key : undefined;
        } catch (e) { return undefined; }
    };

    /*  Tell core this channel is federated so its live fan-out picks it up.
        Core keeps the set; a miss costs a missed live push, which the next
        anti-entropy sync repairs. */
    Env.markFederated = (channel, opts) => {
        if (opts?.level === 'L2') { Env.multiMaster.add(channel); }
        /*  `mirror` tells core whether local writes on this channel have to be
            forwarded to an anchor (M2) or may be committed here. Core needs it
            on the write path, where asking us would cost a round trip per
            message. */
        Env.interface.sendEvent(Env.getCoreId(channel), 'FED_CHANNELS', {
            channel, federated: true,
            mirror: Boolean(opts?.mirror),
            level: opts?.level
        });
    };

    // sync.js handles an INVITE by replicating, exactly as if we had asked
    Env.replicate = (args, cb) => replicate(Env, args, cb);

    // prove a new session end to end immediately rather than at the next beat
    Env.onSessionUp = (session) => {
        beat(Env, session);
        /*  Backfill every channel we replicate with this peer. A reconnect must
            resume replication without an operator doing anything, and the sync
            that follows is what closes whatever gap the outage left.

            `channelsFor` and not `following()`: the latter is the map of
            channels we *mirror from an anchor*, which is an L1 notion and empty
            at L2 — where there is no anchor and every member is a peer. So a
            multi-master pad asked for nothing on reconnect: live messages
            resumed, and everything written while the peer was away was never
            pulled. Silent, and permanent once the R-48 guard starts holding
            those late arrivals back. */
        Env.subscriptions.channelsFor(session).forEach(channel => {
            Sync.requestSync(Env, session, channel);
        });
    };

    const interfaceConfig = {
        connector: WSConnector,
        index,
        infra,
        server: config,
        myId,
        Log: Env.Log
    };

    const callWithEnv = f => {
        return function () {
            [].unshift.call(arguments, Env);
            return f.apply(null, arguments);
        };
    };

    const CORE_COMMANDS = {
        'FED_PEER_PING': callWithEnv(pingPeer),
        'FED_STATUS': callWithEnv(status),
        'FED_PEERS': callWithEnv(listPeers),
        'FED_REPLICATE': callWithEnv(replicate),
        'FED_ENABLE_LOCAL': callWithEnv(enableLocal),
        'FED_CHANNEL_STATUS': callWithEnv(channelStatus),
        // core saw a local write on a federated channel
        'FED_LOCAL_MESSAGE': callWithEnv(Sync.onLocalMessage),
        'FED_WRITE_THROUGH': callWithEnv(Sync.writeThrough),
        'FED_CONTROL_OUT': callWithEnv(Sync.publishControl),
        'FED_ACCEPT': callWithEnv(Sync.acceptLocal),
        'FED_BLOB_FETCH': callWithEnv(BlobTransfer.fetch),
        'NEW_DECREES': callWithEnv(onNewDecrees),
        'SHUTDOWN': callWithEnv(shutdown)
    };

    nThen(w => {
        Env.server = Server.init(Env, Env.public, w());
    }).nThen(w => {
        Env.interface = Interface.init(interfaceConfig, w(err => {
            if (err) {
                w.abort();
                Env.Log.error('INTERFACE_INIT_ERROR', myId, ' error:', err);
                return;
            }
        }));
        Env.interface.handleCommands(CORE_COMMANDS);
    }).nThen(() => {
        /*  Rebuild replication from the state files before dialling out (R-52).

            Which channels are federated, and with whom, is kept in memory here
            and in core because both are consulted on the write path. A restart
            empties both, and nothing else puts them back: a SUBSCRIBE needs a
            capability only a browser can mint, so an instance cannot ask its
            peers to remind it. Left as it was, a restarted instance kept every
            replica's history on disk and quietly replicated to nobody — the pad
            still opened on both sides and simply stopped agreeing.

            Done before `peers.start()` so the routing is in place by the time a
            peer session comes up and the first heartbeat goes out. */
        /*  A short delay before the first attempt: core and storage are still
            finding each other at this point, and asking too early costs a
            round trip and an alarming-looking log line for no gain. Retrying
            covers us if this is not long enough. */
        Env.restoreTimer = setTimeout(() => {
            restoreFederation(Env, () => {
                // dial out only once we can reach core, so a peer never arrives first
                Env.peers.start();
            });
        }, 2000);

        Env.intervals = {
            heartbeat: setInterval(() => {
                Env.peers.sessions.list().forEach(session => { beat(Env, session); });
            }, HEARTBEAT)
        };

        Env.Log.info('FEDERATION_STARTED', { myId, originId: Env.identity.originId });
        if (process.send !== undefined) {
            process.send({ type: 'federation', index, msg: 'READY' });
        }
    });
};

module.exports = { start };
