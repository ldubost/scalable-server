// SPDX-License-Identifier: AGPL-3.0-or-later
// SPDX-FileCopyrightText: 2024 XWiki CryptPad Team <contact@cryptpad.org> and contributors
const Interface = require("../common/interface.js");
const WSConnector = require("../common/ws-connector.js");
const WorkerModule = require("../common/worker-module.js");
const WriteQueue = require("../common/write-queue.js");
const Constants = require("../common/constants.js");
const Core = require("../common/core.js");
const Util = require("../common/common-util.js");
const Logger = require("../common/logger.js");
const Rpc = require("./rpc.js");
const AuthCommands = require("./http-commands.js");
const nThen = require('nthen');

const StorageCommands = require('./commands/storage');

const Environment = require('../common/env.js');
const Backends = require('../common/storage/backend/index.js');

const {
    CHECKPOINT_PATTERN
} = Constants;

let Env = {
    userCache: {}, // user.from, user.authKeys
    historyFromCache: {}, // "from" on history commands (no JOIN command)
    channelKeyCache: {}, // Validate key of each channel
    queueValidation: WriteQueue(),
    Sessions: {},
    intervals: {},
};

const isFrontCmd = id => {
    return /^front:/.test(id);
};
const isStorageCmd = id => {
    return /^storage:/.test(id);
};
const isValidChannel = str => {
    return /^[a-f0-9]?[a-f0-9]{32,33}$/.test(str);
};


const getFrontId = (userId) => {
    return Env.userCache?.[userId]?.from ||
           Env.historyFromCache?.[userId]?.from || 'front:0';
};

let frontToStorage = function(command, validated, isEvent) {
    return function(args, cb, extra) {
        if (!validated) {
            let s = extra.from.split(':');
            if (s[0] !== 'front') {
                Env.Log.error('UNAUTHORIZED_USER_ERROR', command, 'received from unauthorized server:', args, extra);
                cb('UNAUTHORIZED_USER', void 0);
                return;
            }
        }
        let channel = args.channel;

        let storageId = Env.getStorageId(channel);

        if (isEvent) {
            Env.interface.sendEvent(storageId, command, args);
        }
        else {
            Env.interface.sendQuery(storageId, command, args, function(response) {
                cb(response.error, response.data);
            });
        }
    };
};

let storageToFront = function(command) {
    return function(args, cb, extra) {
        let s = extra.from.split(':');
        if (s[0] !== 'storage') {
            Env.Log.error('UNAUTHORIZED_USER_ERROR', command, 'received from unauthorized server:', args, extra);
            cb('UNAUTHORIZED_USER', void 0);
            return;
        }
        let userId = args.userId;

        let frontId = getFrontId(userId);

        Env.interface.sendQuery(frontId, command, args, function(response) {
            cb(response.error, response.data);
        });
    };
};

/*  Federation liveness probe (federation design §1.3, milestone M0).

    A federation node asks core to reach the storage tier on its behalf. This is
    the only federation command core routes in M0, and it exists to prove the
    federation -> core -> storage path that every later federation flow depends
    on. It carries no channel and touches no pad data.
*/
const onFedPing = (args, cb, extra) => {
    if (!/^federation:/.test(extra?.from || '')) {
        Env.Log.error('UNAUTHORIZED_USER_ERROR', 'FED_PING',
            'received from unauthorized server:', extra?.from);
        return void cb('UNAUTHORIZED_USER');
    }
    /*  Route on the originId so the probe lands on a real storage node chosen
        the same way a channel would be, rather than always storage:0. */
    const storageId = Env.getStorageId(args?.originId || '');
    Env.interface.sendQuery(storageId, 'FED_PING', {
        originId: args?.originId,
        nonce: args?.nonce
    }, response => {
        cb(response.error, response.data);
    });
};

/*  Public federation descriptor, for /api/federation (spec R-23).

    Front nodes cannot reach federation nodes directly — nothing can, except
    through core — so this hop exists to let the public endpoint publish the
    instance key. An instance with no federation nodes answers ENOFEDERATION,
    which the endpoint turns into an honest "this instance does not federate"
    rather than an error.
*/
const onFederationInfo = (args, cb) => {
    if (!Env.numberFederations) { return void cb('ENOFEDERATION'); }
    Env.interface.sendQuery('federation:0', 'FED_STATUS', {}, response => {
        cb(response.error, response.data);
    });
};

/*  Ask this instance to start mirroring a channel from a peer (design §5.2).

    Authorisation is the capability, which is signed by the pad's own key — so
    the caller proves the right to replicate the same way they would prove the
    right to write. The federation node verifies it, and the peering policy
    applies regardless of what the capability says.
*/
/*  Which instances this one may federate with. The client needs a peer's id to
    scope a capability to it, and must not ask that peer directly. */
const onFederationPeers = (args, cb, extra) => {
    if (!isFrontCmd(extra?.from || '')) { return void cb('UNAUTHORIZED'); }
    if (!Env.numberFederations) { return void cb('ENOFEDERATION'); }
    Env.interface.sendQuery('federation:0', 'FED_PEERS', {}, response => {
        cb(response.error, response.data);
    });
};

const onFederationEnable = (args, cb, extra) => {
    if (!isFrontCmd(extra?.from || '')) { return void cb('UNAUTHORIZED'); }
    if (!Env.numberFederations) { return void cb('ENOFEDERATION'); }
    const channel = args?.channel;
    if (!isValidChannel(channel)) { return void cb('INVALID_CHAN'); }
    Env.interface.sendQuery(Env.getFederationId(channel), 'FED_ENABLE_LOCAL', args,
        response => { cb(response.error, response.data); });
};

/*  Is this channel federated, and with whom (R-50)?

    The client needs this to federate a pad's auxiliary channels — the pad chat
    is created lazily, often long after the pad was federated, and only the
    client can name it. Without a way to ask, a chat opened after federation
    would never replicate.

    It discloses no more than the channel id already does: knowing the id is
    what grants access to a CryptPad channel in the first place, and the member
    list is the operator's own peer list, already public at
    /api/federation/peers.
*/
const onFederationChannelStatus = (args, cb, extra) => {
    if (!isFrontCmd(extra?.from || '')) { return void cb('UNAUTHORIZED'); }
    if (!Env.numberFederations) { return void cb('ENOFEDERATION'); }
    const channel = args?.channel;
    if (!isValidChannel(channel)) { return void cb('INVALID_CHAN'); }
    Env.interface.sendQuery(Env.getFederationId(channel), 'FED_CHANNEL_STATUS',
        { channel }, response => { cb(response.error, response.data); });
};

const onFederationReplicate = (args, cb, extra) => {
    if (!isFrontCmd(extra?.from || '')) { return void cb('UNAUTHORIZED'); }
    if (!Env.numberFederations) { return void cb('ENOFEDERATION'); }
    const channel = args?.channel;
    if (!isValidChannel(channel)) { return void cb('INVALID_CHAN'); }
    Env.interface.sendQuery(Env.getFederationId(channel), 'FED_REPLICATE', args,
        response => { cb(response.error, response.data); });
};

/*  Federation commands routed from a federation node to the storage node that
    owns the channel (design §1.3). Every one of them is a write or a read of a
    channel's log, so they route on the channel exactly as a front command does.

    The `extra.from` check is the authorisation boundary: these bypass the
    account layer entirely, so nothing but a federation node may reach them.
*/
/*  A control commit made here, on its way out to the replicas (spec §5.4).

    Emitted by storage after it has applied the change locally, so replication
    can never run ahead of the anchor's own state. Fire-and-forget: a peer that
    misses it is repaired by the next anti-entropy sync, exactly like a missed
    PUBLISH.
*/
const onFedControlOut = (args, cb, extra) => {
    cb ||= () => {};
    if (!isStorageCmd(extra?.from || '')) { return void cb('UNAUTHORIZED_USER'); }
    const channel = args?.channel;
    if (!isValidChannel(channel) || !Env.numberFederations) { return void cb(); }
    const fedId = Env.getFederationId(channel);
    if (!fedId) { return void cb(); }
    Env.interface.sendEvent(fedId, 'FED_CONTROL_OUT', args);
    cb();
};

/*  Every federated channel on this instance, gathered from every storage node
    (R-52).

    Unlike every other federation command this one has no channel to route on,
    and asking a single node would be wrong rather than merely incomplete:
    federation state is sharded across the storage tier by channel id, so one
    node holds one slice of it. A restart that rebuilt its routing from one
    slice would resume replication for some pads and silently drop the rest.

    A node that fails to answer is logged and skipped rather than failing the
    whole call, for the same reason: partial recovery beats none. The channels
    it holds resume when it is reachable again, since this is re-run on the
    heartbeat.
*/
const onFederationList = (args, cb, extra) => {
    if (!/^federation:/.test(extra?.from || '')) {
        return void cb('UNAUTHORIZED_USER');
    }
    const channels = [];
    let failed = 0;
    let pending = Env.numberStorages;
    if (!pending) { return void cb(void 0, { channels, incomplete: false }); }

    for (let i = 0; i < Env.numberStorages; i++) {
        Env.interface.sendQuery(`storage:${i}`, 'FED_LIST', {}, response => {
            /*  `sendQuery` answers a *string* when the destination is not
                connected yet, and an object otherwise. Checking `.error` alone
                reads `'EINVALDEST'` as a successful empty answer — which is
                exactly how this failed silently at startup, when storage has not
                yet connected to core: federation restored nothing, said so
                cheerfully, and replicated to no one. */
            const bad = !response || typeof (response) !== 'object' || response.error;
            if (bad) {
                failed++;
                /*  Info, not error: the overwhelmingly common cause is being
                    asked before storage has connected, seconds into a cold
                    start, which the caller handles by retrying. Logging it at
                    error level made a healthy instance fail its own install
                    check — the precise failure R-43 exists to prevent. A
                    genuinely stuck storage node shows up as the retry escalating
                    on the federation node, which is where the count lives. */
                Env.Log.info('FEDERATION_LIST_UNAVAILABLE', {
                    storage: i,
                    error: String((response && response.error) || response)
                });
            } else {
                (response.data?.channels || []).forEach(c => channels.push(c));
            }
            /*  `incomplete` matters more than the channels do: the caller must
                be able to tell "nothing is federated" from "I could not find
                out", because the first is a fact and the second is a retry. */
            if (--pending === 0) { cb(void 0, { channels, incomplete: failed > 0 }); }
        });
    }
};

const federationToStorage = (command, storageCommand) => {
    return function (args, cb, extra) {
        if (!/^federation:/.test(extra?.from || '')) {
            Env.Log.error('UNAUTHORIZED_USER_ERROR', command,
                'received from unauthorized server:', extra?.from);
            return void cb('UNAUTHORIZED_USER');
        }
        const channel = args?.channel;
        if (!isValidChannel(channel)) { return void cb('INVALID_CHAN'); }
        Env.interface.sendQuery(Env.getStorageId(channel),
            storageCommand || command, args, response => {
                cb(response.error, response.data);
            });
    };
};

/*  Blob commands route on the blob id rather than a channel: a blob belongs to
    no channel as far as any server knows, since the reference to it lives inside
    content nothing can read. The same jump hash is used, so every node agrees
    which storage node owns a given blob. */
const federationToBlobStorage = (command) => {
    return function (args, cb, extra) {
        if (!/^federation:/.test(extra?.from || '')) {
            return void cb('UNAUTHORIZED_USER');
        }
        const id = args?.id;
        if (typeof (id) !== 'string' || !/^[a-f0-9]{48}$/.test(id)) {
            return void cb('EBADBLOBID');
        }
        Env.interface.sendQuery(Env.getStorageId(id), command, args, response => {
            cb(response.error, response.data);
        });
    };
};

/*  A storage node is missing a blob a user asked for, and wants federation to
    try the peers. Routed to the federation node that owns... any peer: the
    fetch tries each live session in turn, so node 0 is as good as any. */
const onFedBlobFetch = (args, cb, extra) => {
    if (!isStorageCmd(extra?.from || '')) { return void cb('UNAUTHORIZED_USER'); }
    if (!Env.numberFederations) { return void cb('ENOFEDERATION'); }
    Env.interface.sendQuery('federation:0', 'FED_BLOB_FETCH', args, response => {
        cb(response.error, response.data);
    });
};

/*  Live fan-out to federation (milestone M1).

    Core already sees every message that storage has committed, on its way to the
    front nodes. Hanging the federation publish off that point means the storage
    hot path — `channel-manager.js`, which every message on the instance passes
    through — needs no edit at all in M1.

    The cost to a non-federated channel is one Set lookup. `federatedChannels` is
    populated when a channel is federated and on notification from storage; a
    channel missing from it is simply not published, and the peer's next
    anti-entropy sync picks up anything that was missed. That is the right
    failure direction: the sync is authoritative, the live push is an optimisation.
*/
Env.federatedChannels = new Set();
/*  Channels this instance mirrors rather than anchors (M2). A local write on one
    of these may not be committed here — it is forwarded to the anchor, which is
    the single ordering authority. Kept as a Set so the write path costs a lookup
    rather than a round trip. */
Env.mirroredChannels = new Set();
/*  Channels running multi-master (L2, M3): no anchor, every member writes, and
    the merge derives the order. Disjoint from `mirroredChannels`. */
Env.multiMasterChannels = new Set();

const publishToFederation = (message) => {
    if (!Env.numberFederations) { return; }
    const channel = message?.[3];
    if (!channel) { return; }
    if (!Env.federatedChannels.has(channel)) { return; }
    const content = message[4];
    if (typeof (content) !== 'string') { return; }

    /*  Route on the channel, not the peer: the federation node that publishes a
        channel is the one that will hold its merge state in M3. */
    const fedId = Env.getFederationId(channel);
    if (!fedId) { return; }
    Env.interface.sendEvent(fedId, 'FED_LOCAL_MESSAGE', {
        channel, content, time: message[5]
    });
};

// storage tells core which channels are federated, so the fan-out stays cheap
const onFederationChannels = (args, cb, extra) => {
    if (!isStorageCmd(extra?.from || '') && !/^federation:/.test(extra?.from || '')) {
        return void cb('UNAUTHORIZED_USER');
    }
    const { channel, federated } = args || {};
    if (!isValidChannel(channel)) { return void cb('INVALID_CHAN'); }
    if (federated) { Env.federatedChannels.add(channel); }
    else { Env.federatedChannels.delete(channel); }
    if (federated && args.mirror) { Env.mirroredChannels.add(channel); }
    else { Env.mirroredChannels.delete(channel); }
    if (federated && args.level === 'L2') { Env.multiMasterChannels.add(channel); }
    else { Env.multiMasterChannels.delete(channel); }
    cb();
};

const authenticateUser = (userId, unsafeKey, extra) => {
    const user = Env.userCache[userId] ||= {};
    if (!user.from) { user.from = extra.from; }
    const authKeys = user.authKeys ||= {};
    authKeys[unsafeKey] = +new Date();
};
const unauthenticateUser = (userId, unsafeKey) => {
    const user = Env.userCache[userId];
    if (!user?.authKeys) { return; }
    delete user.authKeys[unsafeKey];
};

const validateMessageHandler = (args, cb, extra) => {
    if (!isStorageCmd(extra.from)) {
        return void cb("UNAUTHORIZED");
    }

    const { channel, validateKey } = args;
    if (!channel || !validateKey) {
        return void cb('INVALID_ARGUMENTS');
    }

    // Store the validate key in memory to save a round-trip
    // to storage for future messages
    // See onChannelMessage
    Env.channelKeyCache[channel] = validateKey;

    Env.queueValidation(channel, next => {
        let avg = Env.plugins?.MONITORING?.average(`inlineValidation`);
        Env.workers.send('VALIDATE_MESSAGE', args, e => {
            avg?.time();
            next();
            cb(e);
        });
    });
};

const dropChannelHandler = (args, cb, extra) => {
    if (!isStorageCmd(extra.from)) { return; }

    const { channel } = args;
    if (!channel) { return; }
    delete Env.channelKeyCache[channel];
};

const sendChannelMessage = (users, message) => {
    const usersByFront = Core.getUsersFront(Env, users);
    Object.keys(usersByFront).forEach(frontId => {
        Env.interface.sendEvent(frontId, 'SEND_CHANNEL_MESSAGE', {
            users: usersByFront[frontId],
            message
        });
    });
};

/*  A mirror's write, arriving at the anchor (M2).

    Deliberately the same storage call a local write makes, so the content is
    validated against `validateKey` (R-15), checkpoint dedup applies, and the
    fan-out to local users and to every other replica happens through one
    mechanism rather than two.
*/
const onFedWrite = (args, cb, extra) => {
    if (!/^federation:/.test(extra?.from || '')) { return void cb('UNAUTHORIZED_USER'); }
    const { channel, msgStruct } = args || {};
    if (!isValidChannel(channel) || !Array.isArray(msgStruct)) {
        return void cb('EINVAL');
    }
    Env.interface.sendQuery(Env.getStorageId(channel), 'CHANNEL_MESSAGE', {
        channel, msgStruct, validated: false
    }, res => {
        if (res.error) { return void cb(res.error); }
        if (!res?.data?.message) { return void cb(); }   // duplicate checkpoint
        const { users, message } = res.data;
        sendChannelMessage(users, message);
        publishToFederation(message);
        cb();
    });
};


// Event: when a user is disconnected, remove it from all its channels
const dropUser = (args, _cb, extra) => {
    if (!isFrontCmd(extra.from)) { return; }

    const { channels, userId } = args;
    if (!userId || !Array.isArray(channels)) { return; }

    const done = new Set();
    const sent = new Set();
    channels.forEach(channel => {
        // And tell storages to clear their memory
        const storageId = Env.getStorageId(channel);
        if (sent.has(storageId)) { return; }
        sent.add(storageId);
        Env.interface.sendQuery(storageId, 'DROP_USER', args, res => {
            if (res.error) { return; }
            const lists = res.data;

            Object.keys(lists).forEach(channel => {
                if (done.has(channel)) { return; }
                const users = lists[channel];
                if (!users) { return; }

                // For each channel, send LEAVE message
                const message = [ 0, userId, 'LEAVE', channel ];
                sendChannelMessage(users, message);
                done.add(channel);
            });
        });
    });

    delete Env.userCache[userId];
};

const joinChannel = (args, cb, extra) => {
    if (!isFrontCmd(extra.from)) { return void cb('UNAUTHORIZED'); }

    const { channel, userId } = args;
    if (!userId || !isValidChannel(channel)) {
        return void cb('EINVAL');
    }

    const user = Env.userCache[userId] ||= {};
    if (!user.from) { user.from = extra.from; }

    const storageId = Env.getStorageId(channel);
    Env.interface.sendQuery(storageId, 'JOIN_CHANNEL', args, res => {
        if (res.error) { return void cb(res.error, res.data); }
        const users = res.data;

        const message = [ 0, userId, 'JOIN', channel ];
        sendChannelMessage(users, message);

        cb(void 0, users);
    });
};
const leaveChannel = (args, cb, extra) => {
    if (!isFrontCmd(extra.from)) { return void cb('UNAUTHORIZED'); }

    const { channel, userId } = args;
    if (!userId || !isValidChannel(channel)) {
        return void cb('EINVAL');
    }

    const storageId = Env.getStorageId(channel);
    Env.interface.sendQuery(storageId, 'LEAVE_CHANNEL', args, res => {
        if (res.error) { return void cb(res.error); }
        const users = res.data;

        const message = [ 0, userId, 'LEAVE', channel ];
        sendChannelMessage(users, message);

        cb();
    });
};

// Message from user to storage to channel members
const onChannelMessage = (args, cb, extra) => {
    if (!isFrontCmd(extra.from)) { return void cb('UNAUTHORIZED'); }

    const { channel, msgStruct } = args;
    if (!Array.isArray(msgStruct) || !isValidChannel(channel)) {
        return void cb('EINVAL');
    }

    /*  Anchored write-through (M2, conformance L1).

        On a channel we mirror, we are not the ordering authority: committing
        here would fork the log. Hand it to the anchor instead and acknowledge
        the client only once the anchor has taken it. The message arrives back
        through the normal PUBLISH path, so nothing echoes it locally.

        If the anchor is unreachable the write fails with a typed error rather
        than being accepted — a mirror that keeps accepting writes during a
        partition is precisely what L1 exists to prevent. */
    /*  L2 (M3): every member accepts writes. The federation node takes it,
        assigns a Lamport clock, makes it durable in the pending log and pushes
        it to the peers; it enters history when the merge lets it (R-3). The
        client is acknowledged as soon as it is durable (R-9), not when it
        commits (R-7). */
    if (Env.multiMasterChannels.has(channel)) {
        const fedId = Env.getFederationId(channel);
        if (!fedId) { return void cb('ENOFEDERATION'); }
        return void Env.interface.sendQuery(fedId, 'FED_ACCEPT', {
            channel, content: msgStruct[4], msgStruct
        }, res => {
            if (res?.error) { return void cb(res.error); }
            /*  Broadcast to our own users straight away. The committed order is
                settled later by the merge; the live tier has always been
                best-effort and separate (spec §1.6, §4.4). */
            const message = msgStruct.slice();
            message.push(+new Date());
            if (res?.data?.users) { sendChannelMessage(res.data.users, message); }
            cb();
        });
    }

    if (Env.mirroredChannels.has(channel)) {
        const fedId = Env.getFederationId(channel);
        if (!fedId) { return void cb('ENOFEDERATION'); }
        return void Env.interface.sendQuery(fedId, 'FED_WRITE_THROUGH', {
            channel, content: msgStruct[4]
        }, res => { cb(res?.error); });
    }

    const todo = (validated) => {
        const storageId = Env.getStorageId(channel);
        Env.interface.sendQuery(storageId, 'CHANNEL_MESSAGE', {
            channel, msgStruct, validated
        }, res => {
            if (res.error) {
                return void cb(res.error);
            }
            if (!res?.data?.message) {
                // duplicate checkpoint: nothing to send
                return void cb();
            }
            const { users, message } = res.data;

            sendChannelMessage(users, message);
            /*  ...and out to any federated replicas (M1).

                This is the write path every ordinary message takes: core answers
                the CHANNEL_MESSAGE *query* by fanning out itself, so the
                SEND_CHANNEL_MESSAGE event never fires here. Hooking it at this
                point still leaves storage/channel-manager.js untouched, which is
                the property worth keeping.  */
            publishToFederation(message);
            cb();
        });
    };

    if (Env.channelKeyCache[channel]) {
        const msg = msgStruct[4].replace(CHECKPOINT_PATTERN, '');
        const vKey = Env.channelKeyCache[channel];
        Env.queueValidation(channel, next => {
            let avg = Env.plugins?.MONITORING?.average(`inlineValidation`);
            Env.workers.send('VALIDATE_MESSAGE', {
                channel,
                signedMsg: msg,
                validateKey: vKey
            }, (e) => {
                avg?.time();
                next();
                if (e === 'FAILED') {
                    Env.Log.error("HK_SIGNED_MESSAGE_REJECTED", {
                        channel,
                        validateKey: vKey,
                        message: msg,
                    });
                    return void cb('FAILED_VALIDATION');
                }
                if (e) { return void cb(e); }
                todo(true);
            });
        });
        return;
    }

    todo(false);
};

// Message from history keeper to user
const onHistoryMessage = (args, cb) => {
    const { userId } = args; // userId, message
    const frontId = getFrontId(userId);
    Env.interface.sendQuery(frontId, 'SEND_USER_MESSAGE', args, res => {
        cb(res?.error, res?.data);
    });
};
// Message from history keeper to all members
const onHistoryChannelMessage = (args, cb) => {
    const { users, message } = args;
    // For each user, change the "dest" to their user id
    nThen(w => {
        users.forEach(userId => {
            const msg = message.slice();
            msg[3] = userId;
            const frontId = getFrontId(userId);
            Env.interface.sendQuery(frontId, 'SEND_USER_MESSAGE', {
                userId, message: msg
            }, w());
        });
    }).nThen(() => {
        if (typeof(cb) !== "function") { return; }
        cb();
    });
};

// Private message to all members
const onSendChannelMessage = (args) => {
    const { users, message, broadcast } = args;

    if (broadcast) { // admin channel, broadcast to all users
        Env.interface.broadcast('front', 'SEND_CHANNEL_MESSAGE', {
            broadcast,
            users,
            message
        });
        return;
    }

    sendChannelMessage(users, message);
};

// Message from user to user
const onUserMessage = (args, cb) => {
    Env.interface.broadcast('front', 'SEND_USER_MESSAGE', args, (err, values) => {
        // If all responses return an error, message has failed
        if (!values.length) {
            return void cb('ERROR');
        }
        // Otherwise, success
        cb();
    });
};

const onAnonRpc = (args, cb, extra) => {
    if (!isFrontCmd(extra.from)) { return void cb('UNAUTHORIZED'); }
    const {userId, /*txid, */data} = args;

    if (!Rpc.isUnauthenticateMessage(data)) {
        return void cb('INVALID_ANON_RPC_COMMAND');
    }

    let avg = Env.plugins?.MONITORING?.average(`rpc_${data[0]}`);

    Rpc.handleUnauthenticated(Env, data, userId, (err, msg) => {
        avg?.time();
        cb(err, msg);
    });
};
const onAuthRpc = (args, cb, extra) => {
    if (!isFrontCmd(extra.from)) { return void cb('UNAUTHORIZED'); }
    const {userId, /*txid, */data} = args;


    const sig = data.shift();
    const publicKey = data.shift();
    const [cookie, command/*, data*/] = data;


    const safeKey = Util.escapeKeyCharacters(publicKey);
    const hadSession = Boolean(Env.Sessions[safeKey]);

    // make sure a user object is initialized in the cookie jar
    if (publicKey) {
        Core.getSession(Env.Sessions, publicKey);
    } else {
        Env.Log.debug("NO_PUBLIC_KEY_PROVIDED", publicKey);
    }

    if (!Core.isValidCookie(Env.Sessions, publicKey, cookie)) {
        // no cookie is fine if the RPC is to get a cookie
        if (command !== 'COOKIE') {
            return void cb('NO_COOKIE');
        }
    }

    let serialized = JSON.stringify(data);
    if (!(serialized && typeof(publicKey) === 'string')) {
        return void cb('INVALID_MESSAGE_OR_PUBLIC_KEY');
    }

    let avg = Env.plugins?.MONITORING?.average(`rpc_${command}`);

    if (command === 'UPLOAD') {
        // UPLOAD is a special case that skips signature validation
        // intentional fallthrough behaviour
        return void Rpc.handleAuthenticated(Env, publicKey, data, cb);
    }

    if (!Rpc.isAuthenticatedCall(command)) {
        Env.Log.warn('INVALID_RPC_CALL', command);
        return void cb("INVALID_RPC_CALL");
    }

    // check the signature on the message
    // refuse the command if it doesn't validate
    let avgVal = Env.plugins?.MONITORING?.average(`detachedValidation`);
    Env.workers.send('VALIDATE_RPC', {
        msg: serialized,
        key: publicKey,
        sig
    }, err => {
        avgVal?.time();
        if (err) {
            return void cb("INVALID_SIGNATURE_OR_PUBLIC_KEY");
        }
        if (command === 'COOKIE' && !hadSession && Env.logIP) {
            Env.Log.info('NEW_RPC_SESSION', {userId: userId, publicKey: publicKey});
        }
        if (command === "DESTROY") {
            unauthenticateUser(userId, publicKey);
            return; // No need to respond, user will close the session
        }

        // XXX COOKIE shouldn't add the key to the user session
        // --> risk of replay attacks
        // We should instead create a new AUTH command, which does
        // nothing but requires a recent cookie to work.
        // We can then update onRejected in async-store to call
        // this AUTH command before retrying to join a pad.
        authenticateUser(userId, publicKey, extra);

        return Rpc.handleAuthenticated(Env, publicKey, data, Util.both(cb, avg?.time));
    });
};

const onGetMultipleFileSize = (channels, cb, extra) => {
    if (!isStorageCmd(extra.from)) { return void cb("UNAUTHORIZED"); }
    StorageCommands.getMultipleFileSize(Env, channels, cb);
};

const onStorageToFront = (args, cb, extra) => {
    if (!isStorageCmd(extra.from)) { return void cb("UNAUTHORIZED"); }
    const { cmd, data } = args;
    Env.interface.broadcast('front', cmd, data, (errors, data) => {
        if (errors && errors.length) { return void cb(errors, data); }
        cb(void 0, data);
    });
};

const onHttpCommand = (args, cb, extra) => {
    if (!isFrontCmd(extra.from)) { return void cb('UNAUTHORIZED'); }
    AuthCommands.handle(Env, args, cb);
};

const onFrontCommand = command => {
    return (args, cb, extra) => {
        if (!isFrontCmd(extra.from)) { return void cb('UNAUTHORIZED'); }
        const channel = args?.channel;
        const userId = args?.userId;
        if (!channel || !userId) { return void cb('INVALID_ARGS'); }
        const storageId = Env.getStorageId(channel);

        const cache = Env.historyFromCache[userId] ||= { from: extra.from };
        cache.atime = +new Date();

        Env.interface.sendQuery(storageId, command, args, function(response) {
            cb(response.error, response.data);
        });
    };
};


// When receiving new decrees from storage:0, update our env
// and broadcast to all the other nodes
const onNewDecrees = (args, cb, extra) => {
    if (!isStorageCmd(extra.from)) { return void cb("UNAUTHORIZED"); }
    const { type, decrees, freshKey, curveKeys } = args;

    Env.FRESH_KEY = freshKey;
    Env.curveKeys ||= curveKeys;

    Env.getDecree(type).loadRemote(Env, decrees);
    Env.cacheDecrees(type, decrees);

    // core:0 also has to broadcast to all the front and storage
    // nodes
    nThen(waitFor => {
        if (Env.myId !== 'core:0') { return; }
        Env.interface.broadcast('front', 'NEW_DECREES', {
            freshKey, curveKeys, type, decrees
        }, waitFor((errors) => {
            errors.forEach(obj => {
                const { id, error } = obj;
                Env.Log.error("BCAST_DECREES_ERROR", { id, error });
            });
        }));
        const exclude = ['storage:0'];
        Env.interface.broadcast('storage', 'NEW_DECREES', {
            freshKey, curveKeys, type, decrees
        }, waitFor((errors) => {
            errors.forEach(obj => {
                const { id, error } = obj;
                Env.Log.error("BCAST_DECREES_ERROR", { id, error });
            });
        }), exclude);
        Env.interface.sendQuery('http:0', 'NEW_DECREES', {
            freshKey, curveKeys, type, decrees
        }, waitFor());
    }).nThen(() => {
        cb();
    });
};

const onAccountsLimits = (args, cb, extra) => {
    if (!isStorageCmd(extra.from)) { return void cb("UNAUTHORIZED"); }
    const { limits } = args;

    Env.limits = limits;
    Env.accountsLimits = limits;
    Core.applyLimits(Env);

    if (Env.myId !== 'core:0') { return void cb(); }

    // Core:0 also has to broadcast to all other storages
    const exclude = ['storage:0'];
    Env.interface.broadcast('storage', 'ACCOUNTS_LIMITS', {
        limits
    }, () => { cb(); }, exclude);
};

const onGetAuthKeys = (args, cb, extra) => {
    if (!isStorageCmd(extra.from)) { return void cb("UNAUTHORIZED"); }
    const { userId } = args;

    const user = Env.userCache[userId];
    if (!user) { return void cb(void 0, {}); }
    const authKeys = user.authKeys || {};

    cb(void 0, authKeys);
};

const initIntervals = () => {
    // expire old sessions once per minute
    Env.intervals.sessionExpirationInterval = setInterval(() => {
        Core.expireSessions(Env.Sessions);
    }, Core.SESSION_EXPIRATION_TIME);
    Env.intervals.historyFromInterval = setInterval(() => {
        const now = +new Date();
        Object.keys(Env.historyFromCache).forEach(userId => {
            const u = Env.historyFromCache[userId];
            if (!u || (now - u.atime) > Core.SESSION_EXPIRATION_TIME) {
                delete Env.historyFromCache[userId];
            }
        });
    }, Core.SESSION_EXPIRATION_TIME);
};

const onIsUserOnline = (safeKey, cb) => {
    if (!Core.isValidPublicKey(safeKey)) { return void cb("EINVAL"); }
    const unsafeKey = Util.unescapeKeyCharacters(safeKey);
    cb(void 0, Object.values(Env.userCache)
        .some(v => v.authKeys && Object.keys(v.authKeys).includes(unsafeKey)));
};

const onFlushCache = Env.flushCache = (_args, cb) => {
    if (Env.myId !== 'core:0') { return void cb('EINVAL'); }

    Env.interface.broadcast('front', 'ADMIN_CMD', {
        cmd: 'FLUSH_CACHE',
        data: { freshKey: +new Date() }
    }, () => { cb(void 0, true); });
};

// SET_MODERATORS triggers a FLUSH_CACHE to refresh the client cache
const onSetModerators = (args) => {
    if (Env.myId !== 'core:0') { return void Env.Log.error('INVALID_CORE_ERROR'); }
    Env.moderators = args;
    Env.freshKey = +new Date();
    // XXX: The following command should not be an ADMIN_CMD
    Env.interface.broadcast('front', 'ADMIN_CMD', {
        cmd: 'SET_MODERATORS', data: {
            moderators: args,
            freshKey: Env.freshKey
        }
    }, () => { });
};

const checkCacheInterval = Util.once(() => {
    const interval = 5*60*1000; // 5min
    const dropUser = (userId) => {
        Env.interface.broadcast('storage', 'DROP_USER', {
            userId
        }, (err, allLists) => {
            allLists.forEach(lists => {
                Object.keys(lists || {}).forEach(channel => {
                    const users = lists[channel];
                    const message = [ 0, userId, 'LEAVE', channel ];
                    sendChannelMessage(users, message);
                });
            });
        });
    };
    Env.intervals.checkCacheInterval = setInterval(() => {
        Env.interface.broadcast('front', 'ADMIN_CMD', {
            cmd: 'GET_ACTIVE_USERS'
        }, (err, data) => {
            // Convert each front userlist into a Set
            const all = {};
            data.forEach(obj => { // for each "front" node
                const { myId, users } = obj;
                const set = new Set(users);
                all[myId] = set;
            });
            // Make sure users from our cache are in the matching Set
            Object.keys(Env.userCache).forEach(userId => {
                let from = Env.userCache[userId].from;
                if (!from || !all[from]) { // disconnected front?
                    delete Env.userCache[userId];
                    dropUser(userId);
                    return;
                }
                if (!all[from].has(userId)) { // disconnected user
                    delete Env.userCache[userId];
                    dropUser(userId);
                }
            });
        });
        Env.interface.broadcast('storage', 'GET_ACTIVE_CHANNELS', {
        }, (err, data) => {
            // Convert each storage channel list into a Set
            const all = {};
            data.forEach(obj => { // for each "front" node
                const { myId, channels } = obj;
                const set = new Set(channels);
                all[myId] = set;
            });

            const allKeys = Object.keys(all);
            // Make sure channels from our cache are in the matching Set
            Object.keys(Env.channelKeyCache).forEach(channel => {
                if (allKeys.some(storageId => {
                    return all[storageId].has(channel);
                })) { return; }

                // This channel is not in any storage's list
                delete Env.channelKeyCache[channel];
            });
        });
    }, interval);
});

const startServers = (mainConfig) => {
    let { myId, index, config, infra } = mainConfig;
    Environment.init(Env, mainConfig);
    Env.Log = Logger(config, myId);

    const interfaceConfig = {
        connector: WSConnector,
        infra,
        server: config,
        myId,
        index,
        Log: Env.Log
    };

    const WORKERS = Env.maxWorkers['core'] || 2;
    const workerConfig = {
        Log: Env.Log,
        workerPath: './build/core.worker.js',
        maxWorkers: WORKERS,
        maxJobs: Env.maxJobs['core'] || 10,
        commandTimers: {}, // time spent on each command
        config: {
        },
        Env: { // Serialized Env (Environment.serialize)
        }
    };

    const paths = Core.getPaths(mainConfig);
    Env.challengePath = paths.challengePath;

    Env.workers = WorkerModule(workerConfig);

    let queriesToStorage = [];
    let queriesToFront = [];
    let eventsToStorage = [];
    let COMMANDS = {
        // From Front
        'DROP_USER': dropUser,
        'JOIN_CHANNEL': joinChannel,
        'LEAVE_CHANNEL': leaveChannel,
        'CHANNEL_MESSAGE': onChannelMessage,
        'USER_MESSAGE': onUserMessage,
        'ANON_RPC': onAnonRpc,
        'AUTH_RPC': onAuthRpc,
        'HTTP_COMMAND': onHttpCommand,
        'GET_HISTORY': onFrontCommand('GET_HISTORY'),
        'GET_FULL_HISTORY': onFrontCommand('GET_FULL_HISTORY'),
        'GET_HISTORY_RANGE': onFrontCommand('GET_HISTORY_RANGE'),
        // From Storage
        'VALIDATE_MESSAGE': validateMessageHandler,
        'DROP_CHANNEL': dropChannelHandler,
        'HISTORY_MESSAGE': onHistoryMessage,
        'HISTORY_CHANNEL_MESSAGE': onHistoryChannelMessage,
        'NEW_DECREES': onNewDecrees,
        'ACCOUNTS_LIMITS': onAccountsLimits,
        'SEND_CHANNEL_MESSAGE': onSendChannelMessage,
        'GET_AUTH_KEYS': onGetAuthKeys,
        'SET_MODERATORS': onSetModerators,
        // From Core
        'IS_USER_ONLINE': onIsUserOnline,
        'FLUSH_CACHE': onFlushCache,

        'GET_MULTIPLE_FILE_SIZE': onGetMultipleFileSize,

        'STORAGE_FRONT': onStorageToFront,

        // From Federation
        'FED_PING': onFedPing,
        'FED_ENABLE': federationToStorage('FED_ENABLE'),
        'FED_STATE': federationToStorage('FED_STATE'),
        'FED_LIST': onFederationList,
        'FED_SINCE': federationToStorage('FED_SINCE'),
        'FED_HEAD': federationToStorage('FED_HEAD'),
        'FED_INGEST': federationToStorage('FED_INGEST'),
        'FED_CHANNELS': onFederationChannels,
        'FED_WRITE': onFedWrite,
        'FED_CONTROL_OUT': onFedControlOut,
        'FED_CONTROL': federationToStorage('FED_CONTROL'),
        // the anchor's metadata, so a new replica can seed its own (R-21)
        'FED_METADATA': federationToStorage('FED_METADATA', 'GET_METADATA'),
        // blobs (R-44): no channel, so routed on the blob id instead
        'FED_BLOB_STAT': federationToBlobStorage('FED_BLOB_STAT'),
        'FED_BLOB_READ': federationToBlobStorage('FED_BLOB_READ'),
        'FED_BLOB_WRITE': federationToBlobStorage('FED_BLOB_WRITE'),
        'FED_BLOB_FETCH': onFedBlobFetch,
        // L2 (M3)
        'FED_ALLOCATE': federationToStorage('FED_ALLOCATE'),
        'FED_ACCEPT_REMOTE': federationToStorage('FED_ACCEPT_REMOTE'),
        'FED_OBSERVE': federationToStorage('FED_OBSERVE'),
        'FED_TAIL': federationToStorage('FED_TAIL'),
        'FED_RECONCILE': federationToStorage('FED_RECONCILE'),
        'FED_HAVE': federationToStorage('FED_HAVE'),
        'FED_TRIM': federationToStorage('FED_TRIM'),
        // From Front, about federation
        'FEDERATION_INFO': onFederationInfo,
        'FEDERATION_REPLICATE': onFederationReplicate,
        'FEDERATION_PEERS': onFederationPeers,
        'FEDERATION_ENABLE': onFederationEnable,
        'FEDERATION_CHANNEL_STATUS': onFederationChannelStatus,
    };
    queriesToStorage.forEach(function(command) {
        COMMANDS[command] = frontToStorage(command);
    });
    queriesToFront.forEach(function(command) {
        COMMANDS[command] = storageToFront(command);
    });
    eventsToStorage.forEach(function(command) {
        COMMANDS[command] = frontToStorage(command, false, true);
    });

    /*  Core nodes store short-lived authentication challenges, and challenges
        gate file uploads. Nothing may be served before the backend that holds
        them exists, or the first upload after a restart fails. */
    const serve = () => {

    initIntervals();

    Env.interface = Interface.init(interfaceConfig, err => {
        if (err) {
            Env.Log.error('INTERFACE_INIT_ERROR', err);
            return;
        }
        if (process.send !== undefined) {
            process.send({ type: 'core', index, msg: 'READY' });
        }
        checkCacheInterval();
    });
    Env.plugins.call('addCoreCommands')(Env, COMMANDS);
    Env.interface.handleCommands(COMMANDS);
    if (Env.myId !== 'core:0') { return; }
    Env.interface.onNewConnection(obj => {
        const id = `${obj.type}:${obj.index}`;
        Env.interface.sendEvent(id, 'ACCOUNTS_LIMITS', {
            limits: Env.accountsLimits
        });
        Object.keys(Env.allDecrees).forEach(type => {
            const decrees = Env.allDecrees[type];
            Env.interface.sendEvent(id, 'NEW_DECREES', {
                curveKeys: Env.curveKeys,
                freshKey: Env.FRESH_KEY,
                type, decrees
            });
        });
        if (obj.type === 'front') {
            Env.interface.sendEvent(id, 'ADMIN_CMD', { cmd: 'SET_MODERATORS', data: { moderators: Env.moderators, freshKey: Env.freshKey } });
        }
    });

    };

    /*  Core only reads and writes small challenge objects, so it skips the
        capability probe: that probe uploads several MiB to test multipart copy,
        which is pointless work for a node that never appends. */
    const storageConfig = config.storage || {};
    const storageType = storageConfig.type || 'fs';
    const coreStorage = Object.assign({}, storageConfig, {
        [storageType]: Object.assign({}, storageConfig[storageType], {
            skipProbe: true
        })
    });

    Backends.create(coreStorage, { root: paths.basePath }, (err, backend) => {
        if (err) {
            Env.Log.error('CORE_STORAGE_BACKEND_ERROR', err.message || err);
            throw err;
        }
        Env.storageBackend = backend;
        serve();
    });
};

module.exports = {
    start: startServers
};
