// SPDX-License-Identifier: AGPL-3.0-or-later
// SPDX-FileCopyrightText: 2024 XWiki CryptPad Team <contact@cryptpad.org> and contributors
const Crypto = require('node:crypto');

const Util = require("./common-util.js");
const Constants = require("../common/constants.js");
const Logger = require("../common/logger.js");
const Core = require("../common/core.js");

const Nacl = require('tweetnacl/nacl-fast');
const nThen = require("nthen");

const HistoryManager = require("./history-manager.js");
const ChannelManager = require("./channel-manager.js");
const FederationManager = require("./federation/manager.js");
const FederationBlobs = require("./federation/blobs.js");
const Merge = require("./federation/merge.js");
const Backends = require("../common/storage/backend/index.js");
const HKUtil = require("./hk-util.js");
const MFAManager = require('./mfa-manager.js');

const Environment = require('../common/env.js');
const Shutdown = require('../common/shutdown.js');

const Interface = require("../common/interface.js");
const WSConnector = require("../common/ws-connector.js");

const BatchRead = require("./batch-read.js");
const WriteQueue = require("../common/write-queue.js");
const WorkerModule = require("../common/worker-module.js");
const Cluster = require("node:cluster");
const File = require("./storage/file.js");
const Blob = require("./storage/blob.js");
const Stores = require("./storage/index.js");
const BlockStore = require("./storage/block.js");
const Sessions = require("./storage/sessions.js");
const Basic = require("./storage/basic.js");

const Decrees = require('./commands/decrees.js');
const Upload = require('./commands/upload.js');
const Pinning = require('./commands/pin.js');
const Quota = require('./commands/quota.js');
const Block = require('./commands/block.js');
const Metadata = require('./commands/metadata.js');
const Admin = require('./commands/admin.js');
const Moderators = require('./commands/moderators.js');

const {
    TEMPORARY_CHANNEL_LIFETIME,
    ADMIN_CHANNEL_LENGTH,
    STANDARD_CHANNEL_LENGTH,
    hkId
} = Constants;

const Env = {
    id: Util.uid(),
    metadata_cache: {},
    channel_cache: {},
    pin_cache: {},
    cache_checks: {},
    intervals: {},
    queueStorage: WriteQueue(),
    queueValidation: WriteQueue(),
    queueMetadata: WriteQueue(),
    /*  Federation state is read-modify-written by several paths at once —
        accepting a local write, accepting a peer's, observing a heartbeat, and
        the merge. They all use a conditional put, so without serialising them
        one loses the race and its update is silently dropped. Its own queue,
        not queueStorage, because the merge appends to the log while holding it. */
    queueFederation: WriteQueue(),
    queueDeletes: WriteQueue(),
    batchIndexReads: BatchRead("HK_GET_INDEX"),
    batchMetadata: BatchRead("GET_METADATA"),
    batchUserPins:  BatchRead('LOAD_USER_PINS'),
    batchTotalSize: BatchRead('GET_TOTAL_SIZE'),
    batchRegisteredUsers: BatchRead("GET_REGISTERED_USERS"),
    batchAccountQuery: BatchRead("QUERY_ACCOUNT_SERVER"),
    batchDiskUsage: BatchRead('GET_DISK_USAGE'),
    selfDestructTo: {},
    blobstage: {} // Store file streams to write blobs
};

Env.checkCache = channel => {
    let f = Env.cache_checks[channel] ||= Util.throttle(() => {
        delete Env.cache_checks[channel];
        if (Env.channel_cache[channel]) { return; }
        delete Env.metadata_cache[channel];
    }, 30000);
    f();
};

Env.channelContainsUser = (channel, userId) => {
    const cache = Env.channel_cache[channel];
    // Check if the channel exists in this storage
    if (!cache || !(cache.users instanceof Set)) { return false; }

    // Check if the user is a member of this channel
    return cache.users.has(userId);
};

const onDropChannel = channel => {
    let meta = Env.metadata_cache[channel];
    delete Env.metadata_cache[channel];
    delete Env.channel_cache[channel];

    if (meta && meta.selfdestruct && Env.selfDestructTo) {
        Env.selfDestructTo[channel] = setTimeout(function() {
            Env.CM?.removeChannel(Env, channel);
        }, TEMPORARY_CHANNEL_LIFETIME);
    }
    if (Env.store) {
        Env.store.closeChannel(channel, function() { });
    }

    const coreId = Env.getCoreId(channel);
    Env.interface.sendEvent(coreId, 'DROP_CHANNEL', { channel });
};

Env.onExpiredChannel = channel => {
    const channelData = Env.channel_cache[channel];
    if (!channelData?.users) { return; }
    const users = Array.from(channelData.users);

    const coreId = Env.getCoreId(channel);
    const message = [0, hkId, 'MSG', null, JSON.stringify({
        error: 'EEXPIRED', channel
    })];
    Env.interface.sendEvent(coreId, 'HISTORY_CHANNEL_MESSAGE', {
        users, message
    });
    onDropChannel(channel);
};

// Handlers
const sendMessage = (userId, channel) => {
    return (message, cb) => {
        if (!userId) { return; }
        const coreId = Env.getCoreId(channel);
        const f = typeof(cb) === "function" ?
            Env.interface.sendQuery :
            Env.interface.sendEvent;
        f(coreId, 'HISTORY_MESSAGE', {
            userId, message
        }, () => {
            // No args for cb, this can be a "readMore" call
            // which will fail if we pass arguments
            cb();
        });
    };
};

const getHistoryHandler = f => {
    return (args, cb) => {
        const parsed = args?.parsed;
        const channel = parsed?.[1];
        const send = sendMessage(args?.userId, channel);
        f(Env, args, send, cb);
    };
};

const onChannelMessageHandler = (args, cb) => {
    Env.CM.onChannelMessage(args, cb);
};

// TODO: move to channel-manager
const joinChannelHandler = (args, cb) => {
    let { channel, userId } = args;

    const channelData = Env.channel_cache[channel] ||= {
        users: new Set()
    };

    if (channel.length === ADMIN_CHANNEL_LENGTH) {
        // don't create a userlist for the broadcast channel
        return void cb(void 0, []);
    }

    const onSuccess = () => {
        // If you're allowed to join the channel, add yourself
        // and callback with the old userlist (without you)
        const _users = Array.from(channelData.users);
        channelData.users.add(userId);
        return void cb(void 0, _users);
    };

    if (channel.length !== STANDARD_CHANNEL_LENGTH) {
        // This is probably an ephemeral channel
        // only conventional channels can be restricted
        return void onSuccess();
    }

    HistoryManager.getMetadata(Env, channel, (err, metadata) => {
        if (err) {
            Env.Log.error('HK_METADATA_ERR', {
                channel, error: err,
            });
        }

        if (metadata?.selfdestruct &&
            metadata.selfdestruct !== Env.id) {
            Env.CM.removeChannel(Env, channel);
            return void cb('ESELFDESTRUCT');
        }

        if (Env.selfDestructTo && Env.selfDestructTo[channel]) {
            clearTimeout(Env.selfDestructTo[channel]);
        }

        if (!metadata?.restricted) {
            // the channel doesn't have metadata, or it does and
            // it's not restricted: either way, let them join.
            return void onSuccess();
        }

        // this channel is restricted. verify that the user in
        // question is in the allow list

        const allowed = HKUtil.listAllowedUsers(metadata);


        const check = (authKeys) => {
            if (HKUtil.isUserSessionAllowed(allowed, authKeys)) {
                return void onSuccess();
            }
            // If the channel is restricted, send the history keeper ID
            // so that they can try to authenticate
            allowed.unshift(hkId);
            // otherwise they're not allowed.
            // respond with a special error that includes the list of keys
            // which would be allowed...
            // FIXME RESTRICT bonus points if you hash the keys to limit data
            //       exposure
            cb("ERESTRICTED", allowed);
        };

        const coreRpc = Env.getCoreId(userId);
        Env.interface.sendQuery(coreRpc, 'GET_AUTH_KEYS', {
            userId
        }, res => {
            check(res?.data || {});
        });
    });
};
const leaveChannelHandler = (args, cb) => {
    const { channel, userId } = args;

    const channelData = Env.channel_cache[channel];
    const users = channelData?.users;
    if (!(users instanceof Set)) {
        return void cb('ENOENT');
    }
    if (!users.has(userId)) {
        return void cb('NOT_IN_CHAN');
    }
    users.delete(userId);

    if (!users.size) { onDropChannel(channel); }

    cb(void 0, Array.from(users));
};

const dropUserFromAll = (userId, cb) => {
    const userLists = {};
    Object.keys(Env.channel_cache).forEach(channel => {
        const channelData = Env.channel_cache[channel];
        const users = channelData.users;
        if (!(users instanceof Set)) { return; }
        if (!users.has(userId)) { return; }
        users.delete(userId);
        if (!users.size) {
            onDropChannel(channel);
            return;
        }
        userLists[channel] = Array.from(users);
    });
    cb(void 0, userLists);
};
const dropUserHandler = (args, cb) => {
    let { channels, userId } = args;

    // If we received this command from "checkCache" in core,
    // it means we won't have the channels list from this user
    // and we must check all our channels...
    if (typeof(channels) === "undefined") {
        return dropUserFromAll(userId, cb);
    }

    const userLists = {};
    channels.forEach(channel => {
        const cache = Env.channel_cache[channel];
        // Check if the channel exists in this storage
        if (!cache || !(cache.users instanceof Set)) { return; }

        // Check if the user is a member of this channel
        if (!cache.users.has(userId)) { return; }

        // Remove the user
        cache.users.delete(userId);

        // Clean the channel if no remaining members
        if (!cache.users.size) {
            onDropChannel(channel);
            return;
        }

        userLists[channel] = Array.from(cache.users);
    });
    cb(void 0, userLists);
};

const newDecreeHandler = (args, cb) => { // bcast from core:0
    const { type, decrees, curveKeys } = args;
    Env.getDecree(type).loadRemote(Env, decrees);
    Env.cacheDecrees(type, decrees);
    if (curveKeys) { Env.curveKeys = curveKeys; }
    Env.clusters.broadcast('NEW_DECREES', {
        type, decrees
    }, () => {
        Env.Log.silly('UPDATE_DECREE_STORAGE_CLUSTER');
    });
    Env.workers.broadcast('NEW_DECREES', {
        type, decrees
    }, () => {
        Env.Log.silly('UPDATE_DECREE_STORAGE_WORKER');
    });
    cb();
};

const getChannelListHandler = (args, cb) => {
    Pinning.getChannelList(Env, args.safeKey, channels => {
        cb(void 0, channels);
    }, true);
};

const accountsLimitsHandler = (args, cb) => { // sent from UI
    Env.limits = args.limits;
    Core.applyLimits(Env);
    cb();
};

// Internal cleanup commands

const getActiveChannels = (args, cb) => {
    cb(void 0, {
        myId: Env.myId,
        channels: Object.keys(Env.channel_cache)
    });
};

/* RPC commands */

const adminDecreeHandler = (decree, cb) => { // sent from UI
    Decrees.onNewDecree(Env, decree, '', cb);
};
const getFileSizeHandler = (channel, cb) => {
    Pinning.getFileSize(Env, channel, cb);
};
const getMultipleFileSizeHandler = (channels, cb) => {
    Pinning.getMultipleFileSize(Env, channels, cb, true);
};
const getDeletedPadsHandler = (channels, cb) => {
    Pinning.getDeletedPads(Env, channels, cb);
};
const getTotalSizeHandler = (args, cb) => {
    Pinning.getTotalSize(Env, args.safeKey, cb, true);
};
const getChannelsTotalSizeHandler = (channels, cb) => {
    Pinning.getChannelsTotalSize(Env, channels, cb, true);
};
const getRegisteredUsersHandler = (args, cb) => {
    Pinning.getRegisteredUsers(Env, cb, true);
};

/*  Metadata on a federated channel is anchor-authoritative (spec §6, L1).

    A mirror must not mutate it: the two replicas would drift apart with no
    mechanism to reconcile them, and R-20 says metadata has to be a pure function
    of the replicated command sequence. So the mirror refuses, and the anchor
    replicates each accepted command as a META control commit.
*/
const setMetadataHandler = (args, cb) => {
    const channel = args?.channel;
    const isAnchor = Env.FM.isAnchor(channel);
    if (isAnchor === false) {
        return void cb('EFEDERATED_MIRROR');
    }
    Metadata.setMetadata(Env, args, (err, metadata, line) => {
        cb(err, metadata);
        if (err || !isAnchor || !line) { return; }
        /*  Replicate the command, not the resulting state: applying the same
            commands in the same order is what makes every replica's metadata a
            pure function of the log (R-20). Shipping the state would paper over
            a divergence instead of preventing one. */
        Env.interface.sendEvent(Env.getCoreId(channel), 'FED_CONTROL_OUT', {
            channel,
            ctrl: { t: 'META', line }
        });
    });
};
const getMetadataHandler = (args, cb) => {
    HistoryManager.getMetadata(Env, args?.channel, cb);
};
const isNewChannelHandler = (args, cb) => {
    Env.CM.isNewChannel(Env, args?.channel, cb);
};

const writePrivateMessageHandler = (args, cb) => {
    Env.CM.writePrivateMessage(Env, args, cb);
};
const deleteChannelLineHandler = (args, cb) => {
    Env.CM.deleteMailboxMessage(Env, args, cb);
};

const getPinningResetHandler = (data, cb) => {
    const { channels, safeKey } = data;
    Pinning.resetUserPins(Env, safeKey, channels, cb);
};
const getPinningPinHandler = (data, cb) => {
    const { channels, safeKey } = data;
    Pinning.pinChannel(Env, safeKey, channels, cb);
};
const getPinningUnpinHandler = (data, cb) => {
    const { channels, safeKey } = data;
    Pinning.unpinChannel(Env, safeKey, channels, cb);
};

const getHashHandler = (data, cb) => {
    Pinning.getHash(Env, data.safeKey, cb);
};
const archivePinLogHandler = (data, cb) => {
    Pinning.removePins(Env, data.safeKey, cb);
};
const trimPinLogHandler = (data, cb) => {
    Pinning.trimPins(Env, data.safeKey, cb);
};

const clearOwnedChannelHandler = (data, cb) => {
    Env.CM.clearOwnedChannel(Env, data, cb);
};
const removeOwnedChannelHandler = (data, cb) => {
    Env.CM.removeOwnedChannel(Env, data, cb);
};
const trimHistoryHandler = (data, cb) => {
    Env.CM.trimHistory(Env, data, cb);
};

const blockCheckHandler = (data, cb) => {
    BlockStore.check(Env, data.blockId, cb, true);
};

/*  Federation commands (milestone M1).

    These are the storage half of the federation flows; they only ever arrive
    from a federation node, routed through core, which checks the sender.

    Note what is NOT here: nothing touches the local write path. A federated
    channel is written by `channel-manager.js` exactly as before, and the
    envelope for a local message is built afterwards from the committed log.
    That is why M1 needs no edit to the hot path at all.
*/

// Enable replication for a channel. The capability was verified upstream.
const fedEnableHandler = (data, cb) => {
    const { channel, validateKey, self, origin, members, metadata, level } = data || {};
    if (!Core.isValidId(channel)) { return void cb('INVALID_CHAN'); }
    Env.FM.enable(channel, {
        validateKey, self, origin, members, metadata, level
    }, (err, state, info) => {
        if (err) {
            Env.Log.error('FEDERATION_ENABLE_ERROR', err.message || err);
            return void cb(String(err.code || err.message || err));
        }
        if (!state) { return void cb('ENOSTATE'); }
        Env.Log.info('FEDERATION_ENABLED', {
            channel, members: state.members.length, already: Boolean(info?.already)
        });
        cb(void 0, { state: state, already: Boolean(info?.already) });
    });
};

/*  Every channel with federation state here, for a restarting instance to
    rebuild its routing from (R-52). Returns only what the caller needs to decide
    where each channel belongs, not the whole state. */
const fedListHandler = (data, cb) => {
    if (!Env.FM?.listFederated) { return void cb('ENOFEDERATION'); }
    Env.FM.listFederated((err, states) => {
        if (err) { return void cb(String(err.message || err)); }
        cb(void 0, {
            channels: (states || []).map(st => ({
                channel: st.channel,
                level: st.level,
                origin: st.origin,
                me: st.me,
                members: st.members || []
            }))
        });
    });
};

/*  The ids in a channel's committed log, and the removal half of a repair
    (R-6). See `FM.logIds` / `FM.excise`. */
const fedLogIdsHandler = (data, cb) => {
    if (!Core.isValidId(data?.channel)) { return void cb('INVALID_CHAN'); }
    if (!Env.FM?.logIds) { return void cb('ENOFEDERATION'); }
    Env.FM.logIds(data.channel, (err, res) => {
        if (err) { return void cb(String(err.message || err)); }
        cb(void 0, res);
    });
};

/*  The two halves of a repair: hand over what a peer lacks, and take in what we
    lack. Both are append-only — see `FM.absorb`. */
const fedContentsHandler = (data, cb) => {
    if (!Core.isValidId(data?.channel)) { return void cb('INVALID_CHAN'); }
    if (!Env.FM?.contentsFor) { return void cb('ENOFEDERATION'); }
    Env.FM.contentsFor(data.channel, data.ids, (err, res) => {
        if (err) { return void cb(String(err.message || err)); }
        cb(void 0, res);
    });
};

const fedAbsorbHandler = (data, cb) => {
    if (!Core.isValidId(data?.channel)) { return void cb('INVALID_CHAN'); }
    if (!Env.FM?.absorb) { return void cb('ENOFEDERATION'); }
    Env.FM.absorb(data.channel, data.messages, (err, res) => {
        if (err) { return void cb(String(err.message || err)); }
        cb(void 0, res);
    });
};

const fedStateHandler = (data, cb) => {
    Env.FM.state(data?.channel, (err, res) => {
        if (err) { return void cb(String(err.message || err)); }
        if (!res) { return void cb(); }
        /*  `promise` is what the heartbeat must advertise: the clock this
            instance guarantees not to go below. Advertising `self.lamport`
            instead would pin a peer's watermark whenever we go quiet. */
        cb(void 0, Object.assign({}, res.state, {
            promise: Merge.promise(res.state)
        }));
    });
};

/*  Serve history as envelopes for SYNC_RES. `limit` bounds one response so a
    large backfill is streamed over several exchanges rather than built in
    memory in one go. */
const fedSinceHandler = (data, cb) => {
    const { channel, sinceId, limit } = data || {};
    if (!Core.isValidId(channel)) { return void cb('INVALID_CHAN'); }
    Env.FM.since(channel, sinceId, limit || 256, (err, messages) => {
        if (err) { return void cb(String(err.message || err)); }
        cb(void 0, { messages });
    });
};

const fedHeadHandler = (data, cb) => {
    if (!Core.isValidId(data?.channel)) { return void cb('INVALID_CHAN'); }
    Env.FM.head(data.channel, (err, info) => {
        if (err) { return void cb(String(err.message || err)); }
        cb(void 0, info);
    });
};

/*  Accept a remote envelope into the committed log. Revalidates content against
    the channel's validateKey first (R-15) — a peer is never trusted about
    content, only about its own ordering claim. */
const fedIngestHandler = (data, cb) => {
    const { channel, envelope } = data || {};
    if (!Core.isValidId(channel)) { return void cb('INVALID_CHAN'); }
    Env.FM.ingest(channel, envelope, (err, res) => {
        if (err) { return void cb(String(err.message || err)); }
        cb(void 0, res);
    });
};

/*  L2 (M3): a local write goes to the pending log and waits for the watermark,
    rather than being committed on arrival. The caller is acknowledged once it is
    durable (R-9); it becomes part of history when the merge says so (R-3). */
const fedAllocateHandler = (data, cb) => {
    if (!Core.isValidId(data?.channel)) { return void cb('INVALID_CHAN'); }
    Env.FM.allocate(data.channel, (err, res) => {
        if (err) { return void cb(String(err.message || err)); }
        cb(void 0, res);
    });
};

const fedAcceptRemoteHandler = (data, cb) => {
    const { channel, envelope } = data || {};
    if (!Core.isValidId(channel)) { return void cb('INVALID_CHAN'); }
    Env.FM.acceptRemote(channel, envelope, (err) => {
        cb(err ? String(err.message || err) : void 0);
    });
};

// a peer's clock from a heartbeat: what keeps the watermark moving when idle (R-4)
const fedObserveHandler = (data, cb) => {
    const { channel, originId, seq, lamport, authoritative } = data || {};
    if (!Core.isValidId(channel)) { return void cb('INVALID_CHAN'); }
    Env.FM.observePeer(channel, originId, { seq, lamport, authoritative }, (err) => {
        cb(err ? String(err.message || err) : void 0);
    });
};

/*  Anti-entropy (R-47): the peer tells us what it holds, we tell it what it is
    missing of ours. */
const fedReconcileHandler = (data, cb) => {
    const { channel, originId, have } = data || {};
    if (!Core.isValidId(channel)) { return void cb('INVALID_CHAN'); }
    Env.FM.reconcile(channel, originId, have, (err, res) => {
        if (err) { return void cb(String(err.message || err)); }
        cb(void 0, res);
    });
};

const fedHaveHandler = (data, cb) => {
    Env.FM.have(data?.channel, (err, res) => {
        if (err) { return void cb(String(err.message || err)); }
        cb(void 0, { have: res });
    });
};

const fedTailHandler = (data, cb) => {
    Env.FM.tail(data?.channel, (err, tail) => {
        if (err) { return void cb(String(err.message || err)); }
        cb(void 0, { tail });
    });
};

const fedTrimHandler = (data, cb) => {
    Env.FM.trim(data?.channel, data?.toLamport, (err, res) => {
        if (err) { return void cb(String(err.message || err)); }
        cb(void 0, res);
    });
};

/*  Blobs (R-44). Immutable and content-addressed, so these are plain reads and
    writes — no ordering, no merge, nothing to reconcile. */
const fedBlobStatHandler = (data, cb) => {
    Env.FB.stat(data?.id, (err, res) => {
        if (err) { return void cb(String(err.message || err)); }
        cb(void 0, res);
    });
};

const fedBlobReadHandler = (data, cb) => {
    Env.FB.read(data?.id, data?.offset || 0, data?.length, (err, res) => {
        if (err) { return void cb(String(err.message || err)); }
        // base64 because this crosses the node bus as JSON
        cb(void 0, { chunk: res.chunk.toString('base64'), total: res.total });
    });
};

const fedBlobWriteHandler = (data, cb) => {
    let buf;
    try {
        buf = Buffer.from(data?.data || '', 'base64');
    } catch (e) { return void cb('EBADBLOB'); }
    Env.FB.write(data?.id, buf, (err) => {
        cb(err ? String(err.message || err) : void 0);
    });
};

/*  A control commit from a peer: a metadata change or a deletion made at the
    anchor (spec §5.4). Applied without re-checking the owner, which is the
    trust-the-peer boundary R-28 describes. */
const fedControlHandler = (data, cb) => {
    const { channel, ctrl, from } = data || {};
    if (!Core.isValidId(channel)) { return void cb('INVALID_CHAN'); }
    Env.FM.control(channel, ctrl, from, (err) => {
        if (err) { return void cb(String(err.message || err)); }
        cb();
    });
};

/*  Federation liveness probe (federation design §1.3, milestone M0).

    The far end of the federation -> core -> storage path. It reports that this
    storage node is up and which node answered, so the M0 round trip proves a
    real hop rather than core answering on storage's behalf. No channel is read
    and no data is written; the federated log arrives in M1.
*/
const fedPingHandler = (data, cb) => {
    cb(void 0, {
        storageId: Env.myId,
        nonce: data?.nonce,
        time: +new Date()
    });
};

/* Start of the node */

const callWithEnv = f => {
    return function () {
        [].unshift.call(arguments, Env);
        return f.apply(null, arguments);
    };
};

// List accepted commands
let COMMANDS = {
    'JOIN_CHANNEL': joinChannelHandler,
    'LEAVE_CHANNEL': leaveChannelHandler,
    'GET_HISTORY': getHistoryHandler(HistoryManager.onGetHistory),
    'GET_FULL_HISTORY': getHistoryHandler(HistoryManager.onGetFullHistory),
    'GET_HISTORY_RANGE': getHistoryHandler(HistoryManager.onGetHistoryRange),
    'CHANNEL_MESSAGE': onChannelMessageHandler,
    'DROP_USER': dropUserHandler,
    'NEW_DECREES': newDecreeHandler,

    'ADMIN_DECREE': adminDecreeHandler,
    'ACCOUNTS_LIMITS': accountsLimitsHandler,

    'GET_CHANNEL_LIST': getChannelListHandler,
    'GET_MULTIPLE_FILE_SIZE': getMultipleFileSizeHandler,
    'GET_TOTAL_SIZE': getTotalSizeHandler,
    'GET_CHANNELS_TOTAL_SIZE': getChannelsTotalSizeHandler,
    'GET_REGISTERED_USERS': getRegisteredUsersHandler,

    'GET_METADATA': getMetadataHandler,

    'FED_PING': fedPingHandler,
    'FED_ENABLE': fedEnableHandler,
    'FED_STATE': fedStateHandler,
    'FED_LIST': fedListHandler,
    'FED_LOG_IDS': fedLogIdsHandler,
    'FED_CONTENTS': fedContentsHandler,
    'FED_ABSORB': fedAbsorbHandler,
    'FED_SINCE': fedSinceHandler,
    'FED_HEAD': fedHeadHandler,
    'FED_INGEST': fedIngestHandler,
    'FED_CONTROL': fedControlHandler,
    'FED_BLOB_STAT': fedBlobStatHandler,
    'FED_BLOB_READ': fedBlobReadHandler,
    'FED_BLOB_WRITE': fedBlobWriteHandler,
    'FED_ALLOCATE': fedAllocateHandler,
    'FED_ACCEPT_REMOTE': fedAcceptRemoteHandler,
    'FED_OBSERVE': fedObserveHandler,
    'FED_TAIL': fedTailHandler,
    'FED_RECONCILE': fedReconcileHandler,
    'FED_HAVE': fedHaveHandler,
    'FED_TRIM': fedTrimHandler,

    'RPC_IS_NEW_CHANNEL': isNewChannelHandler,
    'RPC_WRITE_PRIVATE_MESSAGE': writePrivateMessageHandler,
    'RPC_DELETE_CHANNEL_LINE': deleteChannelLineHandler,
    'RPC_SET_METADATA': setMetadataHandler,

    'RPC_GET_FILE_SIZE': getFileSizeHandler,
    'RPC_GET_DELETED_PADS': getDeletedPadsHandler,
    'RPC_PINNING_RESET': getPinningResetHandler,
    'RPC_PINNING_PIN': getPinningPinHandler,
    'RPC_PINNING_UNPIN': getPinningUnpinHandler,
    'RPC_GET_HASH': getHashHandler,
    'RPC_ARCHIVE_PIN_LOG': archivePinLogHandler,
    'RPC_TRIM_PIN_LOG': trimPinLogHandler,

    'RPC_CLEAR_OWNED_CHANNEL': clearOwnedChannelHandler,
    'RPC_REMOVE_OWNED_CHANNEL': removeOwnedChannelHandler,
    'RPC_TRIM_HISTORY': trimHistoryHandler,

    'HTTP_UPLOAD_COOKIE': callWithEnv(Upload.cookie),
    'RPC_UPLOAD_STATUS': callWithEnv(Upload.status),
    'RPC_UPLOAD_CANCEL': callWithEnv(Upload.cancel),
    'RPC_UPLOAD_CHUNK': callWithEnv(Upload.upload),
    'RPC_UPLOAD_COMPLETE': callWithEnv(Upload.complete),
    'RPC_UPLOAD_COMPLETE_OWNED': callWithEnv(Upload.completeOwned),

    // Block/registration commands
    'HTTP_MFA_CHECK': callWithEnv(MFAManager.checkMFA),
    'HTTP_UPDATE_SESSION': callWithEnv(MFAManager.updateSession),
    'HTTP_WRITE_BLOCK': callWithEnv(Block.writeLoginBlock),
    'HTTP_REMOVE_BLOCK': callWithEnv(Block.removeLoginBlock),

    'TOTP_SETUP': callWithEnv(MFAManager.setupCheck),
    'TOTP_SETUP_COMPLETE': callWithEnv(MFAManager.setupComplete),
    'TOTP_VALIDATE': callWithEnv(MFAManager.validateCheck),
    'TOTP_VALIDATE_COMPLETE': callWithEnv(MFAManager.validateComplete),
    'TOTP_MFA_CHECK': callWithEnv(MFAManager.statusCheck),
    'TOTP_REVOKE': callWithEnv(MFAManager.revokeCheck),
    'TOTP_REVOKE_COMPLETE': callWithEnv(MFAManager.revokeComplete),
    'TOTP_WRITE_BLOCK': callWithEnv(MFAManager.writeCheck),
    'TOTP_WRITE_BLOCK_COMPLETE': callWithEnv(MFAManager.writeComplete),
    'TOTP_REMOVE_BLOCK': callWithEnv(MFAManager.removeCheck),
    'TOTP_REMOVE_BLOCK_COMPLETE': callWithEnv(MFAManager.removeComplete),

    // Block commands from other storages
    'BLOCK_CHECK': blockCheckHandler,
    'BLOCK_GET_MFA': callWithEnv(MFAManager.getMFA),
    'SESSIONS_CMD': callWithEnv(MFAManager.sessionsCmd),
    'USER_REGISTRY_CMD': callWithEnv(MFAManager.userRegistryCmd),
    'INVITATION_CMD': callWithEnv(MFAManager.invitationCmd),

    // Admin commands
    'ADMIN_CMD': callWithEnv(Admin.command),

    // Internal commands
    'GET_ACTIVE_CHANNELS': getActiveChannels
};

const initWorkerCommands = () => {
    Env.worker ||= {};
    Env.worker.computeMetadata = (channel, cb) => {
        Env.store.getWeakLock(channel, _next => {
            let avg = Env.plugins?.MONITORING?.average(`computeMetadata`);
            const next = () => { _next(); avg?.time(); };
            Env.workers.send('COMPUTE_METADATA', {
                channel
            }, Util.both(next, cb));
        });
    };
    Env.worker.computeIndex = (channel, cb) => {
        Env.store.getWeakLock(channel, _next => {
            let avg = Env.plugins?.MONITORING?.average(`computeIndex`);
            const next = () => { _next(); avg?.time(); };
            Env.workers.send('COMPUTE_INDEX', {
                channel
            }, Util.both(next, cb));
        });
    };
    Env.worker.getHashOffset = (channel, hash, cb) => {
        Env.store.getWeakLock(channel, _next => {
            let avg = Env.plugins?.MONITORING?.average(`getHashOffset`);
            const next = () => { _next(); avg?.time(); };
            Env.workers.send('GET_HASH_OFFSET', {
                channel, hash
            }, Util.both(next, cb));
        });
    };
    Env.worker.getOlderHistory = (channel, oldestKnownHash, untilHash, desiredMessages, desiredCheckpoint, cb) => {
        Env.store.getWeakLock(channel, (_next) => {
            let avg = Env.plugins?.MONITORING?.average(`getOlderHistory`);
            const next = () => { _next(); avg?.time(); };
            Env.workers.send('GET_OLDER_HISTORY', {
                channel, oldestKnownHash, untilHash, desiredMessages, desiredCheckpoint
            }, Util.both(next, cb));
        });
    };

    // Pinning
    Env.worker.getMultipleFileSize = (channels, cb) => {
        Env.workers.send("GET_MULTIPLE_FILE_SIZE", {
            channels: channels,
        }, cb, true);
    };
    Env.worker.getTotalSize = (channels, cb) => {
        // we could take out locks for all of these channels,
        // but it's OK if the size is slightly off
        Env.workers.send('GET_TOTAL_SIZE', {
            channels: channels,
        }, cb);
    };
    Env.worker.getPinState = (safeKey, cb) => {
        Env.pinStore.getWeakLock(safeKey, (_next) => {
            let avg = Env.plugins?.MONITORING?.average(`getPinState`);
            const next = () => { _next(); avg?.time(); };
            Env.workers.send('GET_PIN_STATE', {
                key: safeKey
            }, Util.both(next, cb));
        });
    };
    Env.worker.getPinActivity = (safeKey, cb) => {
        Env.pinStore.getWeakLock(safeKey, next => {
            Env.workers.send('GET_PIN_ACTIVITY', {
                key: safeKey
            }, Util.both(next, cb));
        });
    };
    Env.worker.getPinInfo = (safeKey, cb) => {
        Env.pinStore.getWeakLock(safeKey, (_next) => {
            let avg = Env.plugins?.MONITORING?.average(`getPinState`);
            const next = () => { _next(); avg?.time(); };
            Env.workers.send('GET_PIN_INFO', {
                key: safeKey
            }, Util.both(next, cb));
        });
    };

    Env.worker.getDeletedPads = (channels, cb) => {
        let avg = Env.plugins?.MONITORING?.average(`getDeletedPads`);
        const next = Util.both(cb, avg?.time);
        Env.workers.send("GET_DELETED_PADS", {
            channels: channels,
        }, next);
    };
    Env.worker.hashChannelList = (channels, cb) => {
        Env.workers.send('HASH_CHANNEL_LIST', {
            channels: channels,
        }, cb);
    };

    Env.worker.completeUpload = (safeKey, arg, owned, size, cb) => {
        Env.workers.send('COMPLETE_UPLOAD', {
            safeKey, arg, owned, size
        }, cb);
    };

    Env.worker.removeOwnedBlob = (blobId, safeKey, reason, cb) => {
        Env.workers.send('REMOVE_OWNED_BLOB', {
            safeKey, blobId, reason
        }, cb);
    };

    // RPC
    Env.worker.getFileSize = (channel, cb) => {
        Env.workers.send('GET_FILE_SIZE', {
            channel
        }, cb);
    };


    // Tasks
    Env.worker.runTasks = (cb) => {
        // time out after 10 minutes
        Env.workers.send('RUN_TASKS', {}, cb, 1000 * 60 * 10);
    };
    Env.worker.writeTask = (time, command, args, cb) => {
        Env.workers.send('WRITE_TASK', {
            time: time,
            task_command: command,
            args: args,
        }, cb);
    };

    // Admin
    Env.worker.getLastChannelTime = (channel, cb) => {
        Env.workers.send('GET_LAST_CHANNEL_TIME', {
            channel
        }, cb);
    };
    Env.worker.readReport = (args, cb) => {
        Env.workers.send('READ_REPORT', args, cb);
    };
    Env.worker.accountArchivalStart = (args, cb) => {
        Env.workers.send('ACCOUNT_ARCHIVAL_START', args, cb);
    };
    Env.worker.accountRestoreStart = (args, cb) => {
        Env.workers.send('ACCOUNT_RESTORE_START', args, cb);
    };
};

const initAccountsIntervals = () => {
    const pingAccountsDaily = () => {
        Quota.pingAccountsDaily(Env, e => {
            if (e) { Env.Log.warn('dailyPing', e); }
        });
    };
    pingAccountsDaily();
    Env.intervals.dailyPing = setInterval(pingAccountsDaily, 24*3600*1000);

    const updateLimits = () => { Env.updateLimits(); };
    Quota.applyCustomLimits(Env);
    updateLimits();
    if (Env.accounts_api) {
        Env.intervals.quotaUpdate = setInterval(updateLimits, 3600*1000);
    }

};

const initHttpServer = (Env, mainConfig, _cb) => {
    const cb = Util.mkAsync(_cb);

    Cluster.setupPrimary({
        exec: './build/storage.cluster.js',
        args: [],
    });
    const WORKERS = Env.maxWorkers['storage-http'] || 2;
    const workerConfig = {
        Log: Env.Log,
        noTaskLimit: true,
        customFork: () => {
            return Cluster.fork({});
        },
        maxWorkers: WORKERS,
        maxJobs: Env.maxJobs['storage-http'] || 10,
        commandTimers: {}, // time spent on each command
        config: mainConfig,
        Env: { // Serialized Env (Environment.serialize)
        }
    };

    let ready = 0;
    Cluster.on('online', () => {
        ready++;
        if (ready === WORKERS) {
            cb();
        }
    });

    Env.clusters = WorkerModule(workerConfig);
    Env.clusters.onNewWorker(state => {
        Object.keys(Env.allDecrees).forEach(type => {
            const decrees = Env.allDecrees[type];
            Env.clusters.sendTo(state, 'NEW_DECREES', {
                decrees, type
            }, () => {
                Env.Log.silly('UPDATE_DECREE_STORAGE_WORKER');
            });
        });
    });

    Env.clusters.on('STORAGE_INTERFACE', (args, cb) => {
        const { type, id, cmd, data, exclude } = args;
        const f = Env.interface[type];
        if (typeof(f) !== "function") { return void cb('EINVAL'); }
        f(id, cmd, data, cb, exclude);
    });
    Env.clusters.on('UPLOAD_GET_SESSION', (args, cb) => {
        const { safeKey } = args;
        const session = Core.getSession(Env.blobstage, safeKey);
        cb(void 0, {
            currentUploadSize: session.currentUploadSize,
            pendingUploadSize: session.pendingUploadSize
        });
    });
    Env.clusters.on('UPLOAD_REPORT_SESSION', (args, cb) => {
        const { safeKey, len } = args;
        const session = Core.getSession(Env.blobstage, safeKey);
        session.currentUploadSize += len;
        cb();
    });
    Env.clusters.on('UPDATE_LIMITS', (args, cb) => {
        Env.updateLimits();
        cb();
    });
    Env.cluster = {};
    Env.cluster.closeBlobstage = (safeKey, cb) => {
        Env.clusters.broadcast('CLOSE_BLOBSTAGE', { safeKey }, () => {
            Env.Log.verbose('CLOSE_BLOBSTAGE');
            if (typeof(cb) === 'function') { cb(); }
        });
    };
};

/*  Drain sequence for a storage node.

    Registered in the order it must run: stop background work first so the backlog
    stops growing, then flush anything buffered, then close handles and workers.

    The flush step is a no-op for the filesystem store, which has nothing buffered.
    It is the hook that an object-storage backend uses to push cached writes that
    have already been acknowledged to clients.  */
const initShutdown = (Env) => {
    const shutdown = Env.shutdown = Shutdown.create({
        Log: Env.Log,
        label: Env.myId,
        timeout: Env.config?.storage?.shutdownFlushTimeoutMs || 30000
    });

    shutdown.register('stop-background-work', done => {
        Env.draining = true;
        Object.keys(Env.intervals || {}).forEach(name => {
            clearInterval(Env.intervals[name]);
            delete Env.intervals[name];
        });
        done();
    });

    shutdown.register('flush-stores', done => {
        const stores = [Env.store, Env.pinStore, Env.blobStore].filter(Boolean);
        let pending = stores.length;
        if (!pending) { return void done(); }

        stores.forEach(store => {
            // only stores that buffer writes implement this
            if (typeof (store.flushAll) !== 'function') {
                pending--;
                if (!pending) { done(); }
                return;
            }
            store.flushAll(err => {
                if (err) {
                    Env.Log.error('SHUTDOWN_FLUSH_ERROR', {
                        error: err && err.message || err
                    });
                }
                pending--;
                if (!pending) { done(); }
            });
        });
        if (!pending) { done(); }
    });

    shutdown.register('close-channels', done => {
        if (typeof (Env.store?.closeInactiveChannels) !== 'function') {
            return void done();
        }
        // an empty active set means "close everything"
        Env.store.closeInactiveChannels(new Set());
        done();
    });

    shutdown.register('shutdown-stores', done => {
        [Env.store, Env.pinStore, Env.blobStore].forEach(store => {
            if (typeof (store?.shutdown) === 'function') {
                try { store.shutdown(); } catch (err) {
                    Env.Log.error('SHUTDOWN_STORE_ERROR', {
                        error: err && err.message || err
                    });
                }
            }
        });
        done();
    });

    shutdown.register('stop-workers', done => {
        const pools = [Env.workers, Env.clusters].filter(pool => {
            return typeof (pool?.shutdown) === 'function';
        });
        if (!pools.length) { return void done(); }

        let pending = pools.length;
        pools.forEach(pool => {
            pool.shutdown(() => {
                pending--;
                if (!pending) { done(); }
            });
        });
    });

    shutdown.install();
};

const onInitialized = (Env, _cb) => {
    const cb = Util.mkAsync(_cb);

    nThen(waitFor => {
        Env.plugins.call('initStorage')(Env, waitFor);
    }).nThen(() => {
        cb();
    });

};

// Connect to core
const start = (mainConfig) => {
    const { myId, index, infra, config } = mainConfig;

    Environment.init(Env, mainConfig, {
        Block, Pinning, Decrees,
        BlockStore, Blob, File, Sessions, Basic,
        HKUtil
    });

    Env.Log = Logger(config, myId);

    Env.updateLimits = () => {
        if (index !== 0) { return; }
        Quota.updateCachedLimits(Env, (e, limits) => {
            if (!Env.accounts_api) { return; }
            if (e) { return Env.Log.warn('LIMIT_UPDATE', e); }
            if (!limits) { return; }
            Env.interface.broadcast('core', 'ACCOUNTS_LIMITS', {
                limits
            }, () => {});
        });
    };

    const curve = Nacl.box.keyPair();
    let curveKeys = Env.curveKeys = {
        curvePublic: Util.encodeBase64(curve.publicKey),
        curvePrivate: Util.encodeBase64(curve.secretKey)
    };
    Env.sendDecrees = (decrees, type, _cb) => {
        const cb = Util.mkAsync(_cb || function () {});
        const freshKey = String(+new Date());
        Env.cacheDecrees(type, decrees);
        nThen(waitFor => {
            Env.interface.broadcast('core', 'NEW_DECREES', {
                freshKey,
                curveKeys,
                decrees,
                type
            }, waitFor());
            Env.workers.broadcast('NEW_DECREES', {
                decrees, type
            }, waitFor(() => {
                Env.Log.silly('UPDATE_DECREE_STORAGE_WORKER');
            }));
            Env.clusters.broadcast('NEW_DECREES', {
                type, decrees
            }, waitFor(() => {
                Env.Log.silly('UPDATE_DECREE_STORAGE_CLUSTER');
            }));
            curveKeys = undefined;
        }).nThen(() => {
            cb();
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

    const {
        filePath, pinPath, archivePath, blobPath, blobStagingPath, cachePath
    } = Core.getPaths(mainConfig);
    nThen(waitFor => {
        Stores.create({
            paths: {
                filePath, pinPath, archivePath, blobPath, blobStagingPath, cachePath
            },
            config,
            Log: Env.Log,
            monitoring: Env.plugins?.MONITORING,
            getSession: safeKey => {
                return Core.getSession(Env.blobstage, safeKey);
            },
            // a channel with connected users must not be evicted from the cache
            isPinned: rel => {
                const id = rel.replace(/^..[\\/]/, '')
                    .replace(/\.(metadata\.)?ndjson.*$/, '');
                return Boolean(Env.channel_cache[id]);
            }
        }, waitFor((err, stores) => {
            if (err) {
                waitFor.abort();
                Env.Log.error('STORE_INIT_ERROR', err.message || err);
                throw err;
            }
            Env.store = stores.store;
            Env.pinStore = stores.pinStore;
            Env.blobStore = stores.blobStore;
            Env.storageBackend = stores.backend;
        }));
    }).nThen(waitFor => {
        /*  Federation state needs an ObjectBackend. The object-storage path
            already builds one, but the filesystem path uses the legacy file
            store and has none — so make one, rooted at the instance's data
            directory, which puts `fed/` alongside `channel/` as the design
            describes.

            Done unconditionally rather than on demand: it creates a directory
            and nothing else, and a lazily-built backend would have to be
            threaded through every call site as a maybe.  */
        if (Env.storageBackend) {
            Env.federationBackend = Env.storageBackend;
            return;
        }
        Backends.create({ type: 'fs' }, { root: Env.paths.base }, waitFor((err, backend) => {
            if (err) {
                waitFor.abort();
                Env.Log.error('FEDERATION_BACKEND_ERROR', err.message || err);
                throw err;
            }
            Env.federationBackend = backend;
        }));
    }).nThen(waitFor => {
        /*  Reconcile the local cache against the store before serving anything.

            A node that died with writes it had acknowledged but not yet flushed
            still has them on disk; this is where they are pushed. Channels are
            recovered individually, so booting is not serialised on the whole
            cache.  */
        [Env.store, Env.pinStore].forEach(store => {
            if (typeof (store?.recover) !== 'function') { return; }
            store.recover(waitFor((err, report) => {
                if (err) {
                    return void Env.Log.error('CACHE_RECOVERY_ERROR', err.message || err);
                }
                if (report && report.scanned) {
                    Env.Log.info('CACHE_RECOVERED', report);
                }
            }));
        });
    }).nThen(() => {
        let tasks_running;
        Env.intervals.taskExpiration = setInterval(() => {
            if (Env.disableIntegratedTasks) { return; }
            if (tasks_running) { return; }
            tasks_running = true;
            Env.worker.runTasks(err => {
                if (err) {
                    Env.Log.error('TASK_RUNNER_ERR', err);
                }
                tasks_running = false;
            });
        }, 1000 * 60 * 5); // run every five minutes

        /*  The merge has to be able to make progress without an incoming
            message — see FM.sweep. Cheap: it iterates only channels this node
            already holds federation state for, and does nothing at all on an
            instance that federates nothing. */
        Env.intervals.federationMerge = setInterval(() => {
            Env.FM?.sweep();
        }, 5000);

        Env.intervals.pinExpirationInterval = setInterval(() => {
            Core.expireSessions(Env.pin_cache);
        }, Core.SESSION_EXPIRATION_TIME);

        Env.intervals.blobstageExpirationInterval = setInterval(() => {
            // Contains currentUploadSize & pendingUploadSize
            // The file stream is stored in the workers
            Core.expireSessions(Env.blobstage);
        }, Core.SESSION_EXPIRATION_TIME);
    }).nThen((waitFor) => {
        const WORKERS = Env.maxWorkers['storage'] || 2;
        const workerConfig = {
            Log: Env.Log,
            workerPath: './build/storage.worker.js',
            maxWorkers: WORKERS,
            maxJobs: Env.maxJobs['storage'] || 10,
            commandTimers: {}, // time spent on each command
            config: mainConfig,
            Env: { // Serialized Env (Environment.serialize)
            }
        };
        Env.workers = WorkerModule(workerConfig);
        Env.workers.onNewWorker(state => {
            Object.keys(Env.allDecrees).forEach(type => {
                const decrees = Env.allDecrees[type];
                Env.workers.sendTo(state, 'NEW_DECREES', {
                    decrees, type
                }, () => {
                    Env.Log.silly('UPDATE_DECREE_STORAGE_WORKER');
                });
            });
        });
        initWorkerCommands();

        Env.CM = ChannelManager.create(Env);
        /*  Federation state lives beside the channel log and is reached the same
            way. Creating it is free for a non-federating instance: it opens no
            keys until a channel is actually federated. */
        Env.FM = FederationManager.create(Env);
        Env.FB = FederationBlobs.create(Env);

        initHttpServer(Env, mainConfig, waitFor());
    }).nThen(waitFor => {
        Env.interface = Interface.init(interfaceConfig, waitFor(err => {
            if (err) {
                console.error(interfaceConfig.myId, ' error:', err);
                return;
            }
        }));

        // List accepted commands
        Env.plugins.call('addStorageCommands')(Env, COMMANDS);
        Env.interface.handleCommands(COMMANDS);
    }).nThen(() => {
        /*  Tell core which channels are federated, as soon as we can (R-53).

            Core decides whether to publish a write by looking the channel up in
            a memory set. Until something fills that set, a write on a federated
            channel is committed locally and **never federated at all** — and
            because the federation sequence is only allocated at publish time,
            such a message has no sequence, so no gap exists for anti-entropy to
            find and no repair can ever recover it. Silent and permanent.

            The federation node also restores this, but it starts independently
            and has its own peers to dial; storage is the node that holds the
            durable record and is up before the front accepts a client, so it is
            the right one to close the window.

            Announcing an already-known channel is idempotent, so the two
            sources of truth cannot conflict. */
        if (!Env.numberFederations || !Env.FM?.listFederated) { return; }
        Env.FM.listFederated((err, states) => {
            if (err) {
                return void Env.Log.error('FEDERATION_ANNOUNCE_ERROR',
                    String(err.message || err));
            }
            (states || []).forEach(st => {
                if (!st.channel) { return; }
                const isL2 = st.level === 'L2';
                Env.interface.sendEvent(Env.getCoreId(st.channel), 'FED_CHANNELS', {
                    channel: st.channel,
                    federated: true,
                    mirror: Boolean(!isL2 && st.origin && st.origin !== st.me),
                    level: st.level
                });
            });
            Env.Log.info('FEDERATION_ANNOUNCED', { channels: (states || []).length });
        });
    }).nThen(waitFor => {
        // Only storage:0 can manage decrees, moderators and accounts
        if (index !== 0) { return; }
        initAccountsIntervals();

        Moderators.getKeys(Env, waitFor((err, keys) => {
            if (err) {
                Env.Log.error('MODERATORS_LOADING_ERROR', err);
                keys = [];
            }
            Env.moderators = keys.map(safeKey => {
                return Util.unescapeKeyCharacters(safeKey);
            });
            Env.interface.sendEvent('core:0', 'SET_MODERATORS', Env.moderators);
        }));

        Env.adminDecrees.load(Env, waitFor((err, toSend) => {
            if (err) {
                waitFor.abort();
                return Env.Log.error('DECREES_LOADING_ERROR', err);
            }

            Env.sendDecrees(toSend, '');
        }));
    }).nThen(waitFor => {
        onInitialized(Env, waitFor());
    }).nThen(waitFor => {
        // BEARER_SECRET decree (storage:0 only)
        if (index !== 0) { return; }
        if (Env.bearerSecret) { return; }

        const bearerSecret = Util.encodeBase64(Crypto.randomBytes(32));
        const decree = [
            'SET_BEARER_SECRET',
            [bearerSecret],
            'INTERNAL',
            +new Date()
        ];
        Decrees.onNewDecree(Env, decree, '', waitFor());
    }).nThen(() => {
        // INSTALL TOKEN admin decree (storage:0 only)
        if (index !== 0) { return; }

        let admins = Env.admins || [];
        // If we don't have any admin on this instance
        // print an onboarding link
        if (Array.isArray(admins) && admins.length) { return; }
        let token = Env.installToken;
        let printLink = () => {
            let url = `${Env.httpUnsafeOrigin}/install/#${token}`;
            console.log('=============================');
            console.log('Create your first admin account and customize your instance by visiting');
            console.log(url);
            console.log('=============================');

        };
        // If we already have a token, print it
        if (token) { return void printLink(); }

        // Otherwise create a new token
        token = Crypto.randomBytes(32).toString('hex');

        let decree = ["ADD_INSTALL_TOKEN",[token],"",+new Date()];
        Decrees.onNewDecree(Env, decree, '', () => {
            printLink();
        });
    }).nThen(() => {
        // install signal handlers only once everything it has to drain exists
        initShutdown(Env);

        if (process.send !== undefined) {
            process.send({ type: 'storage', index, msg: 'READY' });
        } else {
            console.log(myId, 'started');
        }
    });
};

module.exports = {
    start
};
