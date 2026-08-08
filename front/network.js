const Util = require("../common/common-util");
const Http = require("node:http");
const Express = require("express");
const WebSocketServer = require('ws').Server;
const { setHeaders } = require('../http-server/headers.js');
const Crypto = require('crypto');

const cookieParser = require("cookie-parser");
const bodyParser = require('body-parser');

const Network = {};

const app = Express();

const events = {
    dropUser: Util.mkEvent(),
    message: Util.mkEvent(),
    httpCommand: Util.mkEvent()
};

const initExpress = (Env) => {
    app.use(function (req, res, next) {
        setHeaders(Env, req, res);
        if (/[\?\&]ver=[^\/]+$/.test(req.url)) { res.setHeader("Cache-Control", "max-age=31536000"); }
        else { res.setHeader("Cache-Control", "no-cache"); }
        next();
    });

    app.use(bodyParser.urlencoded({
        extended: true
    }));
    app.use(cookieParser());

    // if dev mode: never cache
    const cacheString = () => {
        return (Env.FRESH_KEY? '-' + Env.FRESH_KEY: '') + (Env.DEV_MODE? '-' + (+new Date()): '');
    };

    const makeRouteCache = (template, cacheName) => {
        const cleanUp = {};

        return function (req, res) {
            const cache = Env[cacheName] ||= {};
            const host = req.headers.host.replace(/\:[0-9]+/, '');
            res.setHeader('Content-Type', 'text/javascript');
            // don't cache anything if you're in dev mode
            if (Env.DEV_MODE) {
                return void res.send(template(host));
            }
            // generate a lookup key for the cache
            let cacheKey = host + ':' + cacheString();

            // we must be able to clear the cache when updating any mutable key
            // if there's nothing cached for that key...
            if (!cache[cacheKey]) {
                // generate the response and cache it in memory
                cache[cacheKey] = template(host);
                // and create a function to conditionally evict cache entries
                // which have not been accessed in the last 20 seconds
                cleanUp[cacheKey] = Util.throttle(function () {
                    delete cleanUp[cacheKey];
                    delete cache[cacheKey];
                }, 20000);
            }

            // successive calls to this function
            if (typeof (cleanUp[cacheKey]) === "function") {
                cleanUp[cacheKey]();
            }
            return void res.send(cache[cacheKey]);
        };
    };
    const serveConfig = makeRouteCache(function () {
        // NOTE: we may extract JSON from this config using slice(27, -5)
        const ssoList = Env.sso && Env.sso.enabled && Array.isArray(Env.sso.list) &&
                        Env.sso.list.map(function (obj) { return obj.name; }) || [];
        const SSOUtils = Env?.plugins?.SSO?.utils;
        const ssoCfg = (SSOUtils && ssoList.length) ? {
            force: (Env.sso && Env.sso.enforced && 1) || 0,
            password: (Env.sso && Env.sso.cpPassword && (Env.sso.forceCpPassword ? 2 : 1)) || 0,
            list: ssoList
        } : false;

        return [
            'define(function(){',
            'return ' + JSON.stringify({
                requireConf: {
                    waitSeconds: 600,
                    urlArgs: 'ver=' + Env.version + cacheString(),
                },
                removeDonateButton: (Env.removeDonateButton === true),
                accounts_api: Env.accounts_api,
                websocketPath: Env.websocketPath,
                httpUnsafeOrigin: Env.httpUnsafeOrigin,
                adminEmail: Env.adminEmail,
                adminKeys: Env.admins,
                moderatorKeys: Env.moderators,
                inactiveTime: Env.inactiveTime,
                supportMailbox: Env.supportMailbox,
                supportMailboxKey: Env.supportMailboxKey,
                defaultStorageLimit: Env.defaultStorageLimit,
                maxUploadSize: Env.maxUploadSize,
                premiumUploadSize: Env.premiumUploadSize,
                restrictRegistration: Env.restrictRegistration,
                restrictSsoRegistration: Env.restrictSsoRegistration,
                appsToDisable: Env.appsToDisable,
                httpSafeOrigin: Env.httpSafeOrigin,
                enableEmbedding: Env.enableEmbedding,
                fileHost: Env.fileHost,
                shouldUpdateNode: Env.shouldUpdateNode || undefined,
                listMyInstance: Env.listMyInstance,
                sso: ssoCfg,
                enforceMFA: Env.enforceMFA,
                onlyOffice: Env.onlyOffice
            }, null, '\t'),
            '});'
        ].join(';\n');
    }, 'configCache');
    const serveBroadcast = makeRouteCache(function () {
        let maintenance = Env.maintenance;
        if (maintenance && maintenance.end && maintenance.end < (+new Date())) {
            maintenance = undefined;
        }
        return [
            'define(function(){',
            'return ' + JSON.stringify({
                curvePublic: Env?.curveKeys?.curvePublic,
                lastBroadcastHash: Env.lastBroadcastHash,
                surveyURL: Env.surveyURL,
                maintenance: maintenance
            }, null, '\t'),
            '});'
        ].join(';\n');
    }, 'broadcastCache');
    const Define = (obj) => {
        return `define(function (){
        return ${JSON.stringify(obj, null, '\t')};
    });`;
    };
    const serveInstance = (req, res) => {
        res.setHeader('Content-Type', 'text/javascript');
        res.send(Define({
            color: Env.accentColor,
            name: Env.instanceName,
            description: Env.instanceDescription,
            location: Env.instanceJurisdiction,
            notice: Env.instanceNotice,
        }));
    };

    /*  Public federation descriptor (federation spec R-23).

        How a would-be peer learns this instance's federation public key and the
        URL to dial. Deliberately JSON rather than the AMD modules the other
        /api routes serve: the consumer is another server, not the client.

        It publishes only what an operator must hand out to be peered with. The
        peers we actually federate with are not listed — that is the operator's
        business and R-34 makes replica-set membership visible to peers over the
        authenticated session, not to the open web.
    */
    const serveFederation = (req, res) => {
        res.setHeader('Content-Type', 'application/json');
        const coreId = Env.getCoreId('federation');
        Env.interface.sendQuery(coreId, 'FEDERATION_INFO', {}, answer => {
            if (answer?.error === 'ENOFEDERATION') {
                return void res.status(404).send(JSON.stringify({
                    federation: false
                }, null, '\t'));
            }
            const data = answer?.data;
            if (answer?.error || !data?.originId) {
                return void res.status(503).send(JSON.stringify({
                    federation: false,
                    error: 'EUNAVAILABLE'
                }, null, '\t'));
            }
            res.send(JSON.stringify({
                federation: true,
                version: 1,
                originId: data.originId,
                origin: Env.httpUnsafeOrigin
            }, null, '\t'));
        });
    };

    app.get('/api/config', serveConfig);
    app.get('/api/broadcast', serveBroadcast);
    app.get('/api/instance', serveInstance);
    app.get('/api/federation', serveFederation);

    /*  The instances this one is peered with (R-46).

        The client needs a peer's originId to scope a replication capability to
        it. It must not fetch that from the peer itself — the browser has no
        relationship with another instance, and its CSP rightly forbids the
        request. So its own server answers. */
    app.get('/api/federation/peers', (req, res) => {
        Env.interface.sendQuery(Env.getCoreId('federation'), 'FEDERATION_PEERS', {},
            answer => {
                if (answer?.error === 'ENOFEDERATION') {
                    return void res.status(404).json({ peers: [] });
                }
                if (answer?.error) {
                    return void res.status(502).json({ error: String(answer.error) });
                }
                res.json(answer.data || { peers: [] });
            });
    });

    /*  Is this channel federated, and with whom (R-50)?

        The client asks this to federate a pad's auxiliary channels — chiefly
        the pad chat, whose id lives in content no server can read and which is
        usually created long after the pad was federated. Without it, opening a
        chat on an already-federated pad would produce a channel that never
        replicates, which reads as federation being broken.
    */
    app.get('/api/federation/channel/:channel', (req, res) => {
        Env.interface.sendQuery(Env.getCoreId('federation'),
            'FEDERATION_CHANNEL_STATUS', { channel: req.params.channel },
            answer => {
                if (answer?.error === 'ENOFEDERATION') {
                    return void res.status(404).json({ federated: false });
                }
                if (answer?.error === 'INVALID_CHAN') {
                    return void res.status(400).json({ error: 'INVALID_CHAN' });
                }
                if (answer?.error) {
                    return void res.status(502).json({ error: String(answer.error) });
                }
                res.json(answer.data || { federated: false, members: [] });
            });
    });

    /*  Ask this instance to mirror a pad from a peer (federation design §5.2,
        step 3). The caller supplies a replication capability signed by the pad's
        own key; that signature IS the authorisation (R-25), verified downstream
        against the `validateKey` the servers already hold. Nothing here can be
        used without one, and the operator's peering policy applies regardless.

        Only the client can mint a capability, because only the client has the
        pad's signing key — which is exactly the property that keeps the server
        unable to federate a pad on its users' behalf.
    */
    /*  Shared shape for the two federation POSTs: hand the body to core, and
        turn a refusal into a client error rather than a server one. A rejected
        capability or an unpeered instance is the caller's problem, not a fault. */
    const federationPost = (command, pick) => (req, res) => {
        const body = req.body || {};
        Env.interface.sendQuery(Env.getCoreId('federation'), command, pick(body),
            answer => {
                const error = answer?.error;
                if (error === 'ENOFEDERATION') {
                    return void res.status(404).json({ error: 'ENOFEDERATION' });
                }
                if (error) {
                    const client = /^E_CAP_|^ENOTPEERED$|^INVALID_CHAN$|^EBADCHANNEL$|^ENOVALIDATEKEY$/
                        .test(String(error));
                    return void res.status(client ? 403 : 502).json({ error: String(error) });
                }
                // pass the answer through: callers need to distinguish
                // "federated it" from "it already was"
                res.json(Object.assign({ ok: true }, answer?.data || {}));
            });
    };

    /*  Mark a pad this instance holds as federated, naming the instances allowed
        to replicate it (design §5.2, step 2). Recording the replica set grants
        nothing on its own: a peer still has to present a pad-key-signed
        capability to subscribe.
    */
    app.post('/api/federation/enable', Express.json(),
        federationPost('FEDERATION_ENABLE', (b) => ({
            channel: b.channel, validateKey: b.validateKey, members: b.members,
            // 'L2' asks for multi-master; anything else is anchored (L1)
            level: b.level,
            /*  Carried through to the peers as an INVITE. The capability is
                minted by the client, which is the only party holding the pad
                key; this server only relays it over the session it already has
                with them. */
            cap: b.cap
        })));

    app.post('/api/federation/replicate', Express.json(), (req, res) => {
        const body = req.body || {};
        const coreId = Env.getCoreId('federation');
        Env.interface.sendQuery(coreId, 'FEDERATION_REPLICATE', {
            channel: body.channel,
            peer: body.peer,
            cap: body.cap,
            validateKey: body.validateKey
        }, answer => {
            const error = answer?.error;
            if (error === 'ENOFEDERATION') {
                return void res.status(404).json({ error: 'ENOFEDERATION' });
            }
            if (error) {
                // a refused capability or an unpeered instance is a client error
                const client = /^E_CAP_|^ENOTPEERED$|^INVALID_CHAN$|^EBADCHANNEL$/.test(String(error));
                return void res.status(client ? 403 : 502).json({ error: String(error) });
            }
            res.json({ ok: true });
        });
    });

    const servePlugins = Env => {
        const plugins = Env.plugins;
        let extensions = plugins._extensions;
        let styles = plugins._styles;
        let str = JSON.stringify(extensions);
        let str2 = JSON.stringify(styles);
        let js = `let extensions = ${str};
    let styles = ${str2};
    let lang = window.cryptpadLanguage;
    let paths = [];
    extensions.forEach(name => {
        paths.push(\`optional!/\${name}/extensions.js\`);
        paths.push(\`optional!json!/\${name}/translations/messages.json\`);
        const l = lang === "en" ? '' : \`\${lang}.\`;
        paths.push(\`optional!json!/\${name}/translations/messages.\${l}json\`);
    });
    styles.forEach(name => {
        paths.push(\`optional!less!/\${name}/style.less\`);
    });
    define(paths, function () {
        let args = Array.prototype.slice.apply(arguments);
        return args;
    }, function () {
        // ignore missing files
    });`;
        app.get('/extensions.js', (req, res) => {
            res.setHeader('Content-Type', 'text/javascript');
            res.send(js);
        });
    };

    app.get('/api/profiling', (/*req, res*/) => {
        // XXX Env.enableProfiling, Env.profilingWindow
        throw new Error('NOT_IMPLEMENTED');
    });

    // HTTP commands
    // This endpoint handles authenticated RPCs over HTTP
    // via an interactive challenge-response protocol
    app.use(Express.json());
    app.post('/api/auth', (req, res) => {
        const body = Util.clone(req.body);
        const cookies = req.cookies;
        body._cookies = cookies;
        events.httpCommand.fire(body, (err, response) => {
            if (err) {
                return res.status(500).json({
                    error: err
                });
            }
            if (response?._cookie) {
                res.setHeader('Set-Cookie', response._cookie);
            }
            res.status(200).json(response);
        });
    });

    servePlugins(Env);
};

Network.init = (Env, config, onEnvReady) => {
    const network = { events };

    initExpress(Env);

    // WEBSOCKET
    const RECV_RATE_LIMIT = 100; // messages per second slot

    const checkWssLimit = (user) => {
        const { wssLimit } = user;
        if (wssLimit.dropped) { return false; } // Silently drop abusing users
        const now = +new Date().setMilliseconds(0);
        if (wssLimit?.lastWindow < now) { // new window
            wssLimit.lastWindow = now;
            wssLimit.count = 1;
            return true;
        }
        if (wssLimit.count >= RECV_RATE_LIMIT) {
            Env.Log.error('WS_LIMIT_EXCEEDED', user.ip);
            wssLimit.dropped = true;
            return false;
        }
        wssLimit.count++;
        return true;
    };

    const now = () => {
        return +new Date();
    };
    const randName = () => {
        return Crypto.randomBytes(16).toString('hex');
    };
    const createUniqueName = (Env) => {
        const name = randName();
        if (typeof(Env.users[name]) === 'undefined') { return name; }
        return createUniqueName(Env);
    };
    const socketSendable = (socket) => {
        return socket && socket.readyState === 1;
    };
    const QUEUE_CHR = 1024 * 1024 * 4;
    const WEBSOCKET_CLOSING = 2;
    const WEBSOCKET_CLOSED = 3;

    const dropUser = (user, reason) => {
        // Clean memory
        delete Env.users[user.id];

        if (!user || !user.socket) { return; }
        if (user.socket.readyState !== WEBSOCKET_CLOSING
            && user.socket.readyState !== WEBSOCKET_CLOSED) {
            try {
                user.socket.close();
            } catch (e) {
                Env.Log.error(e, 'FAIL_TO_DISCONNECT', { id: user.id, });
                try {
                    user.socket.terminate();
                } catch (ee) {
                    Env.Log.error(ee, 'FAIL_TO_TERMINATE', {
                        id: user.id
                    });
                }
            }
        }

        events.dropUser.fire({
            id: user.id, channels: user.channels, reason
        });

        // Log unexpected errors
        if (Env.logIP &&
            !['SOCKET_CLOSED', 'INACTIVITY'].includes(reason)) {
            return void Env.Log.info('USER_DISCONNECTED_ERROR', {
                userId: user.id,
                reason: reason
            });
        }
        if (['BAD_MESSAGE', 'SEND_MESSAGE_FAIL_2'].includes(reason)) {
            return void Env.Log.error('SESSION_CLOSE_WITH_ERROR', {
                userId: user.id,
                reason: reason,
            });
        }

        if (['SOCKET_CLOSED', 'SOCKET_ERROR'].includes(reason)) {
            return;
        }
        Env.Log.silly('SESSION_CLOSE_ROUTINE', {
            userId: user.id,
            reason: reason,
        });
    };

    const sendMsgPromise = (user, msg) => {
        return new Promise((resolve, reject) => {
            // don't bother trying to send if the user doesn't
            // exist anymore
            if (!user) { return void reject("NO_USER"); }
            // or if you determine that it's unsendable
            if (!socketSendable(user.socket)) {
                return void reject("UNSENDABLE");
            }

            Env.Log.silly('Sending', msg, 'to', user.id);

            try {
                const strMsg = JSON.stringify(msg);
                user.inQueue += strMsg.length;
                user.sendMsgCallbacks.push(() => {
                    const length = strMsg.length;
                    Env.plugins?.MONITORING?.increment(`sent`);
                    Env.plugins?.MONITORING?.increment(`sentSize`, length);
                    resolve();
                });
                user.socket.send(strMsg, () => {
                    user.inQueue -= strMsg.length;
                    if (user.inQueue > QUEUE_CHR) { return; }
                    const smcb = user.sendMsgCallbacks;
                    user.sendMsgCallbacks = [];
                    try {
                        smcb.forEach((cb)=>{cb();});
                    } catch (e) {
                        Env.Log.error(e, 'SEND_MESSAGE_FAIL');
                    }
                });
            } catch (e) {
                // call back any pending callbacks before you
                // drop the user
                reject(e);
                Env.Log.error(e, 'SEND_MESSAGE_FAIL_2');
                dropUser(user, 'SEND_MESSAGE_FAIL_2');
            }
        });
    };
    const sendMsg = (user, msg) => {
        sendMsgPromise(user, msg).catch(e => {
            if (['NO_USER', 'UNSENDABLE'].includes(e)) { return; }
            Env.Log.error(e, 'SEND_MESSAGE', {
                user: user.id,
                message: msg
            });
        });
    };

    const handleMessage = (user, msg, cb) => {
        try {
            let json = JSON.parse(msg);
            let seq = json.shift();
            let cmd = json[0];

            Env.plugins?.MONITORING?.increment(`received`);
            Env.plugins?.MONITORING?.increment(`receivedSize`, msg.length);

            user.timeOfLastMessage = now();
            user.pingOutstanding = false;

            events.message.fire({ user, cmd, seq, json }, cb);
        } catch (e) {
            cb(e);
        }
    };

    const LAG_MAX_BEFORE_DISCONNECT = 60000;
    const LAG_MAX_BEFORE_PING = 15000;
    const checkUserActivity = () => {
        const time = now();
        Object.keys(Env.users).forEach((userId) => {
            const u = Env.users[userId];
            try {
                if (time - u.timeOfLastMessage > LAG_MAX_BEFORE_DISCONNECT) {
                    dropUser(u, 'INACTIVITY');
                }
                if (!u.pingOutstanding && time - u.timeOfLastMessage > LAG_MAX_BEFORE_PING) {
                    sendMsg(u, [0, '', 'PING', now()]);
                    u.pingOutstanding = true;
                    Env.plugins?.MONITORING?.increment(`pingSent`);
                }
            } catch (err) {
                Env.Log.error(err, 'USER_ACTIVITY_CHECK');
            }
        });
    };
    const initServerHandlers = () => {
        if (!Env.wss) { throw new Error('No WebSocket Server'); }

        Env.active = true;

        setInterval(() => {
            checkUserActivity();
        }, 5000);


        Env.wss.on('connection', (socket, req) => {
            // refuse new connections if the server is shutting down
            if (!Env.active) { return; }
            if (!socket.upgradeReq) { socket.upgradeReq = req; }

            const ip = (req.headers && req.headers['x-real-ip'])
                          || req.socket.remoteAddress || '';
            const user = {
                socket: socket,
                id: createUniqueName(Env),
                timeOfLastMessage: now(),
                pingOutstanding: false,
                inQueue: 0,
                ip: ip.replace(/^::ffff:/, ''),
                sendMsgCallbacks: [],
                channels: new Set(),
                wssLimit: {
                    dropped: false,
                    lastWindow: +new Date().setMilliseconds(0),
                    count: 0
                },
            };
            Env.users[user.id] = user;
            sendMsg(user, [0, '', 'IDENT', user.id]);

            /*
            setTimeout(() => {
                if (!Env.users[id]) { return; }
                if (!user.validated) { user.isEmpty = true; }
                delete user.validated;
                // TODO: deal with websocket that didn't join any channel
            }, 120000);
            */

            if (Env.logIP && user.ip) {
                Env.Log.info('USER_CONNECTION', {
                    userId: user.id,
                    ip: user.ip,
                });
            }


            socket.on('message', message => {
                if (!Env.users[user.id]) { return; } // websocket closing
                if (!checkWssLimit(user)) { return ; } // silently ignoring an abusing user messages
                Env.Log.silly('Receiving', JSON.parse(message), 'from', user.id);
                handleMessage(user, message, e => {
                    if (!e) { return; }
                    Env.Log.error(e, 'NETFLUX_BAD_MESSAGE', {
                        user: user.id,
                        message: message,
                    });
                    dropUser(user, 'BAD_MESSAGE');
                });
            });
            socket.on('close', function () {
                dropUser(user, 'SOCKET_CLOSED');
            });
            socket.on('error', function (err) {
                Env.Log.error(err, 'NETFLUX_WEBSOCKET_ERROR');
                dropUser(user, 'SOCKET_ERROR');
            });
        });

    };

    network.sendMsgPromise = sendMsgPromise;
    network.sendMsg = sendMsg;

    network.shutdown = () => {
        if (!Env.wss) { return; }
        Env.active = false;
        Env.wss.close();
        delete Env.wss;
    };

    network.broadcast = (message) => {
        if (!message) { return; }
        Object.keys(Env.users).forEach((userId) => {
            const u = Env.users[userId];
            sendMsg(u, message);
        });
        return true;
    };

    network.getWsData = () => {
        return {
            nb: Object.keys(Env.users)?.length,
            nb_wss: Env?.wss?.clients?.size
        };
    };

    onEnvReady.reg(() => {
        const cfg = config?.infra?.front[config.index];
        const server = Http.createServer(app);
        server.listen(cfg.port, cfg.host, () => {
            Env.Log.debug('HTTP worker listening on port', cfg.port);
        });

        Env.wss = new WebSocketServer({ server });
        initServerHandlers(Env);
    });

    return network;
};

module.exports = Network;
