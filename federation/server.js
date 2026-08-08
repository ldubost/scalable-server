// SPDX-License-Identifier: AGPL-3.0-or-later
// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors

/*  Inbound federation endpoint.

    A plain WebSocket server on the federation node's own port, separate from the
    browser-facing front node and from the internal node bus. Peers dial it; the
    handshake in peer-session.js decides whether anything comes of that.

    In production this sits behind TLS (R-24) — either a terminating proxy or a
    `tls` option in infra. It is a separate listener precisely so an operator can
    give it its own certificate and firewall rule.
*/

const WebSocket = require('ws');
const Http = require('node:http');
const Https = require('node:https');
const Fs = require('node:fs');

const PeerSession = require('./peer-session.js');

const PATH = '/federation';

const init = (Env, cfg, cb) => {
    const Log = Env.Log;

    let httpServer;
    if (cfg?.tls?.cert && cfg?.tls?.key) {
        httpServer = Https.createServer({
            cert: Fs.readFileSync(cfg.tls.cert),
            key: Fs.readFileSync(cfg.tls.key)
        });
    } else {
        /*  Plaintext is allowed so a single-machine test topology needs no
            certificates, but it is not what R-24 asks for. Say so loudly rather
            than let an operator discover it in a packet capture. */
        if (Env.policy.peers.length) {
            Log.warn('FEDERATION_NO_TLS',
                'federation listener is running without TLS; terminate TLS in front of it');
        }
        httpServer = Http.createServer();
    }

    const wss = new WebSocket.Server({ noServer: true });

    httpServer.on('upgrade', (req, socket, head) => {
        let path;
        try {
            path = new URL(req.url, 'http://placeholder').pathname;
        } catch (err) { path = req.url; }
        if (path !== PATH) {
            socket.destroy();
            return;
        }
        wss.handleUpgrade(req, socket, head, ws => {
            wss.emit('connection', ws, req);
        });
    });

    wss.on('connection', (ws) => {
        PeerSession.create(Env, ws, {
            role: 'listener',
            onReady: (session) => { Env.sessions.add(session); },
            onClose: (session) => { Env.sessions.remove(session); }
        });
    });

    const port = cfg?.wsPort || cfg?.port;
    httpServer.listen(port, cfg?.host, () => {
        Log.info('FEDERATION_LISTENING', { port, path: PATH, tls: Boolean(cfg?.tls?.cert) });
        cb?.();
    });
    httpServer.on('error', (err) => {
        Log.error('FEDERATION_LISTEN_ERROR', err);
        cb?.(err);
    });

    return {
        shutdown: () => {
            try { wss.close(); } catch (err) { /* already down */ }
            try { httpServer.close(); } catch (err) { /* already down */ }
        }
    };
};

module.exports = { init, PATH };
