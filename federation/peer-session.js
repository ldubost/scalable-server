// SPDX-License-Identifier: AGPL-3.0-or-later
// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors

/*  One authenticated session with one remote instance.

    Wraps a raw WebSocket in: the mutual-key handshake (R-24), the peering
    policy check (R-27), frame decoding through the Codec seam, and a command
    table. Both the dialling and the listening side use this same object; they
    differ only in which handshake frames they send first.

    Everything a peer sends before the handshake completes is refused. This is
    the trust boundary: past this file, callers may assume `session.originId` is
    an instance that proved possession of that key AND that the operator listed
    it.
*/

const Codec = require('../common/federation/codec.js');
const Handshake = require('../common/federation/handshake.js');

// A peer that sends anything larger than this is not talking to us in good
// faith; M0 has no frame that comes close.
const MAX_FRAME = 256 * 1024;

const create = (Env, ws, opts) => {
    const { role, expectedOriginId, onReady, onClose } = opts;
    const Log = Env.Log;

    const session = {
        role,                 // 'dialler' | 'listener'
        originId: undefined,  // set only once authenticated
        authenticated: false,
        peer: undefined,      // policy entry, set with originId
        ws
    };

    let hello, challenge, timer, closed = false;

    /*  Per-peer rate limiting (R-30), as a token bucket over both frames and
        bytes. Refilled continuously rather than in fixed windows, so a peer
        cannot burst a whole second's budget at each boundary.

        A peer that exceeds it is throttled by dropping the frame, not by
        closing the session: replication is self-healing — anti-entropy will
        re-fetch whatever was dropped — whereas tearing down the session would
        turn a transient overload into a reconnect storm. */
    const limits = {
        frames: 0, bytes: 0, last: Date.now(), dropped: 0, warned: 0
    };
    const withinLimits = (size) => {
        const peer = Env.policy.get(session.originId);
        if (!peer) { return true; }
        const now = Date.now();
        const elapsed = Math.max(0, now - limits.last) / 1000;
        limits.last = now;
        limits.frames = Math.max(0, limits.frames - (peer.maxFramesPerSecond * elapsed));
        limits.bytes = Math.max(0, limits.bytes - (peer.maxBytesPerSecond * elapsed));

        if (limits.frames + 1 > peer.maxFramesPerSecond ||
            limits.bytes + size > peer.maxBytesPerSecond) {
            limits.dropped++;
            // log the first, then occasionally: a flood must not flood the log
            if (limits.dropped === 1 || now - limits.warned > 10000) {
                limits.warned = now;
                Log.warn('FEDERATION_RATE_LIMITED', {
                    peer: Env.policy.describe(session.originId),
                    dropped: limits.dropped
                });
            }
            return false;
        }
        limits.frames += 1;
        limits.bytes += size;
        return true;
    };
    session.rateLimited = () => limits.dropped;

    const send = (obj) => {
        if (closed || ws.readyState !== ws.OPEN) { return false; }
        try {
            ws.send(Codec.encode(obj));
            return true;
        } catch (err) {
            Log.error('FEDERATION_SEND_ERROR', err);
            return false;
        }
    };
    session.send = send;

    const close = (reason) => {
        if (closed) { return; }
        closed = true;
        clearTimeout(timer);
        /*  Tell the peer why before hanging up. A peer that is merely not on our
            allowlist should be able to log something better than "socket closed",
            and this leaks nothing it could not learn by trying. */
        if (reason && ws.readyState === ws.OPEN) {
            try { ws.send(Codec.encode({ type: 'ERROR', error: reason })); } catch (err) { /* going away */ }
        }
        try { ws.close(); } catch (err) { /* already gone */ }
        onClose?.(session, reason);
    };
    session.close = close;

    /*  An unauthenticated socket must not be able to hold a slot open. Once the
        handshake completes the timer is cleared and liveness becomes the peer
        manager's problem. */
    timer = setTimeout(() => {
        if (!session.authenticated) { close('EHANDSHAKETIMEOUT'); }
    }, Handshake.TIMEOUT);

    const authenticated = (originId) => {
        // R-27: the key is proved; now, is this instance one we peer with?
        if (!Env.policy.admits(originId)) {
            Log.warn('FEDERATION_REFUSED', { originId, role });
            return close('ENOTPEERED');
        }
        session.originId = originId;
        session.peer = Env.policy.get(originId);
        session.authenticated = true;
        clearTimeout(timer);
        Log.info('FEDERATION_SESSION_UP', {
            peer: Env.policy.describe(originId), role
        });
        onReady?.(session);
    };

    const onFrame = (frame) => {
        // --- pre-authentication: only handshake frames exist -------------
        if (!session.authenticated) {
            if (frame?.type === 'ERROR') {
                return close(undefined), Log.warn('FEDERATION_PEER_ERROR', frame.error);
            }
            if (role === 'listener') {
                if (!hello) {
                    hello = frame;
                    const r = Handshake.onHello(Env.identity, hello);
                    if (r.error) { hello = undefined; return close(r.error); }
                    challenge = r.challenge;
                    return void send(challenge);
                }
                const r = Handshake.onProof(Env.identity, hello, challenge, frame);
                if (r.error) { return close(r.error); }
                return authenticated(r.originId);
            }
            // dialler: the only thing we expect is the CHALLENGE
            const r = Handshake.onChallenge(Env.identity, hello, frame, expectedOriginId);
            if (r.error) { return close(r.error); }
            challenge = frame;
            send(r.proof);
            /*  The dialler knows the session is good as soon as it has verified
                the listener and sent its proof: if its proof were bad the
                listener would close, which we would see. */
            return authenticated(frame.originId);
        }

        // --- authenticated: dispatch -------------------------------------
        const handler = Env.federationCommands?.[frame?.type];
        if (typeof (handler) !== 'function') {
            Log.warn('FEDERATION_UNKNOWN_FRAME', { type: frame?.type, peer: session.originId });
            return void send({ type: 'ERROR', error: 'EUNKNOWNCOMMAND', re: frame?.type });
        }
        handler(Env, session, frame);
    };

    ws.on('message', (data, isBinary) => {
        if (closed) { return; }
        const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
        if (buf.length > MAX_FRAME) { return close('EFRAMETOOLARGE'); }
        const frame = Codec.decode(buf);
        if (!frame || typeof (frame) !== 'object') { return close('EBADFRAME'); }
        // limits apply only once we know who this is
        if (session.authenticated && !withinLimits(buf.length)) { return; }
        try {
            onFrame(frame);
        } catch (err) {
            Log.error('FEDERATION_FRAME_ERROR', err);
            close('EINTERNAL');
        }
        void isBinary;
    });

    ws.on('close', () => {
        if (closed) { return; }
        closed = true;
        clearTimeout(timer);
        onClose?.(session);
    });
    ws.on('error', (err) => {
        Log.warn('FEDERATION_SOCKET_ERROR', err?.message);
        close();
    });

    // the dialler opens the conversation
    if (role === 'dialler') {
        hello = Handshake.mkHello(Env.identity);
        send(hello);
    }

    return session;
};

module.exports = { create, MAX_FRAME };
