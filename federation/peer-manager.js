// SPDX-License-Identifier: AGPL-3.0-or-later
// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors

/*  Outbound dialling and the session registry.

    Two jobs:

      1. Keep one session per peer this node owns, redialling with backoff.
      2. Answer "do I have a live session with <originId>?" for everything above.

    Peer -> node ownership is a jump hash over the originId (design §1.2), so with
    several federation nodes each peer is dialled by exactly one of them and the
    sessions are not duplicated. Both ends may dial simultaneously; the tie-break
    below makes them settle on one session rather than flapping.
*/

const WebSocket = require('ws');

const PeerSession = require('./peer-session.js');

const BACKOFF_MIN = 2 * 1000;
const BACKOFF_MAX = 5 * 60 * 1000;

const create = (Env) => {
    const Log = Env.Log;
    const byOrigin = new Map();   // originId -> session
    const backoff = new Map();    // originId -> ms
    const timers = new Map();     // originId -> timeout
    let stopped = false;

    // which federation node owns the outbound session to this peer
    const owns = (originId) => Env.getFederationId(originId) === Env.myId;

    /*  dial and schedule call each other: a failed dial schedules a retry, and a
        retry dials. Declared here so neither has to be defined first. */
    let dial;

    const schedule = (originId) => {
        if (stopped || !owns(originId)) { return; }
        const peer = Env.policy.get(originId);
        if (!peer?.dial) { return; }
        if (timers.has(originId)) { return; }

        const wait = Math.min(backoff.get(originId) || BACKOFF_MIN, BACKOFF_MAX);
        // jitter, so a shared outage does not resynchronise every instance
        const delay = wait * (0.5 + Math.random());
        backoff.set(originId, wait * 2);
        timers.set(originId, setTimeout(() => {
            timers.delete(originId);
            dial(peer);
        }, delay));
    };

    const registry = {
        add: (session) => {
            const existing = byOrigin.get(session.originId);
            if (existing && existing !== session) {
                /*  Both instances dialled each other and both handshakes
                    succeeded. Keep one deterministically: the session whose
                    dialler has the lexicographically smaller originId. Both ends
                    apply the same rule to the same pair of ids, so they drop
                    opposite sockets and one survives. */
                const mine = Env.identity.originId;
                const theirs = session.originId;
                const keepRole = mine < theirs ? 'dialler' : 'listener';
                const loser = session.role === keepRole ? existing : session;
                const winner = loser === session ? existing : session;
                byOrigin.set(session.originId, winner);
                Log.verbose('FEDERATION_DUPLICATE_SESSION',
                    { peer: Env.policy.describe(theirs), kept: winner.role });
                loser.close();
                return;
            }
            byOrigin.set(session.originId, session);
            backoff.delete(session.originId);
            Env.onSessionUp?.(session);
        },
        remove: (session) => {
            if (!session?.originId) { return; }
            // a dropped session takes its subscriptions with it; the peer
            // re-subscribes on reconnect, which also re-runs the backfill
            Env.subscriptions?.drop(session);
            if (byOrigin.get(session.originId) === session) {
                byOrigin.delete(session.originId);
                Log.info('FEDERATION_SESSION_DOWN',
                    { peer: Env.policy.describe(session.originId) });
                // only the owner redials; a listener-side drop waits to be redialled
                if (owns(session.originId)) { schedule(session.originId); }
            }
        },
        get: (originId) => byOrigin.get(originId),
        has: (originId) => byOrigin.has(originId),
        list: () => Array.from(byOrigin.values()),
        count: () => byOrigin.size
    };

    dial = (peer) => {
        if (stopped || byOrigin.has(peer.originId)) { return; }
        let ws;
        try {
            ws = new WebSocket(peer.url);
        } catch (err) {
            Log.warn('FEDERATION_DIAL_ERROR', { peer: peer.name, error: err?.message });
            return schedule(peer.originId);
        }
        ws.on('open', () => {
            PeerSession.create(Env, ws, {
                role: 'dialler',
                expectedOriginId: peer.originId,
                onReady: (session) => { registry.add(session); },
                onClose: (session) => {
                    if (session.authenticated) { return registry.remove(session); }
                    schedule(peer.originId);
                }
            });
        });
        ws.on('error', (err) => {
            Log.verbose('FEDERATION_DIAL_FAILED', { peer: peer.name, error: err?.message });
        });
        /*  A socket that never opens produces 'error' then 'close' and no
            session, so the redial is scheduled from the error path above. */
        ws.on('close', () => {
            if (!byOrigin.has(peer.originId)) { schedule(peer.originId); }
        });
    };

    return {
        sessions: registry,
        owns,
        start: () => {
            Env.policy.outbound().forEach(peer => {
                if (!owns(peer.originId)) { return; }
                dial(peer);
            });
        },
        shutdown: () => {
            stopped = true;
            timers.forEach(t => clearTimeout(t));
            timers.clear();
            registry.list().forEach(s => s.close());
        }
    };
};

module.exports = { create, BACKOFF_MIN, BACKOFF_MAX };
