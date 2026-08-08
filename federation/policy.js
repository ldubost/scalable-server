// SPDX-License-Identifier: AGPL-3.0-or-later
// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors

/*  Operator peering policy (spec R-27).

    An allowlist, and deliberately nothing cleverer. Federation moves a pad's
    ciphertext onto someone else's disk and accepts theirs onto ours; which
    instances that may happen with is an operator decision, not a user one, and
    not a discovery protocol. The default is an empty list: an instance that has
    not been configured federates with nobody, so building the node type does not
    by itself expose anything.

    Peers are named by their instance public key (the originId), never by URL.
    URLs are a dialling hint; the key is the identity. An attacker who takes over
    a hostname still cannot pass the handshake (R-24).
*/

const Path = require('node:path');
const Fs = require('node:fs');

const Codec = require('../common/federation/codec.js');

const isB64Key = (s) => {
    if (typeof (s) !== 'string') { return false; }
    try {
        const buf = Buffer.from(s, 'base64');
        return buf.length === 32 && buf.toString('base64') === s;
    } catch (err) { return false; }
};

const FILE = 'peers.json';

/*  Generous enough that normal replication never notices, low enough that a
    misbehaving peer cannot saturate a node. Spec §11.2 measured a busy pad at
    roughly 3.3 messages/second per active editor. */
const DEFAULT_FRAMES_PER_SECOND = 200;
const DEFAULT_BYTES_PER_SECOND = 4 * 1024 * 1024;

const num = (v, fallback) =>
    (typeof (v) === 'number' && isFinite(v) && v > 0) ? v : fallback;

/*  Shape on disk:

      {
        "version": 1,
        "peers": [
          { "originId": "<base64 ed25519 pubkey>",
            "url": "wss://other.example/federation",
            "name": "other.example",
            "dial": true }
        ]
      }

    `dial` distinguishes a peer we actively connect to from one we merely accept
    connections from. Both directions still require the peer to be listed. */
const parse = (raw, path, Log) => {
    const obj = Codec.decode(raw);
    if (!obj) {
        Log?.error('FEDERATION_POLICY_PARSE', path);
        return [];
    }
    if (obj.version !== 1 || !Array.isArray(obj.peers)) {
        Log?.error('FEDERATION_POLICY_UNSUPPORTED', path);
        return [];
    }
    const seen = new Set();
    return obj.peers.filter(p => {
        if (!isB64Key(p?.originId)) {
            Log?.error('FEDERATION_POLICY_BAD_PEER', p?.name || p?.url);
            return false;
        }
        if (seen.has(p.originId)) {
            Log?.error('FEDERATION_POLICY_DUPLICATE', p.originId);
            return false;
        }
        seen.add(p.originId);
        return true;
    }).map(p => ({
        originId: p.originId,
        url: typeof (p.url) === 'string' ? p.url : undefined,
        name: typeof (p.name) === 'string' ? p.name : p.originId.slice(0, 12),
        /*  Where a *browser* reaches this peer, as opposed to `url`, which is
            where our federation node dials it. They are different things: one is
            a public HTTPS origin, the other an internal websocket endpoint that
            may not even be routable from outside. */
        origin: typeof (p.origin) === 'string' ? p.origin.replace(/\/+$/, '') : undefined,
        dial: p.dial !== false && typeof (p.url) === 'string',
        /*  R-30: per-peer limits. A peer is trusted to hold our users' pads, not
            to be well-behaved or even well-configured — a runaway loop on a
            friendly instance is as damaging as a hostile one. Defaults apply
            unless an operator raises them deliberately. */
        maxFramesPerSecond: num(p.maxFramesPerSecond, DEFAULT_FRAMES_PER_SECOND),
        maxBytesPerSecond: num(p.maxBytesPerSecond, DEFAULT_BYTES_PER_SECOND)
    }));
};

const mk = (peers, path, present) => {
    const byId = new Map(peers.map(p => [p.originId, p]));
    return {
        path,
        present,
        peers,
        // peers this instance dials out to
        outbound: () => peers.filter(p => p.dial),
        // R-27: may we hold a session with this instance at all?
        admits: (originId) => byId.has(originId),
        get: (originId) => byId.get(originId),
        describe: (originId) => byId.get(originId)?.name || String(originId).slice(0, 12)
    };
};

/*  Load the policy. A missing file is not an error: it is how an instance says
    "I do not federate", which must remain the easy default. */
const load = (dir, Log) => {
    const path = Path.join(dir, FILE);
    let raw;
    try {
        raw = Fs.readFileSync(path, 'utf8');
    } catch (err) {
        if (err.code !== 'ENOENT') { throw err; }
        return mk([], path, false);
    }
    return mk(parse(raw, path, Log), path, true);
};

module.exports = { load, FILE };
