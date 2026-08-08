// SPDX-License-Identifier: AGPL-3.0-or-later
// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors

/*  Fetching a blob from a peer (spec R-44, R-45).

        BLOB_REQ    {txid, id}                 "do you have this, and send it"
        BLOB_CHUNK  {txid, seq, data, last}    one slice, base64
        BLOB_ERR    {txid, error}              typed refusal

    On demand, not up front
    -----------------------
    A pad's blobs are referenced from inside content the server cannot read, so
    no server can enumerate them. Rather than have the client list them at
    federation time — which would only cover blobs that existed then — a replica
    fetches a blob the first time somebody asks for one it does not have. That
    covers blobs added later, costs nothing for pads that have none, and needs
    no cooperation from the client at all.

    What a peer may ask for
    -----------------------
    Any blob id, and we serve it if we hold it. That deserves stating plainly:
    it is the same bearer-capability model CryptPad already uses for blobs —
    the 48-character id is unguessable and knowing it is what grants access,
    which is why blob URLs can be shared. Combined with R-27 (only allowlisted
    instances get a session at all) that is the trust boundary. A peer cannot
    enumerate blob ids, only redeem ones it already knows.
*/

const CHUNK = 64 * 1024;          // raw bytes; ~87KB base64, well under MAX_FRAME
const TIMEOUT = 60 * 1000;

const err = (session, txid, code) => {
    session.send({ type: 'BLOB_ERR', txid, error: code });
};

/*  A peer wants a blob. Stream it in slices if we have it.

    Slices are read and sent one at a time rather than all at once so a large
    blob does not sit in memory twice — once as the buffer, once as base64.
*/
const onRequest = (Env, session, frame) => {
    const { txid, id } = frame || {};
    if (typeof (txid) !== 'string') { return; }

    Env.toStorage('FED_BLOB_STAT', { id }, (e, stat) => {
        if (e) { return void err(session, txid, String(e)); }
        if (!stat?.found) { return void err(session, txid, 'ENOBLOB'); }

        let offset = 0;
        let seq = 0;
        const step = () => {
            Env.toStorage('FED_BLOB_READ', {
                id, offset, length: CHUNK
            }, (e2, res) => {
                if (e2) { return void err(session, txid, String(e2)); }
                const buf = Buffer.from(res.chunk?.data || res.chunk || [], 'base64');
                const total = res.total;
                offset += buf.length;
                const last = offset >= total || buf.length === 0;
                session.send({
                    type: 'BLOB_CHUNK',
                    txid,
                    seq: seq++,
                    data: buf.toString('base64'),
                    last
                });
                if (!last) { setImmediate(step); }
            });
        };
        step();
    });
};

// A slice of a blob we asked for.
const onChunk = (Env, session, frame) => {
    const pending = Env.pendingBlobs.get(frame?.txid);
    if (!pending) { return; }
    if (pending.originId !== session.originId) { return; }

    let buf;
    try {
        buf = Buffer.from(frame.data || '', 'base64');
    } catch (e) { return void pending.done('EBADCHUNK'); }

    /*  Out-of-order slices would silently corrupt the blob, and the corruption
        would only surface as an image that will not render. Refuse instead. */
    if (frame.seq !== pending.expect) { return void pending.done('EOUTOFORDER'); }
    pending.expect++;

    pending.parts.push(buf);
    pending.bytes += buf.length;
    if (pending.bytes > pending.max) { return void pending.done('ETOOLARGE'); }

    if (frame.last) { pending.done(void 0, Buffer.concat(pending.parts)); }
};

const onError = (Env, session, frame) => {
    const pending = Env.pendingBlobs.get(frame?.txid);
    if (!pending || pending.originId !== session.originId) { return; }
    pending.done(String(frame.error || 'EBLOBFAILED'));
};

/*  Ask peers for a blob, in turn, until one has it.

    Sequential rather than parallel: the common case is that exactly one peer
    holds it, and asking everybody at once would multiply the transfer for no
    gain.
*/
const fetch = (Env, args, cb) => {
    const id = args?.id;
    const sessions = Env.peers.sessions.list();
    if (!sessions.length) { return void cb('ENOPEERS'); }

    let i = 0;
    const tryNext = () => {
        if (i >= sessions.length) { return void cb('ENOBLOB'); }
        const session = sessions[i++];
        const txid = require('node:crypto').randomBytes(8).toString('base64');

        let finish;
        const timer = setTimeout(() => finish('ETIMEOUT'), TIMEOUT);
        finish = (e, buf) => {
            if (!Env.pendingBlobs.has(txid)) { return; }
            Env.pendingBlobs.delete(txid);
            clearTimeout(timer);
            if (e) {
                Env.Log.verbose('FEDERATION_BLOB_MISS', {
                    id, peer: Env.policy.describe(session.originId), error: e
                });
                return void tryNext();
            }
            Env.Log.info('FEDERATION_BLOB_FETCHED', {
                id, peer: Env.policy.describe(session.originId), bytes: buf.length
            });
            /*  Store it before answering, so the caller can serve it straight
                from disk and a second request costs nothing. */
            Env.toStorage('FED_BLOB_WRITE', {
                id, data: buf.toString('base64')
            }, (e2) => {
                if (e2) { return void cb(String(e2)); }
                cb(void 0, { found: true, bytes: buf.length });
            });
        };

        Env.pendingBlobs.set(txid, {
            originId: session.originId,
            parts: [], bytes: 0, expect: 0,
            max: args.max || (20 * 1024 * 1024),
            done: finish
        });

        if (!session.send({ type: 'BLOB_REQ', txid, id })) { finish('EUNSENDABLE'); }
    };
    tryNext();
};

module.exports = { onRequest, onChunk, onError, fetch, CHUNK };
