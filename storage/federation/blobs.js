// SPDX-License-Identifier: AGPL-3.0-or-later
// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors

/*  Blob storage access for federation (spec R-44).

    Blobs are the one part of a pad that is not in the channel log: an embedded
    image lives at `blob/<ab>/<id>` and the log only references it — inside
    content the server cannot read. Replicating a pad without its blobs gives a
    document that renders broken on the replica, with no error anywhere.

    Why this needs no merge
    -----------------------
    A blob is immutable and content-addressed: the id *is* the content. Two
    instances can never disagree about what a blob contains, so there is no
    ordering, no watermark and no conflict — only "do I have these bytes yet".
    That makes blob federation a fetch problem rather than a replication one,
    which is why it is a separate mechanism from the channel log.

    Addressed through the ObjectBackend so `fs` and S3 both work: the backend is
    rooted at the instance's data directory and blobs sit under `blob/`, which is
    exactly the layout the local blob store already uses.
*/

const BLOB_ID_LENGTH = 48;

// A blob bigger than this is not fetched from a peer. Matches the default
// upload limit; an operator who raises one should raise both.
const MAX_FEDERATED_BLOB = 20 * 1024 * 1024;

const isValidId = (id) =>
    typeof (id) === 'string' && id.length === BLOB_ID_LENGTH && !/[^a-f0-9]/.test(id);

const key = (id) => `blob/${id.slice(0, 2)}/${id}`;

const enoent = (err) =>
    Boolean(err) && (err.code === 'ENOENT' || /ENOENT/.test(err.message || String(err)));

const create = (Env) => {
    const backend = Env.federationBackend || Env.storageBackend;

    const Blobs = {};

    /*  Do we hold this blob, and how big is it? Answering `size` as well as
        `exists` lets the requester refuse an oversized transfer before it
        starts rather than partway through. */
    Blobs.stat = (id, cb) => {
        if (!isValidId(id)) { return void cb('EBADBLOBID'); }
        backend.head(key(id), (err, stat) => {
            if (err) {
                if (enoent(err)) { return void cb(void 0, { found: false }); }
                return void cb(err);
            }
            cb(void 0, { found: true, size: stat?.size });
        });
    };

    // Read a slice, so a large blob can cross the wire in frames.
    Blobs.read = (id, offset, length, cb) => {
        if (!isValidId(id)) { return void cb('EBADBLOBID'); }
        backend.get(key(id), (err, body) => {
            if (err) {
                if (enoent(err)) { return void cb('ENOBLOB'); }
                return void cb(err);
            }
            const start = Math.max(0, offset | 0);
            const end = length ? Math.min(body.length, start + length) : body.length;
            cb(void 0, { chunk: body.subarray(start, end), total: body.length });
        });
    };

    /*  Store a blob fetched from a peer.

        Written whole rather than appended: a partially-written blob would be
        indistinguishable from a complete one at this layer, and serving half an
        image is worse than serving none. The caller reassembles first.
    */
    Blobs.write = (id, buffer, cb) => {
        if (!isValidId(id)) { return void cb('EBADBLOBID'); }
        if (!Buffer.isBuffer(buffer)) { return void cb('EBADBLOB'); }
        if (buffer.length > MAX_FEDERATED_BLOB) { return void cb('ETOOLARGE'); }
        backend.put(key(id), buffer, {}, (err) => {
            if (err) { return void cb(err); }
            Env.Log.info('FEDERATION_BLOB_STORED', { id, bytes: buffer.length });
            cb();
        });
    };

    return Blobs;
};

module.exports = { create, isValidId, key, MAX_FEDERATED_BLOB, BLOB_ID_LENGTH };
