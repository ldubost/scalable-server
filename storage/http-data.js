// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors
//
// SPDX-License-Identifier: AGPL-3.0-or-later

/*  Serving user data over HTTP.

    The storage node serves three things directly to browsers: encrypted blobs,
    login blocks, and HEAD requests against channels (used to check whether a pad
    exists and how big it is). All three used to be Express.static mounts reading
    from local disk, which stops working the moment the data lives elsewhere.

    Three modes, chosen from configuration and from what the backend can actually
    do:

      static    Express.static, exactly as before. Used when storage is local.
      redirect  302 to a presigned URL. The object goes straight from the store to
                the browser without passing through this node - the cheapest option
                by far for large blobs, but the store then owns the response
                headers, so the bucket needs a CORS policy.
      proxy     stream the object through this node. Costs bandwidth twice, but
                keeps full control of headers and needs nothing from the bucket.

    Range requests are honoured in every mode: media playback and resumable
    downloads depend on them.
*/

const Express = require("express");
const Path = require("node:path");

const HttpData = module.exports;

const DEFAULT_PRESIGN_TTL = 300;

/*  Decide how to serve a family of objects, given the configuration and the
    backend's measured capabilities. A configured 'redirect' silently degrades to
    'proxy' when the backend cannot presign, because failing to serve blobs is a
    much worse outcome than serving them the slower way.  */
const resolveMode = (Env, requested) => {
    const backend = Env.storageBackend;
    if (!backend) { return 'static'; }
    if (requested === 'proxy') { return 'proxy'; }
    if (requested === 'redirect' || !requested) {
        return backend.capabilities && backend.capabilities.presign ? 'redirect' : 'proxy';
    }
    return requested;
};
HttpData.resolveMode = resolveMode;

/*  Turn a request path into an object key, refusing anything that tries to climb
    out of its prefix. Express has already normalised `req.url`, but this is the
    boundary where a mistake would expose unrelated objects, so it is checked
    again here.  */
const keyFromUrl = (prefix, url) => {
    const path = decodeURIComponent((url || '').split('?')[0]);
    const parts = path.split('/').filter(Boolean);
    if (!parts.length) { return; }
    if (parts.some(part => part === '..' || part === '.')) { return; }
    return prefix + parts.join('/');
};
HttpData.keyFromUrl = keyFromUrl;

const parseRange = (header, size) => {
    if (!header) { return; }
    const match = /^bytes=(\d*)-(\d*)$/.exec(String(header).trim());
    if (!match) { return; }

    let start = match[1] === '' ? undefined : Number(match[1]);
    let end = match[2] === '' ? undefined : Number(match[2]);

    if (start === undefined && end === undefined) { return; }
    if (start === undefined) {
        // a suffix range: the last N bytes
        start = Math.max(0, size - end);
        end = size - 1;
    } else if (end === undefined) {
        end = size - 1;
    }
    if (isNaN(start) || isNaN(end) || start > end || start >= size) {
        return { invalid: true };
    }
    return { start, end: Math.min(end, size - 1) };
};
HttpData.parseRange = parseRange;

/*  Build an Express handler that serves objects under `prefix` from the backend. */
HttpData.serve = (Env, opts) => {
    const prefix = (opts.prefix || '').replace(/\/*$/, '/');
    const mode = opts.mode;
    const backend = Env.storageBackend;
    const presignTtl = opts.presignTtl || DEFAULT_PRESIGN_TTL;

    return (req, res, next) => {
        const key = keyFromUrl(prefix, req.url);
        if (!key) { return void next(); }

        backend.head(key, (err, stat) => {
            if (err) {
                // let whatever is mounted after this decide what a miss means
                // (a placeholder explaining an archived pad, usually)
                if (err.code === 'ENOENT') { return void next(); }
                Env.Log.error('HTTP_DATA_HEAD_ERROR', {
                    key, error: err.message || err.code
                });
                return void res.status(500).end();
            }

            res.setHeader('Content-Length', stat.size);
            res.setHeader('Accept-Ranges', 'bytes');
            if (stat.etag) { res.setHeader('ETag', stat.etag); }
            if (opts.cacheControl) { res.setHeader('Cache-Control', opts.cacheControl); }
            if (typeof (opts.setHeaders) === 'function') { opts.setHeaders(res, stat); }

            // HEAD is answered from the metadata alone: no body is fetched at all
            if (req.method === 'HEAD') { return void res.status(200).end(); }
            if (req.method !== 'GET') { return void next(); }

            const range = parseRange(req.headers && req.headers.range, stat.size);
            if (range && range.invalid) {
                /*  Content-Length was set to the whole object above; leaving it in
                    place on an empty 416 would leave the client waiting forever for
                    a body that is never sent.  */
                res.removeHeader('Content-Length');
                res.setHeader('Content-Range', `bytes */${stat.size}`);
                return void res.status(416).end();
            }

            if (mode === 'redirect' && !range) {
                // ranged requests are proxied: a redirect would drop the Range
                // header on the way to the store
                return void backend.presignGet(key, presignTtl, (err, url) => {
                    if (err || !url) {
                        Env.Log.error('HTTP_DATA_PRESIGN_ERROR', {
                            key, error: err && (err.message || err.code)
                        });
                        return void streamThrough();
                    }
                    // the store sets its own length on the real response
                    res.removeHeader('Content-Length');
                    res.redirect(302, url);
                });
            }

            streamThrough();

            function streamThrough() {
                const streamOpts = {};
                if (range) {
                    streamOpts.start = range.start;
                    streamOpts.end = range.end;
                    res.status(206);
                    res.setHeader('Content-Range',
                        `bytes ${range.start}-${range.end}/${stat.size}`);
                    res.setHeader('Content-Length', range.end - range.start + 1);
                }

                backend.getStream(key, streamOpts, (err, stream) => {
                    if (err) {
                        if (err.code === 'ENOENT') { return void next(); }
                        Env.Log.error('HTTP_DATA_STREAM_ERROR', {
                            key, error: err.message || err.code
                        });
                        return void res.status(500).end();
                    }
                    stream.on('error', err => {
                        Env.Log.error('HTTP_DATA_STREAM_ABORTED', {
                            key, error: err && err.message
                        });
                        res.destroy();
                    });
                    // stop paying for a transfer the client has abandoned
                    res.on('close', () => {
                        if (typeof (stream.destroy) === 'function') { stream.destroy(); }
                    });
                    stream.pipe(res);
                });
            }
        });
    };
};

/*  Serve blobs: immutable once uploaded, so they can be cached hard. */
HttpData.blobs = (Env, opts) => {
    opts = opts || {};
    const mode = resolveMode(Env, opts.mode);
    if (mode === 'static') {
        return Express.static(Path.resolve(Env.paths.blob), {
            maxAge: Env.DEV_MODE ? "0d" : "365d"
        });
    }
    return HttpData.serve(Env, {
        prefix: 'blob/',
        mode,
        presignTtl: opts.presignTtl,
        cacheControl: Env.DEV_MODE ? 'no-cache' : 'max-age=31536000'
    });
};

/*  Serve login blocks. Always proxied: the access-control middleware in front of
    this decides who may read one, and a presigned URL would outlive that check. */
HttpData.blocks = (Env, opts) => {
    opts = opts || {};
    const mode = resolveMode(Env, 'proxy');
    if (mode === 'static' || !Env.storageBackend) {
        return Express.static(Path.resolve(Env.paths.block), opts.staticOptions || {});
    }
    return HttpData.serve(Env, { prefix: 'block/', mode: 'proxy' });
};

/*  Channels are never served as bodies - only HEAD, to check existence and size.
    That is answered from object metadata, so a pad is never transferred here. */
HttpData.channels = (Env) => {
    const mode = resolveMode(Env, 'proxy');
    const guard = (req, res, next) => {
        if (req.method !== 'HEAD') { return void res.status(403).end(); }
        next();
    };

    if (mode === 'static' || !Env.storageBackend) {
        return [guard, Express.static(Env.paths.channel, { maxAge: "0d" })];
    }
    return [guard, HttpData.serve(Env, {
        prefix: 'channel/',
        mode: 'proxy',
        cacheControl: 'no-cache'
    })];
};
