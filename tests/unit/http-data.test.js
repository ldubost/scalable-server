// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors
//
// SPDX-License-Identifier: AGPL-3.0-or-later

/*  Serving user data over HTTP from the storage backend.

    Exercised through a real HTTP server against a real backend, because the
    things worth checking here — status codes, Range handling, that a HEAD never
    transfers a body — are properties of the wire, not of the function.
*/

const { test } = require('node:test');
const assert = require('node:assert');
const Fs = require('node:fs/promises');
const Os = require('node:os');
const Path = require('node:path');
const Http = require('node:http');
const Express = require('express');

const FsBackend = require('../../common/storage/backend/fs.js');
const HttpData = require('../../storage/http-data.js');
const { p } = require('./backend-conformance.js');

const quietLog = {
    info: () => {}, warn: () => {}, error: () => {}, verbose: () => {}, silly: () => {}
};

const withServer = (build, body) => {
    return async () => {
        const root = await Fs.mkdtemp(Path.join(Os.tmpdir(), 'cryptpad-http-'));
        const backend = await p(FsBackend.create, { root });
        const Env = {
            Log: quietLog,
            storageBackend: backend,
            paths: { blob: Path.join(root, 'blob'), block: Path.join(root, 'block') },
            DEV_MODE: false
        };

        const app = Express();
        build(Env, app, backend);
        const server = Http.createServer(app);
        await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
        const port = server.address().port;

        const request = (path, opts) => {
            opts = opts || {};
            return new Promise((resolve, reject) => {
                const req = Http.request({
                    host: '127.0.0.1', port, path,
                    method: opts.method || 'GET',
                    headers: opts.headers || {}
                }, res => {
                    const chunks = [];
                    res.on('data', c => chunks.push(c));
                    res.on('end', () => resolve({
                        status: res.statusCode,
                        headers: res.headers,
                        body: Buffer.concat(chunks)
                    }));
                });
                req.on('error', reject);
                req.end();
            });
        };

        try {
            await body({ Env, backend, request, root });
        } finally {
            await new Promise(resolve => server.close(resolve));
            await Fs.rm(root, { recursive: true, force: true });
        }
    };
};

// --- proxy mode -------------------------------------------------------------

const blobServer = (Env, app) => {
    app.use('/blob', HttpData.serve(Env, { prefix: 'blob/', mode: 'proxy' }));
    app.use((req, res) => { res.status(404).end(); });
};

test('http: serves an object', withServer(blobServer, async ctx => {
    await p(ctx.backend.put, 'blob/ab/abcdef', 'file contents', {});

    const res = await ctx.request('/blob/ab/abcdef');
    assert.strictEqual(res.status, 200);
    assert.strictEqual(res.body.toString('utf8'), 'file contents');
    assert.strictEqual(res.headers['content-length'], '13');
    assert.strictEqual(res.headers['accept-ranges'], 'bytes');
}));

test('http: a missing object falls through to the next handler',
    withServer(blobServer, async ctx => {
        // this is what lets a placeholder explain an archived pad
        const res = await ctx.request('/blob/ab/nothing');
        assert.strictEqual(res.status, 404);
    }));

test('http: HEAD reports the size without a body', withServer(blobServer, async ctx => {
    await p(ctx.backend.put, 'blob/ab/abcdef', Buffer.alloc(4096, 'x'), {});

    const res = await ctx.request('/blob/ab/abcdef', { method: 'HEAD' });
    assert.strictEqual(res.status, 200);
    assert.strictEqual(res.headers['content-length'], '4096');
    assert.strictEqual(res.body.length, 0, 'a HEAD must not transfer the object');
}));

test('http: serves a byte range', withServer(blobServer, async ctx => {
    await p(ctx.backend.put, 'blob/ab/abcdef', '0123456789', {});

    const res = await ctx.request('/blob/ab/abcdef', { headers: { Range: 'bytes=2-4' } });
    assert.strictEqual(res.status, 206);
    assert.strictEqual(res.body.toString('utf8'), '234');
    assert.strictEqual(res.headers['content-range'], 'bytes 2-4/10');
    assert.strictEqual(res.headers['content-length'], '3');
}));

test('http: serves an open-ended range', withServer(blobServer, async ctx => {
    await p(ctx.backend.put, 'blob/ab/abcdef', '0123456789', {});

    const res = await ctx.request('/blob/ab/abcdef', { headers: { Range: 'bytes=7-' } });
    assert.strictEqual(res.status, 206);
    assert.strictEqual(res.body.toString('utf8'), '789');
    assert.strictEqual(res.headers['content-range'], 'bytes 7-9/10');
}));

test('http: serves a suffix range', withServer(blobServer, async ctx => {
    await p(ctx.backend.put, 'blob/ab/abcdef', '0123456789', {});

    const res = await ctx.request('/blob/ab/abcdef', { headers: { Range: 'bytes=-3' } });
    assert.strictEqual(res.status, 206);
    assert.strictEqual(res.body.toString('utf8'), '789');
}));

test('http: rejects an unsatisfiable range', withServer(blobServer, async ctx => {
    await p(ctx.backend.put, 'blob/ab/abcdef', '0123456789', {});

    const res = await ctx.request('/blob/ab/abcdef', { headers: { Range: 'bytes=50-60' } });
    assert.strictEqual(res.status, 416);
    assert.strictEqual(res.headers['content-range'], 'bytes */10');
}));

test('http: ignores a malformed range rather than failing',
    withServer(blobServer, async ctx => {
        await p(ctx.backend.put, 'blob/ab/abcdef', '0123456789', {});

        const res = await ctx.request('/blob/ab/abcdef', {
            headers: { Range: 'furlongs=1-2' }
        });
        assert.strictEqual(res.status, 200);
        assert.strictEqual(res.body.toString('utf8'), '0123456789');
    }));

test('http: refuses to escape its prefix', withServer((Env, app) => {
    app.use('/blob', HttpData.serve(Env, { prefix: 'blob/', mode: 'proxy' }));
    app.use((req, res) => { res.status(404).end(); });
}, async ctx => {
    await p(ctx.backend.put, 'block/secret', 'not yours', {});

    for (const path of ['/blob/../block/secret', '/blob/%2e%2e/block/secret']) {
        const res = await ctx.request(path);
        assert.notStrictEqual(res.body.toString('utf8'), 'not yours',
            `${path} must not reach another prefix`);
    }
}));

// --- key mapping and range parsing (unit level) -----------------------------

test('http: keyFromUrl maps request paths onto keys', () => {
    assert.strictEqual(HttpData.keyFromUrl('blob/', '/ab/abcdef'), 'blob/ab/abcdef');
    assert.strictEqual(HttpData.keyFromUrl('blob/', '/ab/abcdef?ver=3'), 'blob/ab/abcdef');
    assert.strictEqual(HttpData.keyFromUrl('blob/', '/'), undefined);
    assert.strictEqual(HttpData.keyFromUrl('blob/', '/../escape'), undefined);
    assert.strictEqual(HttpData.keyFromUrl('blob/', '/a/../../escape'), undefined);
});

test('http: parseRange follows RFC semantics', () => {
    assert.deepStrictEqual(HttpData.parseRange('bytes=0-4', 10), { start: 0, end: 4 });
    assert.deepStrictEqual(HttpData.parseRange('bytes=5-', 10), { start: 5, end: 9 });
    assert.deepStrictEqual(HttpData.parseRange('bytes=-3', 10), { start: 7, end: 9 });
    // an end past the object is clamped, not rejected
    assert.deepStrictEqual(HttpData.parseRange('bytes=8-99', 10), { start: 8, end: 9 });
    assert.deepStrictEqual(HttpData.parseRange('bytes=20-30', 10), { invalid: true });
    assert.strictEqual(HttpData.parseRange(undefined, 10), undefined);
    assert.strictEqual(HttpData.parseRange('nonsense', 10), undefined);
});

// --- mode selection ---------------------------------------------------------

test('http: mode falls back to proxy when the backend cannot presign', () => {
    const Env = { storageBackend: { capabilities: { presign: false } } };
    assert.strictEqual(HttpData.resolveMode(Env, 'redirect'), 'proxy',
        'failing to serve blobs is worse than serving them the slower way');
    assert.strictEqual(HttpData.resolveMode(Env, undefined), 'proxy');
});

test('http: redirect is used when the backend supports it', () => {
    const Env = { storageBackend: { capabilities: { presign: true } } };
    assert.strictEqual(HttpData.resolveMode(Env, 'redirect'), 'redirect');
    assert.strictEqual(HttpData.resolveMode(Env, undefined), 'redirect');
    // an explicit proxy request is always honoured
    assert.strictEqual(HttpData.resolveMode(Env, 'proxy'), 'proxy');
});

test('http: local storage keeps using static', () => {
    assert.strictEqual(HttpData.resolveMode({}, 'redirect'), 'static');
});

// --- redirect mode ----------------------------------------------------------

test('http: redirect mode sends a 302 to the presigned url', withServer((Env, app) => {
    Env.storageBackend.capabilities.presign = true;
    Env.storageBackend.presignGet = (key, ttl, cb) => {
        cb(void 0, `https://example.invalid/${key}?signed=1&ttl=${ttl}`);
    };
    app.use('/blob', HttpData.serve(Env, {
        prefix: 'blob/', mode: 'redirect', presignTtl: 60
    }));
    app.use((req, res) => { res.status(404).end(); });
}, async ctx => {
    await p(ctx.backend.put, 'blob/ab/abcdef', 'contents', {});

    const res = await ctx.request('/blob/ab/abcdef');
    assert.strictEqual(res.status, 302);
    assert.match(res.headers.location, /example\.invalid\/blob\/ab\/abcdef/);
    assert.match(res.headers.location, /ttl=60/);
}));

test('http: a ranged request is proxied even in redirect mode', withServer((Env, app) => {
    Env.storageBackend.capabilities.presign = true;
    Env.storageBackend.presignGet = (key, ttl, cb) => {
        cb(void 0, 'https://example.invalid/signed');
    };
    app.use('/blob', HttpData.serve(Env, { prefix: 'blob/', mode: 'redirect' }));
    app.use((req, res) => { res.status(404).end(); });
}, async ctx => {
    await p(ctx.backend.put, 'blob/ab/abcdef', '0123456789', {});

    // a redirect would lose the Range header on the way to the store
    const res = await ctx.request('/blob/ab/abcdef', { headers: { Range: 'bytes=2-4' } });
    assert.strictEqual(res.status, 206);
    assert.strictEqual(res.body.toString('utf8'), '234');
}));

test('http: a presigning failure degrades to proxying', withServer((Env, app) => {
    Env.storageBackend.capabilities.presign = true;
    Env.storageBackend.presignGet = (key, ttl, cb) => { cb(new Error('signing broke')); };
    app.use('/blob', HttpData.serve(Env, { prefix: 'blob/', mode: 'redirect' }));
    app.use((req, res) => { res.status(404).end(); });
}, async ctx => {
    await p(ctx.backend.put, 'blob/ab/abcdef', 'contents', {});

    const res = await ctx.request('/blob/ab/abcdef');
    assert.strictEqual(res.status, 200, 'the object must still be served');
    assert.strictEqual(res.body.toString('utf8'), 'contents');
}));

// --- channels ---------------------------------------------------------------

test('http: channels answer HEAD but refuse GET', withServer((Env, app) => {
    Env.paths.channel = Path.join(ctxRoot, 'channel');
    app.use('/datastore', HttpData.channels(Env));
    app.use((req, res) => { res.status(404).end(); });
}, async ctx => {
    await p(ctx.backend.put, 'channel/ab/abcdef.ndjson', 'line one\nline two\n', {});

    const head = await ctx.request('/datastore/ab/abcdef.ndjson', { method: 'HEAD' });
    assert.strictEqual(head.status, 200);
    assert.strictEqual(head.headers['content-length'], '18');
    assert.strictEqual(head.body.length, 0);

    // a pad's contents must never be downloadable this way
    const get = await ctx.request('/datastore/ab/abcdef.ndjson');
    assert.strictEqual(get.status, 403);
}));

// `channels` reads Env.paths.channel at build time; this keeps the helper simple
let ctxRoot = Os.tmpdir();
