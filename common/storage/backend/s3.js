// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors
//
// SPDX-License-Identifier: AGPL-3.0-or-later

/*  S3-compatible object storage backend (see ./types.d.ts).

    Targets the S3 API rather than AWS specifically: endpoint, region and path-style
    addressing are all configuration, so this works against Scaleway, MinIO, Ceph,
    Garage, AWS and friends.

    Providers differ at three edges that matter to us — conditional writes, server-side
    multipart copy, and presigned URLs — so rather than carrying a per-provider
    compatibility matrix that goes stale, `probe()` measures the real bucket at startup
    and every capability has a working fallback.

    The AWS SDK is required lazily, so an instance that does not use S3 never loads it
    and does not need it installed.
*/

const { isValidKey, mkError } = require("./key.js");

// S3 requires every part but the last to be at least 5 MiB
const MIN_PART_SIZE = 5 * 1024 * 1024;
const DEFAULT_PART_SIZE = 8 * 1024 * 1024;

const loadSdk = () => {
    try {
        return {
            client: require('@aws-sdk/client-s3'),
            presigner: (() => {
                try { return require('@aws-sdk/s3-request-presigner'); }
                catch (err) { return null; }   // presigning is optional
            })()
        };
    } catch (err) {
        throw mkError('E_MISSING_S3_SDK',
            "The S3 storage backend requires '@aws-sdk/client-s3'. " +
            "Install it with: npm install @aws-sdk/client-s3 @aws-sdk/s3-request-presigner");
    }
};

/*  Translate an SDK error into the vocabulary the rest of the tree speaks.
    `condition` tells us how to read a 412: a failed exclusive create is EEXIST,
    a failed compare-and-swap is EPRECONDITION. */
const translate = (err, condition) => {
    if (!err) { return err; }
    const status = err.$metadata && err.$metadata.httpStatusCode;
    const name = err.name || err.Code;

    if (status === 404 || name === 'NotFound' || name === 'NoSuchKey') {
        return mkError('ENOENT', 'NO_SUCH_KEY');
    }
    if (status === 412 || name === 'PreconditionFailed') {
        return condition === 'ifNoneMatch' ?
            mkError('EEXIST', 'KEY_EXISTS') :
            mkError('EPRECONDITION', 'ETAG_MISMATCH');
    }
    // 409 is what some providers return for a failed exclusive create
    if (status === 409 && condition === 'ifNoneMatch') {
        return mkError('EEXIST', 'KEY_EXISTS');
    }
    if (status === 403 || name === 'AccessDenied') {
        return mkError('EACCES', `ACCESS_DENIED: ${err.message}`);
    }
    if (!err.code) { err.code = name || 'E_S3'; }
    return err;
};

const collect = stream => {
    return new Promise((resolve, reject) => {
        const chunks = [];
        stream.on('data', chunk => chunks.push(chunk));
        stream.on('error', reject);
        stream.on('end', () => resolve(Buffer.concat(chunks)));
    });
};

module.exports.create = (conf, _cb) => {
    const cb = typeof (_cb) === 'function' ? _cb : () => {};

    let sdk;
    try {
        sdk = loadSdk();
    } catch (err) {
        return void cb(err);
    }

    const {
        S3Client, HeadObjectCommand, GetObjectCommand, PutObjectCommand,
        DeleteObjectCommand, DeleteObjectsCommand, ListObjectsV2Command,
        CopyObjectCommand, CreateMultipartUploadCommand, UploadPartCommand,
        UploadPartCopyCommand, CompleteMultipartUploadCommand,
        AbortMultipartUploadCommand
    } = sdk.client;

    if (!conf.bucket) {
        return void cb(mkError('EINVAL', 'config.storage.s3.bucket is required'));
    }

    const bucket = conf.bucket;
    // a prefix lets several instances share one bucket; normalise it to end with '/'
    const prefix = conf.prefix ? conf.prefix.replace(/\/*$/, '/') : '';
    const partSize = Math.max(MIN_PART_SIZE,
        (conf.upload && conf.upload.partSizeMB ? conf.upload.partSizeMB : 0) * 1024 * 1024 ||
        DEFAULT_PART_SIZE);
    const appendThreshold = Math.max(MIN_PART_SIZE,
        (conf.upload && conf.upload.appendThresholdMB ? conf.upload.appendThresholdMB : 0) *
        1024 * 1024 || MIN_PART_SIZE);

    const clientConfig = {
        region: conf.region || 'us-east-1',
        forcePathStyle: Boolean(conf.forcePathStyle)
    };
    if (conf.endpoint) { clientConfig.endpoint = conf.endpoint; }
    if (conf.credentials && conf.credentials.accessKeyId) {
        clientConfig.credentials = {
            accessKeyId: conf.credentials.accessKeyId,
            secretAccessKey: conf.credentials.secretAccessKey
        };
    }
    if (typeof (conf.requestTimeoutMs) === 'number') {
        clientConfig.requestHandler = { requestTimeout: conf.requestTimeoutMs };
    }
    if (typeof (conf.maxRetries) === 'number') {
        clientConfig.maxAttempts = conf.maxRetries + 1;
    }

    const client = new S3Client(clientConfig);

    const toKey = key => prefix + key;
    const fromKey = key => (prefix && key.startsWith(prefix) ? key.slice(prefix.length) : key);

    const send = (command) => client.send(command);

    // bridge a promise onto the callback style used throughout the tree
    const run = (promise, cb, condition, map) => {
        promise.then(res => {
            cb(void 0, typeof (map) === 'function' ? map(res) : res);
        }, err => {
            cb(translate(err, condition));
        });
    };

    const mkStat = res => {
        return {
            size: typeof (res.ContentLength) === 'number' ? res.ContentLength : res.Size,
            etag: res.ETag,
            mtime: res.LastModified ? +new Date(res.LastModified) : Date.now()
        };
    };

    // --- reads -------------------------------------------------------------

    const head = (key, cb) => {
        if (!isValidKey(key)) { return void cb(mkError('EINVAL', 'INVALID_KEY')); }
        run(send(new HeadObjectCommand({ Bucket: bucket, Key: toKey(key) })),
            cb, null, mkStat);
    };

    const exists = (key, cb) => {
        head(key, (err, stat) => {
            if (err) {
                if (err.code === 'ENOENT') { return void cb(void 0, false); }
                return void cb(err);
            }
            cb(void 0, Boolean(stat));
        });
    };

    const get = (key, cb) => {
        if (!isValidKey(key)) { return void cb(mkError('EINVAL', 'INVALID_KEY')); }
        send(new GetObjectCommand({ Bucket: bucket, Key: toKey(key) })).then(res => {
            return collect(res.Body);
        }).then(body => {
            cb(void 0, body);
        }, err => {
            cb(translate(err));
        });
    };

    const getStream = (key, opts, cb) => {
        if (!isValidKey(key)) { return void cb(mkError('EINVAL', 'INVALID_KEY')); }
        opts = opts || {};

        const params = { Bucket: bucket, Key: toKey(key) };
        if (typeof (opts.start) === 'number') {
            // HTTP ranges are inclusive at both ends, like fs.createReadStream
            params.Range = `bytes=${opts.start}-${typeof (opts.end) === 'number' ? opts.end : ''}`;
        } else if (typeof (opts.end) === 'number') {
            params.Range = `bytes=0-${opts.end}`;
        }

        // resolve errors on the callback rather than on the stream, so callers
        // see ENOENT the same way they do from the filesystem backend
        run(send(new GetObjectCommand(params)), cb, null, res => res.Body);
    };

    const list = (prefixArg, opts, cb) => {
        prefixArg = prefixArg || '';
        opts = opts || {};
        if (typeof (prefixArg) !== 'string' || prefixArg.includes('..')) {
            return void cb(mkError('EINVAL', 'INVALID_PREFIX'));
        }

        const params = {
            Bucket: bucket,
            Prefix: toKey(prefixArg)
        };
        if (opts.delimiter) { params.Delimiter = opts.delimiter; }
        if (opts.cursor) { params.StartAfter = toKey(opts.cursor); }
        if (opts.limit) { params.MaxKeys = opts.limit; }

        run(send(new ListObjectsV2Command(params)), cb, null, res => {
            const keys = (res.Contents || []).map(entry => {
                return {
                    key: fromKey(entry.Key),
                    size: entry.Size,
                    etag: entry.ETag,
                    mtime: entry.LastModified ? +new Date(entry.LastModified) : 0
                };
            });
            const prefixes = (res.CommonPrefixes || []).map(p => fromKey(p.Prefix));

            /*  We paginate with StartAfter rather than ContinuationToken so that a
                cursor stays meaningful across calls and matches the filesystem
                backend, whose cursor is simply the last key returned.  */
            let cursor;
            if (res.IsTruncated && keys.length) {
                cursor = keys[keys.length - 1].key;
            }
            return { keys, prefixes, cursor };
        });
    };

    // --- writes ------------------------------------------------------------

    /*  Upload an arbitrary-length stream as a multipart upload. Used for bodies
        whose length we do not know up front (PutObject requires ContentLength). */
    const multipartFromStream = (key, body, opts, cb) => {
        let uploadId;
        const parts = [];

        const abort = (err) => {
            if (!uploadId) { return void cb(err); }
            send(new AbortMultipartUploadCommand({
                Bucket: bucket, Key: toKey(key), UploadId: uploadId
            })).then(() => cb(err), () => cb(err));
        };

        send(new CreateMultipartUploadCommand({
            Bucket: bucket, Key: toKey(key), ContentType: opts.contentType
        })).then(res => {
            uploadId = res.UploadId;

            return new Promise((resolve, reject) => {
                let buffered = [];
                let bufferedLength = 0;
                let chain = Promise.resolve();
                let partNumber = 0;

                const flushPart = (isFinal) => {
                    if (!bufferedLength && !isFinal) { return; }
                    if (!bufferedLength && isFinal && parts.length) { return; }
                    const chunk = Buffer.concat(buffered, bufferedLength);
                    buffered = [];
                    bufferedLength = 0;
                    const number = ++partNumber;
                    chain = chain.then(() => {
                        return send(new UploadPartCommand({
                            Bucket: bucket, Key: toKey(key), UploadId: uploadId,
                            PartNumber: number, Body: chunk
                        })).then(r => { parts.push({ PartNumber: number, ETag: r.ETag }); });
                    });
                };

                body.on('data', chunk => {
                    buffered.push(chunk);
                    bufferedLength += chunk.length;
                    if (bufferedLength >= partSize) { flushPart(false); }
                });
                body.on('error', reject);
                body.on('end', () => {
                    flushPart(true);
                    chain.then(resolve, reject);
                });
            });
        }).then(() => {
            parts.sort((a, b) => a.PartNumber - b.PartNumber);
            return send(new CompleteMultipartUploadCommand({
                Bucket: bucket, Key: toKey(key), UploadId: uploadId,
                MultipartUpload: { Parts: parts }
            }));
        }).then(() => {
            head(key, cb);
        }, err => {
            abort(translate(err));
        });
    };

    const put = (key, body, opts, cb) => {
        if (!isValidKey(key)) { return void cb(mkError('EINVAL', 'INVALID_KEY')); }
        opts = opts || {};

        const isBuffered = typeof (body) === 'string' || Buffer.isBuffer(body);
        if (!isBuffered) {
            if (!body || typeof (body.pipe) !== 'function') {
                return void cb(mkError('EINVAL', 'INVALID_BODY'));
            }
            // conditional headers are not supported on the multipart path;
            // callers that need them (the cache layer) always pass a buffer
            if (opts.ifNoneMatch || opts.ifMatch) {
                return void cb(mkError('EINVAL', 'CONDITIONAL_STREAM_PUT_UNSUPPORTED'));
            }
            return void multipartFromStream(key, body, opts, cb);
        }

        const params = {
            Bucket: bucket,
            Key: toKey(key),
            Body: Buffer.isBuffer(body) ? body : Buffer.from(body, 'utf8')
        };
        if (opts.contentType) { params.ContentType = opts.contentType; }
        if (opts.cacheControl) { params.CacheControl = opts.cacheControl; }

        let condition = null;
        if (opts.ifNoneMatch) {
            params.IfNoneMatch = '*';
            condition = 'ifNoneMatch';
        } else if (opts.ifMatch) {
            params.IfMatch = opts.ifMatch;
            condition = 'ifMatch';
        }

        /*  If the provider ignores conditional headers we must not silently perform
            an unconditional write: check first, which narrows the race without
            closing it, and warn loudly at startup (see probe()).  */
        if (condition && !backend.capabilities.conditionalPut) {
            return void head(key, (err, stat) => {
                if (err && err.code !== 'ENOENT') { return void cb(err); }
                if (condition === 'ifNoneMatch' && stat) {
                    return void cb(mkError('EEXIST', 'KEY_EXISTS'));
                }
                if (condition === 'ifMatch' && (!stat || stat.etag !== opts.ifMatch)) {
                    return void cb(mkError('EPRECONDITION', 'ETAG_MISMATCH'));
                }
                delete params.IfNoneMatch;
                delete params.IfMatch;
                run(send(new PutObjectCommand(params)), cb, condition, res => {
                    return { size: params.Body.length, etag: res.ETag, mtime: Date.now() };
                });
            });
        }

        run(send(new PutObjectCommand(params)), cb, condition, res => {
            return { size: params.Body.length, etag: res.ETag, mtime: Date.now() };
        });
    };

    /*  Append without downloading the object, where the provider allows it.

        A multipart upload can take its first part as a server-side copy of the
        existing object and its second as the new tail, then atomically replace the
        object on completion. The copied part must be at least 5 MiB, which is
        exactly the size at which re-uploading the whole object starts to hurt, so
        below that threshold the simple read-modify-write path is used instead.  */
    const append = (key, tail, opts, cb) => {
        if (!isValidKey(key)) { return void cb(mkError('EINVAL', 'INVALID_KEY')); }
        if (!Buffer.isBuffer(tail)) { return void cb(mkError('EINVAL', 'INVALID_TAIL')); }
        opts = opts || {};

        head(key, (err, stat) => {
            if (err && err.code !== 'ENOENT') { return void cb(err); }

            const current = stat || null;

            if (typeof (opts.expectedSize) === 'number') {
                const size = current ? current.size : 0;
                if (size !== opts.expectedSize) {
                    return void cb(mkError('EPRECONDITION', 'SIZE_MISMATCH'));
                }
            }
            if (opts.expectedEtag && (!current || current.etag !== opts.expectedEtag)) {
                return void cb(mkError('EPRECONDITION', 'ETAG_MISMATCH'));
            }

            // nothing there yet: an append is just a write
            if (!current) { return void put(key, tail, {}, cb); }

            const useServerSide = backend.capabilities.serverSideAppend &&
                current.size >= appendThreshold;

            if (!useServerSide) {
                // read-modify-write: correct everywhere, O(size) in bandwidth
                return void get(key, (err, body) => {
                    if (err) { return void cb(err); }
                    put(key, Buffer.concat([body, tail]), {}, cb);
                });
            }

            let uploadId;
            const finish = (err, result) => {
                if (!err) { return void cb(void 0, result); }
                if (!uploadId) { return void cb(err); }
                // never leave a dangling multipart upload: they are billed
                send(new AbortMultipartUploadCommand({
                    Bucket: bucket, Key: toKey(key), UploadId: uploadId
                })).then(() => cb(err), () => cb(err));
            };

            send(new CreateMultipartUploadCommand({
                Bucket: bucket, Key: toKey(key)
            })).then(res => {
                uploadId = res.UploadId;
                return send(new UploadPartCopyCommand({
                    Bucket: bucket, Key: toKey(key), UploadId: uploadId,
                    PartNumber: 1,
                    CopySource: `${bucket}/${toKey(key)}`,
                    CopySourceRange: `bytes=0-${current.size - 1}`
                }));
            }).then(res => {
                const first = { PartNumber: 1, ETag: res.CopyPartResult.ETag };
                return send(new UploadPartCommand({
                    Bucket: bucket, Key: toKey(key), UploadId: uploadId,
                    PartNumber: 2, Body: tail
                })).then(r => [first, { PartNumber: 2, ETag: r.ETag }]);
            }).then(parts => {
                return send(new CompleteMultipartUploadCommand({
                    Bucket: bucket, Key: toKey(key), UploadId: uploadId,
                    MultipartUpload: { Parts: parts }
                }));
            }).then(() => {
                head(key, finish);
            }, err => {
                finish(translate(err));
            });
        });
    };

    const copy = (srcKey, dstKey, opts, cb) => {
        if (!isValidKey(srcKey) || !isValidKey(dstKey)) {
            return void cb(mkError('EINVAL', 'INVALID_KEY'));
        }
        opts = opts || {};

        const run_ = () => {
            send(new CopyObjectCommand({
                Bucket: bucket,
                Key: toKey(dstKey),
                CopySource: encodeURI(`${bucket}/${toKey(srcKey)}`)
            })).then(() => cb(), err => cb(translate(err)));
        };

        if (opts.overwrite) { return void run_(); }
        exists(dstKey, (err, present) => {
            if (err) { return void cb(err); }
            if (present) { return void cb(mkError('EEXIST', 'KEY_EXISTS')); }
            run_();
        });
    };

    const remove = (key, cb) => {
        if (!isValidKey(key)) { return void cb(mkError('EINVAL', 'INVALID_KEY')); }
        /*  DeleteObject succeeds on a missing key, but the filesystem backend and
            every caller in the tree expect ENOENT, so check first. */
        head(key, (err) => {
            if (err) { return void cb(err); }
            send(new DeleteObjectCommand({ Bucket: bucket, Key: toKey(key) }))
                .then(() => cb(), e => cb(translate(e)));
        });
    };

    const move = (srcKey, dstKey, opts, cb) => {
        copy(srcKey, dstKey, opts, err => {
            if (err) { return void cb(err); }
            remove(srcKey, err => {
                // the copy succeeded; a failure to delete the source leaves a
                // duplicate rather than losing data, so report it and move on
                cb(err && err.code === 'ENOENT' ? void 0 : err);
            });
        });
    };

    const removePrefix = (prefixArg, cb) => {
        if (typeof (prefixArg) !== 'string' || !prefixArg.length) {
            return void cb(mkError('EINVAL', 'EMPTY_PREFIX'));
        }
        if (prefixArg.includes('..')) {
            return void cb(mkError('EINVAL', 'INVALID_PREFIX'));
        }

        const deleteBatch = (cursor) => {
            list(prefixArg, { cursor, limit: 1000 }, (err, result) => {
                if (err) { return void cb(err); }
                if (!result.keys.length) { return void cb(); }

                send(new DeleteObjectsCommand({
                    Bucket: bucket,
                    Delete: {
                        Objects: result.keys.map(entry => ({ Key: toKey(entry.key) })),
                        Quiet: true
                    }
                })).then(() => {
                    if (!result.cursor) { return void cb(); }
                    deleteBatch(result.cursor);
                }, e => cb(translate(e)));
            });
        };
        deleteBatch();
    };

    const presignGet = (key, ttlSeconds, cb) => {
        if (!sdk.presigner) {
            return void cb(mkError('ENOTSUP', 'PRESIGNING_UNAVAILABLE'));
        }
        if (!isValidKey(key)) { return void cb(mkError('EINVAL', 'INVALID_KEY')); }
        sdk.presigner.getSignedUrl(client,
            new GetObjectCommand({ Bucket: bucket, Key: toKey(key) }),
            { expiresIn: ttlSeconds || 300 }
        ).then(url => cb(void 0, url), err => cb(translate(err)));
    };

    const backend = {
        name: 's3',
        bucket,
        prefix,
        // conservative defaults; probe() measures the real bucket and updates them
        capabilities: {
            conditionalPut: false,
            serverSideAppend: false,
            presign: Boolean(sdk.presigner),
            atomicMove: false
        },

        head, exists, get, getStream, list,
        put, append, copy, move, remove, removePrefix,
        presignGet,

        close: cb => {
            try { client.destroy(); } catch (err) { /* already closed */ }
            if (typeof (cb) === 'function') { cb(); }
        }
    };

    /*  Measure what this bucket actually supports, rather than assuming AWS
        semantics. Each capability has a fallback, so a provider that lacks one
        costs bandwidth or narrows a safety window instead of breaking. */
    const probe = (cb) => {
        const stamp = `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
        const base = `_probe/${stamp}`;
        const results = { conditionalPut: false, serverSideAppend: false, presign: false };
        const notes = [];

        const probeConditional = (next) => {
            const key = `${base}/conditional`;
            const params = {
                Bucket: bucket, Key: toKey(key),
                Body: Buffer.from('one'), IfNoneMatch: '*'
            };
            send(new PutObjectCommand(params)).then(() => {
                // the second exclusive create must be rejected
                return send(new PutObjectCommand({
                    Bucket: bucket, Key: toKey(key),
                    Body: Buffer.from('two'), IfNoneMatch: '*'
                })).then(() => {
                    notes.push('conditional writes are accepted but not enforced');
                    results.conditionalPut = false;
                }, err => {
                    const status = err.$metadata && err.$metadata.httpStatusCode;
                    results.conditionalPut = (status === 412 || status === 409);
                    if (!results.conditionalPut) {
                        notes.push(`unexpected status ${status} for a conditional write`);
                    }
                });
            }, err => {
                notes.push(`conditional write rejected outright: ${err.name}`);
            }).then(() => next(), () => next());
        };

        const probeAppend = (next) => {
            const key = `${base}/append`;
            // the copied part must be >= 5 MiB for CompleteMultipartUpload to accept it
            const body = Buffer.alloc(MIN_PART_SIZE + 1024, 'a');
            send(new PutObjectCommand({
                Bucket: bucket, Key: toKey(key), Body: body
            })).then(() => {
                return new Promise(resolve => {
                    append(key, Buffer.from('tail'), {}, err => resolve(err));
                });
            }).then(err => {
                if (err) {
                    notes.push(`server-side append unavailable: ${err.message}`);
                    return;
                }
                results.serverSideAppend = true;
            }, err => {
                notes.push(`server-side append probe failed: ${err.name}`);
            }).then(() => next(), () => next());
        };

        const probePresign = (next) => {
            if (!sdk.presigner) { return void next(); }
            const key = `${base}/presign`;
            send(new PutObjectCommand({
                Bucket: bucket, Key: toKey(key), Body: Buffer.from('presign')
            })).then(() => {
                return new Promise((resolve, reject) => {
                    presignGet(key, 60, (err, url) => err ? reject(err) : resolve(url));
                });
            }).then(url => {
                return fetch(url).then(res => {
                    results.presign = res.ok;
                    if (!res.ok) { notes.push(`presigned GET returned ${res.status}`); }
                });
            }, err => {
                notes.push(`presign probe failed: ${err.message}`);
            }).then(() => next(), () => next());
        };

        // temporarily enable the capability under test so its code path runs
        backend.capabilities.serverSideAppend = true;

        probeConditional(() => {
            probeAppend(() => {
                probePresign(() => {
                    backend.capabilities.conditionalPut = results.conditionalPut;
                    backend.capabilities.serverSideAppend = results.serverSideAppend;
                    backend.capabilities.presign = results.presign;
                    // a copy+delete pair is not atomic on any object store
                    backend.capabilities.atomicMove = false;

                    removePrefix(`${base}/`, () => {
                        cb(void 0, { capabilities: backend.capabilities, notes });
                    });
                });
            });
        });
    };
    backend.probe = probe;

    if (conf.capabilities) {
        // explicit overrides skip the corresponding probe step
        Object.keys(conf.capabilities).forEach(name => {
            if (typeof (conf.capabilities[name]) === 'boolean') {
                backend.capabilities[name] = conf.capabilities[name];
            }
        });
    }

    if (conf.skipProbe) { return void cb(void 0, backend); }

    probe((err, result) => {
        if (err) { return void cb(err); }
        const Log = conf.Log;
        if (Log && typeof (Log.info) === 'function') {
            Log.info('S3_CAPABILITIES', {
                bucket, endpoint: conf.endpoint, region: clientConfig.region,
                capabilities: result.capabilities
            });
            result.notes.forEach(note => {
                Log.warn && Log.warn('S3_CAPABILITY_DEGRADED', note);
            });
        }
        if (!result.capabilities.conditionalPut && Log && Log.warn) {
            Log.warn('S3_NO_CONDITIONAL_WRITES',
                'This provider does not enforce conditional writes. Ownership conflicts ' +
                'will be detected on a best-effort basis only.');
        }
        cb(void 0, backend);
    });
};
