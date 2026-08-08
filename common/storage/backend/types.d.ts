// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors
//
// SPDX-License-Identifier: AGPL-3.0-or-later

/*  The ObjectBackend interface.

    This is the only seam that knows where bytes actually live. Everything above it
    (the Basic key/value store, the blob store, the cached append-log layer) is written
    against this interface and works unchanged on a local filesystem or an S3-compatible
    object store.

    Keys are '/'-separated, relative to the backend's root (a directory for 'fs', a
    bucket+prefix for 's3'). They never start with a '/' and never contain '..'.

    All methods are callback style, matching the rest of the codebase. Callbacks receive
    (err, result). Errors for a missing key MUST have `code === 'ENOENT'`, because
    callers throughout the tree special-case that value.
*/

export type ObjectStat = {
    /** size in bytes */
    size: number;
    /** entity tag; opaque, used for conditional writes */
    etag: string;
    /** last modification time, ms since epoch */
    mtime: number;
};

export type ListEntry = {
    key: string;
    size: number;
    etag: string;
    mtime: number;
};

export type ListResult = {
    /** objects directly under the prefix (or below it, when no delimiter is given) */
    keys: ListEntry[];
    /** "directories": common prefixes, only populated when a delimiter is given */
    prefixes: string[];
    /** opaque continuation token; undefined when the listing is complete */
    cursor?: string;
};

export type PutOptions = {
    /** fail with EEXIST if the key already exists (maps to If-None-Match: *) */
    ifNoneMatch?: boolean;
    /** fail with EPRECONDITION unless the current etag matches (maps to If-Match) */
    ifMatch?: string;
    contentType?: string;
    /** cache-control header to store with the object; ignored by the fs backend */
    cacheControl?: string;
};

export type AppendOptions = {
    /** the size the object is expected to have before the append; guards against
     *  appending to an object that somebody else has grown underneath us */
    expectedSize?: number;
    /** the etag the object is expected to have before the append */
    expectedEtag?: string;
};

export type Capabilities = {
    /** honours If-None-Match / If-Match on PUT */
    conditionalPut: boolean;
    /** can append without downloading and re-uploading the whole object */
    serverSideAppend: boolean;
    /** can mint presigned GET URLs */
    presign: boolean;
    /** move is atomic */
    atomicMove: boolean;
};

export interface ObjectBackend {
    /** human-readable backend name, e.g. 'fs' or 's3' */
    readonly name: string;
    /** what this backend can actually do; probed at startup for remote backends */
    readonly capabilities: Capabilities;

    // --- reads ---------------------------------------------------------
    head(key: string, cb: (err?: Error, stat?: ObjectStat) => void): void;
    get(key: string, cb: (err?: Error, body?: Buffer) => void): void;
    getStream(
        key: string,
        opts: { start?: number; end?: number },
        cb: (err?: Error, stream?: NodeJS.ReadableStream) => void
    ): void;
    list(
        prefix: string,
        opts: { delimiter?: string; cursor?: string; limit?: number },
        cb: (err?: Error, result?: ListResult) => void
    ): void;
    exists(key: string, cb: (err?: Error, exists?: boolean) => void): void;

    // --- writes --------------------------------------------------------
    put(
        key: string,
        body: Buffer | string | NodeJS.ReadableStream,
        opts: PutOptions,
        cb: (err?: Error, stat?: ObjectStat) => void
    ): void;
    append(
        key: string,
        tail: Buffer,
        opts: AppendOptions,
        cb: (err?: Error, stat?: ObjectStat) => void
    ): void;
    copy(srcKey: string, dstKey: string, opts: { overwrite?: boolean }, cb: (err?: Error) => void): void;
    move(srcKey: string, dstKey: string, opts: { overwrite?: boolean }, cb: (err?: Error) => void): void;
    remove(key: string, cb: (err?: Error) => void): void;
    removePrefix(prefix: string, cb: (err?: Error) => void): void;

    // --- optional ------------------------------------------------------
    /** only present when capabilities.presign is true */
    presignGet?(key: string, ttlSeconds: number, cb: (err?: Error, url?: string) => void): void;

    /** release any resources held by the backend */
    close?(cb: (err?: Error) => void): void;
}
