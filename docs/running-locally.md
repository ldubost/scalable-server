<!-- SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors

SPDX-License-Identifier: AGPL-3.0-or-later
-->
# Running a local instance

A working CryptPad on `http://localhost:3000`, served by the scalable server with
the standard client from `../cryptpad`.

## Prerequisites

* Node 22+
* the CryptPad client checked out at `../cryptpad` (this is the default; override
  with `clientRoot` in `config/config.js`)

## Setup

```bash
npm install        # runtime dependencies
npm run build      # bundles each node type into build/
npm start          # starts the whole topology
```

On first start the server prints a one-time onboarding link:

```
=============================
Create your first admin account and customize your instance by visiting
http://localhost:3000/install/#<token>
=============================
```

Open it to create the first admin account. Then browse to
`http://localhost:3000`.

## Topology

Everything is configured in `config/infra.js`. The default is one node of each
type on one machine:

| Port | Node | Purpose |
|---|---|---|
| **3000** | http | the only port a browser needs — serves the client and proxies the rest |
| 3001 | http | sandbox origin: documents render in an iframe on a second origin |
| 3010 | front | websocket endpoint, proxied from `/cryptpad_websocket` |
| 3020 | core | internal message bus |
| 3030 | storage | serves `/blob`, `/block`, `/datastore` to the http node |
| 3040 | storage | internal websocket |

Only 3000 and 3001 need to be reachable by a browser. To add capacity, add
entries to the `front`, `core` or `storage` arrays in `config/infra.js` — the
http node load-balances across front nodes, and storage nodes are sharded by a
consistent hash of each document id.

The sandbox origin is not optional: the client refuses to load without it. In
production it should be a genuinely different domain, not just a different port.

## Storage

`config/config.js` selects where durable data lives.

**Local filesystem** (the default) keeps everything under `./data/<node index>/`:

```js
storage: { type: 'fs' }
```

**Object storage** keeps it in any S3-compatible bucket. Uncomment the `s3`
block, set `type: 's3'`, and supply credentials through the environment:

```bash
export S3_ACCESS_KEY=...
export S3_SECRET_KEY=...
npm start
```

Check a credential set has the permissions the server needs before pointing an
instance at a bucket — this reports exactly which are missing:

```bash
S3_BUCKET=my-bucket S3_ENDPOINT=https://s3.fr-par.scw.cloud S3_REGION=fr-par \
S3_ACCESS_KEY=... S3_SECRET_KEY=... node scripts/s3-check.js
```

Moving an existing instance across (and back) is a copy in either direction:

```bash
node scripts/storage-migrate.js --dry-run   # what would move
node scripts/storage-migrate.js             # move it
node scripts/storage-migrate.js --verify    # confirm both sides agree
node scripts/storage-migrate.js --reverse   # bring it back to local disk
```

Stop the server while migrating. See `docs/s3-storage.md` for the design, and in
particular for how long a document can sit in the local cache before reaching the
object store.

## Stopping

`Ctrl-C`, or `SIGTERM` to the supervisor. It forwards the signal to every node
and waits for each to drain — storage nodes flush anything buffered before
exiting, so a clean stop loses nothing.

## Tests

```bash
npm run test:unit    # offline; no server or credentials needed
```

To run the object-storage tests too, supply the same `S3_*` variables as above.
They work under a unique prefix per run and delete it afterwards, so they are
safe against a real bucket.
