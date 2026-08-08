// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors
//
// SPDX-License-Identifier: AGPL-3.0-or-later

/*  Move an instance's data between storage backends.

    Because keys map onto paths verbatim, a bucket is a mirror of the datastore
    directory: migrating is a plain copy, and the same script run with --reverse
    brings everything back. That exit path matters more than the entry one — an
    operator is much more willing to try object storage knowing they can undo it.

    Usage:

        # local disk -> object storage (dry run first)
        node scripts/storage-migrate.js --dry-run
        node scripts/storage-migrate.js

        # object storage -> local disk
        node scripts/storage-migrate.js --reverse

        # check that both sides agree, copying nothing
        node scripts/storage-migrate.js --verify

    Options:
        --index N       storage node index to migrate (default 0)
        --prefix P      restrict to one prefix, e.g. channel/ or blob/
        --concurrency N parallel transfers (default 16)
        --overwrite     replace objects that already exist at the destination
        --dry-run       report what would be copied, transfer nothing
        --verify        compare sizes on both sides, transfer nothing
        --reverse       treat the configured backend as the source

    The server should be stopped while this runs. It is safe to re-run: without
    --overwrite, anything already present at the destination is skipped, so an
    interrupted migration can simply be started again.
*/

const Path = require('node:path');
const nThen = require('nthen');
const Semaphore = require('saferphore');

const Backends = require('../common/storage/backend/index.js');
const FsBackend = require('../common/storage/backend/fs.js');
const Core = require('../common/core.js');
const { config } = require('../common/load-config.js');

const args = require('minimist')(process.argv.slice(2));

if (args.h || args.help) {
    console.log(require('node:fs').readFileSync(__filename, 'utf8')
        .split('*/')[0].replace(/^\/\*|^\s{4}/gm, ''));
    process.exit(0);
}

const INDEX = Number(args.index || args.i || 0);
const CONCURRENCY = Number(args.concurrency || 16);
const DRY_RUN = Boolean(args['dry-run']);
const VERIFY = Boolean(args.verify);
const OVERWRITE = Boolean(args.overwrite);
const REVERSE = Boolean(args.reverse);
const ONLY_PREFIX = typeof (args.prefix) === 'string' ? args.prefix : '';

const storageConfig = config.storage || {};
if ((storageConfig.type || 'fs') === 'fs') {
    console.error('config.storage.type is "fs", so there is no remote backend to ' +
                  'migrate to or from. Configure it before running this script.');
    process.exit(2);
}

const paths = Core.getPaths({ index: INDEX, config });

/*  The families to move, and where each lives locally. Keys are identical on both
    sides; only the root differs. */
const FAMILIES = [
    { prefix: 'channel/', root: paths.filePath },
    { prefix: 'pins/', root: paths.pinPath },
    { prefix: 'blob/', root: paths.blobPath },
    { prefix: 'block/', root: paths.blockPath },
    { prefix: 'archive/', root: paths.archivePath },
    { prefix: 'tasks/', root: paths.taskPath },
    { prefix: 'decrees/', root: paths.decreePath },
    // the small id-keyed stores all live directly under the base path
    { prefix: 'users/', root: Path.join(paths.basePath, 'users') },
    { prefix: 'invitations/', root: Path.join(paths.basePath, 'invitations') },
    { prefix: 'sessions/', root: Path.join(paths.basePath, 'sessions') },
    { prefix: 'mfa/', root: Path.join(paths.basePath, 'mfa') },
    { prefix: 'support/', root: Path.join(paths.basePath, 'support') },
    { prefix: 'challenges/', root: paths.challengePath }
];

/*  Never migrated. Offsets are a local index that is rebuilt on demand, staged
    uploads are incomplete by definition, and the cache is a working copy of the
    store itself. */
const SKIP = /(\.offset|\.temp|\.s3state|\.hydrating\.|\.tmp\.)/;

const log = (...parts) => { console.log(...parts); };

const migrateFamily = (family, source, destination, stats, done) => {
    const sema = Semaphore.create(CONCURRENCY);

    const copyOne = (entry, next) => {
        const key = entry.key;
        if (SKIP.test(key)) {
            stats.skipped++;
            return void next();
        }

        destination.head(key, (err, existing) => {
            if (err && err.code !== 'ENOENT') {
                stats.errors++;
                console.error(`  ERROR head ${key}: ${err.message || err.code}`);
                return void next();
            }

            if (VERIFY) {
                if (!existing) {
                    stats.missing++;
                    console.error(`  MISSING at destination: ${key}`);
                } else if (existing.size !== entry.size) {
                    stats.mismatched++;
                    console.error(`  SIZE MISMATCH ${key}: ` +
                                  `${entry.size} here, ${existing.size} there`);
                } else {
                    stats.verified++;
                }
                return void next();
            }

            if (existing && !OVERWRITE) {
                stats.skipped++;
                return void next();
            }

            if (DRY_RUN) {
                stats.copied++;
                stats.bytes += entry.size;
                return void next();
            }

            source.getStream(key, {}, (err, stream) => {
                if (err) {
                    stats.errors++;
                    console.error(`  ERROR read ${key}: ${err.message || err.code}`);
                    return void next();
                }
                destination.put(key, stream, {}, err => {
                    if (err) {
                        stats.errors++;
                        console.error(`  ERROR write ${key}: ${err.message || err.code}`);
                    } else {
                        stats.copied++;
                        stats.bytes += entry.size;
                    }
                    next();
                });
            });
        });
    };

    // page through the source so a large instance is never held in memory
    const page = (cursor) => {
        source.list(family.prefix, { cursor, limit: 1000 }, (err, result) => {
            if (err) {
                console.error(`  ERROR listing ${family.prefix}: ${err.message || err.code}`);
                stats.errors++;
                return void done();
            }
            if (!result.keys.length && !cursor) { return void done(); }

            nThen(w => {
                result.keys.forEach(entry => {
                    sema.take(give => { copyOne(entry, w(give())); });
                });
            }).nThen(() => {
                if (result.cursor) { return void page(result.cursor); }
                done();
            });
        });
    };
    page();
};

const round = n => Math.round(n / (1024 * 1024) * 100) / 100;

nThen(w => {
    // the local side: one backend rooted at the base path covers every family,
    // because each family's key prefix is its directory name under that root
    FsBackend.create({ root: paths.basePath }, w((err, backend) => {
        if (err) { w.abort(); throw err; }
        w.local = backend;
        module.exports._local = backend;
    }));
    Backends.create(storageConfig, { root: paths.filePath }, w((err, backend) => {
        if (err) {
            w.abort();
            console.error('Could not build the remote backend:', err.message);
            process.exit(1);
        }
        module.exports._remote = backend;
    }));
}).nThen(() => {
    const local = module.exports._local;
    const remote = module.exports._remote;
    const source = REVERSE ? remote : local;
    const destination = REVERSE ? local : remote;

    const mode = VERIFY ? 'VERIFY' : (DRY_RUN ? 'DRY RUN' : 'MIGRATE');
    log(`\n${mode}: ${REVERSE ? 'object storage -> local disk' : 'local disk -> object storage'}`);
    log(`storage node index ${INDEX}, base path ${paths.basePath}`);
    if (ONLY_PREFIX) { log(`restricted to prefix ${ONLY_PREFIX}`); }
    log('');

    const stats = {
        copied: 0, skipped: 0, errors: 0, bytes: 0,
        verified: 0, missing: 0, mismatched: 0
    };

    const families = FAMILIES.filter(f => {
        return !ONLY_PREFIX || f.prefix.startsWith(ONLY_PREFIX) ||
            ONLY_PREFIX.startsWith(f.prefix);
    });

    let n = nThen;
    families.forEach(family => {
        n = n(w => {
            const before = stats.copied + stats.verified + stats.skipped;
            log(`${family.prefix}`);
            migrateFamily(family, source, destination, stats, w(() => {
                const handled = stats.copied + stats.verified + stats.skipped - before;
                log(`  ${handled} object(s)`);
            }));
        }).nThen;
    });

    n(() => {
        log('');
        if (VERIFY) {
            log(`verified ${stats.verified}, missing ${stats.missing}, ` +
                `mismatched ${stats.mismatched}, errors ${stats.errors}`);
            const bad = stats.missing + stats.mismatched + stats.errors;
            if (bad) {
                log('\nThe two sides do NOT agree.');
                return void process.exit(1);
            }
            log('\nBoth sides agree.');
            return void process.exit(0);
        }

        log(`${DRY_RUN ? 'would copy' : 'copied'} ${stats.copied} object(s), ` +
            `${round(stats.bytes)} MB`);
        log(`skipped ${stats.skipped}, errors ${stats.errors}`);

        if (stats.errors) {
            log('\nFinished with errors. Re-running is safe: anything already ' +
                'transferred is skipped.');
            return void process.exit(1);
        }
        if (!DRY_RUN) {
            log('\nDone. Verify with:  node scripts/storage-migrate.js --verify');
        }
        process.exit(0);
    });
});
