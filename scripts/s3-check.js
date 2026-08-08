// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors
//
// SPDX-License-Identifier: AGPL-3.0-or-later

/*  Check that a set of S3 credentials can do what CryptPad needs.

    Run this before pointing an instance at a bucket: it exercises each permission
    the storage layer relies on and reports exactly which ones are missing, rather
    than letting the server discover it at the first write.

        S3_BUCKET=my-bucket \
        S3_ENDPOINT=https://s3.fr-par.scw.cloud \
        S3_REGION=fr-par \
        S3_ACCESS_KEY=... \
        S3_SECRET_KEY=... \
        node scripts/s3-check.js

    Everything it creates lives under `_probe/` and is deleted on the way out.
*/

const {
    S3Client, PutObjectCommand, GetObjectCommand, DeleteObjectCommand,
    ListObjectsV2Command, CopyObjectCommand, CreateMultipartUploadCommand,
    AbortMultipartUploadCommand
} = require('@aws-sdk/client-s3');

const required = ['S3_BUCKET', 'S3_ACCESS_KEY', 'S3_SECRET_KEY'];
const missing = required.filter(name => !process.env[name]);
if (missing.length) {
    console.error(`Missing environment variables: ${missing.join(', ')}`);
    console.error('See the comment at the top of this file for usage.');
    process.exit(2);
}

const BUCKET = process.env.S3_BUCKET;
const ENDPOINT = process.env.S3_ENDPOINT;
const REGION = process.env.S3_REGION || 'us-east-1';

const client = new S3Client({
    endpoint: ENDPOINT,
    region: REGION,
    forcePathStyle: process.env.S3_FORCE_PATH_STYLE === 'true',
    credentials: {
        accessKeyId: process.env.S3_ACCESS_KEY,
        secretAccessKey: process.env.S3_SECRET_KEY
    }
});

const KEY = `_probe/s3-check-${Date.now()}`;
const results = [];

const check = async (label, needed, fn) => {
    try {
        const detail = await fn();
        results.push({ label, ok: true });
        console.log(`  ok      ${label.padEnd(22)}${detail ? '  ' + detail : ''}`);
        return true;
    } catch (err) {
        const status = (err.$metadata || {}).httpStatusCode;
        results.push({ label, ok: false, needed, err: err.name });
        console.log(`  FAILED  ${label.padEnd(22)}  ${err.name}` +
                    `${status ? ` (HTTP ${status})` : ''}`);
        return false;
    }
};

(async () => {
    console.log(`\nbucket   ${BUCKET}`);
    console.log(`endpoint ${ENDPOINT || '(AWS default)'}`);
    console.log(`region   ${REGION}\n`);

    await check('ListObjectsV2', 'read objects', async () => {
        const res = await client.send(new ListObjectsV2Command({
            Bucket: BUCKET, MaxKeys: 1
        }));
        return `${res.KeyCount || 0} object(s) visible`;
    });

    const wrote = await check('PutObject', 'write objects', async () => {
        await client.send(new PutObjectCommand({
            Bucket: BUCKET, Key: KEY, Body: Buffer.from('cryptpad-s3-check')
        }));
    });

    if (wrote) {
        await check('GetObject', 'read objects', async () => {
            const res = await client.send(new GetObjectCommand({ Bucket: BUCKET, Key: KEY }));
            return JSON.stringify(await res.Body.transformToString());
        });

        // used by archive/restore, which move objects server-side
        await check('CopyObject', 'write objects', async () => {
            await client.send(new CopyObjectCommand({
                Bucket: BUCKET, Key: KEY + '.copy',
                CopySource: `${BUCKET}/${KEY}`
            }));
        });

        // used to append to large channel logs without downloading them
        await check('Multipart upload', 'write objects', async () => {
            const res = await client.send(new CreateMultipartUploadCommand({
                Bucket: BUCKET, Key: KEY + '.mpu'
            }));
            await client.send(new AbortMultipartUploadCommand({
                Bucket: BUCKET, Key: KEY + '.mpu', UploadId: res.UploadId
            }));
        });

        await check('DeleteObject', 'delete objects', async () => {
            await client.send(new DeleteObjectCommand({ Bucket: BUCKET, Key: KEY }));
            await client.send(new DeleteObjectCommand({ Bucket: BUCKET, Key: KEY + '.copy' }))
                .catch(() => {});
        });
    }

    const failed = results.filter(r => !r.ok);
    console.log('');
    if (!failed.length) {
        console.log('All checks passed. This key can be used for CryptPad storage.');
        console.log('Next: run the conformance suite with the same variables:');
        console.log('  npm run test:unit\n');
        return process.exit(0);
    }

    const permissions = [...new Set(failed.map(r => r.needed))];
    console.log(`${failed.length} check(s) failed. The key is missing: ${permissions.join(', ')}.`);
    console.log('');
    console.log('On Scaleway, grant the API key\'s IAM policy ObjectStorageObjectsRead,');
    console.log('ObjectStorageObjectsWrite and ObjectStorageObjectsDelete (or');
    console.log('ObjectStorageFullAccess) scoped to the project holding this bucket.\n');
    process.exit(1);
})().catch(err => {
    console.error('\nUnexpected failure:', err && err.message);
    process.exit(1);
});
