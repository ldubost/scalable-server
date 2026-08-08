// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors
//
// SPDX-License-Identifier: AGPL-3.0-or-later

/*  Backend registry.

    The 'fs' backend is built in and has no dependencies. Any other backend is
    supplied by a plugin, which registers it by exporting

        modules: { storageBackends: { <name>: { create: (conf, cb) => {} } } }

    from its index.js. This keeps optional client libraries (such as the AWS SDK
    used by the S3 plugin) out of a stock installation.
*/

const FsBackend = require('./fs.js');
const plugins = require('../../plugin-manager.js');

const Backends = module.exports;

/*  's3' is required lazily: the AWS SDK is an optional dependency, so an instance
    that does not use object storage never loads it and does not need it installed. */
const BUILT_IN = {
    fs: FsBackend,
    s3: {
        create: (conf, cb) => {
            let impl;
            try {
                impl = require('./s3.js');
            } catch (err) {
                return void cb(err);
            }
            impl.create(conf, cb);
        }
    }
};

/*  Look a backend implementation up by name, built-ins first, then plugins. */
const resolve = name => {
    if (BUILT_IN[name]) { return BUILT_IN[name]; }

    let found;
    Object.keys(plugins).forEach(key => {
        // plugin-manager mixes helper functions in with the plugins themselves
        const plugin = plugins[key];
        if (!plugin || typeof (plugin) !== 'object') { return; }
        const backend = plugin.storageBackends && plugin.storageBackends[name];
        if (backend) { found = backend; }
    });
    return found;
};
Backends.resolve = resolve;

Backends.listAvailable = () => {
    const names = Object.keys(BUILT_IN);
    Object.keys(plugins).forEach(key => {
        const plugin = plugins[key];
        if (!plugin || typeof (plugin) !== 'object') { return; }
        Object.keys(plugin.storageBackends || {}).forEach(name => {
            if (names.indexOf(name) === -1) { names.push(name); }
        });
    });
    return names;
};

/*  Create a backend instance from a storage config block.

        Backends.create({ type: 's3', s3: {...} }, { root: '/data/0' }, cb)

    `defaults.root` is what the 'fs' backend uses as its base directory; remote
    backends ignore it and take their location from their own config section.
*/
Backends.create = (storageConfig, defaults, _cb) => {
    const cb = typeof (_cb) === 'function' ? _cb : () => {};
    storageConfig = storageConfig || {};
    defaults = defaults || {};

    const type = storageConfig.type || 'fs';
    const impl = resolve(type);

    if (!impl || typeof (impl.create) !== 'function') {
        // Fail loudly rather than silently falling back to local disk: an instance
        // configured for remote storage that quietly writes to the local filesystem
        // would look healthy while stranding its data on one node.
        const available = Backends.listAvailable().join(', ');
        const err = new Error(`Storage backend '${type}' is not available. ` +
            `Installed backends: ${available}. ` +
            (type === 's3' ?
                "Install the S3 plugin in plugins/S3 to use S3 storage." :
                "Check config.storage.type."));
        err.code = 'E_UNKNOWN_STORAGE_BACKEND';
        return void cb(err);
    }

    // the backend's own config section, plus whatever defaults the caller supplies
    const conf = Object.assign({}, defaults, storageConfig[type] || {});

    impl.create(conf, cb);
};
