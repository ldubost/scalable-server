// SPDX-FileCopyrightText: 2026 XWiki CryptPad Team <contact@cryptpad.org> and contributors
//
// SPDX-License-Identifier: AGPL-3.0-or-later

/*  Key validation, shared by every ObjectBackend implementation.

    Keys are the backend's public vocabulary, so they are validated at the seam
    rather than trusting each caller. On the filesystem backend an unchecked key
    could escape the storage root; on an object store it would merely create a
    surprising object, but the two backends must agree on what is valid or code
    that works on one will fail on the other.
*/

const mkError = (code, message) => {
    const err = new Error(message || code);
    err.code = code;
    return err;
};

const isValidKey = key => {
    if (typeof (key) !== 'string' || !key.length) { return false; }
    if (key.length > 1024) { return false; }
    if (key.startsWith('/')) { return false; }
    if (key.includes('\0')) { return false; }
    if (key.includes('\\')) { return false; }
    return !key.split('/').some(part => part === '..' || part === '.');
};

module.exports = { isValidKey, mkError };
