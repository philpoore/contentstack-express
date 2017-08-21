/*!
 * contentstack-express
 * copyright (c) Built.io Contentstack
 * MIT Licensed
 */

'use strict';

/**
 * Module Dependencies.
 */
var config = require('../../config'),
    path = require('path'),
    cache = config.get('cache'),
    provider = config.get('storage').provider;
var dataStore = function () {
    provider = (provider === 'FileSystem' && cache) ? 'nedb' : provider;
    try {
        return require(path.join(__dirname, provider));
    } catch (error) {
        console.error('Error in loading db..', error);
    }
};

/**
 * Expose `dataStore()`.
 */
module.exports = dataStore();
