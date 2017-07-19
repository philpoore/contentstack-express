/*!
 * contentstack-express
 * copyright (c) Built.io Contentstack
 * MIT Licensed
 */

'use strict';

/**
 * Module Dependencies.
 */
var config = require('./../../config/index'),
    path = require('path'),
    cache = config.get('cache'),
    provider = config.get('storage').provider;
var dataStore = function () {
    provider = (provider === "FileSystem" && cache) ? "nedb" : provider;
    try {
        return require(path.join(__dirname, provider));
    } catch (e) {
        console.error("Error in datastore loading ...", e);
    }
};

/**
 * Expose `dataStore()`.
 */
module.exports = dataStore();
