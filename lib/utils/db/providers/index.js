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
    path = require('path');

var dataStore = function () {
    var cache = config.get('cache'),
    	providerName = config.get('storage').provider,
        provider = (providerName === "FileSystem" && cache) ? "nedb" : providerName;
    try {
    	if(provider === 'FileSystem' || provider === 'nedb' || provider === 'cdn')
        	return require('./' + provider);
        else
        	return require(path.join(config.get('path.base'), 'providers', provider));
    } catch (e) {
        console.error("Error in datastore loading ...", e);
    }
};

/**
 * Expose `dataStore()`.
 */
module.exports = dataStore();
