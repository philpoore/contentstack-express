/*!
 * contentstack-express
 * copyright (c) Built.io Contentstack
 * MIT Licensed
 */

'use strict';

/**
 * Module dependencies.
 */
var dataStore = require('./query-builder');

// Load InMemory
require('./inmemory');

/**
 * Expose `dataStore()`.
 */
exports = module.exports = new dataStore();
