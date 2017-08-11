/*!
 * contentstack-express
 * copyright (c) Built.io Contentstack
 * MIT Licensed
 */

'use strict';

/**
 * Module dependencies.
 */
var dataStore = require('./query-builder'),
	config 		= require('../config');

require('./inmemory');

/**
 * Expose `dataStore()`.
 */
exports = module.exports = new dataStore();
