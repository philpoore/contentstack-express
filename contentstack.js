/*!
 * contentstack-express
 * copyright (c) Contentstack
 * MIT Licensed
 */

'use strict';

/*!
 * Module dependencies
 */
var framework = require('./lib/framework');
var utils = require('./lib/utils');

/**
 * @method config
 * @description Get configuration using get() method.
 */
framework.config = utils.config;

/**
 * @method Stack
 * @description SDK for CRUD operations, i.e. find(), findOne(), insert(), upsert() and remove().
 */
framework.Stack = function() {
  return utils.db;
};

/**
 * @method  providers
 * @description : Current provider's connection string. Access provider methods directly without wrappers.
 */
framework.db = utils.providers;

/**
 * @method logger
 * @description Add debug logs such as info(), warn(), error(), etc.
 */
framework.logger = utils.debug;

/**
 * Expose framework
 */
module.exports = framework;