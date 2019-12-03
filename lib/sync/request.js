/*!
 * contentstack-express
 * Copyright (c) Contentstack
 * MIT Licensed
 */

'use strict';

/*!
 * Module dependencies
 */
var request = require('request');
var debug = require('debug')('sync:api-requests');
var config = require('../utils').config;
var pkg = require('../../package');
var MAX_RETRY_LIMIT = 5;


/**
 * Validate incoming requests
 * @param  {Object}   req : Request object
 * @param  {Function} cb  : Error first callback
 */
function validate(req, cb) {
  if (typeof req !== 'object' || typeof cb !== 'function') {
    throw new Error(`Invalid params passed for request\n${JSON.stringify(arguments)}`);
  }
  if (typeof req.uri === 'undefined' && typeof req.url === 'undefined') {
    throw new Error(`Missing uri in request!\n${JSON.stringify(req)}`);
  }
  if (typeof req.method === 'undefined') {
    debug(`${req.uri || req.url} had no method, setting it as 'GET'`);
    req.method = 'GET';
  }
  if (typeof req.json === 'undefined') {
    req.json = true;
  }
  if (typeof req.headers === 'undefined') {
    debug(`${req.uri || req.url} had no headers`);
    req.headers = {
      api_key: config.get('contentstack.api_key'),
      access_token: config.get('contentstack.access_token'),
      'X-User-Agent': 'contentstack-express/' + pkg.version
    };
  }
}

/**
 * Make API requests to Contentstack
 * @param  {Object}   req   : Request query object
 * @param  {Function} cb    : Error first callback
 * @param  {Number}   RETRY : Retry counter
 * @return {Object}         : Response JSON
 */
var makeCall = module.exports = function(req, cb, RETRY) {
  try {
    validate(req, cb);
    if (typeof RETRY !== 'number') {
      RETRY = 1;
    } else if (RETRY > MAX_RETRY_LIMIT) {
      return cb(new Error('Max retry limit exceeded!'));
    }
    debug(`${req.method.toUpperCase()}: ${req.uri || req.url}`);
    return request(req, function(err, response, body) {
      if (err) {
        return cb(err);
      }
      var timeDelay;
      if (response.statusCode >= 200 && response.statusCode <= 399) {
        return cb(null, body);
      } else if (response.statusCode === 429) {
        timeDelay = Math.pow(Math.SQRT2, RETRY) * 100;
        debug(`API rate limit exceeded.\nReceived ${response.statusCode} status\nBody ${JSON.stringify(body)}`);
        debug(`Retrying ${req.uri || req.url} with ${timeDelay} sec delay`);
        return setTimeout(function (req, cb, RETRY) {
          return makeCall(req, cb, RETRY);
        }, timeDelay, req, cb, RETRY);
      } else if (response.statusCode >= 500) {
        // retry, with delay
        timeDelay = Math.pow(Math.SQRT2, RETRY) * 100;
        debug(`Recevied ${response.statusCode} status\nBody ${JSON.stringify(body)}`);
        debug(`Retrying ${req.uri || req.url} with ${timeDelay} sec delay`);
        RETRY++;
        return setTimeout(function (req, cb, RETRY) {
          return makeCall(req, cb, RETRY);
        }, timeDelay, req, cb, RETRY);
      } else {
        debug(`Request failed\n${JSON.stringify(req)}`);
        debug(`Response received\n${JSON.stringify(body)}`);
        return cb(body);
      }
    });
  } catch (error) {
    debug(error);
    return cb(error);
  }
};