/*!
 * contentstack-express
 * copyright (c) Built.io Contentstack
 * MIT Licensed
 *
 * Notes
 * @sort : Haven't implemented sorting
 * @content_types : Should we query for content types
 */

'use strict';

/**
 * Module Dependencies.
 */

var events = require('events').EventEmitter,
    util = require('util'),
    fs = require('graceful-fs'),
    _ = require('lodash'),
    path = require('path'),
    request = require('request'),
    config = require('./../../config/index'),
    helper = require('./../helper'),
    contentstack = config.get('contentstack'),
    apiHost = contentstack.host + '/' + contentstack.version,
    assetRoute = '_assets',
    environment = config.get('environment'),
    headers = {
      api_key: config.get('contentstack.api_key'),
      access_token: config.get('contentstack.access_token')
    },
    Provider = require('./Provider');

/**
 * CDN Provider
 * find(), findOne() and count() data from Built.io Contentstack's CDN network
 *
 * This instance of this class will allow user to fetch queries | data from the CDN network
 */

var CDN = function () {
  // Extend Provider abstract class
  this.provider = 'CDN';
};

// Extend from base provider
util.inherits(CDN, Provider);


/**
 * Find for a specific object from assets | entries
 * @param  {Object}   query    - Query object, contains the info based on which filters will be applied
 * @param  {Function} callback - Returns the filtered data on success, error object otherwise
 */

CDN.prototype.findOne = function (query, callback) {
  try {
    if (_.isPlainObject(query) && _.has(query, '_uid')) {
      if(query._content_type_uid === assetRoute)
        query.__url = apiHost + contentstack.urls.assets + query._uid;
      else
        query.__url = apiHost + contentstack.urls.content_types + query._content_type_uid + contentstack.urls.entries + query._uid;
      this.find(query, {}, callback);
    } else
      throw new Error("Kindly provide valid parameters for findOne");
  } catch (error) {
    callback(error);
  }
}


/**
 * Find for assets | entries
 * @param  {Object}   query    - Query object, contains the info based on which filters will be applied
 * @param  {Object}   options  - Optional query filters like sorting, skip, limits
 * @param  {Function} callback - Returns the filtered data on success, error object otherwise
 */

CDN.prototype.find = function (query, options, callback) {
  try {
    if(_.isPlainObject(query)) {
      // maintain domain state for _context
      var domain_state = process.domain;
      var _query = _.cloneDeep(query) || {},
          self = this,
          includeReference = (typeof _query.include_references === 'undefined' || _query.include_references === true) ? true : false,
          locale = _query.locale,
          count = _query.__count || false,
          limit = options.limit || 0,
          skip = options.skip || 0,
          include_count = _query.include_count || false,
          include_content_type = (includeReference) ? true : false,
          qs = {};

      if(options.sort) {
        var key = Object.keys(options.sort)[0];
        key = key.replace(/^_data./, '');
        if(options.sort[key] < 0)
          qs.asc = key;
        else
          qs.desc = key;
      }

      // removes unwated keys
      _query = helper.filterQuery(_query);

      // Set url
      var url = _query.__url || ((_query._content_type_uid === assetRoute) ? (apiHost + contentstack.urls.assets) : (apiHost + contentstack.urls.content_types + _query._content_type_uid + contentstack.urls.entries));

      qs = {
        query: JSON.stringify(_query),
        locale: locale,
        environment: environment,
        count: count,
        include_count: include_count,
        skip: skip,
        limit: limit,
        include_content_type: include_content_type
      }

      var __query = {
        url: url,
        method: 'GET',
        headers: headers,
        qs: qs,
        json: true
      };
      self.request(__query).then(function (result) {
        if(include_content_type) {
          // Traverse schema and find reference fields
          const references = self.findReferences(result.content_type.schema);
          if(references.length > 0) {
            // Build references
            var include = '?';
            for(var i = 0, j = references.length; i < j; i++)
              include += 'include[]=' + references[i].path + '&';

            delete __query.include_content_type;
            // Fetch data including single level referencing
            self.request(__query).then(function (result) {
              process.domain = domain_state;
              callback(null, result);
            }).catch(function (error) {
              process.domain = domain_state;
              callback(error);
            })
          } else {
            process.domain = domain_state;
            // There are no reference fields in the content type
            return callback(null, result);
          }
        } else {
          process.domain = domain_state;
          // include reference is false
          callback(null, result)
        }
      })
    } else
      throw new Error("Kindly provide valid parameters for find");
  } catch (error) {
    callback(error);
  }
}


/**
 * Get the count of assets | entries
 * @param  {Object}   query    - Query object, contains the info based on which filters will be applied
 * @param  {Function} callback - Returns the filtered data on success, error object otherwise
 */

CDN.prototype.count = function (query, callback) {
  try {
    if (query && typeof query == "object") {
      // adding the include_references just to get the count
      query.include_references = false;
      query.include_count = true;
      this.find(query, {}, function (error, result) {
        if (error)
          return callback(error);
        callback(null, {entries: result.entries.length});
      });
    } else {
      throw new Error("Kindly provide valid parameters for find");
    }
  } catch (error) {
    callback(error);
  }
}


/**
 * Finds reference fields in schema and notes them
 * @param  {object} schema      Schema to be traversed
 * @param  {string} uid         Help's to check self referencing content types
 */

CDN.prototype.findReferences = function (schema, parent) {
  var references = [];
  traverseSchemaWithPath(schema, function (path, field) {
     if (field.data_type === 'reference') {
        references.push({uid: field.uid, path: path})
     }
  }, false);
  return references;
}

function traverseSchemaWithPath (schema, fn, path) {
  path = path || '';
  function getPath(uid) {
     return (path === "") ? uid : [path, uid].join('.');
  }

  var promises = schema.map(function(field) {
    var pth = getPath(field.uid);

    if(field.data_type === 'group') {
      return traverseSchemaWithPath(field.schema, fn, pth);
    }
    return fn(pth, field);
  });
  return _.flatten(_.compact(promises));
}

CDN.prototype.request = function (query) {
  return new Promise(function (resolve, reject) {
    request(query, function (error, response, body) {
      if(error)
        reject(error);
      resolve(body);
    })
  })
}

exports = module.exports = new CDN();