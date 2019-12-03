/*!
 * contentstack-express
 * Copyright (c) Contentstack
 * MIT Licensed
 */

'use strict';

/**
 * Module Dependencies.
 */
var datastore = require('nedb');
var path = require('path');
var fs = require('graceful-fs');
var _ = require('lodash');
var async = require('async');
var debug = require('debug')('db:nedb');
var config = require('../../config');
var languages = config.get('languages');
var helper = require('../helper');
var InMemory = require('../inmemory');
var fileStorage = require('./FileSystem');
var assetRouteName = '_assets';
var skipForms = ['_routes', '_content_types'];

/**
 * NEDB database
 */
var nedbStorage = function () {
  this.db = {};
  var self = this,
    databases = {};
  for (var l = 0, lTotal = languages.length; l < lTotal; l++) {
    databases[languages[l]['code']] = (function (language) {
      return function (cb) {
        self.db[language.code] = new datastore({
          inMemoryOnly: true
        });
        var model = language.contentPath;
        if (fs.existsSync(model)) {
          fs.readdir(model, function (err, files) {
            if (err) {
              cb(err, null);
            } else {
              var loadDatabase = [];
              for (var i = 0, total = files.length; i < total; i++) {
                var fileName = files[i].replace('.json', '');
                if (skipForms.indexOf(fileName) === -1) {
                  loadDatabase.push(function (i, filePath) {
                    return function (_cb) {
                      fs.readFile(path.join(model, filePath), function (err, data) {
                        if (err) return _cb(err);
                        data = JSON.parse(data);
                        self.db[language.code].insert(data, _cb);
                      });
                    };
                  }(i, files[i]));
                }
              }
              async.parallel(loadDatabase, function (err, res) {
                if (err) {
                  return cb(err, res);
                } else {
                  self.db[language.code].ensureIndex({
                    fieldName: '_uid',
                    unique: true,
                    sparse: true
                  }, cb);
                }
              });
            }
          });
        } else {
          return cb(null, {});
        }
      };
    }(languages[l]));
  }
  async.parallel(databases, function (err) {
    if (err) {
      debug(`Errorred in loading 'nedb'\n${err.message || err}`);
      process.exit(0);
    }
  });
};

nedbStorage.prototype.processMultipleCT = function (multiReferences, locale, references, parentID, callback) {
  const self = this
  const callbacks = []
  const queries = {}
  let contents = []
  multiReferences.forEach(function (ref) {
    if (queries.hasOwnProperty(ref._content_type_id)) {
      if (typeof ref.values === 'object') {
        queries[ref._content_type_id]._uid.$in = queries[ref._content_type_id]._uid.$in.concat(ref.values)
      } else {
        queries[ref._content_type_id]._uid.$in.push(ref.values)
      } 
    } else {
      queries[ref._content_type_id] = {
        _content_type_uid: ref._content_type_id,
        _uid: {
          $in: (typeof ref.values === 'string') ? [ref.values]: ref.values
        },
        locale: locale
      }
    }
  })

  for (const ct in queries) {
    queries[ct]._uid.$in = _.filter(queries[ct]._uid.$in, function (uid) {
      var flag = helper.checkCyclic(uid, references)
      return !flag
    })

    if (queries[ct]._uid.$in.length === 0) {
      delete queries[ct]
    } else {
      callbacks.push(function () {
        return (function (cb) {
          return self.find(queries[ct], {}, function (error, data) {
            if (error) {
              return cb(error)
            }

            contents = contents.concat(data.entries)
            return cb()
          }, _.clone(references, true), parentID)
        })
      }())
    }
  }

  return async.series(callbacks, function (error) {
    if (error) {
      return callback(error)
    }

    let uids = _.map(multiReferences, 'values')
    uids = _.flatten(uids)
    const output = []
    uids.forEach(function (uid) {
      for (let i = 0, j = contents.length; i < j; i++) {
        if (contents[i].uid === uid) {
          output.push(contents[i])
          break
        }
      }
    })
    return callback(null, output)
  })
}

/**
 * Bind entry references
 * @param  {Object}   data       : Content type entry collection
 * @param  {String}   _locale    : Language code
 * @param  {Object}   references : References mapped so far
 * @param  {String}   parentID   : Parent hierarcy - used to avoid cyclic references
 * @param  {Function} callback   : Error first callback
 * @return {Object}              : Content type collection, with references
 */
// include references
nedbStorage.prototype.includeReferences = function (data, _locale, references, parentID, callback) {
  var self = this,
    calls = [];
  if (_.isEmpty(references)) references = {}
  var _includeReferences = function (data) {
    for (var _key in data) {
      if (data.uid) parentID = data.uid
      if (data[_key] && typeof data[_key] === "object") {
        if (data[_key] instanceof Array && data[_key].length && data[_key][0].hasOwnProperty('_content_type_id')) {
          calls.push(function (_key, data) {
            return (function (_callback) {
              return self.processMultipleCT(data[_key], _locale, references, parentID, function (error, output) {
                if (error) {
                  return setImmediate(function () {
                    return _callback(error)
                  })
                }
                data[_key] = output
                return setImmediate(function () {
                  return _callback(null, data)
                })
              })
            })
          }(_key, data))
        } else if (data[_key]._content_type_id) {
          calls.push(function (_key, data) {
            return (function (_callback) {
              var _uid = (data[_key]['_content_type_id'] === assetRouteName && data[_key]['values'] && typeof data[_key]['values'] === 'string') ? data[_key]['values'] : {
                $in: data[_key]['values']
              };
              var query = {
                _content_type_uid: data[_key]['_content_type_id'],
                _uid: _uid,
                locale: _locale
              };
              if (query._content_type_uid !== assetRouteName) {
                query['_uid']['$in'] = _.filter(query['_uid']['$in'], function(uid) {
                  var flag = helper.checkCyclic(uid, references);
                  return !flag;
                });
              }
              var _calls = [];
              _calls.push(function (field, query) {
                return (function (cb) {
                  self.find(query, {}, function (_err, _data) {
                    if (!_err || (_err.code && _err.code === 422)) {
                      if (_data && (_data.entries || _data.assets)) {
                        // to remove the wrapper from the result set
                        _data = _data.entries || _data.assets;

                        var __data = [];
                        if (query._uid && query._uid.$in) {
                          for (var a = 0, _a = query._uid.$in.length; a < _a; a++) {
                            var _d = _.find(_data, {
                              uid: query._uid.$in[a]
                            });
                            if (_d) __data.push(_d);
                          }
                          data[_key] = __data;
                        } else {
                          data[field] = (_data && _data.length) ? _data[0] : {};
                        }
                      } else {
                        data[field] = [];
                      }
                      return setImmediate(function () {
                        return cb(null, data)
                      });
                    } else {
                      return setImmediate(function () {
                        return cb(_err, null);
                      });
                    }

                  }, _.clone(references, true), parentID);
                });
              }(_key, query));
              async.series(_calls, function (__err, __data) {
                return setImmediate(function () {
                  return _callback(__err, __data);
                });
              });
            });
          }(_key, data));
        } else {
          _includeReferences(data[_key]);
        }
      }
    }
  };

  var recursive = function (data, callback) {
    _includeReferences(data);
    if (calls.length) {
      async.series(calls, function (e) {
        if (e) return callback(e, null);
        calls = [];
        return setImmediate(function () {
          return recursive(data, callback);
        });
      });
    } else {
      return callback(null, data);
    }
  };

  try {
    recursive(data, callback);
  } catch (e) {
    return callback(e, null);
  }
};

/**
 * Fetch a single entry
 * @param  {Object}   query    : Query filter
 * @param  {Function} callback : Error first callback
 * @return {Object}            : Single-filtered entry
 */
nedbStorage.prototype.findOne = function (query, callback) {
  try {
    var _query = _.clone(query, true);
    var self = this;
    var locale;
    if (_query && typeof _query === 'object') {
      locale = _query.locale;
      var remove = _query._remove || false;
      var includeReference = (typeof _query.include_references === 'undefined' || _query.include_references === true)
        ? true : false;
      // to remove the unwanted keys from query
      _query = helper.filterQuery(_query);
      return this.db[locale].findOne(_query)
        .exec(function (err, data) {
          try {
            if (err) return callback(err);
            if (data && data._data) {
              // check if data exists then only search for more
              var _data = (remove) ? data : data._data,
                __data = (!remove) ? {
                  entry: _.clone(_data, true)
                } : _data;
              if (includeReference) {
                return self.includeReferences(__data, locale, undefined, undefined, callback);
              } else {
                return callback(null, __data);
              }
            } else {
              helper.generateCTNotFound(locale, query._content_type_uid);
              return callback(null, {
                entry: null
              });
            }
          } catch (e) {
            return callback(e);
          }
        });
    } else {
      throw new Error('Query parameter should be of type Object');
    }
  } catch (e) {
    return callback(e);
  }
};

/**
 * Find entries based on filters
 * @param  {Object}   query    : Query filter
 * @param  {Object}   options  : Optional projections
 * @param  {Function} callback : Error first callback
 * @return {Object}            : Filtered entries
 */
nedbStorage.prototype.find = function (query, options, callback) {
  try {
    var references = (_.isPlainObject(arguments[3]) && !_.isEmpty(arguments[3])) ? arguments[3] : {};
    var parentID = (_.isString(arguments[4])) ? arguments[4] : undefined;
    if (query && typeof query === 'object' && typeof options === 'object') {
      var self = this;
      var _query = _.clone(query, true);
      var includeReference = (typeof _query.include_references === 'undefined' || _query.include_references === true)
        ? true : false;
      var calls = {};
      var locale = _query.locale;
      var count = _query.include_count;
      var queryObject;
      // to remove the unwanted keys from query and create reference query
      _query = helper.filterQuery(_query);
      // find assets if _content_type_uid is  '_assets'
      if (_query._content_type_uid === assetRouteName) {
        var results = InMemory.get(locale, _query._content_type_uid, _query);
        var _data = {
          assets: (results && results.length) ? results : []
        };
        return callback(null, _data);
      }
      queryObject = self.db[locale].find(_query).sort(options.sort || {
        '_data.published_at': -1
      });
      if (options.limit) queryObject.skip((options.skip || 0)).limit(options.limit);
      calls.entries = function (_cb) {
        queryObject.exec(_cb);
      };
      if (count) {
        calls['count'] = function (_cb) {
          self.db[locale].count(_query, _cb);
        };
      }
      return async.parallel(calls, function (err, result) {
        try {
          if (err) {
            throw err;
          } else if (result && result.entries && result.entries.length) {
            var _data = _.clone(result, true);
            _data.entries = _.map(_data.entries, '_data');
            if (includeReference) {
              if (parentID) {
                var tempResult = _data.entries;
                references[parentID] = references[parentID] || [];
                references[parentID] = _.uniq(references[parentID].concat(_.map(tempResult, 'uid')));
              }
              return self.includeReferences(_data, locale, references, parentID, callback);
            }
            return callback(null, _data);
          } else {
            return callback(null, { entries: [] })
          }
          // helper.generateCTNotFound(locale, query._content_type_uid);
          return callback(null, result);
        } catch (e) {
          return callback(e);
        }
      });
    } else {
      throw new Error('Query & options parameter should be of type Object');
    }
  } catch (e) {
    return callback(e);
  }
};

/**
 * Find total no of entries after filtering
 * @param  {Object}   query    : Query filter
 * @param  {Function} callback : Error first callback
 * @return {Object}            : Entries count
 */
nedbStorage.prototype.count = function (query, callback) {
  try {
    if (query && typeof query === 'object') {
      var locale = query.locale;
      // to remove the unwanted keys from query and create reference query
      query = helper.filterQuery(query);
      this.db[locale].count(query, function (err, count) {
        try {
          if (err) {
            throw err;
          }
          return callback(null, {
            entries: count || 0
          });
        } catch (e) {
          return callback(e);
        }
      });
    } else {
      throw new Error('Query parameter should be an object.');
    }
  } catch (e) {
    return callback(e);
  }
};

/**
 * Insert entries
 * @param  {Object}   data     : Entry to be inserted
 * @param  {Function} callback : Error first callback
 * @return {Number}            : Insert status
 */
nedbStorage.prototype.insert = function (data, callback) {
  try {
    var self = this;
    var contentTypeId = data._content_type_uid;
    var language = data.locale;
    return fileStorage.insert(data, function (err, result) {
      try {
        if (err) throw err;
        // remove the unwanted keys from the local-storage data
        data = helper.filterQuery(data, true);
        data._uid = data._uid || data._data.uid;
        if (contentTypeId === '_routes' || contentTypeId === '_content_types' || contentTypeId ===
          assetRouteName) {
          return callback(null, result);
        }
        return self.db[language].insert(data, function (error) {
          return callback(error, result);
        });
      } catch (e) {
        return callback(e);
      }
    });
  } catch (e) {
    return callback(e);
  }
};

/**
 * Update if entry exists, else insert
 * @param  {Object}   data     : Entry to be inserted
 * @param  {Function} callback : Error first callback
 * @return {Number}            : Upsert status
 */
nedbStorage.prototype.upsert = function (data, callback) {
  try {
    var self = this;
    var language = data.locale;
    return fileStorage.upsert(data, function (err, result) {
      try {
        if (err) {
          throw err;
        } else if (data._content_type_uid === '_routes' || data._content_type_uid === assetRouteName) {
          return callback(null, result);
        }
        // remove the unwanted keys
        data = helper.filterQuery(data, true);
        data._uid = data._uid || data._data.uid;
        return self.db[language].update({
          _uid: data._uid
        }, data, {
          upsert: true
        }, function (error) {
          return callback(error, result);
        });
      } catch (error) {
        return callback(error);
      }
    });
  } catch (e) {
    return callback(e);
  }
};

/**
 * Remove entry from db
 * @param  {Object}   query    : Query filter
 * @param  {Function} callback : Error first callback
 * @return {Number}            : Remove status
 */
nedbStorage.prototype.remove = function (query, callback) {
  try {
    var self = this;
    var language = query.locale;
    return fileStorage.remove(query, function (err, result) {
      try {
        if (err) throw err;
        // to remove the unwanted keys from query and create reference query
        query = helper.filterQuery(query);
        if (query._content_type_uid === '_routes' || query._content_type_uid === assetRouteName) {
          return callback(null, result);
        }
        return self.db[language].remove(query, {}, function (error) {
          return callback(error, result);
        });
      } catch (error) {
        return callback(error);
      }
    });
  } catch (e) {
    return callback(e);
  }
};

/**
 * Insert entries in bulk
 * @param  {Object}   query    : Query filter
 * @param  {Function} callback : Error first callback
 * @return {Number}            : Bulk insert status
 */
nedbStorage.prototype.bulkInsert = function (query, callback) {
  var self = this;
  return fileStorage.bulkInsert(query, function (err, result) {
    try {
      if (err) {
        throw err;
      } else if (query._content_type_uid === '_routes' || query._content_type_uid === assetRouteName) {
        return callback(null, result);
      }
      var entries = query.entries || [];
      var calls = [];
      var language = query.locale;
      for (var i = 0, total = entries.length; i < total; i++) {
        calls.push(function (entry) {
          return function (cb) {
            self.db[language].update({
              _uid: (entry.uid || entry.entry.uid)
            }, {
              _data: entry,
              _uid: (entry.uid || entry.entry.uid),
              _content_type_uid: query._content_type_uid
            }, {
              upsert: true
            }, cb);
          };
        }(entries[i]));
      }
      return async.parallelLimit(calls, 5, callback);
    } catch (error) {
      return callback(error);
    }
  });
};

module.exports = new nedbStorage();
