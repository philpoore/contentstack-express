/*!
 * contentstack-express
 * copyright (c) Built.io Contentstack
 * MIT Licensed
 */

'use strict';

/**
 * Module Dependencies.
 */
var path = require('path'),
    util = require('util'),
    _ = require('lodash'),
    async = require('async'),
    // contentstack.config() : gave issues once
    config = require('../../../config'),
    assetDownloadFlag = config.get('assets.download'),
    // TODO: find better way
    helper = require('../../helper'),
    InMemory = require('../../inmemory'),
    assetRouteName = '_assets',
    mongodb = require('./connection').connect,
    Provider = require('../Provider');

/**
 * Class which handles all the operations related to MongoDB database
 */
var MongodbStorage = function() {

    // Holds the Database connection object
    this.db = mongodb();
    this.provider = 'MongoDB';
};

// Extend from base provider
util.inherits(MongodbStorage, Provider);

/**
 * Function which includes the references' content type entry
 * into the content type entry data
 *
 * @param {Object} data - Data in which references need to be included
 * @param {String} _locale - Contains the locale of the given Content Type
 * @param {Function} callback - Function which is called upon completion
 */
MongodbStorage.prototype.includeReferences = function (data, _locale, references, parentID, callback) {
    var self = this,
        calls = [];
    if (_.isEmpty(references)) references = {};
    var _includeReferences = function (data) {
        for (var _key in data) {
            if (data.uid) parentID = data.uid;
            if (typeof data[_key] == "object") {
                if (data[_key] && data[_key]["_content_type_id"]) {
                    calls.push(function (_key, data) {
                        return (function (_callback) {
                            var _uid = (data[_key]["_content_type_id"] == assetRouteName && data[_key]["values"] && typeof data[_key]["values"] === 'string') ? data[_key]["values"] : {"$in": data[_key]["values"]};
                            var query = {
                                    "_content_type_uid": data[_key]["_content_type_id"],
                                    "_uid": _uid,
                                    "locale": _locale,
                                    "_remove": true
                                },
                                _calls = [];
                            if (query._content_type_uid != assetRouteName) {
                                query["_uid"]["$in"] = _.filter(query["_uid"]["$in"], function (uid) {
                                    var flag = helper.checkCyclic(uid, references)
                                    return !flag
                                });
                            }
                            _calls.push(function (field, query) {
                                return (function (cb) {
                                    self.find(query, {}, function (_err, _data) {
                                        if (!_err || (_err.code && _err.code === 194)) {
                                            if (_data || (_data && _data.assets)) {
                                                var __data = [];
                                                if (query._uid && query._uid.$in) {
                                                    for (var a = 0, _a = query._uid.$in.length; a < _a; a++) {
                                                        var _d = _.find((_data.assets) ? _data.assets: _data, {uid: query._uid.$in[a]});
                                                        if (_d) __data.push(_d);
                                                    }
                                                    data[field] = __data;
                                                } else {
                                                    data[field] = (_data.assets && _data.assets.length) ? _data.assets[0] : {};
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
                                    }, _.clone(references), parentID);
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
            async.series(calls, function (e, d) {
                if (e) throw e;
                calls = [];
                return setImmediate(function () {
                    return recursive(data, callback);
                });
            });
        } else {
            callback(null, data);
        }
    };

    try {
        recursive(data, callback);
    } catch (e) {
        callback(e, null);
    }
};

/**
 * Function which handles the insertion of the data in the given MongoDB database
 *
 * @param {Object} data - data which need to be upserted into the database
 * @param {Function} callback - Function which is used as a callback
 */
MongodbStorage.prototype.insert = function(data, callback) {

    try {
        if (data && typeof data == "object" && data._content_type_uid && data._uid) {
            var self = this,
                contentTypeId = data._content_type_uid,
                language = data.locale,
                collectionName,
                insertionData;

            data._lang = language;

            /*
             * Collection names in MongoDB to store the data are
             *      content_types       - Holds Metadata about the Content types' entry
             *      entries             - Holds all the Content types' entry
             *      routes              - Holds all the routes for the Content types' entry
             */
            switch(contentTypeId) {
                case '_content_types': collectionName = 'content_types';
                    break;
                case '_routes': collectionName = 'routes';
                    break;
                case '_assets': collectionName = 'assets';
                    break;
                default: collectionName = 'entries';
                    break;
            }

            data = helper.filterQuery(data, true);
            data._uid = data._uid || data._data.uid;

            insertionData = _.cloneDeep(data);

            // Removing the _data level
            if (_.has(insertionData, '_data')) {
                // Assigns the contents of _data property to the lower level
                insertionData = _.assign(insertionData, insertionData._data);
                // Deletes the _data property
                delete insertionData._data;
            }

            // updating the references based on the new schema
            if (contentTypeId == "_content_types") insertionData = helper.findReferences(insertionData);
            self.db
                .collection(collectionName)
                .count({
                    "_content_type_uid": contentTypeId,
                    "_uid": data._uid,
                    "_lang": language
                }, function (error, result) {
                    if (error) {
                        return callback(error, null);
                    }
                    if (result !== 0) {
                        return callback(new Error("Data already exists, use update instead of insert."), null);
                    } else {
                        self.db.collection(collectionName).insert(insertionData, function (error, result) {
                            if(error)
                                return callback(error, 1);
                            // Updating inmemory data with the latest changes
                            InMemory.set(language, contentTypeId, data._uid, insertionData);
                            callback(null, 1);
                        });
                    }
                });
        } else {
            throw new Error("data should be an object with at least content_type_id and _uid.");
        }

    } catch (e) {
        callback(e, null);
    }
};

/**
 * Function which handles the insertion of data or updation if data already exists, in the given MongoDB database
 *
 * @param {Object} data - data which need to be upserted into the database
 * @param {Function} callback - Function which is used as a callback
 */
MongodbStorage.prototype.upsert = function(data, callback) {
    try {
        if (data && typeof data == "object" && data._content_type_uid && data._uid) {
            var self = this,
                contentTypeId = data._content_type_uid,
                language = data.locale,
                collectionName,
                insertionData;

            data._lang = language;

            switch(contentTypeId) {
                case '_content_types': collectionName = 'content_types';
                    break;
                case '_routes': collectionName = 'routes';
                    break;
                case '_assets': collectionName = 'assets';
                    break;
                default: collectionName = 'entries';
                    break;
            }

            data = helper.filterQuery(data, true);

            data._uid = data._uid || data._data.uid;

            // Clones the actual Data and alters to a format which is suitable for
            // the provider
            insertionData = _.cloneDeep(data);

            // Removing the _data level
            if (_.has(insertionData, '_data')) {
                // Assigns the contents of _data property to the lower level - this one causes the no display in template issue
                insertionData = _.assign(insertionData, insertionData._data);
                // Deletes the _data property
                delete insertionData._data;
            }

            // updating the references based on the new schema
            if (contentTypeId == "_content_types") insertionData = helper.findReferences(insertionData);

            // Performs an Upsert operation for the given data in MongoDB
            self.db
                .collection(collectionName)
                .update({
                    "_content_type_uid": contentTypeId,
                    "_uid": data._uid,
                    "_lang": language
                }, {
                    "$set": insertionData
                }, {
                    "upsert": true
                }, function (error, result) {
                    if(error)
                        return callback(error, 1);
                    // Updating the inmemory to reflect latest changes
                    InMemory.set(language, contentTypeId, data._uid, insertionData);
                    callback(null, 1);
                });
        } else {
            throw new Error("data should be an object with at least content_type_id and _uid.");
        }

    } catch (e) {
        return callback(e, null);
    }
};

/**
 * Function which is used to find a data based on the
 * given query
 *
 * The value returned should be in the format:
 *
 *              {
 *                  entry:
 *                      {
 *                          [Object]
 *                      }
 *              }
 *
 * @param {Object} query - Holds the query to find the data
 * @param {Function} callback - Function which is going to be called on the completion
 */
MongodbStorage.prototype.findOne = function (query, callback) {
    try {
        if (typeof query === "object" && !_.isEmpty(query) && _.has(query, "_content_type_uid")) {
            // maintain domain state, in case it deviates in promises or callbacks
            var domain_state = process.domain;
            // variable declarations
            var self = this,
                _query = _.clone(query, true),
                language = _query.locale,
                remove = _query._remove || false,
                includeReference = (typeof _query.include_references == 'undefined' || _query.include_references == true) ? true : false,
                options = {},
                collectionName,
                _projectionFields = {'_id': 0, '_lang': 0, '_content_type_uid': 0, '_uid': 0};

            // to remove the unwanted keys from query and create reference query
            _query = helper.filterQuery(_query);

            // explicitly set language
            _query._lang = language;

            switch(_query._content_type_uid) {
                case '_content_types': collectionName = 'content_types';
                    break;
                case '_routes': collectionName = 'routes';
                    break;
                case '_assets': collectionName = 'assets';
                    break;
                default: collectionName = 'entries';
                    break;
            }

            self.db
                .collection(collectionName)
                .findOne(_query, _projectionFields, function (error, data) {
                    process.domain = domain_state;
                    try {
                        if (error) {
                            return callback(error);
                        }
                        if(data) {
                            var __data = (!remove) ? {entry: _.clone(data, true)} : data;
                            // Checks if there is any References need to be included in the given Content stack entry
                            if (includeReference) {
                                self.includeReferences(__data, language, undefined, undefined, callback);
                            } else {
                                return callback(null, __data);
                            }
                        } else {
                            return callback(null, {entry: null});
                        }
                    } catch(error) {
                        return callback(error);
                    }
                });
        } else {
            throw new Error('Query parameter should be an Object and contains atleast _content_type_uid.');
        }
    } catch(error) {
        callback(error);
    }
};

/**
 * Function which is used to find the necessary data based on the given information
 * from MongoDB
 *
 * The value returned should be in the format:
 *
 *              {
 *                  entries:
 *                      [
 *                          {
 *                              [Object]
 *                          },
 *                          {
 *                              [Object]
 *                          },
 *                          ....
 *                      ]
 *              }
 *
 * @param {Object} query - Object which contains data to be queried with
 * @param {Object} options - Object which containts options for find operation
 * @param {Function} callback - Function which is called on this function completion
 */
MongodbStorage.prototype.find = function(query, options, callback) {
    try {
        if (!_.isEmpty(query) && typeof query == "object" && typeof options === "object" && _.has(query, '_content_type_uid')) {
            // maintain domain state, in case it deviates in promises or callbacks
            var domain_state = process.domain;
            var references = (_.isPlainObject(arguments[3]) && !_.isEmpty(arguments[3])) ? arguments[3] : {},
                parentID = (_.isString(arguments[4])) ? arguments[4] : undefined;

            var self = this,
                _query = _.cloneDeep(query) || {},
                _sort = options.sort || {'published_at': -1},
                contentTypeId = _query._content_type_uid,
                language = _query.locale,
                remove = _query._remove || false,
                includeReference = (typeof _query.include_references == 'undefined' || _query.include_references == true) ? true : false,
                calls = {},
                count = _query.include_count,
                collectionName,
                queryObject,
                _projectionFields = {'_id': 0, '_lang': 0, '_content_type_uid': 0, '_uid': 0};

            _query._lang = language;

            switch(contentTypeId) {
                case '_content_types': collectionName = 'content_types';
                    break;
                case '_routes': collectionName = 'routes';
                    break;
                case '_assets': collectionName = 'assets';
                    break;
                default: collectionName = 'entries';
                    break;
            }

            _query = helper.filterQuery(_query);

            // if its a find() for assets in _assets route
            if(!assetDownloadFlag && contentTypeId === assetRouteName) {
                var results = InMemory.get(language, contentTypeId, _query),
                    data = {assets: (results && results.length) ? results : []};
                callback(null, data);
            } else {
                queryObject = self.db.collection(collectionName).find(_query, _projectionFields).sort(options.sort || {"published_at": -1});

                if (options.limit) {
                    queryObject.limit(options.limit || 0);
                }

                if (options.skip) {
                    queryObject.skip(options.skip || 0);
                }

                calls['entries'] = function (_cb) {
                    queryObject.toArray(_cb);
                };

                if(count) {
                    calls['count'] = function (_cb) {
                        self.db.collection(collectionName).count(_query, _cb);
                    }
                }

                async.parallel(calls, function (err, result) {
                    try {
                        process.domain = domain_state;
                        if (err) {
                            throw err;
                        }
                        if (result) {
                            var _data = _.cloneDeep(result);
                            _data = (!remove) ? {"entries": _data} : _data
                            // Checks whether references need to be inlcuded in the content stack entry
                            if (includeReference) {
                                if (parentID) {
                                    if(_data.entries)
                                        _data = _data.entries;
                                    var tempResult = (!remove) ? _data.entries : _data;
                                    references[parentID] = references[parentID] || [];
                                    references[parentID] = _.uniq(references[parentID].concat(_.map(tempResult, "uid")));
                                }
                                self.includeReferences(_data, language, references, parentID, function (error, result) {
                                    if (error) {
                                        return callback(error);
                                    }
                                    return callback(null, result);
                                });
                            } else {
                                callback(null, _data);
                            }
                        } else {
                            var dummy = {entries: []};
                            if(count) dummy.count = 0;
                            return callback(null, ((!remove) ? dummy : []));
                        }
                    } catch(e) {
                        return callback(e, null);
                    }
                });
            }

        } else {
            throw new Error('query and options should be an object and query should have atleast _content_type_id.');
        }
    } catch (e) {
        callback(e, null);
    }

};

/**
 * Function which handles the count for the given query in given content type
 *
 * @param {Object} query - Object which contains data to be queried in the database
 * @param {Function} callback - Function which is called on completion
 */
MongodbStorage.prototype.count = function(query, callback) {
    try {
        if (query && typeof query === "object" && _.has(query, '_content_type_uid')) {
            var self = this,
                // contentTypeId = query._content_type_uid,
                options = { sort: { 'published_at': -1 } };

            query.include_references = false;
            query.include_count = false;

            self.find(query, options, function (err, data) {
                if (err) {
                    throw err;
                }

                callback(null, data.entries.length);
            });
        } else {
            throw new Error("Query parameter should be an object. and contains _content_type_uid");
        }

    } catch(e) {
        callback(e, null);
    }
};

/**
 * Function which removes an entry from the MongoDB database based on the given query
 *
 * @param {Object} query - Object which is used to select the document that needs to be removed
 * @param {Function} callback - Function which is called on completion
 */
MongodbStorage.prototype.remove = function(query, callback) {
    try {
        if (query && typeof query === "object" && _.has(query, '_content_type_uid')) {
            var self = this,
                language = query.locale,
                contentTypeId = query._content_type_uid,
                _query = _.cloneDeep(query),
                uid = _query._uid,
                collectionName;

            _query = helper.filterQuery(_query, true);
            _query._lang = language;

            switch(contentTypeId) {
                case '_content_types': collectionName = 'content_types';
                    break;
                case '_routes': collectionName = 'routes';
                    break;
                case '_assets': collectionName = 'assets';
                    break;
                default: collectionName = 'entries';
                    break;
            }

            // remove all entries belonging to the specified _uid (content type's uid)
            if (Object.keys(_query).length === 2 && contentTypeId && language) {
                self.db
                    .collection(collectionName)
                    .remove({"_content_type_uid": contentTypeId, "_lang": language}, function (error, result) {
                    if (error)
                        return callback(error, null);
                    InMemory.set(language, contentTypeId, null, []);
                    callback(null, 1);
                });
            } else if (contentTypeId) { /* Removes the specified content type from content_type collection */
                // Removes the Entry from the given collection
                self.db
                    .collection(collectionName)
                    .remove({
                        "_content_type_uid": contentTypeId,
                        "_uid": uid,
                        "_lang": language
                    }, function (error) {
                        if (error)
                            return callback(error, null);
                        InMemory.set(language, contentTypeId, _query._uid);
                        callback(null, 1);
                    });
            } else {
                callback(null, 0);
            }

        } else {
            throw new Error("Query parameter should be an object. and contains _content_type_uid");
        }
    } catch(e) {
        callback(e, null);
    }
};

// Exports the mongodbStorage instance
module.exports = exports = new MongodbStorage();
