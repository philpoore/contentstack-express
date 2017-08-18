/*!
 * contentstack-express
 * copyright (c) Built.io Contentstack
 * MIT Licensed
 */

'use strict';

/**
 * Module Dependencies.
 */
var path          = require('path'),
    util          = require('util'),
    events        = require('events').EventEmitter,
    _             = require('lodash'),
    async         = require('async'),
    config        = require('../../../config'),
    logger        = require('../../../logger'),
    helper        = require('../../helper'),
    _db           = require('./connection').connect,
    assetDwldFlag = config.get('assets.download'),
    del_keys      = ['created_by', 'updated_by', '_uid', '_data', 'include_references', '_remove'],
    // Common keys
    assetRoutes   = '_assets',
    cacheRoutes   = '_routes',
    entryRoutes   = '_entries',
    contentRoutes = '_content_types',
    // Not used
    ct            = '_content_type_uid',
    locale        = 'locale',
    uid           = 'uid',
    data          = '_data';

/**
 * Class which handles all the operations related to Mongodb
 */

var MongodbStorage = function() {
    var self = this;
    // Inherit methods from EventEmitter
    events.call(this);

    // Remove memory-leak warning about max listeners
    this.setMaxListeners(0);
    // Holds the Database connection object
    _db().then(function (connectionObjekt) {
        console.log('Connection to mongodb established!');
        self.db = connectionObjekt;
    });

    this.provider = 'MongoDB';
};

// Extend from EventEmitter to allow hooks to listen to stuff
util.inherits(MongodbStorage, events);


/**
 * Function which includes the references' content type entry
 * into the content type entry data
 *
 * @param {Object} data         - Data in which references need to be included
 * @param {String} _locale      - Contains the locale of the given Content Type
 * @param {Function} callback   - Function which is called upon completion
 */

// TODO: when this method fails, there's no error generated!
MongodbStorage.prototype.includeReferences = function (data, _locale, references, parentID, callback) {
    var self = this,
        calls = [];
    if (_.isEmpty(references)) references = {};
    var _includeReferences = function (data) {
        for (var _key in data) {
            if (data.uid) parentID = data.uid;
            if (typeof data[_key] === "object") {
                if (data[_key] && data[_key]["_content_type_id"]) {
                    calls.push(function (_key, data) {
                        return (function (_callback) {
                            var _uid = (data[_key]["_content_type_id"] === assetRoutes && data[_key]["values"] && typeof data[_key]["values"] === 'string') ? data[_key]["values"] : {"$in": data[_key]["values"]};
                            var query = {
                                    "_content_type_uid": data[_key]["_content_type_id"],
                                    "_uid": _uid,
                                    "locale": _locale,
                                    "_remove": true
                                },
                                _calls = [];
                            if (query._content_type_uid != assetRoutes) {
                                query["_uid"]["$in"] = _.filter(query["_uid"]["$in"], function (uid) {
                                    var flag = helper.checkCyclic(uid, references);
                                    return !flag;
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
 * Function which handles the insertion of the data into mongodb
 *
 * @param {Object} data         - data which need to be upserted into the database
 * @param {Function} callback   - Function which is used as a callback
 */

MongodbStorage.prototype.insert = function (data, callback) {
    try {
        var self = this,
            domain_state = process.domain,
            _data = _.cloneDeep(data);
        console.log('@mongodb insert', JSON.stringify(data));
        if (_.isPlainObject(_data) && _data._content_type_uid && _data._uid) {

            var collection = (_data._content_type_uid === contentRoutes || _data._content_type_uid === assetRoutes || _data._content_type_uid === _data.cacheRoutes) ? _data._content_type_uid: entryRoutes;

            if (_.has(_data, '_data')) {
                if(_data._uid)
                    _data.uid = _data._uid;
                if(_data.locale)
                    _data._data.locale = _data.locale;
                if(_data._content_type_uid)
                    _data._data._content_type_uid = _data._content_type_uid;
                _data = _.cloneDeep(_data._data);
            }
            // Omit unwanted keys
            _data = _.omit(_data, del_keys);

            // updating the references based on the new schema
            if (_data._content_type_uid === contentRoutes)
                _data = helper.findReferences(_data);

            self.db.collection(collection).count({
                '_content_type_uid': _data._content_type_uid,
                'uid': _data.uid,
                'locale': _data.locale
                }, function (error, result) {
                    if (error)
                        return callback(error, null);
                    if (result !== 0) {
                        // if process.domain expires, restore to previous state
                        process.domain = (process.domain) ? process.domain: domain_state;
                        return callback(new Error('Data already exists, use update instead of insert'), null);
                    } else {
                        self.db.collection(collection).insert(_data, function (error, result) {
                            if(error)
                                return callback(error, null);
                            // if process.domain expires, restore to previous state
                            process.domain = (process.domain) ? process.domain: domain_state;
                            return callback(null, 1);
                        });
                    }
                });
        } else {
            throw new Error('Data should be an object with at least content_type_id and _uid.');
        }
    } catch (error) {
        return callback(error, null);
    }
};


/**
 * Function which handles the insertion of data or updation if data already exists, into mongodb
 *
 * @param {Object} data         - data which need to be upserted into the database
 * @param {Function} callback   - Function which is used as a callback
 */

MongodbStorage.prototype.upsert = function (data, callback) {
    try {
        var self = this,
            domain_state = process.domain,
            _data = _.cloneDeep(data);
        console.log('@mongodb upsert', JSON.stringify(data));
        if (_.isPlainObject(_data) && _data._content_type_uid && _data._uid) {

            var collection = (_data._content_type_uid === contentRoutes || _data._content_type_uid === assetRoutes || _data._content_type_uid === cacheRoutes) ? _data._content_type_uid: entryRoutes;

            if (_.has(_data, '_data')) {
                if(_data._uid)
                    _data._data.uid = _data._uid;
                if(_data.locale)
                    _data._data.locale = _data.locale;
                if(_data._content_type_uid)
                    _data._data._content_type_uid = _data._content_type_uid;
                _data = _.cloneDeep(_data._data);
            }

            // Omit unwanted keys
            _data = _.omit(_data, del_keys);

            // updating the references based on the new schema
            if (collection === contentRoutes)
                _data = helper.findReferences(_data);

            // Performs an Upsert operation for the given data in mongodb
            // TODO: update is deprecated
            // TODO: $set is not ideal
            self.db.collection(collection).update({'_content_type_uid': _data._content_type_uid,
                'uid': _data.uid,
                'locale': _data.locale
                }, { "$set": _data },
                { "upsert": true }, function (error, result) {
                    if(error)
                        return callback(error, null);
                    // if process.domain expires, restore to previous state
                    process.domain = (process.domain) ? process.domain: domain_state;
                    callback(null, 1);
                });
        } else {
            throw new Error("data should be an object with at least content_type_id and _uid.");
        }
    } catch (error) {
        return callback(error, null);
    }
};


/**
 * Function which is used to find `document` based on the given query
 *
 * @param {Object} query        - Holds the query to find the data
 * @param {Function} callback   - Function which is going to be called on the completion
 */

MongodbStorage.prototype.findOne = function (query, callback) {
    try {
        var self = this,
            domain_state = process.domain,
            _query = _.cloneDeep(query);
        console.log('@query', query);
        if (_.isPlainObject(_query) && !_.isEmpty(_query) && _query._content_type_uid) {
            var remove = _query._remove || false,
                includeReferences = (typeof _query.include_references == 'undefined' || _query.include_references == true) ? true : false,
                collection = (_query._content_type_uid === contentRoutes || _query._content_type_uid === assetRoutes || _query._content_type_uid === cacheRoutes) ? _query._content_type_uid: entryRoutes,
                // TODO: add user custom projection
                projection = {'_id': 0};

            // Omit unwanted keys
            _query = _.omit(_query, del_keys);

            self.db.collection(collection).findOne(_query, projection, function (error, data) {
                // if process.domain expires, restore to previous state
                process.domain = domain_state;
                try {
                    if (error)
                        return callback(error, null);
                    if (data) {
                        var _data = (!remove) ? { entry: _.cloneDeep(data) } : data;
                        // Checks if there is any References need to be included in the given Content stack entry
                        if (includeReferences) {
                            self.includeReferences(_data, _query.locale, undefined, undefined, callback);
                        } else {
                            return callback(null, _data);
                        }
                    } else {
                        return callback(null, {entry: null});
                    }
                } catch (error) {
                    return callback(error, null);
                }
            });
        } else {
            throw new Error('Query parameter should be an Object and contains atleast _content_type_uid & _uid.');
        }
    } catch (error) {
        return callback(error);
    }
};


/**
 * Function which is used to find the necessary data based on the given information from MongoDB
 *
 * @param {Object} query        - Object which contains data to be queried with
 * @param {Object} options      - Object which containts options for find operation
 * @param {Function} callback   - Function which is called on this function completion
 */

MongodbStorage.prototype.find = function (query, options, callback) {
    try {
        var self = this,
            domain_state = process.domain,
            _query = _.cloneDeep(query);
        console.log('@mongo find _query', _query);

        if (_.isPlainObject(_query) && !_.isEmpty(_query) && _.isPlainObject(options) && _query._content_type_uid) {
            var references = (_.isPlainObject(arguments[3]) && !_.isEmpty(arguments[3])) ? arguments[3] : {},
                parentID = (_.isString(arguments[4])) ? arguments[4] : undefined,
                // TODO: make sure options.sort doesn't have _data
                _sort = options.sort || { 'published_at': -1 },
                remove = _query._remove || false,
                count = _query.include_count,
                // TODO: add user's custom projections option
                projection = { '_id': 0},
                includeReferences = (_query.include_references === true || typeof _query.include_references === 'undefined') ? true : false,
                collection = (_query._content_type_uid === contentRoutes || _query._content_type_uid === assetRoutes || _query._content_type_uid === cacheRoutes) ? _query._content_type_uid: entryRoutes;

            // Delete unwanted keys
            _query = _.omit(_query, del_keys);

            self.db.collection(collection)
                .find(_query, projection)
                .sort(options.sort || { 'published_at': -1 })
                .limit(options.limit || 0)
                .skip(options.skip || 0)
                .toArray(function (error, objects) {
                    try {
                        // If process.domain expires, restore to previous state
                        process.domain = (process.domain) ? process.domain: domain_state;
                        if (error)
                            throw error;
                        if (objects && objects.length) {
                            if (count) objects.count = objects.length;
                            // If the call is for assets
                            if (_query._content_type_uid === assetRoutes && !assetDwldFlag) {
                                objects = { assets: objects };
                                return callback(null, objects);
                            }
                            // If the call's for entries
                            objects = (!remove) ? { entries: objects }: objects;

                            // Checks whether references need to be inlcuded in the content stack entry
                            if (includeReferences) {
                                if (parentID) {
                                    var tmp = (!remove) ? objects.entries: objects;
                                    references[parentID] = references[parentID] || [];
                                    references[parentID] = _.uniq(references[parentID].concat(_.map(tmp, "uid")));
                                }
                                self.includeReferences(objects, _query.locale, references, parentID, function (error, result) {
                                    if (error)
                                        return callback(error, null);
                                    return callback(null, result);
                                });
                            } else {
                                return callback(null, objects);
                            }
                        } else {
                            if (count) objects.count = 0;
                            // Queried entry | asset is empty
                            if (_query._content_type_uid === assetRoutes && !assetDwldFlag)
                                objects = { assets: [] };
                            else
                                objects = (!remove) ? { entries: [] }: []; // TODO: Check if this results into an error
                            return callback(null, objects);
                        }
                    } catch (error) {
                        return callback(error, null);
                    }
                });
        } else {
            throw new Error('Query and options should be an object and query should have atleast _content_type_id.');
        }
    } catch (error) {
        callback(error, null);
    }
};


/**
 * Function which handles the count for the given query in given content type
 *
 * @param {Object} query        - Object which contains data to be queried in the database
 * @param {Function} callback   - Function which is called on completion
 */

MongodbStorage.prototype.count = function (query, callback) {
    try {
        var self = this,
            domain_state = process.domain,
            _query = _.cloneDeep(query);

        if (_.isPlainObject(_query) && _query._content_type_uid) {
            var options = { sort: { 'published_at': -1 } };

            _query.include_references = false,
            _query.include_count = false;

            self.find(_query, options, function (error, data) {
                if (error)
                    throw error;
                return callback(null, data.entries.length);
            });
        } else {
            throw new Error('Query parameter should be an object and contain _content_type_uid');
        }
    } catch (error) {
        return callback(error, null);
    }
};


/**
 * Function which removes an entry from the Mongodb based on the given query
 *
 * @param {Object} query        - Object which is used to select the document that needs to be removed
 * @param {Function} callback   - Function which is called on completion
 */

MongodbStorage.prototype.remove = function (query, callback) {
    try {
        var self = this,
            domain_state = process.domain,
            _query = _.cloneDeep(query);
        console.log('@mongodb remove', query);
        if (_.isPlainObject(_query) && _query._content_type_uid) {
            var collection = (_query._content_type_uid === contentRoutes || _query._content_type_uid === assetRoutes || _query._content_type_uid === cacheRoutes) ? _query._content_type_uid: entryRoutes,
                queryLength = Object.keys(_query).length;

            // remove all entries & routes belonging to the specified _uid (content type's uid)
            if (queryLength === 2 && _query._content_type_uid && _query.locale) {
                self.db.collection(collection).remove({'_content_type_uid': _query._content_type_uid, 'locale': _query.locale}, function (error, result) {
                    if (error)
                        return callback(error, null);
                    return callback(null, 1);
                });
            } else if (queryLength === 3 && _query._content_type_uid && _query.locale && _query._uid) {
                // Removes the Entry from the given collection
                self.db.collection(collection).remove({'_content_type_uid': _query._content_type_uid, 'uid': _query._uid, 'locale': _query.locale }, function (error) {
                    if (error)
                        return callback(error, null);
                    return callback(null, 1);
                });
            } else {
                return callback(null, 0);
            }
        } else {
            throw new Error('Query parameter should be an object and contain _content_type_uid');
        }
    } catch(error) {
        return callback(error, null);
    }
};

// Exports the mongodbStorage instance
module.exports = exports = new MongodbStorage();