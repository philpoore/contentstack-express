/*!
 * contentstack-express
 * copyright (c) Built.io Contentstack
 * MIT Licensed
 */

'use strict';

/**
 * Module Dependencies.
 */
var sift            = require('sift'),
    path            = require('path'),
    fs              = require('graceful-fs'),
    events          = require('events').EventEmitter,
    util            = require('util'),
    _               = require('lodash'),
    async           = require('async'),
    config          = require('../../config'),
    helper          = require('../helper'),
    InMemory        = require('../inmemory'),
    languages       = config.get('languages'),
    assetDwldFlag   = config.get('assets.download'),
    assetRoute      = '_assets',
    entryRoute      = '_routes',
    schemaRoute     = '_content_types';

var fileStorage = function () {
    // Inherit methods from EventEmitter
    events.call(this);
    // Remove memory-leak warning about max listeners
    this.setMaxListeners(0);
    // Keep track of spawned child processes
    this.childProcesses = [];
    this.isEndsWith = function (str, suffix) {
        return str.indexOf(suffix, str.length - suffix.length) !== -1;
    };
};

// Extend from base provider
util.inherits(fileStorage, events);

// include references
fileStorage.prototype.includeReferences = function (data, _locale, references, parentID, callback) {
    var self = this,
        calls = [];
    if (_.isEmpty(references)) references = {};
    var _includeReferences = function (data) {
        for (var _key in data) {
            if (data.uid) parentID = data.uid;
            if (_.isPlainObject(data[_key])) {
                if (data[_key] && data[_key]["_content_type_id"]) {
                    calls.push(function (_key, data) {
                        return (function (_callback) {
                            var _uid = (data[_key]["_content_type_id"] === assetRoute && data[_key]["values"] && typeof data[_key]["values"] === 'string') ? data[_key]["values"] : {"$in": data[_key]["values"]};
                            var query = {
                                    "_content_type_uid": data[_key]["_content_type_id"],
                                    "_uid": _uid,
                                    "locale": _locale,
                                    "_remove": true
                                },
                                _calls = [];
                            if (query._content_type_uid !== assetRoute) {
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
                                    }, _.cloneDeep(references), parentID);
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

// find single entry
fileStorage.prototype.findOne = function (query, callback) {
    try {
        if (_.isPlainObject(query) && !_.isEmpty(query)) {
            var self = this,
                _query = _.cloneDeep(query),
                language = _query.locale,
                model = helper.getContentPath(language),
                remove = _query._remove || false,
                includeReference = (typeof _query.include_references === 'undefined' || _query.include_references === true) ? true : false,
                contentTypeId = _query._content_type_uid,
                jsonPath = (contentTypeId) ? path.join(model, contentTypeId + ".json") : undefined;

            // Delete unwanted keys
            // TODO: use _.omit instead
            _query = helper.filterQuery(_query);

            if (jsonPath && fs.existsSync(jsonPath)) {
                fs.readFile(jsonPath, 'utf-8', function (error, models) {
                    try {
                        if (error) throw error;
                        var data;
                        models = JSON.parse(models);
                        if (models && models.length) data = sift(_query, models);
                        if (data && data.length) {
                            var _data = (remove) ? data[0] : data[0]._data,
                                __data = (!remove) ? {entry: _.cloneDeep(_data)} : _data;
                            if (includeReference) {
                                self.includeReferences(__data, language, undefined, undefined, callback);
                            } else {
                                return callback(null, __data);
                            }
                        } else {
                            return callback(null, { entry: null });
                        }
                    } catch (error) {
                        return callback(error);
                    }
                });
            } else {
                // Throws error
                helper.generateCTNotFound(language, query._content_type_uid);
            }
        } else {
            throw new Error('Query parameter should be an object & not empty');
        }
    } catch (error) {
        return callback(error, null);
    }
};

// find and sort(optional) entries using query
fileStorage.prototype.find = function (query, options, callback) {
    try {
        var references = (_.isPlainObject(arguments[3]) && !_.isEmpty(arguments[3])) ? arguments[3]: {},
        parentID = (_.isString(arguments[4])) ? arguments[4] : undefined;

        if (_.isPlainObject(query) && !_.isEmpty(query) && _.isPlainObject(options)) {
            var self = this,
                _query = _.cloneDeep(query) || {},
                sort = options.sort || {'_data.published_at': -1},
                language = _query.locale,
                remove = _query._remove || false,
                _count = _query.include_count || false,
                includeReference = (typeof _query.include_references === 'undefined' || _query.include_references === true) ? true : false,
                model = helper.getContentPath(language),
                jsonPath = (_query._content_type_uid) ? path.join(model, _query._content_type_uid + ".json") : undefined;

            // Delete unwanted keys
            // TODO: use _.omit instead
            _query = helper.filterQuery(_query);

            if (jsonPath && fs.existsSync(jsonPath)) {
                fs.readFile(jsonPath, 'utf-8', function (error, models) {
                    try {
                        if (error) throw error;
                        models = JSON.parse(models);
                        if (models && models.length) models = sift(_query, models);
                        var _data = _.map(models, "_data") || [],
                            __data;

                        /* Sorting Logic */
                        var keys = Object.keys(sort),
                            __sort = {keys: [], order: []};
                        for (var i = 0, total = keys.length; i < total; i++) {
                            var __order = (sort[keys[i]] === 1) ? 'asc' : 'desc';
                            // removing the _data. key to make the default sorting work
                            __sort.keys.push(keys[i].replace('_data.', ''));
                            __sort.order.push(__order);
                        }
                        _data = _.sortByOrder(_data, __sort.keys, __sort.order);
                        /* Sorting Logic */

                        if (options.limit) {
                            options.skip = options.skip || 0;
                            _data = _data.splice(options.skip, options.limit);
                        } else if (options.skip > 0) {
                            _data = _data.slice(options.skip);
                        }
                        __data = (!remove) ? {"entries": _data} : _data
                        if (_count) __data.count = _data.length;
                        if (includeReference) {
                            if (parentID) {
                                var tempResult = (!remove) ? __data.entries : __data;
                                references[parentID] = references[parentID] || [];
                                references[parentID] = _.uniq(references[parentID].concat(_.map(tempResult, "uid")));
                            }
                            self.includeReferences(__data, language, references, parentID, function (error, results) {
                                if (error) {
                                    return callback(error);
                                } else {
                                    return callback(null, results);
                                }
                            });
                        } else {
                            return callback(null, __data);
                        }
                    } catch (error) {
                        return callback(error, null);
                    }
                });
            } else if (!assetDwldFlag && _query._content_type_uid === assetRoute) {
                var results = InMemory.get(language, _query._content_type_uid, _query),
                    data = { assets: (results && results.length) ? results: [] };
                return callback(null, data);
            } else {
                // Throws error
                helper.generateCTNotFound(language, query._content_type_uid);
            }
        } else {
            throw new Error('Query & options parameter should be of type `object` and `query` object should not be empty!');
        }
    } catch (error) {
        return callback(error, null);
    }
};

// find entries count
fileStorage.prototype.count = function (query, callback) {
    try {
        if (_.isPlainObject(query) && !_.isEmpty(query)) {
            // adding the include_references just to get the count
            query.include_references = false;
            query.include_count = false;
            this.find(query, {sort: {'_data.published_at': -1}}, function (error, data) {
                try {
                    if (error) throw error;
                    return callback(null, { entries: data.entries.length });
                } catch (error) {
                    return callback(error, null);
                }
            });
        } else {
            throw new Error('Query parameter should be an object & not empty');
        }
    } catch (error) {
        return callback(error, null);
    }
};

// add entry in to db
fileStorage.prototype.insert = function (data, callback) {
    try {
        if (_.isPlainObject(data) && !_.isEmpty(data) && data._content_type_uid && data._uid) {
            var language = data.locale,
                contentTypeId = data._content_type_uid,
                model = (contentTypeId !== assetRoute) ? helper.getContentPath(language): helper.getAssetPath(language),
                jsonPath = path.join(model, contentTypeId + ".json"),
                entries = [];

            // Delete unwanted keys
            // TODO: use _.omit instead
            data = helper.filterQuery(data, true);

            var _callback = function (_entries) {
                fs.writeFile(jsonPath, JSON.stringify(_entries), function (err) {
                    if (err) throw err;
                    callback(null, 1);
                });
            };

            // updating the references based on the new schema
            // ~Modified :: findReferences method now accepts parent key
            if (contentTypeId === schemaRoute) data['_data'] = helper.findReferences(data['_data'], '_data');
            if (fs.existsSync(jsonPath)) {
                fs.readFile(jsonPath, 'utf-8', function (error, entries) {
                    if (error) throw error;
                    entries = JSON.parse(entries) || [];
                    var idx = _.findIndex(entries, { '_uid': data._uid });
                    if (~idx) {
                        callback(new Error("Data already exists, use update instead of insert."), null);
                    } else {
                        InMemory.set(language, contentTypeId, data._uid, data);
                        entries.unshift(data);
                        return _callback(entries);
                    }
                });
            } else {
                InMemory.set(language, contentTypeId, data._uid, data);
                return _callback([data]);
            }
        } else {
            throw new Error('Data should be an object with at least `content_type_id` and `_uid`');
        }
    } catch (error) {
        return callback(error, null);
    }
};

// find entry, if found then update else insert
fileStorage.prototype.upsert = function (data, callback) {
    try {
        if (_.isPlainObject(data) && !_.isEmpty(data) && data._content_type_uid && data._uid) {
            var entries = [],
                _query = _.cloneDeep(data),
                contentTypeId = _query._content_type_uid,
                language = _query.locale,
                model = (contentTypeId !== assetRoute) ? helper.getContentPath(language): helper.getAssetPath(language),
                jsonPath = path.join(model, contentTypeId + ".json");

            // to remove the unwanted keys from query/data and create reference query
            _query = helper.filterQuery(_query, true);

            var _callback = function (__data) {
                fs.writeFile(jsonPath, JSON.stringify(__data), function (err) {
                    if (err) throw err;
                    callback(null, 1);
                });
            };

            // updating the references based on the new schema
            // ~Modified :: findReferences method now accepts parent key
            if (contentTypeId === schemaRoute) _query['_data'] = helper.findReferences(_query['_data'], '_data');

            if (fs.existsSync(jsonPath)) {
                fs.readFile(jsonPath, 'utf-8', function (error, entries) {
                    if (error) throw error;
                    entries = JSON.parse(entries);
                    var idx = _.findIndex(entries, { '_uid': _query._uid });
                    if (idx !== -1) entries.splice(idx, 1);
                    entries.unshift(_query);
                    InMemory.set(language, contentTypeId, _query._uid, _query);
                    return _callback(entries);
                });
            } else {
                InMemory.set(language, contentTypeId, _query._uid, _query);
                return _callback([_query]);
            }
        } else {
            throw new Error('Data should be an object with at least `content_type_id` and `_uid`');
        }
    } catch (error) {
        return callback(error, null);
    }
};

// delete entry from db
fileStorage.prototype.bulkInsert = function (query, callback) {
    try {
        if (_.isPlainObject(query) && !_.isEmpty(query) && query._content_type_uid && query.entries) {
            var entries = query.entries || [],
                contentTypeId = query._content_type_uid,
                language = query.locale,
                model = helper.getContentPath(language),
                jsonPath = path.join(model, contentTypeId + ".json"),
                _entries = [];

            for (var i = 0, total = entries.length; i < total; i++) {
                _entries.push({
                    _data: entries[i],
                    _content_type_uid: contentTypeId,
                    _uid: entries[i]['uid'] || entries[i]['entry']['uid'] // entry is just for the _routes
                });
            }

            fs.writeFile(jsonPath, JSON.stringify(_entries), function (error) {
                if (error) throw error;
                InMemory.set(language, contentTypeId, null, _entries);
                return callback(null, 1);
            });
        } else {
            throw new Error('Query should be an object with at least `content_type_id` and `entries`');
        }
    } catch (error) {
        return callback(error, null);
    }
};

// delete entry from db
fileStorage.prototype.remove = function (query, callback) {
    try {
        if (_.isPlainObject(query) && !_.isEmpty(query)) {
            var language = query.locale,
                contentTypeId = query._content_type_uid,
                model = (contentTypeId !== assetRoute) ? helper.getContentPath(language): helper.getAssetPath(language),
                jsonPath = path.join(model, contentTypeId + ".json"),
                _query = _.cloneDeep(query);

            // if the object to be removed does not exist in that path
            if (!fs.existsSync(jsonPath)) {
                return callback(null, 1);
            } else {
                // Removing Content Type object
                if (Object.keys(_query).length === 2 && contentTypeId && language) {
                    fs.unlink(jsonPath, function (error) {
                        if (error) throw error;
                        InMemory.set(language, contentTypeId, null, []);

                        // Removing the specified 'content_type' uid from '_routes'
                        var _q = {
                            _content_type_uid: entryRoute,
                            locale: language
                        };
                        // Fetch from InMemory || eval disadvantages of getting from Inmemory
                        self.find(_q, {}, function (error, result) {
                            if(result && result.entries && result.entries.length > 0) {
                                _q.entries = _.reject(result.entries, {content_type: { uid: contentTypeId}});
                                self.bulkInsert(_q, callback);
                            } else {
                                return callback(null, 1);
                            }
                        });

                    });
                } else if (contentTypeId) {
                    var idx, entries, idxData;
                    // remove the unwanted keys from query/data
                    _query = helper.filterQuery(_query);

                    fs.readFile(jsonPath, 'utf-8', function (error, entries) {
                        if (error) throw error;
                        entries = JSON.parse(entries);
                        idx = _.findIndex(entries, {"_uid": _query._uid});
                        if (~idx) entries.splice(idx, 1);
                        fs.writeFile(jsonPath, JSON.stringify(entries), function (err) {
                            if (err) throw err;
                            InMemory.set(language, contentTypeId, _query._uid);
                            return callback(null, 1);
                        });
                    });
                } else {
                    return callback(null, 0);
                }
            }
        } else {
            throw new Error('Query parameter should be an `object` and not empty!');
        }
    } catch (error) {
        return callback(error, null);
    }
};

// custom sort function
fileStorage.prototype.sortByKey = function (array, key, asc) {
    var _keys = key.split('.'),
        len = _keys.length;

    return array.sort(function (a, b) {
        var x = a, y = b;
        for (var i = 0; i < len; i++) {
            x = x[_keys[i]];
            y = y[_keys[i]];
        }
        if (asc) {
            return ((x < y) ? -1 : ((x > y) ? 1 : 0));
        }
        return ((y < x) ? -1 : ((y > x) ? 1 : 0));
    });
};

exports = module.exports = new fileStorage();
