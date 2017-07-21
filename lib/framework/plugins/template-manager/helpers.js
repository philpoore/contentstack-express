/*!
 * contentstack-express
 * copyright (c) Built.io Contentstack
 * MIT Licensed
 */

'use strict';

/*!
 * Module dependencies
 */
var fs = require('graceful-fs'),
    path = require('path'),
    _ = require('lodash'),
    async = require('async'),
    sift = require('sift'),
    utils = require('../../../utils'),
    InMemory = require('../../../utils/db/inmemory');

var context = utils.context,
    config = utils.config,
    languages = config.get('languages'),
    assetRoute = '_assets',
    provider = config.get('storage.provider'),
    _db = require('../../../utils/db/providers');

module.exports = (function () {
    // To get the partials | split into 2, find() and findAsync()
    function get (partial, lim, locale, includeReference) {
        return new Promise(function (resolve, reject) {
            var language = locale || context.get('lang'),
                limit = lim || 1,
                data = InMemory.get(language, partial, {}, true);
            if(data || provider === 'FileSystem' || provider === 'nedb') {
                if(!data)
                    var entry = find({"content_type": partial, "language": language, "include_references": includeReference});
                if (entry && entry.length)
                    entry = (limit == 1) ? entry[0] : entry.slice(0, limit);
                return resolve(entry);
            } else {
                findAsync({"_content_type_uid": partial, "locale": language, "include_references": includeReference}).then(function (entry) {
                    if (entry && entry.length)
                        entry = (limit == 1) ? entry[0] : entry.slice(0, limit);
                    return resolve(entry);
                });
            }
        })
    };

    function findAsync (query) {
        return new Promise(function (resolve, reject) {
            _db.find(query, {}, function (error, result) {
                if(error)
                    reject(error);
                resolve(result.entries);
            });
        })
    }

    function find(query) {
        var references = (_.isPlainObject(arguments[1]) && !_.isEmpty(arguments[1])) ? arguments[1] : {};
        var __data,
            contentTypeUid = query.content_type,
            contentPath = (contentTypeUid === assetRoute) ? path.join.call(null, getAssetPath(query.language), contentTypeUid + '.json') : path.join.call(null, getContentPath(query.language), contentTypeUid + '.json'),
            language = query.language,
            data = InMemory.get(language, contentTypeUid, {}, true),
            include_references = (typeof query.include_references === 'boolean') ? query.include_references : true,
            query = filterQuery(query);
        if(data) {
            data = sift(query, data);
        } else if(fs.existsSync(contentPath)) {
            var model = JSON.parse(fs.readFileSync(contentPath, 'utf-8'));
            InMemory.set(language, contentTypeUid, null, data, true);
            data = sift(query, model);
        }

        if(data) {
            __data = _.map(_.cloneDeep(data), '_data');
            if(include_references) __data = includeReferences(__data, language, references);
        }
        return __data;
    };

    function includeReferences(data, language, references) {
        if (_.isEmpty(references)) references = {};
        var flag = false;
        var _includeReferences = function (data, parentID) {
            for (var _key in data) {
                if ((_.isUndefined(parentID) || _.isNull(parentID)) && data && data.uid) parentID = data.uid
                if (typeof data[_key] === "object") {
                    if (data[_key] && data[_key]["_content_type_id"]) {
                        flag = true;
                        references[parentID] = references[parentID] || [];
                        references[parentID] = _.uniq(references[parentID].concat(data[_key]["values"]));
                        var _uid = (data[_key]["_content_type_id"] == assetRoute && data[_key]["values"] && typeof data[_key]["values"] === 'string') ? data[_key]["values"] : {"$in": data[_key]["values"]};
                        var _query = {"content_type": data[_key]["_content_type_id"], "_uid": _uid, "language": language};

                        if(_query.content_type != assetRoute) {
                            _query["_uid"]["$in"] = _.filter(_query["_uid"]["$in"], function (uid) {
                                var flag = checkCyclic(uid, references)
                                return !flag
                            });
                        }
                        var _data = find(_query, references);
                        data[_key] = (_query["_uid"]["$in"] && _data) ? (_data.length) ? _data : [] : (_data && _data.length) ?  _data[0] : {};
                    } else {
                        _includeReferences(data[_key], parentID);
                    }
                }
            }
        };

        var recursive = function (data) {
            _includeReferences(data);
            if (flag) {
                flag = false;
                return setImmediate(function () {
                    return recursive(data);
                });
            }
        };

        try {
            recursive(data);
        } catch (e) {
            console.error("View-Helper Include Reference Error: ", e.message);
        }
        return data;
    };

    function getContentPath(langCode) {
        var idx = _.findIndex(languages, {"code": langCode});
        if(~idx) {
            return languages[idx]['contentPath'];
        } else {
            console.error("Language doesn't exists");
        }
    };

    function getAssetPath(langCode) {
        var idx = _.findIndex(languages, {"code": langCode});
        if(~idx) {
            return languages[idx]['assetsPath'];
        } else {
            console.error("Language doesn't exists");
        }
    };

    function filterQuery(_query) {
        var keys = ['include_references', 'language', 'content_type'];
        for(var i = 0, total = keys.length; i < total; i++) {
            delete _query[keys[i]];
        }
        return _query;
    };

    function checkCyclic (uid, mapping) {
        var flag = false
        var list = [uid]
        var getParents = function (child) {
            var parents = []
            for(var key in mapping) {
                if(~mapping[key].indexOf(child)) {
                    parents.push(key)
                }
            }
            return parents
        }
        for(var i = 0; i < list.length; i++)    {
            var parent = getParents(list[i])
            if(~parent.indexOf(uid)) {
                flag = true
                break
            }
            list = _.uniq(list.concat(parent))

        }
        return flag
    }
    return {
        get
    }
})();