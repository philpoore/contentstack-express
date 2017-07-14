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
    sift = require('sift'),
    utils = require('../../../utils'),
    InMemory = require('../../../utils/db/inmemory');

var context = utils.context,
    config = utils.config,
    languages = config.get('languages'),
    assetRoute = '_assets',
    provider = config.get('storage.provider'),
    _db;
// load custom db
if(provider !== 'FileSystem' && provider !== 'nedb' && provider !== 'cdn') {
    _db = require(path.join(config.get('path.base'), 'providers', provider.toLowerCase(), 'partials'));
} else if (provider === 'cdn') {
    _db = require('../../../utils/db/providers');
}

module.exports = (function () {
// To get the partials
    // var get = function (partial, limit, language, include_references) {
    //     return new Promise(function (resolve, reject) {
    //         find({"content_type": partial, "language": language, "include_references": include_references}).then(function (data) {
    //             var entry;
    //             if(data) {
    //                 if(provider === 'FileSystem' || provider === 'nedb')
    //                     entry = _.map(_.clone(data, true), '_data');
    //                 else
    //                     entry = data; /* Custom providers */
    //                 if(include_references && (provider === 'FileSystem' || provider === 'nedb' && provider !== 'cdn'))
    //                     entry = includeReferences(entry, language, references);
    //             }

    //             if (entry && entry.length)
    //                 entry = (limit == 1) ? entry[0] : entry.slice(0, limit);
    //             resolve(entry);
    //         }).catch(function (error) {
    //             reject(error);
    //         });
    //     })
    // };


    function find(query) {
        var references = (_.isPlainObject(arguments[1]) && !_.isEmpty(arguments[1])) ? arguments[1] : {};
        var _query = _.cloneDeep(query);
        return new Promise(function (resolve, reject) {
            var references = (_.isPlainObject(arguments[1]) && !_.isEmpty(arguments[1])) ? arguments[1] : {};
            var __data,
                contentTypeUid = _query.content_type,
                language = _query.language,
                data = InMemory.get(language, contentTypeUid, {}, true),
                include_references = (typeof _query.include_references === 'boolean') ? _query.include_references : true,
                query = filterQuery(_query),
                contentPath;
            if(provider === 'FileSystem' || provider === 'nedb')
                contentPath = (contentTypeUid === assetRoute) ? path.join.call(null, getAssetPath(language), contentTypeUid + '.json') : path.join.call(null, getContentPath(language), contentTypeUid + '.json');

            if(data) {
                data = sift(query, data);
                data = _.map(_.clone(data, true), '_data');
                data = includeReferences(data, language, references);
                resolve(data);
            } else if((provider === 'FileSystem' || provider === 'nedb') && fs.existsSync(contentPath)) {
                var model = JSON.parse(fs.readFileSync(contentPath, 'utf-8'));
                // do we need to do this?!
                InMemory.set(language, contentTypeUid, null, data, true);
                data = sift(query, model);
                data = _.map(_.clone(data, true), '_data');
                data = includeReferences(data, language, references);
                resolve(data);
            } else if (provider === 'cdn') {
                var query = {
                    locale: language,
                    include_references: true,
                    _content_type_uid: contentTypeUid
                };
                _db.find(query, {}, function (error, result) {
                    if(error)
                        reject(error);
                    data = result.entries;
                    resolve(data);
                })
            } else if(typeof provider === 'string') {
                // Queries the custom database for the given Content Type UID
                var query = {
                    locale: language,
                    include_references: include_references,
                    _content_type_uid: contentTypeUid
                };
                _db(contentTypeUid, query).then(function (data) {
                    // update inmemory with the fetched data
                    InMemory.set(language, contentTypeUid, null, data, true);
                    data = includeReferences(data, language, references);
                    resolve(data);
                });
            }
        })
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

    function getRelativePrefix(langCode) {
        var idx = _.findIndex(languages, {"code": langCode});
        if(~idx) {
            return languages[idx]['relative_url_prefix'];
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