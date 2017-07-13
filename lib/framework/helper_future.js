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
    deasync = require('deasync'),
    utils = require('./../utils'),
    InMemory = require('./../utils/db/inmemory');

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
    _db = require('../utils/db/providers');
}

module.exports = function(app) {
    // To get the partials
    app.locals.get = function(partial, limit, language, includeReference) {
        var language = language || context.get('lang'),
            limit = limit || 1,
            entry = find({"content_type": partial, "language": language, "include_references": includeReference});
        if (entry && entry.length)
            entry = (limit == 1) ? entry[0] : entry.slice(0, limit);
        return entry;
    };

    // get the asset url
    app.locals.getAssetUrl = function(asset) {
        return (asset && asset._internal_url) ? encodeURI(asset._internal_url) : "";
    };

    // To get the current url
    app.locals.getUrl = function(url) {
        var lang = context.get('lang'),
            prefix = getRelativePrefix(lang).slice(0, -1);
        url =  prefix + ((!url) ? context.get('entry').url : url);
        return url;
    };

    // To get the title of the current page
    app.locals.getTitle = function() {
        return context.get('entry').title;
    };
};

function find(query) {
    var references = (_.isPlainObject(arguments[1]) && !_.isEmpty(arguments[1])) ? arguments[1] : {};

    var __data,
        contentTypeUid = query.content_type,
        language = query.language,
        data = InMemory.get(language, contentTypeUid, {}, true),
        include_references = (typeof query.include_references === 'boolean') ? query.include_references : true,
        query = filterQuery(query),
        contentPath;
    if(provider === 'FileSystem' || provider === 'nedb')
        contentPath = (contentTypeUid === assetRoute) ? path.join.call(null, getAssetPath(language), contentTypeUid + '.json') : path.join.call(null, getContentPath(language), contentTypeUid + '.json');
    if(data) {
        data = sift(query, data);
    } else if((provider === 'FileSystem' || provider === 'nedb') && fs.existsSync(contentPath)) {
        var model = JSON.parse(fs.readFileSync(contentPath, 'utf-8'));
        InMemory.set(language, contentTypeUid, null, data, true);
        data = sift(query, model);
    } else if (provider === 'cdn') {
        var query = {
            locale: language,
            include_references: true,
            _content_type_uid: contentTypeUid
        }, done = false;
        _db.find(query, {}, function (error, result) {
            if(error)
                throw error;
            data = result.entries;
            InMemory.set(language, contentTypeUid, null, data, true);
            done = true;
        })
        deasync.loopWhile(function () {
            return !done;
        })
    } else if(typeof provider === 'string') {
        // Queries the custom database for the given Content Type UID
        var query = {
            locale: language,
            include_references: include_references,
            _content_type_uid: contentTypeUid
        };
        data = _db(contentTypeUid, query);
        // update inmemory with the fetched data
        InMemory.set(language, contentTypeUid, null, data, true);
    }
    if(data) {
        if(provider === 'FileSystem' || provider === 'nedb')
            __data = _.map(_.clone(data, true), '_data');
        else if (provider === 'cdn')
            return data;
        else
            __data = data; /* Custom providers */
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