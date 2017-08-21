/*!
 * contentstack-express
 * copyright (c) Built.io Contentstack
 * MIT Licensed
 */

'use strict';

/**
 * Module Dependencies.
 */

var _           = require('lodash'),
    fs          = require('graceful-fs'),
    sift        = require('sift'),
    path        = require('path'),
    async       = require('async'),
    helper      = require('./../helper'),
    config      = require('./../../config/index'),
    languages   = config.get('languages'),
    contentTypeName = "_content_types",
    entryRoutesName = "_routes",
    assetRouteName  = "_assets";

function Inmemory() {
    this.cache = null;
    if(!this.cache) {
        setTimeout(function() {
            this.reload();
        }.bind(this), 0);
    }
    this._inmemory = config.get("indexes") || {};
    // binding the methods
    this.reload = _.bind(this.reload, this);
}

Inmemory.prototype.reload = function () {
    try {
        // if _inmemory config set then only load the entries in the cache
        if(this._inmemory) {
            var calls = {},
                self = this;
            this.cache = this.cache || {};
            // loading the data firstime in the system
            for (var l = 0, lTotal = languages.length; l < lTotal;l++) {
                calls[languages[l]['code']] = (function (language) {
                    return function (cb) {
                        var model = language.contentPath,
                            assets  = language.assetsPath,
                            results = {},
                            assetPath = (assets && fs.existsSync(assets)) ? path.join(assets, assetRouteName + ".json"): null;
                        //load all the assets
                        results[assetRouteName] = (assetPath && fs.existsSync(assetPath)) ? JSON.parse(fs.readFileSync(assetPath, 'utf-8')) : [];

                        if(fs.existsSync(model)) {
                            var loadDatabase = {},
                                contentTypePath = path.join(model, contentTypeName + ".json");
                            // setting content_types, assets and entry routes in the memory
                            results[contentTypeName] = [];
                            results[entryRoutesName] = [];
                            // load all the _content_types and load the entries then
                            if(fs.existsSync(contentTypePath)) {
                                var schema = JSON.parse(fs.readFileSync(contentTypePath, 'utf-8'));
                                results[contentTypeName] = helper.filterSchema(JSON.parse(fs.readFileSync(contentTypePath, 'utf-8')), false, '_data');
                                contentTypePath = path.join(model, entryRoutesName + ".json");
                                if(fs.existsSync(contentTypePath)) results[entryRoutesName] = JSON.parse(fs.readFileSync(contentTypePath, 'utf-8'));
                                for(var ctuid in self._inmemory) {
                                    var formIndex = _.findIndex(results[contentTypeName], {"_uid": ctuid}),
                                        form;
                                    // filter the data based on the form schema that has been provided
                                    if(~formIndex) form = results[contentTypeName][formIndex]['_data'];

                                    contentTypePath = path.join(model, ctuid + ".json");

                                    if(fs.existsSync(contentTypePath) && form) {
                                        loadDatabase[ctuid] = (function (filePath, form) {
                                            return function (_cb) {
                                                fs.readFile(filePath, 'utf-8', function (err, data) {
                                                    if (err) {
                                                        _cb(err);
                                                    } else {
                                                        // get the form_id for the form schema
                                                        _cb(null, helper.filterEntries(form.uid, self._inmemory[form.uid], JSON.parse(data), '_data'));
                                                    }
                                                });
                                            }
                                        }(contentTypePath, form));
                                    }
                                }
                            }

                            async.parallel(loadDatabase, function (err, res) {
                                if (err) {
                                    cb(err, res);
                                } else {
                                    cb(null, _.merge(res, results));
                                }
                            });
                        } else {
                            cb(null, {});
                        }
                    };
                })(languages[l])
            }

            async.parallel(calls, function (error, data) {
                if(error) throw error;
                self.cache = data;
            });
        }
    } catch (error) {
        console.error("Error while loading cache ", err.message);
    }
};

Inmemory.prototype.get = function(language, content_type_id, query, includeWrapper) {
    var result;
    if(language && content_type_id && this.cache[language]) {
        result = this.cache[language][content_type_id];
        if(result && _.isPlainObject(query)) {
            query = helper.filterQuery(query);
            result = sift(query, result);
            if(!includeWrapper && _.some(result, function (obj) { return _.has(obj, '_data')})) result = _.map(result, "_data");
        }
    }
    return result;
};

Inmemory.prototype.set = function(language, content_type_id, uid, data, partial) {
    try {
        if(language && content_type_id) {
            if(this.cache[language]) {
                data = _.cloneDeep(data);
                if(content_type_id === contentTypeName || content_type_id === entryRoutesName) {
                    if(content_type_id === contentTypeName) data = helper.filterSchema(data, false, '_data');
                    if(uid) {
                        var idx = _.findIndex(this.cache[language][content_type_id], {"_uid": uid});
                        if (~idx) this.cache[language][content_type_id].splice(idx, 1);
                        if (data) this.cache[language][content_type_id].unshift(data);
                    } else {
                        this.cache[language][content_type_id] = data;
                    }
                } else if((this._inmemory[content_type_id] && this._inmemory[content_type_id].length) || partial || (content_type_id != assetRouteName && this.cache[language][content_type_id] )) {
                    var fields = this._inmemory[content_type_id];
                    data = helper.filterEntries(content_type_id, fields, data, '_data');
                    this.cache[language][content_type_id] = this.cache[language][content_type_id] || [];
                    if(uid) {
                        var idx = _.findIndex(this.cache[language][content_type_id], {"_uid": uid});
                        if(~idx) this.cache[language][content_type_id].splice(idx, 1);
                        if(data) this.cache[language][content_type_id].unshift(data);
                    } else {
                        this.cache[language][content_type_id] = data;
                    }
                } else if( content_type_id == assetRouteName && this.cache[language][content_type_id]) {
                    if(uid) {
                        var idx = _.findIndex(this.cache[language][content_type_id], {"_uid": uid});
                        if (~idx) this.cache[language][content_type_id].splice(idx, 1);
                        if (data) this.cache[language][content_type_id].unshift(data);
                    } else {
                        this.cache[language][content_type_id] = data;
                    }
                }
            } else {
                console.error('Language is not defined in cache');
            }
        } else {
            console.error('Invalid parameters passed to Inmemory cache');
        }
    } catch (error) {
        console.error('Inmemory set error:', error)
    }
};

module.exports = Inmemory;
