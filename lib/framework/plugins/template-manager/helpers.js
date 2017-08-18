/*!
 * contentstack-express
 * copyright (c) Built.io Contentstack
 * MIT Licensed
 */

'use strict';

/*!
 * Module dependencies
 */
var _       = require('lodash'),
    utils   = require('../../../utils'),
    context = utils.context,
    _db     = require('../../../utils/db/providers');

module.exports = (function () {
    // To get the partials | split into 2, find() and findAsync()
    function get (partial, lim, locale, includeReference) {
        return new Promise(function (resolve, reject) {
            var language = locale || context.get('lang'),
                limit = lim || 1;
            find({"_content_type_uid": partial, "locale": language, "include_references": includeReference}).then(function (entry) {
                if (entry && entry.length)
                    entry = (limit == 1) ? entry[0] : entry.slice(0, limit);
                return resolve(entry);
            });
        })
    };

    function find (query) {
        return new Promise(function (resolve, reject) {
            _db.find(query, {}, function (error, result) {
                if(error)
                    reject(error);
                return resolve(result.entries || result);
            });
        })
    }
    return {
        get
    }
})();