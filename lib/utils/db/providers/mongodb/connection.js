/*!
 * contentstack-express
 * copyright (c) Built.io Contentstack
 * MIT Licensed
 */

'use strict';

/**
 *
 * Module which handles the Connection to the MongoDB database, creation of
 * Indexes on collections, creates and serves singleton MongoDB connection object
 *
 */

var mongodb = require('mongoskin');
var _ = require('lodash');
var async = require('async');

var config = require('../../../config');

var collectionNames = ['content_types', 'entries', 'routes', 'assets'];
var assetsIndex = {'uid': 1, '_lang': 1};
var othersIndex = {'_content_type_uid': 1, '_uid': 1, '_lang': 1};

/**
 * Module which is used to handle the MongoDB database connection
 *
 * @return Object
 */
module.exports = exports = (function() {

    var _dbCache;
    var _databaseConfig = config.get('storage');

    /*
        Prepares the Database name by using API key, environment name, server name
        in the pattern <api_key>_<environment_name>_<server_name>

        Example: blt7863f9429b8342c3_development2_default
    */
    var _databaseName = config.get('contentstack').api_key + "_" + config.get('environment') + "_" + config.get('server');

    // Checks for the Database Name length to be less than 63.
    // Since, allowed size of MongoDB database name is less than or equal to 63
    if (_databaseName.length > 63) {
        _databaseName = _databaseName.slice(0,63);
    }

    var _connectionUri = 'mongodb://' + _databaseConfig.options.hostname + ':' + _databaseConfig.options.port + '/' + _databaseName;

    // Function which handles the MongoDB database connection
    var connect = function() {

        // Checks for the Database connection prior
        if (_.isEmpty(_dbCache)) {

            try {
                // Makes a connection to the given MongoDB connection URI and stores the connection object
                _dbCache = mongodb.db(_connectionUri);

                /*
                    Loops through the collection names and creates indexes on it.

                    For collections such as content_types, entries, routes, the
                    index is created on _content_types, _lang and _uid properties.

                    For assets collection, the index is created on _lang and uid
                    properties

                    The limit of maximum async process at a time is 2

                 */
                async.eachLimit(collectionNames, 2, function (collectionName, collectionCallback) {

                    var indexObject = {};

                    if (collectionName === 'assets') {
                        indexObject = assetsIndex;
                    } else {
                        indexObject = othersIndex;
                    }

                    _dbCache.collection(collectionName)
                        .createIndex(indexObject, function (indexError) {
                            if (indexError) {
                                return collectionCallback(indexError);
                            }
                            collectionCallback();
                        });
                }, function (collectionError) {
                    if (collectionError) {
                        console.error('MongoDB Error on creating index : ', collectionError);
                        throw collectionError;
                    }
                });

            } catch(connectionError) {
                console.error('Error in connecting to MongoDB datastore : ', connectionError.message);
                throw connectionError;
                // process.exit(1);
            }
        }

        return _dbCache;

    };

    return {
        connect: connect
    };

})();
