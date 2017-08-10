/*!
 * contentstack-express
 * copyright (c) Built.io Contentstack
 * MIT Licensed
 */

'use strict';

/**
 * Module which handles the Connection to the MongoDB database, creation of
 * Indexes on collections, creates and serves singleton MongoDB connection object
 */

var mongodb     = require('mongodb'),
    _           = require('lodash'),
    async       = require('async'),
    MongoClient = mongodb.MongoClient,
    config      = require('../../../config'),
    logger      = require('../../../logger'),
    dbConfig    = config.get('storage'),
    collections = ['_content_types', '_entries', '_assets'],
    indexes     = {'_content_type_uid': 1, 'uid': 1};

/**
 * Module which is used to handle the MongoDB database connection
 * @return Object
 */

module.exports = (function () {

    var dbName        = config.get('contentstack').api_key + "_" + config.get('environment'),
        connectionStr = 'mongodb://' + dbConfig.options.hostname + ':' + dbConfig.options.port + '/' + dbName,
        _db           = {};

    /**
     * Function which handles the connection to mongodb
     * @return {Object}  : DB object
     */

    var connect = function () {
        return new Promise(function (resolve, reject) {
            // Checks for the prior DB connection
            if (_.isEmpty(_db)) {
                try {
                    // Connect to Mongodb
                    MongoClient.connect(connectionStr, function (error, db) {
                        if(error)
                            throw error;
                        // Create required collections and set indexes
                        async.eachLimit(collections, 1, function (collection, cb) {
                            db.collection(collection).createIndex(indexes, function (error) {
                                if(error)
                                    return cb(error);
                                return cb();
                            })
                        }, function (error) {
                            if(error) {
                                logger.error('Error creating indexes on Mongodb:', error.message);
                                throw error;
                            }
                        });
                        // export db
                        _db = db;
                        return resolve(_db);
                    });
                } catch (error) {
                    logger.error('Error in connecting to MongoDB datastore:', error.message);
                    return reject(error);
                }
            } else {
                return resolve(_db);
            }
        });
    };

    return {
        connect: connect
    };

})();
