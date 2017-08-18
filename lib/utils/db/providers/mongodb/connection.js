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
    util        = require('util'),
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
    var _db = {};

    /**
     * Function which handles the connection to mongodb
     * @return {Object}  : DB object
     */

    var connect = function () {
        return new Promise(function (resolve, reject) {
            // Checks for the prior DB connection
            if (_.isEmpty(_db)) {
                try {
                    let connectionUri = buildUri(),
                        options = (_.isPlainObject(dbConfig.options)) ? dbConfig.options: {};
                    // Del basedir option
                    if(options && options.basedir)
                        delete options.basedir;
                    // Connect to Mongodb
                    MongoClient.connect(connectionUri, options, function (error, db) {
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

    var buildUri = function () {
        let uri = 'mongodb://';
        // If DB requires authentication
        if(dbConfig.username && dbConfig.password)
            uri += util.format('%s:%s@', dbConfig.username, dbConfig.password);
        // If DB has replica sets
        if(_.isArray(dbConfig.servers)) {
            let serversUri = dbConfig.servers.map(function (server) {
                return util.format('%s:%d', server.host, server.port);
            }).join(',');
            uri += serversUri;
        } else if (_.isPlainObject(dbConfig.server)) {
            // Single DB instance
            uri += util.format('%s:%d', dbConfig.server.host, dbConfig.server.port);
        } else {
            throw new Error('Error in mongodb configuration settings')
        }
        // If user provides DB name
        if(dbConfig.dbName) {
            uri = util.format('%s/%s', uri, dbConfig.dbName);
        } else {
        // Create DB name based on api_key & environment
            let dbName = util.format('%s_%s', config.get('contentstack').api_key, config.get('environment'));
            uri = util.format('%s/%s', uri, dbName);
        }
        return uri;
    }

    return {
        connect: connect
    };

})();
