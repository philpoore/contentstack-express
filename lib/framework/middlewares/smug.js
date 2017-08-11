/*!
 * contentstack-express
 * copyright (c) Built.io Contentstack
 * MIT Licensed
 */

'use strict';


var _ = require('lodash');
/**
 * search for the requested route in the system.
 */
module.exports = function (utils) {
	var db 		= utils.db,
		config 	= utils.config;
	return function smug(req, res, next) {
		var lang = req.contentstack.get('lang');

		if(config.get('cache') === true || typeof config.get('cache') === 'undefined') {
			var	Query1 = db.ContentType('_routes').Query().where('entry.url', lang.url),
				Query2 = db.ContentType('_routes').Query().where('entry.url', req._contentstack.parsedUrl);

			db
				.ContentType('_routes')
				.language(lang.code)
				.Query()
				.or(Query1, Query2)
				.toJSON()
				.findOne()
				.then(function (data) {
					if (data && typeof data === "object") {
						db
							.ContentType(data.content_type.uid)
							.language(lang.code)
							.Entry(data.entry.uid)
							.toJSON()
							.fetch()
							.then(function (entry) {
								req.contentstack.set('content_type', data.content_type.uid, true);
								req.contentstack.set('entry', entry);
								next();
							}, function (err) {
								next(err);
							});
					} else {
						next();
					}
				}, function (err) {
					next();
				});
		} else {
			// Running persistent storage
			var	Query1 = db.ContentType('_entries').Query().where('url', lang.url),
				Query2 = db.ContentType('_entries').Query().where('url', req._contentstack.parsedUrl);
			db.ContentType('_entries')
				.language(lang.code)
				.Query()
				.or(Query1, Query2)
				.toJSON()
				.findOne()
				.then(function (entry) {
					// TODO: verify data here
					if(_.isPlainObject(entry) && entry._content_type_uid) {
						req.contentstack.set('content_type', entry._content_type_uid, true);
						req.contentstack.set('entry', entry);
						next();
					} else {
						req.contentstack.set('content_type', null, true);
						req.contentstack.set('entry', null);
						next();
					}
				}, function (error) {
					next(error);
				});
		}
	};
};

