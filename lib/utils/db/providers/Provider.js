/*
	Provider class methods are to be overriden by custom provider implementations
 */

var Provider = function () {
	if(this.constructor === Provider)
		throw new Error('You cannot instantiate Provider class');
};

Provider.prototype.find = function (query, options, callback) {
	callback(new Error('' + this.provider + ' is missing find() implementation!'));
};

Provider.prototype.findOne = function (query, callback) {
	callback(new Error('' + this.provider + ' is missing findOne() implementation!'));
};

Provider.prototype.count = function (query, callback) {
	callback(new Error('' + this.provider + ' is missing count() implementation!'));
};

Provider.prototype.insert = function (data, callback) {
	callback(new Error('' + this.provider + ' is missing insert() implementation!'));
};

Provider.prototype.upsert = function (data, callback) {
	callback(new Error('' + this.provider + ' is missing upsert() implementation!'));
};

Provider.prototype.remove = function (query, callback) {
	callback(new Error('' + this.provider + ' is missing remove() implementation!'));
};

module.exports = exports = Provider;