const async = require('async');
const mongodb = require('./connection').connect();

module.exports = (language, langCallback) => {
  async.parallel({
  		'_content_types': function(callback) {
	      const languageCode = language.code;
	      mongodb
	        .collection('content_types')
	        .find({"_lang": languageCode}, {'_id': 0})
	        .toArray(function (error, result){
	          if(error)
	            return callback(error);
	          callback(null, filterSchema(result));
	        });
    }, '_routes': function(callback) {
      const languageCode = language.code;
      mongodb
        .collection('routes')
        .find({"_lang": languageCode}, {'_id': 0})
        .toArray(function (error, result){
            if(error)
              return callback(error);
            callback(null, result);
          });
    }, '_assets': function(callback) {
      const languageCode = language.code;
      mongodb
        .collection('assets')
        .find({"_lang": languageCode}, {'_id': 0})
        .toArray(function (error, result){
          if(error)
            return callback(error);
          callback(null, result);
        });
    }
  }, (error, results) => {
    if (error)
      return langCallback(error);
    langCallback(null, results);
  });
}

var filterSchema = function(_forms, _remove) {
    var _keys = ['schema'].concat(_remove || []);
    var _removeKeys = function(object) {
        for(var i = 0, total = _keys.length; i < total; i++)
            delete object[_keys[i]];
        return object;
    };

    if(_forms && _forms instanceof Array) {
        for(var i = 0, total = _forms.length; i < total; i++) {
          if(_forms[i])
              _forms[i] = _removeKeys(_forms[i]);
        }
    } else if(_forms && typeof _forms == "object") {
        _forms = _removeKeys(_forms);
    }
    return _forms;
};