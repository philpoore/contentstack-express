var async = require('async'),
  mongodb = require('./connection').connect(),
  helper = require('../../helper');


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
	          callback(null, helper.filterSchema(result));
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