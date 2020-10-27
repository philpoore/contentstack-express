/*!
 * contentstack-express
 * Copyright (c) Contentstack
 * MIT Licensed
 */

'use strict';

/*!
 * Module dependencies
 */
var path = require('path');
var fs = require('graceful-fs');

const fsExistsCache = {}

/**
 * fs.existsSync with caching
 * useful for when running in containers
 * @param {string} filename 
 */
const fsExistsSyncCache = (filename) => {
  const resolvedFilename = path.resolve(filename)

  if (resolvedFilename in fsExistsCache) {
    return fsExistsCache[resolvedFilename]
  }
  const exists = fs.existsSync(resolvedFilename)
  fsExistsCache[resolvedFilename] = exists
  return exists
}

const resolveTemplate = (_templates, lang, ext = 'html') => {
  let template = undefined
  // for language based templates
  if (lang && fsExistsSyncCache(path.join(_templates, lang, 'index.' + ext))) {
    template = path.join(_templates, lang, 'index.' + ext);
    // for templates with content_type/index.html or content_type.html
  } else if (fsExistsSyncCache(path.join(_templates, 'index.' + ext))) {
    template = path.join(_templates, 'index.' + ext);
  } else if (fsExistsSyncCache(path.join(_templates + '.' + ext))) {
    template = _templates + '.' + ext;
  } else if (fsExistsSyncCache(path.join(_templates, 'single.' + ext))) {
    template = path.join(_templates, "single");
  }

  return template
}
/**
 * template manager
 */
module.exports = function (utils) {
  var config = utils.config;

  return function matrix(req, res, next) {
    try {
      if (
        req.contentstack.get('response_type') != 'json' &&
        req.contentstack.get('content_type')
      ) {
        var template;
        if (req.contentstack.get('content_type')) {
          var content_type = req.contentstack.get('content_type'),
            templatesDir = 'pages',
            ext = config.get('view.extension') || 'html',
            _templates = path.join(
              config.get('path.templates')[0],
              templatesDir,
              content_type
            );

          var lang = req.contentstack.get('lang').code;
          template = resolveTemplate(_templates, lang, ext)
        }
        req.contentstack.set('template', template);
      }
      return next();
    } catch (matrix_error) {
      return next(matrix_error);
    }
  };
};
