const async = require('async')
const bunyan = require('bunyan')
const levelup = require('levelup')
const sw = require('stopword')
const _defaults = require('lodash.defaults')

exports.getKeySet = function (clause, filter) {
  var keySet = []
  for (var fieldName in clause) {
    clause[fieldName].forEach(function (token) {
      // keySet[clause].push('DF' + '￮' + fieldName + '￮' + token + '￮￮')
      if (filter && Array.isArray(filter)) {
        // Filters: TODO
        filter.forEach(function (filter) {
          keySet.push([
            'DF￮' + fieldName + '￮' + token + '￮' + filter.field + '￮' + filter.gte,
            'DF￮' + fieldName + '￮' + token + '￮' + filter.field + '￮' + filter.lte + '￮'
          ])
        })
      } else {
        keySet.push([
          'DF￮' + fieldName + '￮' + token + '￮￮',
          'DF￮' + fieldName + '￮' + token + '￮￮￮'
        ])
      }
    })
  }
  return keySet
}

exports.getOptions = function (givenOptions, callbacky) {
  givenOptions = givenOptions || {}
  async.parallel([
    function (callback) {
      var defaultOps = {}
      defaultOps.deletable = true
      defaultOps.fieldedSearch = true
      defaultOps.fieldsToStore = 'all'
      defaultOps.indexPath = 'si'
      defaultOps.logLevel = 'error'
      defaultOps.nGramLength = 1
      defaultOps.nGramSeparator = ' '
      defaultOps.separator = /[\|' \.,\-|(\n)]+/
      defaultOps.stopwords = sw.getStopwords('en').sort()
      defaultOps.log = bunyan.createLogger({
        name: 'search-index',
        level: givenOptions.logLevel || defaultOps.logLevel
      })
      callback(null, defaultOps)
    },
    function (callback) {
      if (!givenOptions.indexes) {
        levelup(givenOptions.indexPath || 'si', {
          valueEncoding: 'json'
        }, function (err, db) {
          callback(err, db)
        })
      } else {
        callback(null, null)
      }
    }
  ], function (err, results) {
    var options = _defaults(givenOptions, results[0])
    if (results[1] != null) {
      options.indexes = results[1]
    }
    return callbacky(err, options)
  })
}

exports.getQueryDefaults = function (q) {
  return _defaults(q || {}, {
    query: {
      AND: {'*': ['*']}
    },
    offset: 0,
    pageSize: 20
  })
}
