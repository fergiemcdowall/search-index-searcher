const Transform = require('stream').Transform
const _difference = require('lodash.difference')
const _intersection = require('lodash.intersection')
const _spread = require('lodash.spread')
const async = require('async')
const siUtil = require('./siUtil.js')
const util = require('util')

exports.CalculateResultSet = CalculateResultSet = function(options, filter) {
  this.options = options
  this.filter = filter
  Transform.call(this, { objectMode: true })
}
util.inherits(CalculateResultSet, Transform)
CalculateResultSet.prototype._transform = function (queryClause, encoding, end) {
  const that = this
  const frequencies = {}
  async.map(siUtil.getKeySet(queryClause.AND, this.filter), function (item, callback) {
    var include = []
    that.options.indexes.createReadStream({gte: item[0], lte: item[1] + '￮'})
      .on('data', function (data) {
        include = uniqFast(include.concat(data.value))
      })
      .on('error', function (err) {
        that.options.log.debug(err)
      })
      .on('end', function () {
        var fKey = item[0].split('￮')[1] + '￮' + item[0].split('￮')[2]
        frequencies[fKey] = include.length
        return callback(null, include.sort())
      })
  }, function (asyncerr, includeResults) {
    const bigIntersect = _spread(_intersection)
    var include = bigIntersect(includeResults)
    // NOTing
    async.map(siUtil.getKeySet(queryClause.NOT, this.filter), function (item, callback) {
      var exclude = []
      that.options.indexes.createReadStream({gte: item[0], lte: item[1] + '￮'})
        .on('data', function (data) {
          exclude = uniqFast(exclude.concat(data.value))
        })
        .on('error', function (err) {
          that.options.log.debug(err)
        })
        .on('end', function () {
          return callback(null, exclude.sort())
        })
    }, function (asyncerr, excludeResults) {
      excludeResults.forEach(function (excludeSet) {
        include = _difference(include, excludeSet)
      })
      that.push(JSON.stringify({
        queryClause: queryClause,
        set: include,
        termFrequencies: frequencies,
        BOOST: queryClause.BOOST || 0
      }))
      return end()
    })
  })
}



// supposedly fastest way to get unique values in an array
// http://stackoverflow.com/questions/9229645/remove-duplicates-from-javascript-array
const uniqFast = function (a) {
  var seen = {}
  var out = []
  var len = a.length
  var j = 0
  for (var i = 0; i < len; i++) {
    var item = a[i]
    if (seen[item] !== 1) {
      seen[item] = 1
      out[j++] = item
    }
  }
  return out
}
