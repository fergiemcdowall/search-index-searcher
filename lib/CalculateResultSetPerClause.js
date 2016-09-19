const Transform = require('stream').Transform
const _difference = require('lodash.difference')
const _intersection = require('lodash.intersection')
const _spread = require('lodash.spread')
const siUtil = require('./siUtil.js')
const util = require('util')

const CalculateResultSetPerClause = function (options) {
  this.options = options
  Transform.call(this, { objectMode: true })
}
exports.CalculateResultSetPerClause = CalculateResultSetPerClause
util.inherits(CalculateResultSetPerClause, Transform)
CalculateResultSetPerClause.prototype._transform = function (queryClause, encoding, end) {
  const that = this
  const frequencies = []

  var NOT = function (includeResults) {
    const bigIntersect = _spread(_intersection)
    var include = bigIntersect(includeResults)
    // if there are no NOT conditions, simply end()
    if (siUtil.getKeySet(queryClause.NOT).length === 0) {
      that.push(JSON.stringify({
        queryClause: queryClause,
        set: include,
        termFrequencies: frequencies,
        BOOST: queryClause.BOOST || 0
      }))
      return end()
    }
    // if there ARE "NOT"-conditions, remove all IDs specified by NOT
    var i = 0
    var excludeResults = []
    siUtil.getKeySet(queryClause.NOT).forEach(function (item) {
      var exclude = []
      that.options.indexes.createReadStream({gte: item[0], lte: item[1] + '￮'})
        .on('data', function (data) {
          exclude = uniqFast(exclude.concat(data.value))
        })
        .on('error', function (err) {
          that.options.log.debug(err)
        })
        .on('end', function () {
          excludeResults.push(exclude.sort())
          if (++i === siUtil.getKeySet(queryClause.NOT).length) {
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
          }
        })
    })
  }
  // Get all of the IDs in the AND conditions
  var IDSets = []
  siUtil.getKeySet(queryClause.AND).forEach(function (item) {
    var include = []
    var setLength = 0
    that.options.indexes.createReadStream({gte: item[0], lte: item[1] + '￮'})
      .on('data', function (data) {
        setLength += data.value.length
        include = uniqFast(include.concat(data.value))
      })
      .on('error', function (err) {
        that.options.log.debug(err)
      })
      .on('end', function () {
        frequencies.push({
          gte: item[0].split('￮')[1] + '￮' + item[0].split('￮')[2],
          lte: item[1].split('￮')[1] + '￮' + item[1].split('￮')[2],
          tf: include.length, // actual term frequency across docs
          setLength: setLength // number of array elements that need to be traversed
        })
        IDSets.push(include.sort())
        if (IDSets.length === siUtil.getKeySet(queryClause.AND).length) {
          NOT(IDSets)
        }
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
