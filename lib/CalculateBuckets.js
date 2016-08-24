const _intersection = require('lodash.intersection')
const _uniq = require('lodash.uniq')
const Transform = require('stream').Transform
const async = require('async')
const util = require('util')

exports.CalculateBuckets = CalculateBuckets = function(options, filter, requestedBuckets) {
  this.buckets = requestedBuckets || []
  this.filter = filter
  this.options = options
  Transform.call(this, { objectMode: true })
}
util.inherits(CalculateBuckets, Transform)
CalculateBuckets.prototype._transform = function (queryClause, encoding, end) {
  var that = this
  queryClause = JSON.parse(queryClause)
  // Shouldnt get every key in the AND set- should just get key with
  // lowest frequency
  // get lowest frequency key
  const lowestFrequencyKey = Object.keys(queryClause.termFrequencies)
    .map(function (key) {
      return [key, queryClause.termFrequencies[key]]
    }).sort(function (a, b) {
      return a[1] - b[1]
    })[0]
  async.map(that.buckets, function (bucket, bucketProcessed) {
    var fieldName = lowestFrequencyKey[0].split('￮')[0]
    var token = lowestFrequencyKey[0].split('￮')[1]
    var gte = 'DF￮' + fieldName + '￮' + token + '￮' +
    bucket.field + '￮' +
    bucket.gte
    var lte = 'DF￮' + fieldName + '￮' + token + '￮' +
      bucket.field + '￮' +
      bucket.lte + '￮'
    // TODO: add some logic to see if keys are within ranges before doing a lookup
    that.options.indexes.createReadStream({gte: gte, lte: lte})
      .on('data', function (data) {
        var IDSet = _intersection(data.value, queryClause.set)
        if (IDSet.length > 0) {
          bucket.IDSet = bucket.IDSet || []
          bucket.IDSet = _uniq(bucket.IDSet.concat(IDSet).sort())
        }
        // TODO: make loop aware of last iteration so that stream can
        // be pushed before _flush
      })
      .on('close', function () {
        return bucketProcessed(null)
      })
  }, function (err) {
    if (err) {
      // what to do?
    }
    return end()
  })
}
CalculateBuckets.prototype._flush = function (end) {
  var that = this
  this.buckets.forEach(function (bucket) {
    that.push(JSON.stringify(bucket))
  })
  return end()
}
