const _intersection = require('lodash.intersection')
const _uniq = require('lodash.uniq')
const Transform = require('stream').Transform
const async = require('async')
const util = require('util')

const CalculateBuckets = function (options, filter, requestedBuckets) {
  this.buckets = requestedBuckets || []
  this.filter = filter
  this.options = options
  Transform.call(this, { objectMode: true })
}
exports.CalculateBuckets = CalculateBuckets
util.inherits(CalculateBuckets, Transform)
CalculateBuckets.prototype._transform = function (queryClause, encoding, end) {
  const that = this
// does this actually have to be async?

  async.map(that.buckets, function (bucket, bucketProcessed) {
    const gte = 'DF￮' + bucket.field + '￮' + bucket.gte
    const lte = 'DF￮' + bucket.field + '￮' + bucket.lte + '￮'
    that.options.indexes.createReadStream({gte: gte, lte: lte})
      .on('data', function (data) {
        var IDSet = _intersection(data.value, queryClause.set)
        if (IDSet.length > 0) {
          bucket.value = bucket.value || []
          bucket.value = _uniq(bucket.value.concat(IDSet).sort())
        }
      })
      .on('close', function () {
        if (!bucket.set) {
          bucket.value = bucket.value.length
        }
        that.push(bucket)
        return bucketProcessed(null)
      })
  }, function (err) {
    if (err) {
      // what to do?
    }
    return end()
  })
}
