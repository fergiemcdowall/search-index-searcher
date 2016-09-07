const _intersection = require('lodash.intersection')
const Transform = require('stream').Transform
const util = require('util')

const CalculateCategories = function (options, category) {
  category.values = []
  this.category = category
  this.options = options
  Transform.call(this, { objectMode: true })
}
exports.CalculateCategories = CalculateCategories
util.inherits(CalculateCategories, Transform)
CalculateCategories.prototype._transform = function (queryClause, encoding, end) {
  const that = this
  // Shouldnt get every key in the AND set- should just get key with
  // lowest frequency:
  // get lowest frequency key
  // const lowestFrequencyKey = Object.keys(queryClause.termFrequencies)
  //   .map(function (key) {
  //     return [key, queryClause.termFrequencies[key]]
  //   }).sort(function (a, b) {
  //     return a[1] - b[1]
  //   })[0]
  const gte = 'DF￮' + this.category.field + '￮+'
  const lte = 'DF￮' + this.category.field + '￮￮'
  this.category.values = this.category.values || []
  that.options.indexes.createReadStream({gte: gte, lte: lte})
    .on('data', function (data) {
      var IDSet = _intersection(data.value, queryClause.set)
      if (IDSet.length > 0) { // make this optional
        var key = data.key.split('￮')[2]
        var value = IDSet.length
        if (that.category.set) {
          value = IDSet
        }
        that.push({
          key: key,
          value: value
        })
      }
    // TODO: make loop aware of last iteration so that stream can
    // be pushed before _flush
    })
    .on('close', function () {
      return end()
    })
}
