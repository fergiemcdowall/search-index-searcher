const _intersection = require('lodash.intersection')
const Transform = require('stream').Transform
const util = require('util')

const CalculateCategories = function (options, filter, category) {
  category.values = []
  this.category = category
  this.filter = filter
  this.options = options
  Transform.call(this, { objectMode: true })
}
exports.CalculateCategories = CalculateCategories
util.inherits(CalculateCategories, Transform)
CalculateCategories.prototype._transform = function (queryClause, encoding, end) {
  const that = this
  queryClause = JSON.parse(queryClause)
  // Shouldnt get every key in the AND set- should just get key with
  // lowest frequency:
  // get lowest frequency key
  const lowestFrequencyKey = Object.keys(queryClause.termFrequencies)
    .map(function (key) {
      return [key, queryClause.termFrequencies[key]]
    }).sort(function (a, b) {
      return a[1] - b[1]
    })[0]
  // async.map(that.categories, function (category, categoryProcessed) {
  const fieldName = lowestFrequencyKey[0].split('￮')[0]
  const token = lowestFrequencyKey[0].split('￮')[1]
  const gte = 'DF￮' + fieldName + '￮' + token + '￮' +
    this.category.field + '￮'
  const lte = 'DF￮' + fieldName + '￮' + token + '￮' +
    this.category.field + '￮￮'
  this.category.values = this.category.values || []
  // TODO: add some logic to see if keys are within ranges before doing a lookup
  that.options.indexes.createReadStream({gte: gte, lte: lte})
    .on('data', function (data) {
      var IDSet = _intersection(data.value, queryClause.set)
      if (IDSet.length > 0) {
        var key = data.key.split('￮')[4]
        that.category.values.push({
          key: key,
          value: data.value
        })
      }
    // TODO: make loop aware of last iteration so that stream can
    // be pushed before _flush
    })
    .on('close', function () {
      return end()
    })
}
CalculateCategories.prototype._flush = function (end) {
  const that = this
  if (this.category.values.set) {
    this.category.values.map(function (elem) {
      elem.value = elem.value.length
      return elem
    })
  }
  this.category.values.sort(this.category.sort)
  this.category.values =
    this.category.values.slice(0, this.category.limit ||
      this.category.values.length)
  if (!this.category.set) {
    this.category.values.map(function (elem) {
      elem.value = elem.value.length
      return elem
    })
  }
  this.category.values.forEach(function (elem) {
    that.push(elem)
  })
  return end()
}
