const H = require('highland')
const Readable = require('stream').Readable
const _defaults = require('lodash.defaults')
const async = require('async')
const bunyan = require('bunyan')
const levelup = require('levelup')
const sw = require('stopword')

exports.match = function (q, options) {
  q = _defaults(q || {}, {
    beginsWith: '',
    field: '*',
    threshold: 3,
    limit: 10,
    type: 'simple'
  })

  if (q.beginsWith.length < q.threshold) {
    var s = new Readable()
    s.push(null)
    return s
  }

  return H(options.indexes.createReadStream({
    start: 'DF￮' + q.field + '￮' + q.beginsWith,
    end: 'DF￮' + q.field + '￮' + q.beginsWith + '￮￮￮'
  }))
    .on('error', function (err) {
      console.log('DISASTER')
      log.error('Oh my!', err)
    })
    .on('close', function (err) {
      console.log('ended')
      log.error('Oh my!', err)
    })
    .filter(function (data) {
      return data.key.substring(data.key.length, data.key.length - 2) === '￮￮'
    })
    .map(function (data) {
      return [data.key.split('￮')[2], data.value] // suggestions
    })
    .sortBy(function (a, b) {
      return b[1].length - a[1].length // sortedSuggestions
    })
    .map(function (data) {
      if (q.type === 'ID') {
        return data
      }
      if (q.type === 'count') {
        return [data[0], data[1].length]
      }
      return data[0] // fall back to a simple format
    })
    .take(q.limit)
}

