const Readable = require('stream').Readable
const Transform = require('stream').Transform
const util = require('util')

const MatcherStream = function (q, options) {
  this.options = options
  Transform.call(this, { objectMode: true })
}
util.inherits(MatcherStream, Transform)
MatcherStream.prototype._transform = function (q, encoding, end) {
  q = JSON.parse(q)
  const that = this
  var results = []

  const formatMatches = function (item) {
    if (q.type === 'ID') {
      that.push([item.key.split('￮')[2], item.value])
    } else if (q.type === 'count') {
      that.push([item.key.split('￮')[2], item.value.length])
    } else {
      that.push(item.key.split('￮')[2])
    }
  }

  const sortResults = function (err) {
    err // TODO: what to do with this?
    results.sort(function (a, b) {
      return b.value.length - a.value.length
    })
      .slice(0, q.limit)
      .forEach(formatMatches)
    end()
  }

  this.options.indexes.createReadStream({
    start: 'DF￮' + q.field + '￮' + q.beginsWith,
    end: 'DF￮' + q.field + '￮' + q.beginsWith + '￮￮￮'
  })
  .on('data', function (data) {
    results.push(data)
  })
  .on('error', function (err) {
    that.options.log.error('Oh my!', err)
  })
  .on('end', sortResults)
}

exports.match = function (q, options) {
  var s = new Readable()
  q = Object.assign({}, {
    beginsWith: '',
    field: '*',
    threshold: 3,
    limit: 10,
    type: 'simple'
  }, q)

  if (q.beginsWith.length < q.threshold) {
    s.push(null)
    return s
  }

  s.push(JSON.stringify(q))
  s.push(null)

  return s.pipe(new MatcherStream(q, options))
}
