const Readable = require('stream').Readable
const Transform = require('stream').Transform
const util = require('util')

const MatchMostFrequent = function (q, options) {
  this.options = options
  Transform.call(this, { objectMode: true })
}
util.inherits(MatchMostFrequent, Transform)
MatchMostFrequent.prototype._transform = function (q, encoding, end) {
  const sep = this.options.keySeparator
  const that = this
  var results = []
  this.options.indexes.createReadStream({
    start: 'DF' + sep + q.field + sep + q.beginsWith,
    end: 'DF' + sep + q.field + sep + q.beginsWith + sep
  })
  .on('data', function (data) {
    results.push(data)
  })
  .on('error', function (err) {
    that.options.log.error('Oh my!', err)
  })
  // .on('end', sortResults)
  .on('end', function () {
    results.sort(function (a, b) {
      var frequencySort = b.value.length - a.value.length
      if (frequencySort !== 0) {
        // sort by frequency (value.length)
        return frequencySort
      } else {
        // sort alphabetically by key ascending
        if (b.key > 0) return 1
        if (b.key < 0) return -1
        return 0
      }
    })
      .slice(0, q.limit)
      .forEach(function (item) {
        var m = {}
        switch (q.type) {
          case 'ID': m = {
            token: item.key.split(sep)[2],
            documents: item.value
          }; break
          case 'count': m = {
            token: item.key.split(sep)[2],
            documentCount: item.value.length
          }; break
          default: m = item.key.split(sep)[2]
        }
        that.push(m)
      })
    return end()
  })
}

exports.match = function (q, options) {
  var s = new Readable({ objectMode: true })
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
  s.push(q)
  s.push(null)
  return s.pipe(new MatchMostFrequent(q, options))
}
