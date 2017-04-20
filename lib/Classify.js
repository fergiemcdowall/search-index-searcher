const Transform = require('stream').Transform
const util = require('util')

const Classify = function (searcher) {
  this.options = searcher.options || {}
  this.match = searcher.match
  this.nGramLength = this.options.nGramLength.lte || 1
  this.stack = []
  Transform.call(this, { objectMode: true })
}
exports.Classify = Classify
util.inherits(Classify, Transform)
Classify.prototype._transform = function (token, encoding, end) {
  var stack = this.stack
  var t = token.toString()
  var that = this
  var potentialMatches = []
  stack.push(t)
  if (stack.length < this.nGramLength) return end()
  stack.forEach(function (item, i) {
    potentialMatches.push(stack.slice(0, (1 + i)).join(' ').toLowerCase())
  })
  var counter = 0
  potentialMatches.forEach(function(item) {
    that.match({
      beginsWith: item,
      type: 'ID',
      limit: 1
    }).on('data', function (match) {
      if (Object.keys(match)[0] === item)
        that.push(match)
    }).on('end', function () {
      if (++counter == potentialMatches.length) {
        stack.shift()
        return end()
      }
    })
  })
}
