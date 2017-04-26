const Transform = require('stream').Transform
const ngraminator = require('ngraminator')
const util = require('util')

const checkTokens = function (that, potentialMatches, field, done) {
  var counter = 0
  // if there are no potential matches then just return
  if (potentialMatches.length === 0) return done()
  potentialMatches.forEach(function (item) {
    that.match({
      beginsWith: item,
      field: field,
      type: 'ID',
      threshold: 0,
      limit: 1
    }).on('data', function (match) {
      if (match.token === item) { that.push(match) }
    }).on('end', function () {
      if (++counter === potentialMatches.length) {
        return done()
      }
    })
  })
}

const Classify = function (searcher, options) {
  this.options = Object.assign({}, searcher.options, options)
  this.match = searcher.match
  // should maybe be maxNGramLength
  this.maxNGramLength = this.options.maxNGramLength || 1
  this.field = this.options.field || '*'
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
  if (stack.length < this.maxNGramLength) return end()
  stack.forEach(function (item, i) {
    potentialMatches.push(stack.slice(0, (1 + i)).join(' ').toLowerCase())
  })
  checkTokens(that, potentialMatches, this.field, function () {
    stack.shift()
    return end()
  })
}

Classify.prototype._flush = function (end) {
  var that = this
  var stack = this.stack
  var potentialMatches = ngraminator.ngram(stack, {
    gte: 1,
    lte: that.maxNGramLength
  }).map(function (token) {
    return token.join(' ').toLowerCase()
  })
  checkTokens(that, potentialMatches, this.field, function () {
    return end()
  })
}
