const Transform = require('stream').Transform
const async = require('async')
const util = require('util')

exports.ScoreTopScoringDocs = ScoreTopScoringDocs = function(options) {
  this.options = options
  Transform.call(this, { objectMode: true })
}
util.inherits(ScoreTopScoringDocs, Transform)
ScoreTopScoringDocs.prototype._transform = function (clause, encoding, end) {
  const that = this
  clause = JSON.parse(clause)
  var fields = Object.keys(clause.queryClause.AND)
  async.each(clause.topScoringDocs, function (docID, nextDocCallback) {
    docID = docID[1]
    async.map(fields, function (field, callback) {
      that.options.indexes.get(
        'DOCUMENT-VECTOR￮' + docID + '￮' + field + '￮', function (err, docVector) {
          var vector = {}
          docVector.forEach(function (element) {
            if (clause.queryClause.AND[field].indexOf(element[0][0]) !== -1) {
              vector[field + '￮' + element[0]] = element[1]
            }
          })
          return callback(err, vector)
        })
    }, function (err, results) {
      if (err) return nextDocCallback(err)
      that.options.indexes.get(
        'DOCUMENT￮' + docID + '￮', function (err, doc) {
          var tfidf = {}
          Object.keys(clause.termFrequencies).forEach(function (key) {
            var tf = +clause.termFrequencies[key]
            var df = +results[0][key]
            var idf = Math.log10(1 + (1 / df))
            tfidf[key] = tf * idf
          })
          var score = (Object.keys(tfidf).reduce(function (prev, cur) {
            return (tfidf[prev] || 0) + tfidf[cur]
          }, 0) / Object.keys(tfidf).length)
          const document = {
            score: {
              tf: clause.termFrequencies,
              df: results[0],
              tfidf: tfidf,
              score: score
            },
            document: doc
          }
          that.push(JSON.stringify(document))
          return nextDocCallback(err)
        })
    })
  }, function (err) {
    if (err) {
      // do something clever
    }
    return end()
  })
}
