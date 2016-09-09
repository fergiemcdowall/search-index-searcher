const Transform = require('stream').Transform
const async = require('async')
const util = require('util')

const ScoreTopScoringDocs = function (options) {
  this.options = options
  Transform.call(this, { objectMode: true })
}
exports.ScoreTopScoringDocs = ScoreTopScoringDocs
util.inherits(ScoreTopScoringDocs, Transform)
ScoreTopScoringDocs.prototype._transform = function (clause, encoding, end) {
  const that = this
  clause = JSON.parse(clause)
  clause.queryClause.BOOST = clause.queryClause.BOOST || 0 // put this somewhere better
  const fields = Object.keys(clause.queryClause.AND)
  async.each(clause.topScoringDocs, function (docID, nextDocCallback) {
    docID = docID[1]
    async.map(fields, function (field, callback) {
      that.options.indexes.get(
        'DOCUMENT-VECTOR￮' + docID + '￮' + field + '￮', function (err, docVector) {
          var vector = {}
          clause.queryClause.AND[field].forEach(function (token) {
            vector[field + '￮' + token] = docVector[token]
          })
          return callback(err, vector)
        })
    }, function (err, results) {
      if (err) return nextDocCallback(err)
      // that.options.indexes.get(
      //   'DOCUMENT￮' + docID + '￮', function (err, doc) {
          const tfidf = {}
          clause.termFrequencies.forEach(function (item) {
            const tf = +item.tf
            const df = +results[0][item.gte] // should this be gte?
            const idf = Math.log10(1 + (1 / df))
            tfidf[item.gte] = (tf * idf)
          })
          var score = (Object.keys(tfidf).reduce(function (prev, cur) {
            return (tfidf[prev] || 0) + tfidf[cur]
          }, 0) / Object.keys(tfidf).length)
          const document = {
            id: docID,
            scoringCriteria: [{
              tf: clause.termFrequencies,
              df: results[0],
              tfidf: tfidf,
              boost: +clause.queryClause.BOOST,
              score: score - +clause.queryClause.BOOST
            }],
            score: score - +clause.queryClause.BOOST
            // document: doc
          }
          that.push(JSON.stringify(document))
          return nextDocCallback(err)
        // })
    })
  }, function (err) {
    if (err) {
      // do something clever
    }
    return end()
  })
}
