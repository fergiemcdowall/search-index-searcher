const Transform = require('stream').Transform
const util = require('util')

exports.SortTopScoringDocs = SortTopScoringDocs = function(options) {
  this.resultSet = []
  Transform.call(this, { objectMode: true })
}
util.inherits(SortTopScoringDocs, Transform)
SortTopScoringDocs.prototype._transform = function (doc, encoding, end) {
  doc = JSON.parse(doc)
  this.resultSet.push(doc)
  return end()
}
SortTopScoringDocs.prototype._flush = function (end) {
  var that = this
  this.resultSet = this.resultSet.sort(function (a, b) {
    if (a.score.score < b.score.score) return 1
    if (a.score.score > b.score.score) return -1
    if (a.document.id < b.document.id) return 1
    if (a.document.id > b.document.id) return -1
    return 0
  })
  this.resultSet.forEach(function (hit) {
    that.push(JSON.stringify(hit, null, 2))
  })
  this.push(JSON.stringify({metadata: {totalHits: 'tbs'}}))
  return end()
}
