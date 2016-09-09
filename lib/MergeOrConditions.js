const Transform = require('stream').Transform
const util = require('util')

const MergeOrConditions = function () {
  this.resultSet = []
  Transform.call(this, { objectMode: true })
}
exports.MergeOrConditions = MergeOrConditions
util.inherits(MergeOrConditions, Transform)
MergeOrConditions.prototype._transform = function (doc, encoding, end) {
  doc = JSON.parse(doc)
  this.resultSet.push(doc)
  return end()
}
MergeOrConditions.prototype._flush = function (end) {
  var that = this
  // get rid of OR conditions- merge on doc id
  var mergedResultSet = this.resultSet.sort(function (a, b) {
    return a.id < b.id
  }).reduce(function(merged, cur) {
    var lastDoc = merged[merged.length - 1]
    if (merged.length > 0 && (cur.id == lastDoc.id)) {
      // merge here
      lastDoc.scoringCriteria.push(cur.scoringCriteria[0])
      lastDoc.score = ((lastDoc.score + cur.score) / 2)
    } else {
      merged.push(cur)
    }
    return merged
  }, [])
  mergedResultSet.forEach(function(item) {
    that.push(JSON.stringify(item))
  })
  return end()
}
