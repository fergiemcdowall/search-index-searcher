const Transform = require('stream').Transform
const util = require('util')

const MergeOrConditionsTFIDF = function (offset, pageSize) {
  this.resultSet = []
  this.offset = offset
  this.pageSize = pageSize
  Transform.call(this, { objectMode: true })
}
exports.MergeOrConditionsTFIDF = MergeOrConditionsTFIDF
util.inherits(MergeOrConditionsTFIDF, Transform)
MergeOrConditionsTFIDF.prototype._transform = function (doc, encoding, end) {
  // console.log(JSON.stringify(doc, null, 2))
  this.resultSet = this.resultSet.concat(doc.matches)
  return end()
}
MergeOrConditionsTFIDF.prototype._flush = function (end) {
  var that = this
  // turn top scoring results into a merged map
  var resultMap = getResultMap(this.resultSet)

  // turn map back into array and score
  Object.keys(resultMap).map((id) => {
    return {
      id: id,
      scoringCriteria: resultMap[id],
      score: resultMap[id].reduce((sum, matches) => {
        return sum + matches.score
      }, 0) / resultMap[id].length
    }
  }).sort((a, b) => {
    // sort by score then id descending
    if (b.score === a.score) {
      return (a.id < b.id) ? 1 : (a.id > b.id) ? -1 : 0;
    }
    return (b.score - a.score)
  }).slice(
    // do paging
    this.offset, this.offset + this.pageSize
  ).forEach((doc) => {
    // stream docs
    that.push(doc)
  })
  return end()
}

// Make a merged map of the top scoring documents
const getResultMap = (resultSet) => {
  var resultMap = {}
  resultSet.forEach((docMatch) => {
    resultMap[docMatch.id] = resultMap[docMatch.id] || []
    resultMap[docMatch.id].push(docMatch)
    delete docMatch.id
  })
  return resultMap
}
