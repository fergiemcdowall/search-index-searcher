const Transform = require('stream').Transform
const util = require('util')

const MergeOrConditionsFieldSort = function (q) {
  this.resultSet = []
  this.offset = q.offset
  this.pageSize = q.pageSize
  this.sortDirection = q.sort.direction || 'desc'
  Transform.call(this, { objectMode: true })
}
exports.MergeOrConditionsFieldSort = MergeOrConditionsFieldSort
util.inherits(MergeOrConditionsFieldSort, Transform)
MergeOrConditionsFieldSort.prototype._transform = function (clause, encoding, end) {
  this.resultSet = this.resultSet.concat(clause.topScoringDocs)
  return end()
}
MergeOrConditionsFieldSort.prototype._flush = function (end) {
  var that = this
  // turn top scoring results into a merged map
  var resultMap = getResultMap(this.resultSet)
  Object.keys(resultMap).map((id) => {
    return {
      id: id,
      scoringCriteria: resultMap[id],
      score: resultMap[id][0].score // all scores should be the same so just take first one
    }
  }).sort((a, b) => {
    // sort by score then id descending
    if (b.score === a.score) {
      return (a.id < b.id) ? 1 : (a.id > b.id) ? -1 : 0
    }

    // TODO add numeric sorts

    if (that.sortDirection === 'asc') {
      return (b.score < a.score)
    } else {
      return (b.score > a.score)
    }
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
