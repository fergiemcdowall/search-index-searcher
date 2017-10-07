const Transform = require('stream').Transform
const _sortedIndexOf = require('lodash.sortedindexof')
const util = require('util')

const ScoreDocsOnField = function (options, seekLimit, sort) {
  this.options = options
  this.seekLimit = seekLimit
  this.sort = sort
  Transform.call(this, { objectMode: true })
}
exports.ScoreDocsOnField = ScoreDocsOnField
util.inherits(ScoreDocsOnField, Transform)
ScoreDocsOnField.prototype._transform = function (clauseSet, encoding, end) {
  const sep = this.options.keySeparator
  // clauseSet = JSON.parse(clauseSet)
  const that = this

  const sortDirection = this.sort.direction || 'asc'
  
  // if sort direction is asc
  // walk down the DF array of lowest frequency hit until (offset +
  // pagesize) hits have been found
  var tally = 0;
  var rangeOptions = {
    gte: 'DF' + sep + this.sort.field + sep,
    lte: 'DF' + sep + this.sort.field + sep + sep
  }
  if (sortDirection === 'desc') rangeOptions.reverse = true
  clauseSet.topScoringDocs = []
  that.options.indexes.createReadStream(rangeOptions)
      .on('data', function (data) {
        var token = data.key.split(sep)[2]
        if (sortDirection === 'desc') data.value = data.value.reverse()
        data.value.some(id => {
          if (_sortedIndexOf(clauseSet.set, id) !== -1) {
            clauseSet.topScoringDocs.push({
              id: id,
              score: token
            })
            // only match docs up to the seek limit (offset + pagesize)
            if (++tally === that.seekLimit) return true
          }
        })
      }).on('end', function () {
        that.push(clauseSet)
        return end()
      })
}
