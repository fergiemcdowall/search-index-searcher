const async = require('async')
const Readable = require('stream').Readable
const Transform = require('stream').Transform
const util = require('util')
const _uniqWith = require('lodash.uniqwith')
const _defaults = require('lodash.defaults')
const _flatten = require('lodash.flatten')
const _intersection = require('lodash.intersection')
const _isEqual = require('lodash.isequal')
const _ = require('lodash')
const JSONStream = require('JSONStream')

// TODO
// Unlodashify!


exports.searchStream = function(q, options) {
  q = _defaults(q || {}, {
    query: {
      AND: [{'*': ['*']}]
    },
    offset: 0,
    pageSize: 20
  })
  const s = new Readable()
  q.query.forEach(function (clause) {
    s.push(JSON.stringify(clause))
  })
  s.push(null)
  return s
    .pipe(JSONStream.parse())
    .pipe(new CalculateResultSet(options, q.filter || {}))
    .pipe(new CalculateTopScoringDocs(options, (q.offset + q.pageSize)))
    .pipe(new ScoreTopScoringDocs(options, (q.offset + q.pageSize)))
    .pipe(new SortTopScoringDocs())
}

function CalculateResultSet (options, filter) {
  this.options = options
  this.filter = filter
  Transform.call(this, { objectMode: true })
}
util.inherits(CalculateResultSet, Transform)
CalculateResultSet.prototype._transform = function (queryClause, encoding, end) {
  const that = this
  const frequencies = {}
  async.map(getKeySet(queryClause.AND, this.filter), function (item, callback) {
    var uniq = []
    that.options.indexes.createReadStream({gte: item[0], lte: item[1] + '￮'})
      .on('data', function (data) {
        uniq = uniqFast(uniq.concat(data.value))
      })
      .on('error', function (err) {
        options.log.debug(err)
      })
      .on('end', function () {
        // frequencies.push([item[0], uniq.length])
        frequencies[item[0].substring(3).slice(0, - 2)] = uniq.length
        return callback(null, uniq.sort())
      })
  }, function (asyncerr, results) {
    const bigIntersect = _.spread(_.intersection)
 
    // TODO: deal with NOTing

    that.push(JSON.stringify({
      queryClause: queryClause,
      set: bigIntersect(results),
      termFrequencies: frequencies,
      BOOST: queryClause.BOOST || 0
    }))
    end()
  })
}

function CalculateTopScoringDocs (options, seekLimit) {
  this.options = options
  this.seekLimit = seekLimit
  Transform.call(this, { objectMode: true })
}
util.inherits(CalculateTopScoringDocs, Transform)
CalculateTopScoringDocs.prototype._transform = function (clauseSet, encoding, end) {
  console.log(clauseSet)
  clauseSet = JSON.parse(clauseSet)
  const that = this

  var lowestFrequency = Object.keys(clauseSet.termFrequencies)
    .map(function (key) {
      return [key, clauseSet.termFrequencies[key]]
    }).sort(function(a, b) {
      return a[1] - b[1]
    })[0]

  var gte = 'TF￮' + lowestFrequency[0] + '￮￮'
  var lte = 'TF￮' + lowestFrequency[0] + '￮￮￮'

  // walk down the DF array of lowest frequency hit until (offset +
  // pagesize) hits have been found

  var topScoringDocs = []
  that.options.indexes.createReadStream({gte: gte, lte: lte})
    .on('data', function (data) {
      var intersections = []
      // Do intersection and pagination cutoffs here- only push
      // results that are in the resultset
      for (var i = 0; ((i < data.value.length) && (intersections.length < that.seekLimit)); i++) {
        if (_.sortedIndexOf(clauseSet.set, data.value[i][1]) != -1) {
          intersections.push(data.value[i])
        }
      }
      topScoringDocs = topScoringDocs.concat(intersections)
    })
    .on('error', function (err) {
      options.log.debug(err)
    })
    .on('end', function () {
      // fetch document vectors for the highest scores and work out
      // complete score for each selected doc.
      clauseSet['topScoringDocs'] = topScoringDocs
      that.push(JSON.stringify(clauseSet))
      end()
    })
}


function ScoreTopScoringDocs (options, seekLimit) {
  this.options = options
  this.queryClause
  this.seekLimit = seekLimit
  Transform.call(this, { objectMode: true })
}
util.inherits(ScoreTopScoringDocs, Transform)
ScoreTopScoringDocs.prototype._transform = function (clause, encoding, end) {
  const that = this
  clause = JSON.parse(clause)
  var fields = Object.keys(clause.queryClause.AND)
  async.each(clause.topScoringDocs, function(docID, nextDocCallback) {
    docID = docID[1]
    async.map(fields, function(field, callback) {
      that.options.indexes.get(
        'DOCUMENT-VECTOR￮' + docID + '￮' + field + '￮', function(err, docVector) {
          var vector = {}
          docVector.forEach(function (element) {
            if (clause.queryClause.AND[field].indexOf(element[0][0]) != -1) {
              vector[field + '￮' + element[0]] = element[1]
            }
          })
          return callback(err, vector)
        })
    }, function(err, results) {
      that.options.indexes.get(
        'DOCUMENT￮' + docID + '￮', function(err, doc) {
          var tfidf = {}
          Object.keys(clause.termFrequencies).forEach(function(key) {
            var tf = +clause.termFrequencies[key]
            var df = +results[0][key]
            var idf = Math.log10(1 + (1/df))
            tfidf[key] = tf * idf
          })
          var score = (Object.keys(tfidf).reduce(function(prev, cur) {
            return tfidf[prev] + tfidf[cur]
          }) / Object.keys(tfidf).length)
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
  }, function(err) {
    return end()
  })
}


function SortTopScoringDocs (options) {
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
  this.resultSet = this.resultSet.sort(function(a, b) {
    return a.score.score - b.score.score
  })
  this.resultSet.forEach(function(hit) {
    that.push(JSON.stringify(hit, null, 2))
  })
  this.push(JSON.stringify({metadata: {totalHits: 'tbs'}}))
  return end()
}


var getKeySet = function (clause, filter) {
  var keySet = []
  for (var fieldName in clause) {
    clause[fieldName].forEach(function (token) {
      // keySet[clause].push('DF' + '￮' + fieldName + '￮' + token + '￮￮')
      if (filter && Array.isArray(filter)) {
        // Filters: TODO
        filter.forEach(function (filter) {
          keySet.push([
            'DF￮' + fieldName + '￮' + token + '￮' + filter.field + '￮' + filter.gte,
            'DF￮' + fieldName + '￮' + token + '￮' + filter.field + '￮' + filter.lte + '￮'
          ])
        })
      } else {
        keySet.push([
          'DF￮' + fieldName + '￮' + token + '￮￮',
          'DF￮' + fieldName + '￮' + token + '￮￮￮'
        ])
      }
    })
  }
  return keySet
}


// supposedly fastest way to get unique values in an array
// http://stackoverflow.com/questions/9229645/remove-duplicates-from-javascript-array
var uniqFast = function (a) {
  var seen = {}
  var out = []
  var len = a.length
  var j = 0
  for (var i = 0; i < len; i++) {
    var item = a[i]
    if (seen[item] !== 1) {
      seen[item] = 1
      out[j++] = item
    }
  }
  return out
}
