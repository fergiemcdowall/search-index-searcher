const async = require('async')
const Readable = require('stream').Readable
const Transform = require('stream').Transform
const util = require('util')
const _defaults = require('lodash.defaults')
const _intersection = require('lodash.intersection')
const _uniq = require('lodash.uniq')
const _ = require('lodash')
const JSONStream = require('JSONStream')

// TODO
// Unlodashify!

exports.searchStream = function (q, options) {
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

exports.bucketStream = function (q, options) {
  q = _defaults(q || {}, {
    query: {
      AND: [{'*': ['*']}]
    },
    buckets: []
  })
  const s = new Readable()
  q.query.forEach(function (clause) {
    s.push(JSON.stringify(clause))
  })
  s.push(null)
  return s.pipe(JSONStream.parse())
    .pipe(new CalculateResultSet(options, q.filter || {}))
    .pipe(new CalculateBuckets(options, q.filter || {}, q.buckets))
}

exports.categoryStream = function (q, options) {
  q = _defaults(q || {}, {
    query: {
      AND: [{'*': ['*']}]
    },
    category: {}
  })
  const s = new Readable()
  q.query.forEach(function (clause) {
    s.push(JSON.stringify(clause))
  })
  s.push(null)
  return s.pipe(JSONStream.parse())
    .pipe(new CalculateResultSet(options, q.filter || {}))
    .pipe(new CalculateCategories(options, q.filter || {}, q.category))
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
    var include = []
    that.options.indexes.createReadStream({gte: item[0], lte: item[1] + '￮'})
      .on('data', function (data) {
        include = uniqFast(include.concat(data.value))
      })
      .on('error', function (err) {
        that.options.log.debug(err)
      })
      .on('end', function () {
        var fKey = item[0].split('￮')[1] + '￮' + item[0].split('￮')[2]
        frequencies[fKey] = include.length
        return callback(null, include.sort())
      })
  }, function (asyncerr, includeResults) {
    const bigIntersect = _.spread(_.intersection)
    var include = bigIntersect(includeResults)
    // NOTing
    async.map(getKeySet(queryClause.NOT, this.filter), function (item, callback) {
      var exclude = []
      that.options.indexes.createReadStream({gte: item[0], lte: item[1] + '￮'})
        .on('data', function (data) {
          exclude = uniqFast(exclude.concat(data.value))
        })
        .on('error', function (err) {
          that.options.log.debug(err)
        })
        .on('end', function () {
          return callback(null, exclude.sort())
        })
    }, function (asyncerr, excludeResults) {
      excludeResults.forEach(function (excludeSet) {
        include = _.difference(include, excludeSet)
      })
      that.push(JSON.stringify({
        queryClause: queryClause,
        set: include,
        termFrequencies: frequencies,
        BOOST: queryClause.BOOST || 0
      }))
      return end()
    })
  })
}

function CalculateTopScoringDocs (options, seekLimit) {
  this.options = options
  this.seekLimit = seekLimit
  Transform.call(this, { objectMode: true })
}
util.inherits(CalculateTopScoringDocs, Transform)
CalculateTopScoringDocs.prototype._transform = function (clauseSet, encoding, end) {
  clauseSet = JSON.parse(clauseSet)
  const that = this
  const lowestFrequency = Object.keys(clauseSet.termFrequencies)
    .map(function (key) {
      return [key, clauseSet.termFrequencies[key]]
    }).sort(function (a, b) {
      return a[1] - b[1]
    })[0]
  const gte = 'TF￮' + lowestFrequency[0] + '￮￮'
  const lte = 'TF￮' + lowestFrequency[0] + '￮￮￮'

  // walk down the DF array of lowest frequency hit until (offset +
  // pagesize) hits have been found

  var topScoringDocs = []
  that.options.indexes.createReadStream({gte: gte, lte: lte})
    .on('data', function (data) {
      var intersections = []
      // Do intersection and pagination cutoffs here- only push
      // results that are in the resultset
      for (var i = 0
        ; ((i < data.value.length) && (intersections.length < that.seekLimit)); i++) {
        if (_.sortedIndexOf(clauseSet.set, data.value[i][1]) !== -1) {
          intersections.push(data.value[i])
        }
      }
      topScoringDocs = topScoringDocs.concat(intersections)
    })
    .on('error', function (err) {
      that.options.log.debug(err)
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

function CalculateBuckets (options, filter, requestedBuckets) {
  this.buckets = requestedBuckets || []
  this.filter = filter
  this.options = options
  Transform.call(this, { objectMode: true })
}
util.inherits(CalculateBuckets, Transform)
CalculateBuckets.prototype._transform = function (queryClause, encoding, end) {
  var that = this
  queryClause = JSON.parse(queryClause)
  // Shouldnt get every key in the AND set- should just get key with
  // lowest frequency
  // get lowest frequency key
  const lowestFrequencyKey = Object.keys(queryClause.termFrequencies)
    .map(function (key) {
      return [key, queryClause.termFrequencies[key]]
    }).sort(function (a, b) {
      return a[1] - b[1]
    })[0]
  async.map(that.buckets, function (bucket, bucketProcessed) {
    var fieldName = lowestFrequencyKey[0].split('￮')[0]
    var token = lowestFrequencyKey[0].split('￮')[1]
    var gte = 'DF￮' + fieldName + '￮' + token + '￮' +
    bucket.field + '￮' +
    bucket.gte
    var lte = 'DF￮' + fieldName + '￮' + token + '￮' +
      bucket.field + '￮' +
      bucket.lte + '￮'
    // TODO: add some logic to see if keys are within ranges before doing a lookup
    that.options.indexes.createReadStream({gte: gte, lte: lte})
      .on('data', function (data) {
        var IDSet = _intersection(data.value, queryClause.set)
        if (IDSet.length > 0) {
          bucket.IDSet = bucket.IDSet || []
          bucket.IDSet = _uniq(bucket.IDSet.concat(IDSet).sort())
        }
        // TODO: make loop aware of last iteration so that stream can
        // be pushed before _flush
      })
      .on('close', function () {
        return bucketProcessed(null)
      })
  }, function (err) {
    if (err) {
      // what to do?
    }
    return end()
  })
}
CalculateBuckets.prototype._flush = function (end) {
  var that = this
  this.buckets.forEach(function (bucket) {
    that.push(JSON.stringify(bucket))
  })
  return end()
}

function CalculateCategories (options, filter, category) {
  category.values = []
  this.category = category
  this.filter = filter
  this.options = options
  Transform.call(this, { objectMode: true })
}
util.inherits(CalculateCategories, Transform)
CalculateCategories.prototype._transform = function (queryClause, encoding, end) {
  var that = this
  queryClause = JSON.parse(queryClause)
  // Shouldnt get every key in the AND set- should just get key with
  // lowest frequency
  // get lowest frequency key
  const lowestFrequencyKey = Object.keys(queryClause.termFrequencies)
    .map(function (key) {
      return [key, queryClause.termFrequencies[key]]
    }).sort(function (a, b) {
      return a[1] - b[1]
    })[0]
  // async.map(that.categories, function (category, categoryProcessed) {
  var fieldName = lowestFrequencyKey[0].split('￮')[0]
  var token = lowestFrequencyKey[0].split('￮')[1]
  var gte = 'DF￮' + fieldName + '￮' + token + '￮' +
    this.category.field + '￮'
  var lte = 'DF￮' + fieldName + '￮' + token + '￮' +
    this.category.field + '￮￮'
  this.category.values = this.category.values || []
  // TODO: add some logic to see if keys are within ranges before doing a lookup
  that.options.indexes.createReadStream({gte: gte, lte: lte})
    .on('data', function (data) {
      var IDSet = _intersection(data.value, queryClause.set)
      if (IDSet.length > 0) {
        var key = data.key.split('￮')[4]
        that.category.values.push({
          key: key,
          value: data.value
        })
      }
      // TODO: make loop aware of last iteration so that stream can
      // be pushed before _flush
    })
    .on('close', function () {
      return end()
    })
}
CalculateCategories.prototype._flush = function (end) {
  var that = this
  if (this.category.values.set) {
    this.category.values.map(function (elem) {
      elem.value = elem.value.length
      return elem
    })
  }
  this.category.values.sort(this.category.sort)
  this.category.values =
    this.category.values.slice(0, this.category.limit ||
      this.category.values.length)
  if (!this.category.set) {
    this.category.values.map(function (elem) {
      elem.value = elem.value.length
      return elem
    })
  }
  this.category.values.forEach(function (elem) {
    that.push(elem)
  })
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
