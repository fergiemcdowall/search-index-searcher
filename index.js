const CalculateBuckets = require('./lib/CalculateBuckets.js').CalculateBuckets
const CalculateCategories = require('./lib/CalculateCategories.js').CalculateCategories
const CalculateResultSet = require('./lib/CalculateResultSet.js').CalculateResultSet
const CalculateTopScoringDocs = require('./lib/CalculateTopScoringDocs.js').CalculateTopScoringDocs
const FetchDocsFromDB = require('./lib/FetchDocsFromDB.js').FetchDocsFromDB
const GetIntersectionStream = require('./lib/GetIntersectionStream.js').GetIntersectionStream
const JSONStream = require('JSONStream')
const Readable = require('stream').Readable
const ScoreTopScoringDocs = require('./lib/ScoreTopScoringDocs.js').ScoreTopScoringDocs
const SortTopScoringDocs = require('./lib/SortTopScoringDocs.js').SortTopScoringDocs
const siUtil = require('./lib/siUtil.js')

module.exports = function (givenOptions, callback) {
  siUtil.getOptions(givenOptions, function (err, options) {
    var Searcher = {} // const?

    Searcher.scan = function (q) {
      q = siUtil.getQueryDefaults(q)
      // just make this work for a simple one clause AND
      // TODO: add filtering, NOTting, multi-clause AND
      var s = new Readable()
      s.push('init')
      s.push(null)
      return s.pipe(
        new GetIntersectionStream(options,
          siUtil.getKeySet(q.query.AND, q.filter || {})))
        .pipe(new FetchDocsFromDB(options))
    }

    Searcher.searchStream = function (q) {
      q = siUtil.getQueryDefaults(q)
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

    Searcher.bucketStream = function (q) {
      q = siUtil.getQueryDefaults(q)
      const s = new Readable()
      q.query.forEach(function (clause) {
        s.push(JSON.stringify(clause))
      })
      s.push(null)
      return s.pipe(JSONStream.parse())
        .pipe(new CalculateResultSet(options, q.filter || {}))
        .pipe(new CalculateBuckets(options, q.filter || {}, q.buckets))
    }

    Searcher.categoryStream = function (q) {
      q = siUtil.getQueryDefaults(q)
      const s = new Readable()
      q.query.forEach(function (clause) {
        s.push(JSON.stringify(clause))
      })
      s.push(null)
      return s.pipe(JSONStream.parse())
        .pipe(new CalculateResultSet(options, q.filter || {}))
        .pipe(new CalculateCategories(options, q.filter || {}, q.category))
    }

    Replicator.dbReadStream = function (ops) {
      ops = _defaults(ops || {}, {gzip: false})
      if (ops.gzip) {
        return options.indexes.createReadStream()
          .pipe(JSONStream.stringify('', '\n', ''))
          .pipe(zlib.createGzip())
      } else {
        return options.indexes.createReadStream()
      }
    }

    return callback(err, Searcher)
  })
}
