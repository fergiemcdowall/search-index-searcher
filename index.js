const CalculateBuckets = require('./lib/CalculateBuckets.js').CalculateBuckets
const CalculateCategories = require('./lib/CalculateCategories.js').CalculateCategories
const CalculateResultSetPerClause = require('./lib/CalculateResultSetPerClause.js').CalculateResultSetPerClause
const CalculateEntireResultSet = require('./lib/CalculateEntireResultSet.js').CalculateEntireResultSet
const CalculateTopScoringDocs = require('./lib/CalculateTopScoringDocs.js').CalculateTopScoringDocs
const MergeOrConditions = require('./lib/MergeOrConditions.js').MergeOrConditions
const FetchDocsFromDB = require('./lib/FetchDocsFromDB.js').FetchDocsFromDB
const FetchStoredDoc = require('./lib/FetchStoredDoc.js').FetchStoredDoc
const GetIntersectionStream = require('./lib/GetIntersectionStream.js').GetIntersectionStream
const JSONStream = require('JSONStream')
const Readable = require('stream').Readable
const ScoreTopScoringDocsTFIDF = require('./lib/ScoreTopScoringDocsTFIDF.js').ScoreTopScoringDocsTFIDF
const SortTopScoringDocs = require('./lib/SortTopScoringDocs.js').SortTopScoringDocs
const ScoreDocsOnField = require('./lib/ScoreDocsOnField.js').ScoreDocsOnField
const _defaults = require('lodash.defaults')
const siUtil = require('./lib/siUtil.js')
const matcher = require('./lib/matcher.js')
const zlib = require('zlib')

module.exports = function (givenOptions, callback) {
  siUtil.getOptions(givenOptions, function (err, options) {
    var Searcher = {} // const?

    Searcher.match = function (q) {
      return matcher.match(q, options)
    }

    Searcher.scan = function (q) {
      q = siUtil.getQueryDefaults(q)
      // just make this work for a simple one clause AND
      // TODO: add filtering, NOTting, multi-clause AND
      var s = new Readable()
      s.push('init')
      s.push(null)
      return s.pipe(
        new GetIntersectionStream(options, siUtil.getKeySet(q.query.AND)))
        .pipe(new FetchDocsFromDB(options))
    }

    Searcher.search = function (q) {
      q = siUtil.getQueryDefaults(q)
      const s = new Readable()
      // more forgivable querying
      if (Object.prototype.toString.call(q.query) !== '[object Array]') {
        q.query = [q.query]
      }
      q.query.forEach(function (clause) {
        s.push(JSON.stringify(clause))
      })
      s.push(null)

      if (q.sort) {
        return s
          .pipe(JSONStream.parse())
          .pipe(new CalculateResultSetPerClause(options))
          .pipe(new CalculateTopScoringDocs(options, (q.offset + q.pageSize)))
          .pipe(new ScoreDocsOnField(options, (q.offset + q.pageSize), q.sort))
          .pipe(new MergeOrConditions(q))
          .pipe(new SortTopScoringDocs(q))
          .pipe(new FetchStoredDoc(options))
      } else {
        return s
          .pipe(JSONStream.parse())
          .pipe(new CalculateResultSetPerClause(options))
          .pipe(new CalculateTopScoringDocs(options, (q.offset + q.pageSize)))
          .pipe(new ScoreTopScoringDocsTFIDF(options))
          .pipe(new MergeOrConditions(q))
          .pipe(new SortTopScoringDocs(q))
          .pipe(new FetchStoredDoc(options))
      }
    }

    Searcher.bucketStream = function (q) {
      q = siUtil.getQueryDefaults(q)
      const s = new Readable()
      q.query.forEach(function (clause) {
        s.push(JSON.stringify(clause))
      })
      s.push(null)
      return s.pipe(JSONStream.parse())
        .pipe(new CalculateResultSetPerClause(options, q.filter || {}))
        .pipe(new CalculateEntireResultSet(options))
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
        .pipe(new CalculateResultSetPerClause(options, q.filter || {}))
        .pipe(new CalculateEntireResultSet(options))
        .pipe(new CalculateCategories(options, q.category))
    }

    Searcher.dbReadStream = function (ops) {
      ops = _defaults(ops || {}, {gzip: false})
      if (ops.gzip) {
        return options.indexes.createReadStream()
          .pipe(JSONStream.stringify('', '\n', ''))
          .pipe(zlib.createGzip())
      } else {
        return options.indexes.createReadStream()
      }
    }

    Searcher.close = function (callback) {
      options.indexes.close(function (err) {
        while (!options.indexes.isClosed()) {
          options.log.debug('closing...')
        }
        if (options.indexes.isClosed()) {
          options.log.debug('closed...')
          callback(err)
        }
      })
    }

    return callback(err, Searcher)
  })
}
