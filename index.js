const CalculateBuckets = require('./lib/CalculateBuckets.js').CalculateBuckets
const CalculateCategories = require('./lib/CalculateCategories.js').CalculateCategories
const CalculateEntireResultSet = require('./lib/CalculateEntireResultSet.js').CalculateEntireResultSet
const CalculateResultSetPerClause = require('./lib/CalculateResultSetPerClause.js').CalculateResultSetPerClause
const CalculateTopScoringDocs = require('./lib/CalculateTopScoringDocs.js').CalculateTopScoringDocs
const CalculateTotalHits = require('./lib/CalculateTotalHits.js').CalculateTotalHits
const FetchDocsFromDB = require('./lib/FetchDocsFromDB.js').FetchDocsFromDB
const FetchStoredDoc = require('./lib/FetchStoredDoc.js').FetchStoredDoc
const GetIntersectionStream = require('./lib/GetIntersectionStream.js').GetIntersectionStream
const JSONStream = require('JSONStream')
const MergeOrConditions = require('./lib/MergeOrConditions.js').MergeOrConditions
const Readable = require('stream').Readable
const ScoreDocsOnField = require('./lib/ScoreDocsOnField.js').ScoreDocsOnField
const ScoreTopScoringDocsTFIDF = require('./lib/ScoreTopScoringDocsTFIDF.js').ScoreTopScoringDocsTFIDF
const SortTopScoringDocs = require('./lib/SortTopScoringDocs.js').SortTopScoringDocs
const _defaults = require('lodash.defaults')
const bunyan = require('bunyan')
const levelup = require('levelup')
const matcher = require('./lib/matcher.js')
const siUtil = require('./lib/siUtil.js')
const sw = require('stopword')

const initModule = function (err, Searcher, moduleReady) {
  Searcher.bucketStream = function (q) {
    q = siUtil.getQueryDefaults(q)
    const s = new Readable()
    q.query.forEach(function (clause) {
      s.push(JSON.stringify(clause))
    })
    s.push(null)
    return s.pipe(JSONStream.parse())
      .pipe(new CalculateResultSetPerClause(Searcher.options, q.filter || {}))
      .pipe(new CalculateEntireResultSet(Searcher.options))
      .pipe(new CalculateBuckets(Searcher.options, q.filter || {}, q.buckets))
  }

  Searcher.categoryStream = function (q) {
    q = siUtil.getQueryDefaults(q)
    const s = new Readable()
    q.query.forEach(function (clause) {
      s.push(JSON.stringify(clause))
    })
    s.push(null)
    return s.pipe(JSONStream.parse())
      .pipe(new CalculateResultSetPerClause(Searcher.options, q.filter || {}))
      .pipe(new CalculateEntireResultSet(Searcher.options))
      .pipe(new CalculateCategories(Searcher.options, q.category))
  }

  Searcher.close = function (callback) {
    Searcher.options.indexes.close(function (err) {
      while (!Searcher.options.indexes.isClosed()) {
        Searcher.options.log.debug('closing...')
      }
      if (Searcher.options.indexes.isClosed()) {
        Searcher.options.log.debug('closed...')
        callback(err)
      }
    })
  }

  Searcher.dbReadStream = function () {
    return Searcher.options.indexes.createReadStream()
  }

  Searcher.get = function (docIDs) {
    var s = new Readable()
    docIDs.forEach(function (id) {
      s.push(id)
    })
    s.push(null)
    return s.pipe(new FetchDocsFromDB(Searcher.options))
  }

  Searcher.match = function (q) {
    return matcher.match(q, Searcher.options)
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
        .pipe(new CalculateResultSetPerClause(Searcher.options))
        .pipe(new CalculateTopScoringDocs(Searcher.options, (q.offset + q.pageSize)))
        .pipe(new ScoreDocsOnField(Searcher.options, (q.offset + q.pageSize), q.sort))
        .pipe(new MergeOrConditions(q))
        .pipe(new SortTopScoringDocs(q))
        .pipe(new FetchStoredDoc(Searcher.options))
    } else {
      return s
        .pipe(JSONStream.parse())
        .pipe(new CalculateResultSetPerClause(Searcher.options))
        .pipe(new CalculateTopScoringDocs(Searcher.options, (q.offset + q.pageSize)))
        .pipe(new ScoreTopScoringDocsTFIDF(Searcher.options))
        .pipe(new MergeOrConditions(q))
        .pipe(new SortTopScoringDocs(q))
        .pipe(new FetchStoredDoc(Searcher.options))
    }
  }

  Searcher.scan = function (q) {
    q = siUtil.getQueryDefaults(q)
    // just make this work for a simple one clause AND
    // TODO: add filtering, NOTting, multi-clause AND
    var s = new Readable()
    s.push('init')
    s.push(null)
    return s.pipe(
      new GetIntersectionStream(Searcher.options, siUtil.getKeySet(q.query.AND)))
      .pipe(new FetchDocsFromDB(Searcher.options))
  }

  Searcher.totalHits = function (q, callback) {
    q = siUtil.getQueryDefaults(q)
    const s = new Readable()
    q.query.forEach(function (clause) {
      s.push(JSON.stringify(clause))
    })
    s.push(null)
    s.pipe(JSONStream.parse())
      .pipe(new CalculateResultSetPerClause(Searcher.options, q.filter || {}))
      .pipe(new CalculateEntireResultSet(Searcher.options))
      .pipe(new CalculateTotalHits(Searcher.options)).on('data', function (totalHits) {
        return callback(null, totalHits)
      })
  }

  return moduleReady(err, Searcher)
}

const getOptions = function (options, done) {
  var Searcher = {}
  Searcher.options = _defaults(options, {
    deletable: true,
    fieldedSearch: true,
    store: true,
    indexPath: 'si',
    logLevel: 'error',
    nGramLength: 1,
    nGramSeparator: ' ',
    separator: /[\|' \.,\-|(\n)]+/,
    stopwords: sw.en
  })
  options.log = bunyan.createLogger({
    name: 'search-index',
    level: options.logLevel
  })
  if (!options.indexes) {
    levelup(Searcher.options.indexPath || 'si', {
      valueEncoding: 'json'
    }, function (err, db) {
      Searcher.options.indexes = db
      return done(err, Searcher)
    })
  } else {
    return done(null, Searcher)
  }
}

module.exports = function (givenOptions, moduleReady) {
  getOptions(givenOptions, function (err, Searcher) {
    initModule(err, Searcher, moduleReady)
  })
}
