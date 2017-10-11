const Logger = require('js-logger')
const AvailableFields = require('./lib/AvailableFields.js').AvailableFields
const CalculateBuckets = require('./lib/CalculateBuckets.js').CalculateBuckets
const CalculateCategories = require('./lib/CalculateCategories.js').CalculateCategories
const CalculateEntireResultSet = require('./lib/CalculateEntireResultSet.js').CalculateEntireResultSet
const CalculateResultSetPerClause = require('./lib/CalculateResultSetPerClause.js').CalculateResultSetPerClause
const CalculateTopScoringDocs = require('./lib/CalculateTopScoringDocs.js').CalculateTopScoringDocs
const CalculateTotalHits = require('./lib/CalculateTotalHits.js').CalculateTotalHits
const Classify = require('./lib/Classify.js').Classify
const FetchDocsFromDB = require('./lib/FetchDocsFromDB.js').FetchDocsFromDB
const FetchStoredDoc = require('./lib/FetchStoredDoc.js').FetchStoredDoc
const GetIntersectionStream = require('./lib/GetIntersectionStream.js').GetIntersectionStream
const MergeOrConditionsTFIDF = require('./lib/MergeOrConditionsTFIDF.js').MergeOrConditionsTFIDF
const MergeOrConditionsFieldSort = require('./lib/MergeOrConditionsFieldSort.js').MergeOrConditionsFieldSort
const Readable = require('stream').Readable
const ScoreDocsOnField = require('./lib/ScoreDocsOnField.js').ScoreDocsOnField
const ScoreTopScoringDocsTFIDF = require('./lib/ScoreTopScoringDocsTFIDF.js').ScoreTopScoringDocsTFIDF
const levelup = require('levelup')
const matcher = require('./lib/matcher.js')
const siUtil = require('./lib/siUtil.js')
const sw = require('stopword')

const initModule = function (err, Searcher, moduleReady) {
  Searcher.bucketStream = function (q) {
    q = siUtil.getQueryDefaults(q)
    const s = new Readable({ objectMode: true })
    q.query.forEach(function (clause) {
      s.push(clause)
    })
    s.push(null)
    return s
      .pipe(new CalculateResultSetPerClause(Searcher.options, q.filter || {}))
      .pipe(new CalculateEntireResultSet(Searcher.options))
      .pipe(new CalculateBuckets(Searcher.options, q.filter || {}, q.buckets))
  }

  Searcher.categoryStream = function (q) {
    q = siUtil.getQueryDefaults(q)
    const s = new Readable({ objectMode: true })
    q.query.forEach(function (clause) {
      s.push(clause)
    })
    s.push(null)
    return s
      .pipe(new CalculateResultSetPerClause(Searcher.options, q.filter || {}))
      .pipe(new CalculateEntireResultSet(Searcher.options))
      .pipe(new CalculateCategories(Searcher.options, q))
  }

  Searcher.classify = function (ops) {
    return new Classify(Searcher, ops)
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

  Searcher.availableFields = function () {
    const sep = Searcher.options.keySeparator
    return Searcher.options.indexes.createReadStream({
      gte: 'FIELD' + sep,
      lte: 'FIELD' + sep + sep
    }).pipe(new AvailableFields(Searcher.options))
  }

  Searcher.get = function (docIDs) {
    var s = new Readable({ objectMode: true })
    docIDs.forEach(function (id) {
      s.push(id)
    })
    s.push(null)
    return s.pipe(new FetchDocsFromDB(Searcher.options))
  }

  Searcher.fieldNames = function () {
    return Searcher.options.indexes.createReadStream({
      gte: 'FIELD',
      lte: 'FIELD'
    })
  }

  Searcher.match = function (q) {
    return matcher.match(q, Searcher.options)
  }

  Searcher.dbReadStream = function () {
    return Searcher.options.indexes.createReadStream()
  }

  Searcher.search = function (q) {
    q = siUtil.getQueryDefaults(q)
    const s = new Readable({ objectMode: true })
    q.query.forEach(function (clause) {
      s.push(clause)
    })
    s.push(null)
    if (q.sort) {
      /*

         Can be 2 scenarios:
         1) Lots of hits in a smallish index (dense)
         2) a few hits in a gigantic index (sparse)

         Since 2) can be solved by simply sorting afterwards- solve 1) first

         should be

         1) Calculate resultset per clause
         2) Traverse TF arrays and check for intersections down to "seek limit"
         3) Merge/trim pages/sort
         4) Fetch stored docs

       */
      return s
        .pipe(new CalculateResultSetPerClause(Searcher.options))
        .pipe(new ScoreDocsOnField(Searcher.options, (q.offset + q.pageSize), q.sort))
        .pipe(new MergeOrConditionsFieldSort(q))
        .pipe(new FetchStoredDoc(Searcher.options))
    } else {
      return s
        .pipe(new CalculateResultSetPerClause(Searcher.options))
        .pipe(new CalculateTopScoringDocs(Searcher.options, q.offset, q.pageSize))
        .pipe(new ScoreTopScoringDocsTFIDF(Searcher.options))
        .pipe(new MergeOrConditionsTFIDF(q.offset, q.pageSize))
        .pipe(new FetchStoredDoc(Searcher.options))
    }
  }

  // TODO: seriously needs a rewrite
  Searcher.scan = function (q) {
    q = siUtil.getQueryDefaults(q)
    // just make this work for a simple one clause AND
    // TODO: add filtering, NOTting, multi-clause AND
    var s = new Readable({ objectMode: true })
    s.push('init')
    s.push(null)
    return s
      .pipe(new GetIntersectionStream(Searcher.options,
                                      siUtil.getKeySet(
                                        q.query[0].AND,
                                        Searcher.options.keySeparator
                                      )))
      .pipe(new FetchDocsFromDB(Searcher.options))
  }

  Searcher.totalHits = function (q, callback) {
    q = siUtil.getQueryDefaults(q)
    const s = new Readable({ objectMode: true })
    q.query.forEach(function (clause) {
      s.push(clause)
    })
    s.push(null)
    s.pipe(new CalculateResultSetPerClause(Searcher.options, q.filter || {}))
      .pipe(new CalculateEntireResultSet(Searcher.options))
      .pipe(new CalculateTotalHits(Searcher.options)).on('data', function (totalHits) {
        return callback(null, totalHits)
      })
  }

  return moduleReady(err, Searcher)
}

const getOptions = function (options, done) {
  var Searcher = {}
  Searcher.options = Object.assign({}, {
    deletable: true,
    fieldedSearch: true,
    store: true,
    indexPath: 'si',
    keySeparator: '￮',
    logLevel: 'ERROR',
    logHandler: Logger.createDefaultHandler(),
    nGramLength: 1,
    nGramSeparator: ' ',
    separator: /[|' .,\-|(\n)]+/,
    stopwords: sw.en
  }, options)
  Searcher.options.log = Logger.get('search-index-searcher')
  // We pass the log level as string because the Logger[logLevel] returns
  // an object, and Object.assign deosn't make deep assign so it breakes
  // We used toUpperCase() for backward compatibility
  Searcher.options.log.setLevel(Logger[Searcher.options.logLevel.toUpperCase()])
  // Use the global one because the library doesn't support providing handler to named logger
  Logger.setHandler(Searcher.options.logHandler)

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
