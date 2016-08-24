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
const Transform = require('stream').Transform
const _defaults = require('lodash.defaults')
const _intersection = require('lodash.intersection')
const _uniq = require('lodash.uniq')
const async = require('async')
const siUtil = require('./lib/siUtil.js')
const util = require('util')

exports.scan = function (q, options) {
  q = _defaults(q || {}, {
    query: {
      AND: {'*': ['*']}
    },
    offset: 0,
    pageSize: 20
  })
  var keySet = siUtil.getKeySet(q.query.AND, q.filter || {})
  // just make this work for a simple one clause AND
  // TODO: add filtering, NOTting, multi-clause AND
  var s = new Readable()
  s.push('init')
  s.push(null)
  return s.pipe(new GetIntersectionStream(options, keySet))
    .pipe(new FetchDocsFromDB(options))
}


exports.searchStream = function (q, options) {
  q = _defaults(q || {}, {
    query: {
      AND: {'*': ['*']}
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
      AND: {'*': ['*']}
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
      AND: {'*': ['*']}
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
