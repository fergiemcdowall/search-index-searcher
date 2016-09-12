const SearchIndexAdder = require('search-index-adder')
const SearchIndexSearcher = require('../')
const logLevel = process.env.NODE_ENV || 'info'
const test = require('tape')
const Readable = require('stream').Readable
const JSONStream = require('JSONStream')

var sia, sis

test('should initialize the search index', function (t) {
  t.plan(2)
  SearchIndexAdder({
    indexPath: 'test/sandbox/si-get-test',
    logLevel: logLevel
  }, function (err, thisSi) {
    t.error(err)
    sia = thisSi
    SearchIndexSearcher(sia.options, function (err, thisSi) {
      t.error(err)
      sis = thisSi
    })
  })
})

test('should index test data into the index', function (t) {
  t.plan(6)
  var s = new Readable()
  s.push(JSON.stringify({
    id: 1,
    name: 'The First Doc',
    test: 'this is the first doc'
  }))
  s.push(JSON.stringify({
    id: 2,
    name: 'The Second Doc',
    test: 'this is the second doc'
  }))
  s.push(JSON.stringify({
    id: 3,
    name: 'The Third Doc',
    test: 'this is the third doc doc'
  }))
  s.push(JSON.stringify({
    id: 4,
    name: 'The Fourth Doc',
    test: 'this is the fourth doc'
  }))
  s.push(null)
  s.pipe(JSONStream.parse())
    .pipe(sia.defaultPipeline())
    .pipe(sia.add())
    .on('data', function (data) {
      t.ok(true, 'indexed')
    }).on('end', function () {
      t.ok(true, 'ended')
    })
})

test('should .get a doc', function (t) {
  t.plan(2)
  sis.get('3', function (err, doc) {
    t.error(err)
    t.looseEqual(doc, {
      id: '3',
      name: 'The Third Doc',
      test: 'this is the third doc doc'
    })
  })
})
