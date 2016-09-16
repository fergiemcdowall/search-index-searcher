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
  var results = [
    {
      id: '3',
      name: 'The Third Doc',
      test: 'this is the third doc doc'
    }
  ]
  sis.get(['3']).on('data', function (data) {
    data = JSON.parse(data)
    t.looseEqual(data, results.shift())
  }).on('end', function (end) {
    t.equal(results.length, 0)
  })
})

test('should .get 2 docs', function (t) {
  t.plan(3)
  var results = [{
    id: 4,
    name: 'The Fourth Doc',
    test: 'this is the fourth doc'
  }, {
    id: 2,
    name: 'The Second Doc',
    test: 'this is the second doc'
  }]
  sis.get(['4', '2']).on('data', function (data) {
    data = JSON.parse(data)
    t.looseEqual(data, results.shift())
  }).on('end', function (end) {
    t.equal(results.length, 0)
  })
})
