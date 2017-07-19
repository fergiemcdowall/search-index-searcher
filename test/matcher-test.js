const sandbox = 'test/sandbox/'
const test = require('tape')
const SearchIndexAdder = require('search-index-adder')
const SearchIndexSearcher = require('../')
const num = require('written-number')
const Readable = require('stream').Readable
const batchSize = 20
const levelup = require('levelup')

var sia, sis

test('initialize a search index', t => {
  t.plan(3)
  levelup(sandbox + 'matcher-test', {
    valueEncoding: 'json'
  }, function (err, db) {
    t.error(err)
    SearchIndexAdder({
      indexes: db
    }, (err, newSi) => {
      sia = newSi
      t.error(err)
    })
    SearchIndexSearcher({
      indexes: db
    }, (err, newSi) => {
      sis = newSi
      t.error(err)
    })
  })
})

test('make an index with storeDocument: false', t => {
  t.plan(1)
  var s = new Readable({ objectMode: true })
  for (var i = 1; i <= batchSize; i++) {
    var tokens = 'this is the amazing' + num(i) + ' doc number ' + num(i) +
      ((i % 2 === 0) ? ' amazingfive' : '')
    s.push({
      id: i,
      tokens: tokens
    })
  }
  s.push(null)
  s.pipe(sia.feed({
    objectMode: true,
    storeDocument: false,
    wildcard: false,
    compositeField: false
  }))
    .on('finish', function () {
      t.pass('finished')
    })
    .on('error', function (err) {
      t.error(err)
    })
})

test('can match, alphabetical sort, limit to 5', t => {
  var result = [
    'amazingeight',
    'amazingeighteen',
    'amazingeleven',
    'amazingfifteen',
    'amazingfive'
  ]
  t.plan(5)
  sis.match({
    beginsWith: 'amazing',
    field: 'tokens',
    type: 'ID',
    limit: 5,
    sort: 'alphabetical'
  })
    .on('data', function (d) {
      t.equal(d.token, result.shift())
    })
    .on('error', function (err) {
      t.equals(result.length, 0)
      t.error(err)
    })
})

test('can match, frequency sort', t => {
  t.plan(11)
  var result = [
    'amazingfive',
    'amazingeight',
    'amazingeighteen',
    'amazingeleven',
    'amazingfifteen',
    'amazingfour',
    'amazingfourteen',
    'amazingnine',
    'amazingnineteen',
    'amazingone'
  ]
  sis.match({
    beginsWith: 'amazing',
    field: 'tokens',
    type: 'ID'
  })
    .on('data', function (d) {
      t.equal(d.token, result.shift())
    })
    .on('end', function () {
      t.equals(result.length, 0)
    })
    .on('error', function (err) {
      t.error(err)
    })
})
