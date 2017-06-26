const Readable = require('stream').Readable
const SearchIndexAdder = require('search-index-adder')
const SearchIndexSearcher = require('../')
const logLevel = process.env.NODE_ENV || 'error'
const sandbox = process.env.SANDBOX || 'test/sandbox'
const test = require('tape')
const converter = require('number-to-words')

const indexPath = sandbox + '/si-wildcard-test'

var sis

const s = new Readable({ objectMode: true })
for (var i = 1; i <= 10; i++) {
  s.push({
    id: i,
    body: 'a really interesting document about ' + converter.toWords(i)
  })
}
s.push(null)

test('initialize a search index', function (t) {
  t.plan(2)
  SearchIndexAdder({
    indexPath: indexPath,
    logLevel: logLevel
  }, function (err, indexer) {
    t.error(err)
    s.pipe(indexer.feed({ objectMode: true }))
      .on('finish', function () {
        indexer.close(function (err) {
          t.error(err)
        })
      })
  })
})

test('initialize a searcher', function (t) {
  t.plan(1)
  SearchIndexSearcher({
    indexPath: indexPath
  }, function (err, thisSis) {
    t.error(err)
    sis = thisSis
  })
})

test('do a simple streamy search', function (t) {
  t.plan(1)
  const results = []
  sis.search({
    query: [{
      AND: {'*': ['*']} // should be watch
    }],
    pageSize: 10
  }).on('data', function (thing) {
    if (!thing.metadata) {
      results.push(thing)
    }
  }).on('end', function () {
    t.looseEqual(
      results.map(function (item) {
        return item.document.id
      }),
      [ 9, 8, 7, 6, 5, 4, 3, 2, 10, 1 ]
    )
  })
})
