const Readable = require('stream').Readable
const SearchIndexAdder = require('search-index-adder')
const SearchIndexSearcher = require('../')
const logLevel = process.env.NODE_ENV || 'info'
const sandbox = process.env.SANDBOX || 'test/sandbox'
const test = require('tape')

var sis

const batch = [
  {
    id: '1',
    name: 'Apple Watch',
    description: 'Receive and respond to notiﬁcations in an instant.',
    price: '20002',
    age: '346'
  },
  {
    id: '2',
    name: 'Victorinox Swiss Army',
    description: 'You have the power to keep time moving with this Airboss automatic watch.',
    price: '99',
    age: '33342'
  },
  {
    id: '3',
    name: "Versace Men's Swiss",
    description: "Versace Men's Swiss Chronograph Mystique Sport Two-Tone Ion-Plated Stainless Steel Bracelet Watch",
    price: '4716',
    age: '8293'
  },
  {
    id: '4',
    name: "CHARRIOL Men's Swiss Alexandre",
    description: 'With CHARRIOLs signature twisted cables, the Alexander C timepiece collection is a must-have piece for lovers of the famed brand.',
    price: '2132',
    age: '33342'
  },
  {
    id: '5',
    name: "Ferragamo Men's Swiss 1898",
    description: 'The 1898 timepiece collection from Ferragamo offers timeless luxury.',
    price: '99999',
    age: '33342'
  },
  {
    id: '6',
    name: 'Bulova AccuSwiss',
    description: 'The Percheron Treble timepiece from Bulova AccuSwiss sets the bar high with sculpted cases showcasing sporty appeal. A Manchester United® special edition.',
    price: '1313',
    age: '33342'
  },
  {
    id: '7',
    name: 'TW Steel',
    description: 'A standout timepiece that boasts a rich heritage and high-speed design. This CEO Tech watch from TW Steel sets the standard for elite. Armani',
    price: '33333',
    age: '33342'
  },
  {
    id: '8',
    name: 'Invicta Bolt Zeus ',
    description: "Invicta offers an upscale timepiece that's as full of substance as it is style. From the Bolt Zeus collection.",
    price: '8767',
    age: '33342'
  },
  {
    id: '9',
    name: 'Victorinox Night Vision ',
    description: "Never get left in the dark with Victorinox Swiss Army's Night Vision watch. First at Macy's!",
    price: '1000',
    age: '33342'
  },
  {
    id: '10',
    name: 'Armani Swiss Moon Phase',
    description: 'Endlessly sophisticated in materials and design, this Emporio Armani Swiss watch features high-end timekeeping with moon phase movement and calendar tracking.',
    price: '30000',
    age: '33342'
  }
]

const s = new Readable({ objectMode: true })
batch.forEach(function (item) {
  s.push(item)
})
s.push(null)

test('initialize a search index', function (t) {
  t.plan(2)
  SearchIndexAdder({
    indexPath: sandbox + '/si',
    logLevel: logLevel
  }, function (err, indexer) {
    t.error(err)
    s.pipe(indexer.defaultPipeline())
      .pipe(indexer.add({
        fieldOptions: [{
          fieldName: 'price',
          filter: true
        }]
      }))
      .on('data', function (data) {
        // t.ok(true, ' data recieved')
      })
      .on('end', function () {
        indexer.close(function (err) {
          t.error(err)
        })
      })
  })
})

test('initialize a searcher', function (t) {
  t.plan(1)
  SearchIndexSearcher({
    indexPath: sandbox + '/si'
  }, function (err, thisSis) {
    t.error(err)
    sis = thisSis
  })
})

test('do a simple scan', function (t) {
  t.plan(1)
  var results = []
  sis.scan({
    query: {
      AND: {'*': ['swiss', 'watch']}
    }
  }).on('data', function (doc) {
    results.push(doc.id)
    // results.push(JSON.parse(doc).id)
  }).on('end', function () {
    t.looseEqual(results, [ '10', '2', '3', '9' ])
  })
})

test('do a simple scan with one word', function (t) {
  t.plan(1)
  var results = []
  sis.scan({
    query: {
      AND: {'*': ['watch']}
    }
  }).on('data', function (doc) {
    results.push(doc.id)
  }).on('end', function () {
    t.looseEqual(results, [ '1', '10', '2', '3', '7', '9' ])
  })
})

test('do a simple scan with one word on a given field', function (t) {
  t.plan(1)
  var results = []
  sis.scan({
    query: {
      AND: {'name': ['swiss']}
    }
  }).on('data', function (doc) {
    results.push(doc.id)
  }).on('end', function () {
    t.looseEqual(results, [ '10', '2', '3', '4', '5' ])
  })
})

test('do a simple scan with one word on a given field and filter', function (t) {
  t.plan(1)
  var results = []
  sis.scan({
    query: {
      AND: {
        name: ['swiss'],
        price: [{
          gte: '30000',
          lte: '99'
        }]
      }
    }
  }).on('data', function (doc) {
    results.push(doc.id)
  }).on('end', function () {
    t.looseEqual(results, [ '10', '2', '3' ])
  })
})

test('do a simple search with a nicely formatted query object', function (t) {
  t.plan(1)
  var results = []
  sis.search({
    query: {
      AND: {
        '*': ['swiss', 'watch']
      }
    }
  }).on('data', function (doc) {
    results.push(doc.id)
  }).on('end', function () {
    t.looseEqual(results, [ '9', '3', '2', '10' ])
  })
})

test('do a simple search with an empty object', function (t) {
  t.plan(1)
  var results = []
  sis.search({}).on('data', function (doc) {
    results.push(doc.id)
  }).on('end', function () {
    t.looseEqual(results, [ '9', '8', '7', '6', '5', '4', '3', '2', '10', '1' ])
  })
})

test('do a simple search with a simple string', function (t) {
  t.plan(1)
  var results = []
  sis.search('swiss watch').on('data', function (doc) {
    results.push(doc.id)
  }).on('end', function () {
    t.looseEqual(results, [ '9', '3', '2', '10' ])
  })
})

test('searching with no query returns everything, sorted by ID', function (t) {
  t.plan(1)
  var results = []
  sis.search().on('data', function (doc) {
    results.push(doc.id)
  }).on('end', function () {
    t.looseEqual(results, [ '9', '8', '7', '6', '5', '4', '3', '2', '10', '1' ])
  })
})

test('searching with BOOST and OR query', function (t) {
  t.plan(1)
  var results = []
  sis.search({
    query: [
      {
        AND: {
          'description': ['collection']
        },
        BOOST: 1
      },
      {
        AND: {
          'name': ['swiss']
        },
        BOOST: 5
      }
    ]
  }).on('data', function (doc) {
    results.push(doc.id)
  }).on('end', function () {
    t.looseEqual(results, [ '3', '2', '10', '5', '4', '8' ])
  })
})

test('get total results simple query', function (t) {
  t.plan(2)
  sis.totalHits('swiss watch', function (err, totalHits) {
    t.error(err)
    t.equal(totalHits, 4)
  })
})

test('get total results OR query', function (t) {
  t.plan(2)
  sis.totalHits({
    query: [
      {
        AND: {
          '*': ['swiss', 'watch']
        }
      },
      {
        AND: {
          '*': ['apple']
        }
      }
    ]
  }, function (err, totalHits) {
    t.error(err)
    t.equal(totalHits, 5)
  })
})

test('simple matching', function (t) {
  t.plan(5)
  var results = ['timepiece',
                 'time',
                 'timekeeping',
                 'timeless']
  sis.match({
    beginsWith: 'time'
  }).on('data', function (data) {
    t.equal(data, results.shift())
  }).on('end', function () {
    t.equal(results.length, 0)
  }).on('error', function (e) {
    t.error(e)
  })
})

test('ID matching', function (t) {
  t.plan(5)
  var results = [
    {
      timepiece: [ '4', '5', '6', '7', '8' ]
    },
    {
      time: [ '2' ]
    },
    {
      timekeeping: [ '10' ]
    },
    {
      timeless: [ '5' ]
    }
  ]
  sis.match({
    beginsWith: 'time',
    type: 'ID'
  }).on('data', function (data) {
    t.looseEqual(data, results.shift())
  }).on('end', function () {
    t.equal(results.length, 0)
  }).on('error', function (e) {
    t.error(e)
  })
})

test('count matching', function (t) {
  t.plan(5)
  var results = [
    {
      timepiece: 5
    },
    {
      time: 1
    },
    {
      timekeeping: 1
    },
    {
      timeless: 1
    }
  ]
  sis.match({
    beginsWith: 'time',
    type: 'count'
  }).on('data', function (data) {
    t.looseEqual(data, results.shift())
  }).on('end', function () {
    t.equal(results.length, 0)
  }).on('error', function (e) {
    t.error(e)
  })
})
