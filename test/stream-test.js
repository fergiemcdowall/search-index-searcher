const SearchIndex = require('search-index')
const SearchIndexAdder = require('search-index-adder')
const SearchIndexSearcher = require('../')
const test = require('tape')
const sandbox = process.env.SANDBOX || 'test/sandbox'

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
    name: 'Versace Men\'s Swiss',
    description: 'Versace Men\'s Swiss Chronograph Mystique Sport Two-Tone Ion-Plated Stainless Steel Bracelet Watch',
    price: '4716',
    age: '8293'
  },
  {
    id: '4',
    name: 'CHARRIOL Men\'s Swiss Alexandre',
    description: 'With CHARRIOLs signature twisted cables, the Alexander C timepiece collection is a must-have piece for lovers of the famed brand.',
    price: '2132',
    age: '33342'
  },
  {
    id: '5',
    name: 'Ferragamo Men\'s Swiss 1898',
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
    description: 'Invicta offers an upscale timepiece that\'s as full of substance as it is style. From the Bolt Zeus collection.',
    price: '8767',
    age: '33342'
  },
  {
    id: '9',
    name: 'Victorinox Night Vision ',
    description: 'Never get left in the dark with Victorinox Swiss Army\'s Night Vision watch. First at Macy\'s!',
    price: '1000',
    age: '33342'
  },
  {
    id: '10',
    name: 'Armani Swiss Moon Phase',
    description: 'Endlessly sophisticated in materials and design, this Emporio Armani Swiss watch features high-end timekeeping with moon phase movement and calendar tracking.',
    price: '30000',
    age: '33342'
  },
]

test('initialize a search index', function (t) {
  t.plan(3)
  SearchIndexAdder({
    indexPath: sandbox + '/si-stream'
  }, function(err, thisSi) {
    t.error(err)
    thisSi.add(batch, {
      fieldOptions: [{
        fieldName: 'price',
        filter: true
      },{
        fieldName: 'age',
        filter: true
      }]
    }, function (err) {
      t.error(err)
      thisSi.close(function(err){
        t.error(err)
      })
    })
  })
})

test('initialize a searcher', function (t) {
  t.plan(1)
  SearchIndexSearcher({
    indexPath: sandbox + '/si-stream'
  }, function(err, thisSis) {
    t.error(err)
    sis = thisSis
  })
})

test('do a simple search', function (t) {
  t.plan(2)
  sis.search({
    query: [{
      AND: {'*': ['swiss', 'watch']}
    }]
  }, function (err, results) {
    t.error(err)
    t.looseEqual(
      results.hits.map(function (item) {
        return item.document.id
      }),
      [ '3', '10', '9', '2' ]
    )
  })
})


test('do a simple search', function (t) {
  t.plan(1)
  const results = []
  sis.searchStream({
    query: [{
      AND: {'*': ['swiss', 'watch']}
    }],
    pageSize: 10
  }).on('data', function (thing) {
    thing = JSON.parse(thing)
    if (!thing.metadata)
      results.push(thing)
  }).on('end', function () {
    t.looseEqual(
      results.map(function (item) {
        return item.document.id
      }),
      [ '3', '10', '9', '2' ]
    )
  })
})


test('do a simple search with NOT', function (t) {
  t.plan(1)
  const results = []
  sis.searchStream({
    query: [{
      AND: {'*': ['swiss', 'watch']},
      NOT: {'description': ['timekeeping']}
    }],
    pageSize: 10
  }).on('data', function (thing) {
    thing = JSON.parse(thing)
    if (!thing.metadata)
      results.push(thing)
  }).on('end', function () {
    t.looseEqual(
      results.map(function (item) {
        return item.document.id
      }),
      [ '3', '9', '2' ]
    )
  })
})


test('do a simple search with NOT and filter', function (t) {
  t.plan(1)
  const results = []
  sis.searchStream({
    query: [{
      AND: {'*': ['swiss', 'watch']},
      NOT: {'description': ['timekeeping']}
    }],
    filter: [{
      field: 'price',
      gte: '3',
      lte: '5'
    }],
    pageSize: 10
  }).on('data', function (thing) {
    thing = JSON.parse(thing)
    if (!thing.metadata)
      results.push(thing)
  }).on('end', function () {
    t.looseEqual(
      results.map(function (item) {
        return item.document.id
      }),
      [ '3' ]
    )
  })
})

test('do a simple search with NOT and filter', function (t) {
  t.plan(1)
  const results = []
  sis.searchStream({
    query: [{
      AND: {'*': ['swiss', 'watch']},
      NOT: {'description': ['timekeeping']}
    }],
    filter: [
      {
        field: 'price',
        gte: '2',
        lte: '9'
      },
      {
        field: 'age',
        gte: '3',
        lte: '4'
      }
    ],
    pageSize: 10
  }).on('data', function (thing) {
    thing = JSON.parse(thing)
    if (!thing.metadata)
      results.push(thing)
  }).on('end', function () {
    t.looseEqual(
      results.map(function (item) {
        return item.document.id
      }),
      [ '2' ]
    )
  })
})
