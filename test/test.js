const SearchIndex = require('search-index')
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
  SearchIndex({
    indexPath: sandbox + '/si'
  }, function(err, thisSi) {
    t.error(err)
    thisSi.add(batch, {
      fieldOptions: [{
        name: 'price',
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
    indexPath: sandbox + '/si'
  }, function(err, thisSis) {
    t.error(err)
    sis = thisSis
  })
})


test('do a simple scan', function (t) {
  t.plan(1)
  var results = []
  sis.scan({
    query: {
      AND: [{'*': ['swiss', 'watch']}]
    }
  }).on('data', function (doc) {
    results.push(JSON.parse(doc).id)
  }).on('end', function () {
    t.looseEqual(results, [ '10', '2', '3', '9' ])
  })
})

test('do a simple scan with one word', function (t) {
  t.plan(1)
  var results = []
  sis.scan({
    query: {
      AND: [{'*': ['watch']}]
    }
  }).on('data', function (doc) {
    results.push(JSON.parse(doc).id)
  }).on('end', function () {
    t.looseEqual(results, [ '1', '10', '2', '3', '7', '9' ])
  })
})

test('do a simple scan with one word on a given field', function (t) {
  t.plan(1)
  var results = []
  sis.scan({
    query: {
      AND: [{'name': ['swiss']}]
    }
  }).on('data', function (doc) {
    results.push(JSON.parse(doc).id)
  }).on('end', function () {
    t.looseEqual(results, [ '10', '2', '3', '4', '5' ])
  })
})

// TODO: make filters work

// test('do a simple scan with one word on a given field and filter', function (t) {
//   t.plan(1)
//   var results = []
//   sis.scan({
//     query: {
//       AND: [{'name': ['swiss']}]
//     },
//     filter: [{
//       field: 'price',
//       gte: '3',
//       lte: '9'
//     }]
//   }).on('data', function (doc) {
//     results.push(JSON.parse(doc).id)
//   }).on('end', function () {
//     t.looseEqual(results, [ '10', '2', '3', '4', '5' ])
//   })
// })
