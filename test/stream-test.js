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

test('initialize a search index', function (t) {
  t.plan(3)
  SearchIndexAdder({
    indexPath: sandbox + '/si-stream'
  }, function (err, thisSi) {
    t.error(err)
    thisSi.add(batch, {
      fieldOptions: [{
        fieldName: 'price',
        filter: true
      }, {
        fieldName: 'age',
        filter: true
      }]
    }, function (err) {
      t.error(err)
      thisSi.close(function (err) {
        t.error(err)
      })
    })
  })
})

test('initialize a searcher', function (t) {
  t.plan(1)
  SearchIndexSearcher({
    indexPath: sandbox + '/si-stream'
  }, function (err, thisSis) {
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

test('do a simple streamy search', function (t) {
  t.plan(1)
  const results = []
  sis.searchStream({
    query: [{
      AND: {'*': ['swiss', 'watch']}
    }],
    pageSize: 10
  }).on('data', function (thing) {
    thing = JSON.parse(thing)
    if (!thing.metadata) {
      results.push(thing)
    }
  }).on('end', function () {
    t.looseEqual(
      results.map(function (item) {
        return item.document.id
      }),
      [ '9', '3', '2', '10' ]
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
    if (!thing.metadata) {
      results.push(thing)
    }
  }).on('end', function () {
    t.looseEqual(
      results.map(function (item) {
        return item.document.id
      }),
      [ '9', '3', '2' ]
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
    if (!thing.metadata) {
      results.push(thing)
    }
  }).on('end', function () {
    t.looseEqual(
      results.map(function (item) {
        return item.document.id
      }),
      [ '3' ]
    )
  })
})

test('do a simple search with NOT and two filters', function (t) {
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
    if (!thing.metadata) {
      results.push(thing)
    }
  }).on('end', function () {
    t.looseEqual(
      results.map(function (item) {
        return item.document.id
      }),
      [ '2' ]
    )
  })
})

test('search with OR', function (t) {
  t.plan(1)
  const results = []
  sis.searchStream({
    query: [{
      AND: {'*': ['swiss', 'watch']}
    }, {
      AND: {'*': ['elite']}
    }]
  }).on('data', function (thing) {
    thing = JSON.parse(thing)
    if (!thing.metadata) {
      results.push(thing)
    }
  }).on('end', function () {
    t.looseEqual(
      results.map(function (item) {
        return item.document.id
      }),
      [ '9', '3', '2', '10', '7' ]
    )
  })
})

test('buckets', function (t) {
  t.plan(1)
  sis.bucketStream({
    query: [{
      AND: {'*': ['*']}
    }],
    buckets: [{
      field: 'price',
      gte: 2,
      lte: 3
    }]
  }).on('data', function (thing) {
    thing = JSON.parse(thing)
    t.looseEqual(
      thing,
      { field: 'price', gte: 2, lte: 3, IDSet: [ '1', '10', '4', '7' ] }
    )
  })
})

test('buckets', function (t) {
  t.plan(1)
  var result = []
  sis.bucketStream({
    query: [{
      AND: {'*': ['*']}
    }],
    buckets: [{
      field: 'price',
      gte: 2,
      lte: 3
    }, {
      field: 'price',
      gte: 6,
      lte: 9
    }]
  }).on('data', function (thing) {
    thing = JSON.parse(thing)
    result.push(thing)
  }).on('end', function () {
    t.looseEqual(
      result,
      [
        { field: 'price', gte: 2, lte: 3, IDSet: [ '1', '10', '4', '7' ] },
        { field: 'price', gte: 6, lte: 9, IDSet: [ '2', '5', '8' ] }
      ]
    )
  })
})

test('two buckets', function (t) {
  t.plan(1)
  var result = []
  sis.bucketStream({
    query: [{
      AND: {'*': ['swiss', 'watch']}
    }],
    buckets: [{
      field: 'price',
      gte: 2,
      lte: 3
    }, {
      field: 'price',
      gte: 6,
      lte: 9
    }]
  }).on('data', function (thing) {
    thing = JSON.parse(thing)
    result.push(thing)
  }).on('end', function () {
    t.looseEqual(
      result,
      [
        { field: 'price', gte: 2, lte: 3, IDSet: [ '10' ] },
        { field: 'price', gte: 6, lte: 9, IDSet: [ '2' ] }
      ]
    )
  })
})

test('two buckets plus OR', function (t) {
  t.plan(1)
  var result = []
  sis.bucketStream({
    query: [{
      AND: {'*': ['swiss', 'watch']}
    }, {
      AND: {'*': ['elite']}
    }],
    buckets: [{
      field: 'price',
      gte: 2,
      lte: 3
    }, {
      field: 'price',
      gte: 6,
      lte: 9
    }]
  }).on('data', function (thing) {
    thing = JSON.parse(thing)
    result.push(thing)
  }).on('end', function () {
    t.looseEqual(
      result,
      [
        { field: 'price', gte: 2, lte: 3, IDSet: [ '10', '7' ] },
        { field: 'price', gte: 6, lte: 9, IDSet: [ '2' ] }
      ]
    )
  })
})

test('three buckets plus OR', function (t) {
  t.plan(1)
  var result = []
  sis.bucketStream({
    query: [{
      AND: {'*': ['swiss', 'watch']}
    }, {
      AND: {'*': ['elite']}
    }],
    buckets: [{
      field: 'price',
      gte: 2,
      lte: 3
    }, {
      field: 'price',
      gte: 6,
      lte: 9
    }, {
      field: 'age',
      gte: 5,
      lte: 9
    }]
  }).on('data', function (thing) {
    thing = JSON.parse(thing)
    result.push(thing)
  }).on('end', function () {
    t.looseEqual(
      result,
      [
        { field: 'price', gte: 2, lte: 3, IDSet: [ '10', '7' ] },
        { field: 'price', gte: 6, lte: 9, IDSet: [ '2' ] },
        { field: 'age', gte: 5, lte: 9, IDSet: [ '3' ] }
      ]
    )
  })
})

test('categories with set', function (t) {
  t.plan(1)
  var result = []
  sis.categoryStream({
    query: [{
      AND: {'*': ['swiss', 'watch']}
    }],
    category: {
      field: 'age',
      set: true
    }
  }).on('data', function (thing) {
    // thing = JSON.parse(thing)
    result.push(thing)
  }).on('end', function () {
    t.looseEqual(
      result,
      [
        { key: '33342', value: [ '10', '2', '4', '5', '9' ] },
        { key: '8293', value: [ '3' ] }
      ]
    )
  })
})

test('categories with limit and set', function (t) {
  t.plan(1)
  var result = []
  sis.categoryStream({
    query: [{
      AND: {'*': ['swiss', 'watch']}
    }],
    category: {
      field: 'price',
      limit: '2',
      set: true
    }
  }).on('data', function (thing) {
    result.push(thing)
  }).on('end', function () {
    t.looseEqual(
      result,
      [
        { key: '1000', value: [ '9' ] },
        { key: '30000', value: [ '10' ] }
      ]
    )
  })
})

test('categories with sort', function (t) {
  t.plan(1)
  var result = []
  sis.categoryStream({
    query: [{
      AND: {'*': ['swiss', 'watch']}
    }],
    category: {
      field: 'price',
      sort: function (a, b) {
        return b.key > a.key
      }
    }
  }).on('data', function (thing) {
    result.push(thing)
  }).on('end', function () {
    t.looseEqual(
      result,
      [
        { key: '99', value: 1 },
        { key: '4716', value: 1 },
        { key: '30000', value: 1 },
        { key: '1000', value: 1 }
      ]
    )
  })
})

test('categories with value', function (t) {
  t.plan(1)
  var result = []
  sis.categoryStream({
    query: [{
      AND: {'*': ['*']}
    }],
    category: {
      field: 'age',
      sort: function (a, b) {
        return b.value - a.value
      }
    }
  }).on('data', function (thing) {
    result.push(thing)
  }).on('end', function () {
    t.looseEqual(
      result,
      [
        { key: '33342', value: 8 },
        { key: '8293', value: 1 },
        { key: '346', value: 1 }
      ]
    )
  })
})
