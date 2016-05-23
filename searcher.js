const _defaults = require('lodash.defaults')
// TODO: consider compiling up a custom lodash lib

const _difference = require('lodash.difference')
const _filter = require('lodash.filter')
const _find = require('lodash.find')
const _findIndex = require('lodash.findindex')
const _flatten = require('lodash.flatten')
const _forEach = require('lodash.foreach')
const _groupBy = require('lodash.groupby')
const _intersection = require('lodash.intersection')
const _isEqual = require('lodash.isequal')
const _map = require('lodash.map')
const _minBy = require('lodash.minby')
const _union = require('lodash.union')
const _uniq = require('lodash.uniq')
const _uniqWith = require('lodash.uniqwith')
const async = require('async')
const bunyan = require('bunyan')
const levelup = require('levelup')
const scontext = require('search-context')
const sw = require('stopword')

// const _ = require('lodash') // just for testing

var queryDefaults = {
  maxFacetLimit: 100,
  offset: 0,
  pageSize: 100,
  categories: [],
  teaserHighlighter : function(string) {
    return '<b>' + string + '</b>'
  }
}

module.exports = function (givenOptions, callback) {
  getOptions(givenOptions, function (err, options) {
    var Searcher = {}

    Searcher.search = function (q, callback) {
      _defaults(q, queryDefaults)
      // q.query = removeStopwordsFromQuery(q.query, options.stopwords)
      var keySet = getKeySet(q)
      if (keySet.length === 0) return callback(getEmptyResultSet(q))
      options.log.info(JSON.stringify(q))
      getDocumentFreqencies(q, keySet, options.indexes, function (err, frequencies) {
        // improve returned resultset here:
        if (err) return callback(err, getEmptyResultSet(q))
        async.parallel([
          function (callback) {
            getResults(q, frequencies, keySet, options, function (err, hits) {
              callback(err, hits)
            })
          },
          function (callback) {
            getCategories(q, frequencies, options.indexes, function (err, categories) {
              callback(err, categories)
            })
          },
          function (callback) {
            getBuckets(q, frequencies, options.indexes, function (err, buckets) {
              callback(err, buckets)
            })
          }], function (err, results) {
          if (results[0].length === 0) {
            return callback(err, getEmptyResultSet(q))
          }
          var response = {}

          // TODO: if any of these are empty- remove them

          response.totalHits = frequencies.allDocsIDsInResultSet.length
          response.totalDocsInIndex = frequencies.totalDocsInIndex
          response.documentFrequencies = frequencies.df
          // response.fieldWeight = frequencies.fieldWeight
          response.query = q.query
          response.buckets = results[2]
          response.categories = results[1]
          response.hits = results[0]
          callback(err, response)
        })
      })
    }
    return callback(err, Searcher)
  })
}

var sort = function (sortName) {
  if (sortName === 'keyAsc') {
    return function (a, b) {
      if (a.key > b.key) return 1
      if (a.key < b.key) return -1
      return 0
    }
  } else if (sortName === 'keyDesc') {
    return function (a, b) {
      if (a.key > b.key) return -1
      if (a.key < b.key) return 1
      return 0
    }
  } else if (sortName === 'valueAsc') {
    return function (a, b) {
      if ((a.value - b.value) < 0) return -1
      if ((a.value - b.value) > 0) return 1
      if (a.key > b.key) return 1
      if (a.key < b.key) return -1
      return 0
    }
  } else {
    // default is 'valueDesc'
    return function (a, b) {
      if ((a.value - b.value) < 0) return 1
      if ((a.value - b.value) > 0) return -1
      if (a.key > b.key) return 1
      if (a.key < b.key) return -1
      return 0
    }
  }
}

var getCategories = function (q, frequencies, indexes, callback) {
  // 1. scan docfreqs, determine least frequent pair in this andSet
  // 2. scan every df key for that filter name and do an
  // intersection to get filters per q.filter, per ORSet
  // 3. munge the ORSets together

  var categories = []
  async.eachSeries(q.categories, function (category, categoryProcessed) {
    var categoriesForORSet = []
    async.eachSeries(frequencies.ORSets, function (ORSet, ORSetProcessed) {
      var IDsInSet = ORSet.ORSet.map(function (item) {
        return item.id
      })
      var gte = ORSet.leastFrequent.key[0].split('￮')
        .slice(0, 3).join('￮') + '￮' + category.field
      var lte = ORSet.leastFrequent.key[1].split('￮')
          .slice(0, 3).join('￮') + '￮' + category.field + '￮￮'
      var thisCategory = {key: category.field, value: []}
      indexes.createReadStream({gte: gte, lte: lte})
        .on('data', function (data) {
          var categoryPropertyValue = data.key.split('￮')[4]
          var categorySet = _intersection(data.value, IDsInSet)
          // TODO: possibly allow for 0 values?
          if (categorySet.length > 0) {
            thisCategory.value.push({
              key: categoryPropertyValue,
              value: categorySet
            })
          }
        })
        .on('end', function () {
          categoriesForORSet.push(thisCategory)
          ORSetProcessed(null)
        })
    }, function (err) {
      var mungedCategory = {}
      mungedCategory.key = category.field
      mungedCategory.value = []
      _flatten(categoriesForORSet.map(function (item) {
        return item.value
      })).forEach(function (item) {
        var i = _findIndex(mungedCategory.value, {key: item.key})
        if (i === -1) {
          mungedCategory.value.push(item)
        } else {
          mungedCategory.value[i].value =
            _union(mungedCategory.value[i].value, item.value)
        }
      })

      var sortKey = category.sort || 'valueDesc'
      var limit = category.limit || 50

      mungedCategory.value.map(function (item) {
        item.value = item.value.length
        if (q.filter) {
          q.filter.forEach(function (qFilter) {
            if (qFilter.field === mungedCategory.key) {
              if ((qFilter.gte >= item.key) && (item.key >= qFilter.lte)) {
                item.active = true
              }
            }
          })
        }

        return item
      })

      mungedCategory.value = mungedCategory.value.sort(
        sort(sortKey)
      )
      mungedCategory.value = mungedCategory.value.slice(0, limit)

      categories.push(mungedCategory)
      categoryProcessed(err)
    })
  }, function (err) {
    callback(err, categories)
  })
}

var getBuckets = function (q, frequencies, indexes, callback) {
  var buckets = []
  async.eachSeries(frequencies.ORSets, function (ORSet, ORSetProcessed) {
    var IDsInSet = ORSet.ORSet.map(function (item) {
      return item.id
    })
    async.eachSeries(ORSet.keySet.AND, function (ANDKeys, ANDSetProcessed) {
      async.eachSeries(q.buckets, function (bucket, bucketProcessed) {
        var fieldName = ANDKeys[0].split('￮')[1]
        var token = ANDKeys[0].split('￮')[2]

        // var gte = ANDKeys[0].slice(0, -1)
        var gte = 'DF￮' + fieldName + '￮' + token + '￮' +
          bucket.field + '￮' +
          bucket.gte
        var lte = 'DF￮' + fieldName + '￮' + token + '￮' +
          bucket.field + '￮' +
          bucket.lte + '￮'
        var thisBucket = _find(buckets, bucket) || bucket

        // TODO: add some logic to see if keys are within ranges before doing a lookup

        indexes.createReadStream({gte: gte, lte: lte})
          .on('data', function (data) {
            var IDSet = _intersection(data.value, IDsInSet)
            if (IDSet.length > 0) {
              thisBucket.IDSet = thisBucket.IDSet || []
              thisBucket.IDSet = _uniq(thisBucket.IDSet.concat(IDSet))
            }
          })
          .on('close', function () {
            buckets.push(thisBucket)
            buckets = _uniqWith(buckets, _isEqual)
            return bucketProcessed(null)
          })
      }, function (err) {
        ANDSetProcessed(err)
      })
    }, function (err) {
      ORSetProcessed(err)
    })
  }, function (err) {
    buckets.map(function (bucket) {
      if (!bucket.IDSet) {
        bucket.count = 0
      } else {
        bucket.count = bucket.IDSet.length
      }
      delete bucket.IDSet
      return bucket
    })
    callback(err, buckets)
  })
}

// supposedly fastest way to get unique values in an array
// http://stackoverflow.com/questions/9229645/remove-duplicates-from-javascript-array
var uniqFast = function (a) {
  var seen = {}
  var out = []
  var len = a.length
  var j = 0
  for (var i = 0; i < len; i++) {
    var item = a[i]
    if (seen[item] !== 1) {
      seen[item] = 1
      out[j++] = item
    }
  }
  return out
}

var getKeySet = function (q) {
  // generate keyset
  var keySet = []

  if (!Array.isArray(q.query)) {
    q.query = [q.query]
  }
  q.query.forEach(function (or) {
    var orKeySet = {}
    orKeySet.AND = []
    orKeySet.NOT = []
    orKeySet.BOOST = or.boost || 0
    // AND condtions

    ;['AND', 'NOT'].forEach(function (bool) {
      if (!Array.isArray(or[bool])) {
        or[bool] = [or[bool]]
      }
      or[bool].forEach(function (set) {
        for (var fieldName in set) {
          set[fieldName].forEach(function (token) {
            // orKeySet[bool].push('DF' + '￮' + fieldName + '￮' + token + '￮￮')
            if (q.filter && Array.isArray(q.filter)) {
              // Filters: TODO
              q.filter.forEach(function (filter) {
                orKeySet[bool].push([
                  'DF￮' + fieldName + '￮' + token + '￮' + filter.field + '￮' + filter.gte,
                  'DF￮' + fieldName + '￮' + token + '￮' + filter.field + '￮' + filter.lte + '￮'
                ])
              })
            } else {
              orKeySet[bool].push([
                'DF￮' + fieldName + '￮' + token + '￮￮',
                'DF￮' + fieldName + '￮' + token + '￮￮￮'
              ])
            }
          })
        }
      })
    })
    keySet.push(orKeySet)
  })
  return keySet
}

var getEmptyResultSet = function (q) {
  var resultSet = {}
  resultSet.query = q.query
  resultSet.hits = []
  resultSet.totalHits = 0
  resultSet.facets = q.facets
  return resultSet
}

var getDocumentFreqencies = function (q, keySets, indexes, callback) {

  // Check document frequencies
  var keySetsUniq = keySets.map(function (set) {
    return set.AND.concat(set.NOT)
  })
  keySetsUniq = _uniqWith(_flatten(keySetsUniq), _isEqual)

  async.map(keySetsUniq, function (item, callback) {
    var uniq = []
    // loop through each AND condition
    indexes.createReadStream({gte: item[0], lte: item[1] + '￮'})
      .on('data', function (data) {
        uniq = uniqFast(uniq.concat(data.value))
      })
      .on('error', function (err) {
        console.log(err)
      })
      .on('end', function () {
        callback(null, {key: item, value: uniq.sort()})
      })
  }, function (asyncerr, results) {

    if (!results[0]) {
      // array is empty
      return callback(asyncerr, [])
    }

    var docFreqs = {}
    docFreqs.allDocsIDsInResultSet = []
    docFreqs.ORSets = []
    docFreqs.totalDocsInIndex = 0
    docFreqs.df = {}
    docFreqs.idf = {}
    // docFreqs.fieldWeight = {}
    docFreqs.docFreqs = []

    // get document frequencies

    results.forEach(function (item) {
      var dfToken = item.key[1].split('￮')[1] + '￮' + item.key[1].split('￮')[2]
      var dfValue = item.value.length
      docFreqs.df[dfToken] = dfValue
    })

    // // get field weight
    // Object.keys(docFreqs.df).forEach(function (i) {
    //   docFreqs.fieldWeight[i] = 0
    //   if (q.weight) {
    //     if (q.weight[i.split('￮')[0]]) {
    //       docFreqs.fieldWeight[i] = q.weight[i.split('￮')[0]]
    //     }
    //   }
    // })

    // for each OR
    keySets.forEach(function (keySet) {
      // determine which keys return the smallest result set
      var leastFrequentKeys = _minBy(results.map(function (item) {
        if (keySet.AND.indexOf(item.key) !== -1) {
          return item
        }
      }), function (item) {
        if (item) {
          return item.value.length
        }
      })

      // console.log(keySet)
      // for each AND
      var ANDset = []

      // ANDing
      keySet.AND.forEach(function (tf) {
        ANDset = ANDset.concat(_filter(results, function (o) {
          return _isEqual(o.key, tf)
        }).map(function (o) {
          return o.value
        }))
      })

      var NOTset = []

      // ANDing
      // keySet.forEach(function (tf) {
      keySet.NOT.forEach(function (tf) {
        NOTset = NOTset.concat(_filter(results, function (o) {
          return _isEqual(o.key, tf)
        }).map(function (o) {
          return o.value
        }))
      })

      // do an intersection on AND values- token must be in all sets
      if (ANDset.length > 0) {
        ANDset = ANDset.reduce(function (prev, cur) {
          return _intersection(prev, cur)
        })
      }
      // do an intersection on NOT values- token must be in all sets
      if (NOTset.length > 0) {
        NOTset = NOTset.reduce(function (prev, cur) {
          return _intersection(prev, cur)
        })
      }

      // take away NOTSet from ANDSet
      // ORSet one set of OR conditions
      var ORSet = _difference(ANDset, NOTset)

      docFreqs.ORSets.push({
        keySet: keySet,
        leastFrequent: leastFrequentKeys,
        ORSet: ORSet.map(function (item) {
          return {
            id: item,
            tfidf: []
          }
        })
      })

      docFreqs.allDocsIDsInResultSet = _union(ORSet, docFreqs.allDocsIDsInResultSet)
    })

    // do docFreqs for working out ranges and stuff
    for (var i = 0; i < results.length; i++) {
      docFreqs.docFreqs.push([results[i].value.length, keySetsUniq[i]])
    }

    indexes.get('DOCUMENT-COUNT', function (err, value) {
      docFreqs.totalDocsInIndex = value

      // TODO: get inverse document frequencies here
      _forEach(docFreqs.df, function (val, key) {
        docFreqs.idf[key] = Math.log10(1 + (docFreqs.totalDocsInIndex / val))
      })

      return callback(err, docFreqs)
    })
  })
}

var getResults = function (q, frequencies, keySet, options, callback) {
  if (q.sort) {
    getResultsSortedByField(q, frequencies, keySet, options, callback)
  } else {
    getResultsSortedByTFIDF(q, frequencies, options, callback)
  }
}

var getResultsSortedByField = function (q, frequencies, keySet, options, callback) {
  var sortKey = q.sort[0]
  var sortDirection = q.sort[1]
  glueDocs(frequencies.allDocsIDsInResultSet.map(function (item) {
    return {id: item}
  }), q, options, function (result) {
    result = result.sort(function (a, b) {
      if (sortDirection === 'asc') {
        return a.document[sortKey] - b.document[sortKey]
      } else {
        return b.document[sortKey] - a.document[sortKey]
      }
    }).slice((+q.offset), (+q.offset) + (+q.pageSize))
    return callback(null, result)
  })
}

// var getResults = function (q, frequencies, indexes, callbackX) {
var getResultsSortedByTFIDF = function (q, frequencies, options, callbackX) {

  async.mapSeries(frequencies.docFreqs, function (item, callbacker) {
    var gte = item[1][0].replace(/^DF￮/, 'TF￮')
    var lte = item[1][1].replace(/^DF￮/, 'TF￮')
    var field = gte.split('￮')[1]
    var token = gte.split('￮')[2]
    var idf = frequencies.idf[field + '￮' + token]
    var hits = {
      field: field,
      token: token,
      rangeStart: gte.split('￮')[4],
      rangeEnd: lte.split('￮')[4],
      idf: idf,
      tf: [],
      tfidf: []
    }

    // TODO: only has to go up to page size (otherwise: WHATS THE POINT?!)
    options.indexes.createReadStream({gte: gte, lte: lte + '￮'})
      .on('data', function (data) {
        for (var i = 0; i < data.value.length; i++) {
          var thisID = data.value[i][1]
          var thisTF = +data.value[i][0]
          if (frequencies.allDocsIDsInResultSet.indexOf(thisID) !== -1) {
            frequencies.ORSets = frequencies.ORSets.map(function (ORSet) {
              ORSet.ORSet = ORSet.ORSet.map(function (hit) {
                if (hit.id === thisID) {
                  ORSet.keySet.AND.forEach(function (key) {
                    if (_isEqual(key, item[1])) {
                      // hit.tf.push(data.value[i])
                      hit.tfidf.push([
                        token,
                        field,
                        thisTF * idf,
                        thisTF,
                        idf,
                        ORSet.keySet.BOOST
                      ])
                    }
                  })
                }
                return hit
              })
              return ORSet
            })
          }
        }
      })
      .on('error', function (err) {
        console.log('Oh my!', err)
      })
      .on('end', function () {
        return callbacker(null, hits)
      })
  }, function (err, result) {

    // Safe OR results are now in frequencies.ORSets so this should now
    // be edited to read form frequencies.ORSets

    // Should now have the top n (n = offset + pagesize) for every token
    var hits = []
    // TODO: weight results by field

    hits = _map(frequencies.ORSets, function (item) {
      return item.ORSet
    })

    hits = _flatten(hits)
    hits = _groupBy(hits, function (item) {
      return item.id
    })

    hits = _map(hits, function (val, key) {
      var hit = {}
      hit.id = key
      hit.score = 0
      hit.tfidf = []
      // OR
      val.forEach(function (item) {
        hit.tfidf.push(item.tfidf)
        // AND
        item.tfidf.forEach(function (ANDTokens) {
          const tfidf = ANDTokens[2]
          const boost = ANDTokens[5]
          hit.score += +(tfidf + boost)
        })
      })
      return hit
    })

    hits = hits.sort(function (a, b) {
      if (a.score < b.score) return 1
      if (a.score > b.score) return -1
      if (a.id < b.id) return 1
      if (a.id > b.id) return -1
      return 0
    })

    hits = hits.slice((+q.offset), (+q.offset) + (+q.pageSize))

    glueDocs(hits, q, options, function (result) {
      return callbackX(err, result)
    })
  })
}

var glueDocs = function (hits, q, options, callbackX) {
  async.mapSeries(hits, function (item, callback) {
    options.indexes.get('DOCUMENT￮' + item.id + '￮', function (err, value) {
      item.document = value
      if (q.teaser && item.document[q.teaser]) {
        var teaserTerms = []
        q.query.forEach(function (item) {
          item.AND.forEach(function (ANDitem) {
            if (ANDitem[q.teaser]) {
              teaserTerms = teaserTerms.concat(ANDitem[q.teaser])
            }
            if (ANDitem['*']) {
              teaserTerms = teaserTerms.concat(ANDitem['*'])
            }
          })
        })
        try {
          item.document.teaser = scontext(
            item.document[q.teaser],
            teaserTerms,
            400,
            q.teaserHighlighter
          )
        } catch (e) {
          console.log('error with teaser generation: ' + e)
        }
      }
      callback(err, item)
    })
  }, function (err, result) {
    if (err) {} // TODO
    return callbackX(result)
  })
}

var getOptions = function (givenOptions, callbacky) {
  givenOptions = givenOptions || {}
  async.parallel([
    function (callback) {
      var defaultOps = {}
      defaultOps.deletable = true
      defaultOps.fieldedSearch = true
      defaultOps.fieldsToStore = 'all'
      defaultOps.indexPath = 'si'
      defaultOps.logLevel = 'error'
      defaultOps.nGramLength = 1
      defaultOps.nGramSeparator = ' '
      defaultOps.separator = /[\|' \.,\-|(\n)]+/
      defaultOps.stopwords = sw.getStopwords('en').sort()
      defaultOps.log = bunyan.createLogger({
        name: 'search-index',
        level: givenOptions.logLevel || defaultOps.logLevel
      })
      callback(null, defaultOps)
    },
    function (callback) {
      if (!givenOptions.indexes) {
        levelup(givenOptions.indexPath || 'si', {
          valueEncoding: 'json'
        }, function (err, db) {
          callback(err, db)
        })
      } else {
        callback(null, null)
      }
    }
  ], function (err, results) {
    var options = _defaults(givenOptions, results[0])
    if (results[1] != null) {
      options.indexes = results[1]
    }
    return callbacky(err, options)
  })
}
