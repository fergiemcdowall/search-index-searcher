const _compact = require('lodash.compact')
const _defaults = require('lodash.defaults')
const _filter = require('lodash.filter')
const _flatten = require('lodash.flatten')
const _forEach = require('lodash.foreach')
const _groupBy = require('lodash.groupby')
const _intersection = require('lodash.intersection')
const _isEqual = require('lodash.isequal')
const _map = require('lodash.map')
const _sortBy = require('lodash.sortby')
const _union = require('lodash.union')
const _uniqWith = require('lodash.uniqwith')
const async = require('async')
const bunyan = require('bunyan')
const levelup = require('levelup')
const scontext = require('search-context')
const skeleton = require('log-skeleton')
const sw = require('stopword')

var queryDefaults = {
  maxFacetLimit: 100,
  offset: 0,
  pageSize: 100
}

module.exports = function (givenOptions, callback) {
  getOptions(givenOptions, function (err, options) {
    var log = skeleton((options) ? options.log : undefined)
    var Searcher = {}

    Searcher.search = function (q, callback) {
      _defaults(q, queryDefaults)
      q.query = removeStopwordsFromQuery(q.query, options.stopwords)
      var keySet = getKeySet(q)
      if (keySet.length === 0) return callback(getEmptyResultSet(q))
      log.info(JSON.stringify(q))
      getDocumentFreqencies(q, keySet, options.indexes, function (err, frequencies) {
        // console.log(JSON.stringify(frequencies, null, 2))
        // improve returned resultset here:
        if (err) return callback(err, getEmptyResultSet(q))
        async.parallel([
          function (callback) {
            getResults(q, frequencies, keySet, options.indexes, function (err, hits) {
              callback(err, hits)
            })
          },
          function (callback) {
            getFacets(q, frequencies, options.indexes, function (facets) {
              callback(null, facets)
            })
          }], function (err, results) {
          if (results[0].length === 0) {
            return callback(err, getEmptyResultSet(q))
          }
          var response = {}
          response.totalHits = frequencies.allDocsIDsInResultSet.length
          response.totalDocsInIndex = frequencies.totalDocsInIndex
          response.documentFrequencies = frequencies.df
          response.fieldWeight = frequencies.fieldWeight
          response.query = q
          response.facets = results[1]
          response.hits = results[0]
          callback(err, response)
        })
      })
    }
    return callback(err, Searcher)
  })
}

var removeStopwordsFromQuery = function (qquery, stopwords) {
  for (var i in qquery) {
    if (qquery.hasOwnProperty(i)) {
      for (var k = 0; k < qquery[i].length; k++) {
        var swops = {inputSeparator: '￮',
          outputSeparator: '￮',
        stopwords: stopwords}
        qquery[i] = sw.removeStopwords(qquery[i].join('￮'), swops)
      }
    }
  }
  return qquery
}

var getSearchFieldQueryTokens = function (query) {
  var searchFieldQueryTokens = []
  for (var queryField in query) {
    if (query.hasOwnProperty(queryField)) {
      for (var i = 0; i < query[queryField].length; i++) {
        searchFieldQueryTokens.push(queryField + '￮' + query[queryField][i])
      }
    }
  }
  return searchFieldQueryTokens
}

var sortFacet = function (facet, sortType) {
  if (sortType === 'keyAsc') return _sortBy(facet, 'key')
  else if (sortType === 'keyDesc') return _sortBy(facet, 'key').reverse()
  else if (sortType === 'valueAsc') return _sortBy(facet, 'value')
  else return _sortBy(facet, 'value').reverse()
}

var getBucketedFacet = function (filter, facetRangeKeys, activeFilters, indexes, callbacky) {
  async.reduce(facetRangeKeys, [], function (memo, item, callback) {
    var gte = item.start.split('￮')[4]
    var lte = item.end.split('￮')[4]
    var key = gte + '-' + lte
    var thisSet = []
    indexes.createReadStream({gte: item.start, lte: item.end})
      .on('data', function (data) {
        thisSet = thisSet.concat(data.value)
      })
      .on('end', function () {
        var facetEntry = {key: key,
          gte: gte,
          lte: lte,
        value: uniqFast(thisSet).sort()}
        // for (var i = 0; i < activeFilters.length; i++) {
        activeFilters.forEach(function (af) {
          if ((af[0] === gte) && (af[1] === lte)) {
            facetEntry.active = true
          }
        })
        memo.push(facetEntry)
        return callback(null, memo)
      })
  }, function (err, result) {
    if (err) {}  // do something with the err?
    // intersect IDs for every query token to enable multi-word faceting
    result.reduce(function (a, b, i, arr) {
      if (a.key === b.key) {
        b.value = intersectionDestructive(a.value, b.value)
        delete arr[i - 1]
      }
      return b
    })
    result = _compact(result)
    // TODO: to return sets instead of totals- do something here.
    for (var i in result) {
      if (result.hasOwnProperty(i)) {
        var filterClone = JSON.parse(JSON.stringify(filter))
        result[i].value = intersectionDestructive(result[i].value, filterClone)
        result[i].value = result[i].value.length
      }
    }
    callbacky(result)
  })
}

var getNonRangedFacet = function (totalQueryTokens, facetRangeKeys,
  filterSet, filter, indexes, callbacky) {
  async.reduce(facetRangeKeys, [], function (memo, item, callback) {
    indexes.createReadStream({gte: item.start, lte: item.end})
      .on('data', function (data) {
        var thisKey = data.key.split('￮')[4]
        memo.push({key: thisKey,
          gte: thisKey,
          lte: thisKey,
        value: data.value})
      })
      .on('end', function () {
        callback(null, memo)
      })
  }, function (err, result) {
    if (err) {}  // do something with the err?
    // intersect IDs for every query token to enable multi-word faceting
    if (result.length === 0) return callbacky([])
    _sortBy(result, 'key').reduce(function (a, b, i, arr) {
      if (a.key === b.key) {
        b.counter = (a.counter + 1)
        b.value = intersectionDestructive(a.value, b.value)
        delete arr[i - 1]
      } else {
        if (a.counter < totalQueryTokens) delete arr[i - 1]
        if (a.value.length === 0) delete arr[i - 1]
        delete a.counter
        b.counter = 1
      }
      return b
    })
    if (result[result.length - 1].counter < totalQueryTokens) {
      delete result[result.length - 1]
    } else if (result[result.length - 1].counter === totalQueryTokens) {
      delete result[result.length - 1].counter
    }
    result = _compact(result)

    // filter
    if (filterSet) {
      for (var k in result) {
        if (result.hasOwnProperty(k)) {
          var f = filterSet.slice()
          result[k].value = intersectionDestructive(result[k].value, f)
          if (result[k].value.length === 0) delete result[k]
        }
      }
      result = _compact(result)
    }

    for (var i in result) {
      if (result.hasOwnProperty(i)) {
        result[i].value = result[i].value.length
        for (var j = 0; j < filter.length; j++) {
          if ((filter[j][0] <= result[i].key) && (result[i].key <= filter[j][1])) {
            result[i].active = true
          }
        }
      }
    }
    callbacky(result)
  })
}

var getFacets = function (q, frequencies, indexes, callbacky) {
  if (!q.facets) return callbacky({})
  var searchFieldQueryTokens = getSearchFieldQueryTokens(q.query[0])
  async.map(Object.keys(q.facets), function (facetName, callback) {
    var item = q.facets[facetName]
    var facetRangeKeys = []
    var ranges = item.ranges || [['', '']]
    var limit = item.limit || queryDefaults.maxFacetLimit
    var sortType = item.sort || 'valueDesc'
    for (var i = 0; i < ranges.length; i++) {
      for (var sfqt in searchFieldQueryTokens) {
        if (searchFieldQueryTokens.hasOwnProperty(sfqt)) {
          var range = {}
          var prefix = 'TF￮' + searchFieldQueryTokens[sfqt] + '￮' + facetName + '￮'
          range.start = prefix + ranges[i][0]
          range.end = prefix + ranges[i][1] + '￮'
          facetRangeKeys.push(range)
        }
      }
    }
    if (item.ranges) {
      // set the filterSetKeys to the facet function, do an intersection to derive filters
      var activeFilters = []
      if (q.filter) if (q.filter[facetName]) activeFilters = q.filter[facetName]
      getBucketedFacet(frequencies.allDocsIDsInResultSet,
        facetRangeKeys,
        activeFilters,
        indexes,
        function (facet) {
          callback(null, {key: facetName,
          value: sortFacet(facet, sortType).slice(0, limit)})
        })
    } else {
      var filterValues = []
      if (q.filter) filterValues = q.filter[facetName] || []
      getNonRangedFacet(
        searchFieldQueryTokens.length,
        facetRangeKeys,
        frequencies.allDocsIDsInResultSet,
        filterValues,
        indexes,
        function (facet) {
          callback(null, {
            key: facetName,
            value: sortFacet(facet, sortType).slice(0, limit)
          })
        })
    }
  }, function (err, result) {
    if (err) {} // handle err?
    callbacky(result)
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
    keySet.push([])
    for (var queryField in or) {
      if (or.hasOwnProperty(queryField)) {
        for (var j = 0; j < or[queryField].length; j++) {
          if (q.filter) {
            for (var k in q.filter) {
              if (q.filter.hasOwnProperty(k)) {
                for (var i = 0; i < q.filter[k].length; i++) {
                  keySet[keySet.length - 1].push(
                    ['TF￮' + queryField + '￮' + or[queryField][j] +
                    '￮' + k + '￮' + q.filter[k][i][0],
                      'TF￮' + queryField + '￮' + or[queryField][j] +
                      '￮' + k + '￮' + q.filter[k][i][1]])
                }
              }
            }
          } else {
            keySet[keySet.length - 1].push(
              ['TF￮' + queryField + '￮' + or[queryField][j] + '￮￮',
                'TF￮' + queryField + '￮' + or[queryField][j] + '￮￮￮'])
          }
        }
      }
    }
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

  // just do a DB lookup for each unique value
  var keySetsUniq = _uniqWith(_flatten(keySets), _isEqual)

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
    docFreqs.fieldWeight = {}
    docFreqs.docFreqs = []

    // get document frequencies
    results.forEach(function (item) {
      var dfToken = item.key[1].split('￮')[1] + '￮' + item.key[1].split('￮')[2]
      var dfValue = item.value.length
      docFreqs.df[dfToken] = dfValue
    })

    // get field weight
    Object.keys(docFreqs.df).forEach(function (i) {
      docFreqs.fieldWeight[i] = 0
      if (q.weight) {
        if (q.weight[i.split('￮')[0]]) {
          docFreqs.fieldWeight[i] = q.weight[i.split('￮')[0]]
        }
      }
    })

    // for each OR
    keySets.forEach(function (keySet) {
      // console.log(keySet)
      // for each AND
      var set = []

      // ANDing
      keySet.forEach(function (tf) {
        set = set.concat(_filter(results, function (o) {
          return _isEqual(o.key, tf)
        }).map(function (o) {
          return o.value
        }))
      })

      // do an intersection on AND values- token must be in all sets

      var ORSet = set.reduce(function (prev, cur) {
        return _intersection(prev, cur)
      })

      // TODO: NOTing

      console.log(ORSet)

      docFreqs.ORSets.push({
        keySet: keySet,
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

function intersectionDestructive (a, b) {
  var result = []
  while (a.length > 0 && b.length > 0) {
    if (a[0] < b[0]) {
      a.shift()
    } else if (a[0] > b[0]) {
      b.shift()
    } else /* they're equal */ {
      result.push(a.shift())
      b.shift()
    }
  }
  return result
}

var getResults = function (q, frequencies, keySet, indexes, callback) {
  if (q.sort) {
    getResultsSortedByField(q, frequencies, keySet, indexes, callback)
  } else {
    getResultsSortedByTFIDF(q, frequencies, indexes, callback)
  }
}

var getResultsSortedByField = function (q, frequencies, keySet, indexes, callback) {
  var sortKey = q.sort[0]
  var sortDirection = q.sort[1]
  glueDocs(frequencies.allDocsIDsInResultSet.map(function (item) {
    return {id: item}
  }), q, indexes, function (result) {
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
var getResultsSortedByTFIDF = function (q, frequencies, indexes, callbackX) {
  async.mapSeries(frequencies.docFreqs, function (item, callbacker) {
    var gte = item[1][0].replace(/^TF￮/, 'RI￮')
    var lte = item[1][1].replace(/^TF￮/, 'RI￮')
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
    indexes.createReadStream({gte: gte, lte: lte + '￮'})
      .on('data', function (data) {
        for (var i = 0; i < data.value.length; i++) {
          var thisID = data.value[i][1]
          var thisTF = +data.value[i][0]
          if (frequencies.allDocsIDsInResultSet.indexOf(thisID) !== -1) {
            frequencies.ORSets = frequencies.ORSets.map(function (ORSet) {
              ORSet.ORSet = ORSet.ORSet.map(function (hit) {
                if (hit.id === thisID) {
                  ORSet.keySet.forEach(function (key) {
                    if (_isEqual(key, item[1])) {
                      // hit.tf.push(data.value[i])
                      hit.tfidf.push([token,
                        field,
                        thisTF * idf,
                        thisTF,
                        idf])
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
          hit.score += +ANDTokens[2]
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
    glueDocs(hits, q, indexes, function (result) {
      return callbackX(err, result)
    })
  })
}

var glueDocs = function (hits, q, indexes, callbackX) {
  async.mapSeries(hits, function (item, callback) {
    indexes.get('DOCUMENT￮' + item.id + '￮', function (err, value) {
      item.document = value
      var terms = q.query[0]['*']
      if (q.query[0][q.teaser]) {
        terms = q.query[0][q.teaser] // this is a nasty hack- remove the [0]
      }
      if (q.teaser && item.document[q.teaser]) {
        try {
          item.document.teaser = scontext(
            item.document[q.teaser],
            terms,
            400,
            function hi (string) {
              return '<span class="sc-em">' + string + '</span>'
            })
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
