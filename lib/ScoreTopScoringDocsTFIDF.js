/*
   Look at the top scoring docs, and work out which terms give hits in them
 */

const Transform = require('stream').Transform
const util = require('util')

// TODO: handle offset and pagesize

const ScoreTopScoringDocsTFIDF = function (options, offset, pageSize) {
  this.options = options
  Transform.call(this, { objectMode: true })
}
exports.ScoreTopScoringDocsTFIDF = ScoreTopScoringDocsTFIDF
util.inherits(ScoreTopScoringDocsTFIDF, Transform)
ScoreTopScoringDocsTFIDF.prototype._transform = function (clause, encoding, end) {
  var that = this
  var promises = []
  clause.topScoringDocs.forEach((doc) => {
    var docID = doc[1]
    promises.push(getMatchesForDoc(docID, that.options, clause))
  })
  Promise.all(promises).then((res) => {
    clause.matches = res
    that.push(clause)
    return end()
  }, (err) => {
    console.log(err)
  })
}

// work out matches for each (top scoring) doc
const getMatchesForDoc = (docID, options, clause) => {
  return new Promise((resolve, reject) => {
    var termVectors = clause.documentFrequencies.map((freq) => {
      return gett(freq.field, docID, freq.gte, freq.lte, freq.df, clause.WEIGHT, options)
    })
    Promise.all(termVectors).then((matchedTerms) => {
      var matches = [].concat.apply([], matchedTerms)  // voodoo magic to flatten array
      resolve({
        id: docID,
        matches: matches,
        score: getScore(matches)
      })
    }, (err) => {
      console.log(err)
    })
  })
}

// average score for all matches in clause
const getScore = (matchedTerms) => {
  return matchedTerms.reduce(function (sum, match) {
    return sum + match.score
  }, 0) / matchedTerms.length
}

// get term frequency from db, work out tfidf and score
const gett = (field, key, gte, lte, df, weight, options) => {
  var s = options.keySeparator
  return new Promise((resolve, reject) => {
    options.indexes.get('DOCUMENT-VECTOR' + s + field + s + key + s, (err, value) => {

      // ERROR
      // an error here probably means that the
      // index was created with storeVector: false, so all vectors are
      // given a default magnitude of 1
      if (err) {
        return resolve([
          {
            field: field,
            term: gte,     // just pick gte here (could also be lte)
            tf: 1,
            df: 1,
            tfidf: 1,
            weight: 1,
            score: 1
          } 
        ])
      }

      // NO ERROR
      // Document vector was found, so do a proper weighting
      var matchedTerms = Object.keys(value).filter((t) => {
        if ((t >= gte) && (t <= lte)) return true
      })
      var matchedTermsWithMagnitude = []
      matchedTerms.forEach((t) => {
        var tf = value[t]
        var tfidf = tf * (Math.log10(1 + (1 / df)))
        var term = {
          field: field,
          term: t,
          tf: tf,
          df: df,
          tfidf: tfidf,
          weight: weight,
          score: tfidf * weight
        }
        matchedTermsWithMagnitude.push(term)
      })
      return resolve(matchedTermsWithMagnitude)
    })
  })
}
