const Transform = require('stream').Transform
const util = require('util')

const ReplaceSynonyms = function (options, filter, requestedBuckets) {
  this.options = options
  Transform.call(this, { objectMode: true })
}
exports.ReplaceSynonyms = ReplaceSynonyms
util.inherits(ReplaceSynonyms, Transform)
ReplaceSynonyms.prototype._transform = function (clause, encoding, end) {
  const that = this
  var tokens = {}   // should probably be a Set
  var synonyms = {}
  var queryChain = []

  const queryDB = function (key) {
    return new Promise((resolve, reject) => {
      that.options.indexes.get(
        'SYNONYM' + that.options.keySeparator + key, (err, res) => {
          if (err) resolve({})
          else {
            resolve({
              key: key,
              value: res
            })
          }
        })
    })
  }

  // only consider AND tokens when checking for synonyms
  // (TODO: NOT tokens)

  // make set of synonyms.
  // One token can appear in query several times, so only ask index
  // for a synonym once
  for (let k in clause.AND) {
    clause.AND[k].forEach(token => {
      tokens[token] = '' // init with empty value
    })
  }

  // Queue synonym queries
  for (let k in tokens) {
    queryChain.push(queryDB(k))
  }

  // Run synonym queries
  Promise.all(queryChain)
    .then(values => {
      // if found, push synonyms into set
      values.forEach(value => {
        if (value.key !== undefined) {
          synonyms[value.key] = value.value
        }
      })
      // replace query tokens that have synonyms
      for (let k in clause.AND) {
        clause.AND[k] = clause.AND[k].map(token => {
          return synonyms[token] || token
        })
      }
      // push clause back into pipeline
      that.push(clause)
      return end()
    })
}
