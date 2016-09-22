const _defaults = require('lodash.defaults')

exports.getKeySet = function (clause) {
  var keySet = []
  for (var fieldName in clause) {
    clause[fieldName].forEach(function (token) {
      var gte = token.gte || token
      var lte = token.lte || token
      keySet.push([
        'DF￮' + fieldName + '￮' + gte + '￮￮',
        'DF￮' + fieldName + '￮' + lte + '￮￮￮'
      ])
    })
  }
  return keySet
}

exports.getQueryDefaults = function (q) {
  // if a string is given- turn it into a query
  if (typeof q === 'string') {
    q = {
      query: [{
        AND: {'*': q.split(' ')}
      }]
    }
  }
  return _defaults(q || {}, {
    query: [{
      AND: {'*': ['*']}
    }],
    offset: 0,
    pageSize: 20
  })
}
