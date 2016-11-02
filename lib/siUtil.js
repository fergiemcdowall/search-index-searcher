exports.getKeySet = function (clause, sep) {
  var keySet = []
  for (var fieldName in clause) {
    clause[fieldName].forEach(function (token) {
      var gte = token.gte || token
      var lte = token.lte || token
      keySet.push([
        'DF' + sep + fieldName + sep + gte,
        'DF' + sep + fieldName + sep + lte
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
  return Object.assign({}, {
    query: [{
      AND: {'*': ['*']}
    }],
    offset: 0,
    pageSize: 20
  }, q)
}
