exports.getKeySet = function (clause, filter) {
  var keySet = []
  for (var fieldName in clause) {
    clause[fieldName].forEach(function (token) {
      // keySet[clause].push('DF' + '￮' + fieldName + '￮' + token + '￮￮')
      if (filter && Array.isArray(filter)) {
        // Filters: TODO
        filter.forEach(function (filter) {
          keySet.push([
            'DF￮' + fieldName + '￮' + token + '￮' + filter.field + '￮' + filter.gte,
            'DF￮' + fieldName + '￮' + token + '￮' + filter.field + '￮' + filter.lte + '￮'
          ])
        })
      } else {
        keySet.push([
          'DF￮' + fieldName + '￮' + token + '￮￮',
          'DF￮' + fieldName + '￮' + token + '￮￮￮'
        ])
      }
    })
  }
  return keySet
}
