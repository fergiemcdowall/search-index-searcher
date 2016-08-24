const Transform = require('stream').Transform
const util = require('util')

exports.FetchDocsFromDB = FetchDocsFromDB = function(options, filter) {
  this.options = options
  Transform.call(this, { objectMode: true })
}
util.inherits(FetchDocsFromDB, Transform)
FetchDocsFromDB.prototype._transform = function (line, encoding, end) {
  var that = this
  this.options.indexes.get('DOCUMENT￮' + line.toString() + '￮', function (err, doc) {
    if (!err) {
      that.push(JSON.stringify(doc) + '\n')
    }
    end()
  })
}
