const Transform = require('stream').Transform
const util = require('util')

const FetchDocsFromDB = function (options, filter) {
  this.options = options
  Transform.call(this, { objectMode: true })
}
exports.FetchDocsFromDB = FetchDocsFromDB
util.inherits(FetchDocsFromDB, Transform)
FetchDocsFromDB.prototype._transform = function (line, encoding, end) {
  const that = this
  this.options.indexes.get('DOCUMENT￮' + line.toString() + '￮', function (err, doc) {
    if (!err) {
      that.push(JSON.stringify(doc) + '\n')
    }
    end()
  })
}
