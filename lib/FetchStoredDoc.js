const Transform = require('stream').Transform
const util = require('util')

const FetchStoredDoc = function (options) {
  this.options = options
  Transform.call(this, { objectMode: true })
}
exports.FetchStoredDoc = FetchStoredDoc
util.inherits(FetchStoredDoc, Transform)
FetchStoredDoc.prototype._transform = function (doc, encoding, end) {
  var that = this
  doc = JSON.parse(doc)
  that.options.indexes.get('DOCUMENT￮' + doc.id + '￮', function (err, stored) {
    doc.document = stored
    that.push(JSON.stringify(doc))
    return end()
  })
}
