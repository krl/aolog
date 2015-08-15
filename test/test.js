'use strict'

var assert = require('assert')
var BUCKET_SIZE = 2
var ipfs = require('ipfs-api')()
var aolog = require('../index.js')(ipfs, BUCKET_SIZE)
var _ = require('lodash')
var async = require('async')

var add_many = function (log, count, fn, cb) {
  var i = 0
  async.forever(function (next) {
    if (!count--) return next(1)
    var add = fn(i++, log)
    log.append(add, function (err, res) {
      if (err) throw err
      log = res
      next()
    })
  }, function () { cb(null, log) })
}

/* global describe, it, before */

describe('logs', function () {
  var log, log2, log3, log4, log5

  log = aolog.empty()

  it('got log interface', function () {
    assert(log)
  })

  before(function (done) {
    log.append(0, function (err, res) {
      if (err) throw err
      log2 = res
      done()
    })
  })

  it('should have an entry', function () {
    assert.equal(log2.elements[0], 0)
  })

  before(function (done) {
    add_many(log2, BUCKET_SIZE, function (i) { return i + 1 },
             function (err, log) {
               if (err) throw err
               log3 = log
               done()
             })
  })

  it('should have split', function () {
    assert.equal(log3.tail.ref.elements[0], BUCKET_SIZE)
  })

  before(function (done) {
    log3.append(BUCKET_SIZE+1, function (err, res) {
      if (err) throw err
      log4 = res
      done()
    })
  })

  it('should have appended in head', function () {
    assert.equal(log4.tail.ref.elements[0], BUCKET_SIZE)
  })

  before(function (done) {
    add_many(log4, BUCKET_SIZE -1, function (i) { return i + BUCKET_SIZE + 2 },
             function (err, log) {
               if (err) throw err
               log5 = log
               done()
             })
  })

  it('should have pushed a bucket down the middle!', function () {
    assert.equal(log5.rest.ref.refs[0].ref.elements[0], BUCKET_SIZE)
  })
})

describe('iterators', function () {

  describe('bucket iterator', function () {
    var log
    var expected = []

    before(function (done) {
      add_many(aolog.empty(), BUCKET_SIZE,
               function (i) {
                 expected.push(i)
                 return i
               },
               function (err, res) {
                 if (err) throw err
                 log = res
                 done()
               })
    })

    var result = []

    before(function (done) {
      var iter = log.iterator()
      async.forever(function (next) {
        iter.next(function (err, value, status) {
          if (err) throw (err)
          if (status === aolog.eof) return next(1)

          result.push(value)
          next()
        })
      }, function () { done() })
    })

    it('should have gotten the right elements with next', function () {
      assert.deepEqual(expected, result)
    })
  })

  describe('finger iterator', function () {

    var SIZE = BUCKET_SIZE + 1

    var log
    var expected = []

    before(function (done) {
      add_many(aolog.empty(), SIZE,
               function (i) {
                 expected.push(i)
                 return i
               },
               function (err, res) {
                 if (err) throw err
                 log = res
                 done()
               })
    })

    var result = []

    before(function (done) {
      var iter = log.iterator()
      async.forever(function (next) {
        iter.next(function (err, value, status) {
          if (err) throw (err)
          if (status === aolog.eof) return next(1)

          result.push(value)
          next()
        })
      }, function () { done() })
    })

    it('should have gotten the right elements', function () {
      assert.deepEqual(expected, result)
    })

    var nr = Math.floor(SIZE/3)
    var resultPart = []

    before(function (done) {
      var iter = log.iterator()

      iter.take(nr, function (err, array) {
        if (err) throw err
        resultPart = array
        done()
      })
    })

    it('should have taken ' + nr + ' of ' + SIZE + ' elements', function () {
      assert.deepEqual(resultPart, expected.slice(0, nr))
    })

    var resultTakeMore

    before(function (done) {
      var iter = log.iterator()

      iter.take(SIZE * 2, function (err, array) {
        if (err) throw err
        resultTakeMore = array
        done()
      })

    })

    it('should have stopped at ' + SIZE + ' elements', function () {
      assert.deepEqual(resultTakeMore, expected)
    })

    var resultAll = []
    before(function (done) {
      var iter = log.iterator()

      iter.all(function (err, array) {
        if (err) throw err
        resultAll = array
        done()
      })

    })

    it('should have taken all elements', function () {
      assert.deepEqual(resultAll, expected)
    })

    var SIZE = BUCKET_SIZE * 64

    var log
    var reference = []

    before(function (done) {
      add_many(aolog.empty(), SIZE,
               function (i) {
                 reference.push(i)
                 return i
               },
               function (err, res) {
                 if (err) throw err
                 log = res
                 done()
               })
    })

    // helper
    function range (from, to) {
      var arr = []
      while (from != to) {
        arr.push(from++)
      }
      return arr
    }

    it('should take all from all offsets', function (done) {
      var count = 0

      _.map(range(0, SIZE), function (ofs) {
        var iter = log.iterator({offset: ofs})
        count++
        iter.all(function (err, array) {
          if (err) throw err
          assert.deepEqual(array, reference.slice(ofs))
          if (!--count) done()
        })
      })
    })

    it('should take 1 from all offsets', function (done) {
      var count = 0
      _.map(range(0, SIZE), function (ofs) {
        var iter = log.iterator({offset: ofs})
        iter.next(function (err, res) {
          if (err) throw err
          assert.deepEqual(res, reference[ofs])
          if (++count === SIZE) done()
        })
      })
    })
  })

  describe('finger iterator reverse', function () {
    var log
    var SIZE = BUCKET_SIZE * 5
    var expected = []

    before(function (done) {
      add_many(aolog.empty(), SIZE,
               function (i) {
                 expected.unshift(i)
                 return i
               },
               function (err, res) {
                 if (err) throw err
                 log = res
                 done()
               })
    })

    var result = []

    it('should have gotten the right elements', function (done) {
      var iter = log.iterator({reverse: true})
      async.forever(function (next) {
        iter.next(function (err, value, status) {
          if (err) throw (err)
          if (status === aolog.eof) return next(1)

          result.push(value)
          next()
        })
      }, function () {
        assert.deepEqual(expected, result)
        done()
      })
    })

    var nr = Math.floor(SIZE/3)
    var resultPart = []

    before(function (done) {
      var iter = log.iterator({reverse: true})

      iter.take(nr, function (err, array) {
        if (err) throw err
        resultPart = array
        done()
      })
    })

    it('should have taken ' + nr + ' of ' + SIZE + ' elements', function () {
      assert.deepEqual(resultPart, expected.slice(0, nr))
    })

    var resultTakeMore

    before(function (done) {
      var iter = log.iterator({reverse: true})

      iter.take(SIZE * 2, function (err, array) {
        if (err) throw err
        resultTakeMore = array
        done()
      })

    })

    it('should have stopped at ' + SIZE + ' elements', function () {
      assert.deepEqual(resultTakeMore, expected)
    })

    var resultAll = []
    before(function (done) {
      var iter = log.iterator({reverse: true})

      iter.all(function (err, array) {
        if (err) throw err
        resultAll = array
        done()
      })

    })

    it('should have taken all elements', function () {
      assert.deepEqual(resultAll, expected)
    })
  })
})

describe('filters', function () {

  describe('fizzbuzz', function () {

    var SIZE = 1000
    var log

    var reference = []

    before(function (done) {
      add_many(aolog.empty(), SIZE,
               function (i) {
                 var a = i % 3 == 0
                 var b = i % 5 == 0
                 var val
                 if (a && b) {
                   val = {msg: 'fizz buzz'}
                 } else if (a) {
                   val = {msg: 'fizz'}
                 } else if (b) {
                   val = {msg: 'buzz'}
                 } else {
                   val = {msg: i}
                 }
                 reference.push(val)
                 return val
               },
               function (err, res) {
                 if (err) throw err
                 log = res
                 done()
               })
    })

    var refcount = 0

    before(function (done) {
      _.forEach(reference, function (val) {
        if (typeof val.msg === 'string' &&
            val.msg.match('buzz')) refcount++
      })
      done()
    })

    var count = 0
    before(function (done) {
      var iter = log.iterator({filter: {msg: 'buzz'}})
      async.forever(function (next) {
        iter.next(function (err, value, status) {
          if (err) throw (err)
          if (status === aolog.eof) return next(1)
          count++

          if (!value.msg.match('buzz')) {
            throw 'no buzz!'
          }
          next()
        })
      }, function () { done() })
    })

    it('should have found x elements', function () {
      assert.equal(count, refcount)
    })

  })

  describe('haystack', function () {

    var HAYSIZE = 10000

    var haystack = []
    for (let i = 0 ; i < HAYSIZE ; i++) {
      haystack.push({is: "haystrand #" + i})
    }
    haystack.push({is: "needle"})
    haystack = _.shuffle(haystack)

    var result = []
    var log

    before(function (done) {
      this.timeout(20000)
      add_many(aolog.empty(), HAYSIZE + 1,
               function (i) { return haystack[i] },
               function (err, res) {
                 if (err) throw err
                 log = res
                 done()
               })
    })

    before(function (done) {
      var iter = log.iterator({filter: {is: 'needle'}})

      async.forever(function (next) {
        iter.next(function (err, value, status) {
          if (err) throw (err)
          if (status === aolog.eof) return next(1)
          result.push(value)
          next()
        })
      }, function () { done() })
    })

    it('should have found the needle', function () {
      assert.equal(result.length, 1)
      assert.deepEqual(result[0], {is: 'needle'})
    })
  })
})

describe('count', function () {
  var SIZE = BUCKET_SIZE * 10
  it('should have the correct count always', function (done) {
    add_many(aolog.empty(), SIZE,
             function (i, current) {
               assert.equal(i, current.count)
               return i
             },
             function (err, res) {
               if (err) throw err
               done()
             })
  })
})

describe('persistance', function () {
  describe('persist bucket', function () {

    var log
    var hash

    before(function (done) {
      add_many(aolog.empty(), BUCKET_SIZE, function (i) { return i },
               function (err, res) {
                 if (err) throw err
                 log = res
                 done()
               })
    })

    before(function (done) {
      log.persist(function (err, res) {
        if (err) throw err
        hash = res.Hash
        done()
      })
    })

    it('should have persisted the bucket', function () {
      assert.equal(hash.substr(0, 2), 'Qm')
    })

    var restored
    before(function (done) {
      aolog.restore(hash, function (err, res) {
        if (err) throw err
        restored = res
        done()
      })
    })

    it('should have restored the bucket', function () {
      assert(restored)
    })

    var resultA = []
    var resultB = []

    before(function (done) {

      var iterA = log.iterator()
      var iterB = restored.iterator()

      var nrdone = 0
      var iterdone = function (done) {
        return function () {
          if (++nrdone === 2) done()
        }
      }

      async.forever(function (next) {
        iterA.next(function (err, value, status) {
          if (err) throw (err)
          if (status === aolog.eof) return next(1)
          resultA.push(value)
          next()
        })
      }, iterdone(done))

      async.forever(function (next) {
        iterB.next(function (err, value, status) {
          if (err) throw (err)
          if (status === aolog.eof) return next(1)
          resultB.push(value)
          next()
        })
      }, iterdone(done))
    })

    it('should have the same elements', function () {
      assert.deepEqual(resultA, resultB)
    })
  })

  describe('persist large tree', function () {

    var SIZE = 1000

    var log
    var hash

    before(function (done) {
      this.timeout(20000)
      add_many(aolog.empty(), SIZE, function (i) { return { is: "i = " + i } },
               function (err, res) {
                 if (err) throw err
                 log = res
                 done()
               })
    })

    before(function (done) {
      this.timeout(20000)
      log.persist(function (err, res) {
        if (err) throw err
        hash = res.Hash
        done()
      })
    })

    var restored
    before(function (done) {
      aolog.restore(hash, function (err, res) {
        if (err) throw err
        restored = res
        done()
      })
    })

    var resultA = []
    var resultB = []

    before(function (done) {
      this.timeout(40000)
      var iterA = log.iterator()
      var iterB = restored.iterator()

      var nrdone = 0
      var iterdone = function (done) {
        return function () {
          if (++nrdone === 2) done()
        }
      }
      async.forever(function (next) {
        iterA.next(function (err, value, status) {
          if (err) throw (err)
          if (status === aolog.eof) return next(1)
          resultA.push(value)
          next()
        })
      }, iterdone(done))
      async.forever(function (next) {
        iterB.next(function (err, value, status) {
          if (err) throw (err)
          if (status === aolog.eof) return next(1)
          resultB.push(value)
          next()
        })
      }, iterdone(done))
    })

    it('should have the same elements', function () {
      assert.deepEqual(resultA, resultB)
    })
  })


  describe('persist filters', function () {

    var log
    var SIZE = BUCKET_SIZE * 32 + 1

    before(function (done) {
      this.timeout(10000)
      add_many(aolog.empty(), SIZE, function (i) { return { is: "i = " + i } },
               function (err, res) {
                 if (err) throw err
                 log = res
                 done()
               })
    })

    it('should have filters on all refs', function () {
      assert(log.head.filters.is)
      assert(log.rest.filters.is)
      assert(log.tail.filters.is)
    })

    var hash
    before(function (done) {
      log.persist(function (err, res) {
        if (err) throw err
        hash = res.Hash
        done()
      })
    })

    var restored
    before(function (done) {
      aolog.restore(hash, function (err, res) {
        if (err) throw err
        restored = res
        done()
      })
    })

    before(function (done) {
      // make sure it's all in memory
      var iter = restored.iterator()
      async.forever(function (next) {
        iter.next(function (err, value, status) {
          if (err) throw (err)
          if (status === aolog.eof) return next(1)
          next()
        })
      }, function () { done() })
    })

    it('should have restored the filters', function () {

      assert.equal(log.head.filters.is.toString(),
                   restored.head.filters.is.toString())
      assert.equal(log.rest.filters.is.toString(),
                   restored.rest.filters.is.toString())
      assert.equal(log.tail.filters.is.toString(),
                   restored.tail.filters.is.toString())

      for (var i = 0 ; i < BUCKET_SIZE ; i++) {
        assert.equal(
          log.rest.ref.head.ref.refs[i].filters.is.toString(),
          restored.rest.ref.head.ref.refs[i].filters.is.toString())
      }
    })
  })

  describe('persist count', function () {
    var SIZE = BUCKET_SIZE * 10
    var log, plog, nlog
    before(function (done) {
      add_many(aolog.empty(), SIZE, function (i) { return { is: "i = " + i } },
               function (err, res) {
                 if (err) throw err
                 log = res
                 done()
               })
    })

    before(function (done) {
      log.persist(function (err, persisted) {
        if (err) throw err
        aolog.restore(persisted.Hash, function (err, res) {
          if (err) throw err
          nlog = res
          done()
        })
      })
    })

    it('should have persisted count', function () {
      assert.equal(log.count, nlog.count)
    })

  })

  describe('persist, restore, add, persist', function () {

    var log
    var SIZE = BUCKET_SIZE * 8

    var expected = []

    before(function (done) {
      this.timeout(10000)
      add_many(aolog.empty(), SIZE,
               function (i) {
                 var val = { is: "i = " + i }
                 expected.push(val)
                 return  val
               },
               function (err, res) {
                 if (err) throw err
                 log = res
                 done()
               })
    })

    var hash

    before(function (done) {
      log.persist(function (err, res) {
        if (err) throw err
        hash = res.Hash
        done()
      })
    })

    it('should have persisted', function () {
      assert.equal(hash.substr(0, 2), 'Qm')
    })

    var restored, iterated
    before(function (done) {
      aolog.restore(hash, function (err, res) {
        if (err) throw err
        restored = res
        done()
      })
    })

    before(function (done) {
      var val = {is: 'added after'}
      expected.push(val)

      restored = restored.append(val, function (err, res) {
        if (err) throw err
        res.iterator().all(function (err, res) {
          if (err) throw err

          iterated = res

          done()
        })
      })
    })

    it('should have added to restored bucket', function () {
      assert.deepEqual(expected, iterated)
    })
  })
})
