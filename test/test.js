'use strict'

var assert = require('assert')
var BUCKET_SIZE = 16
var ipfs = require('ipfs-api')()
var aolog = require('../log.js')(ipfs, BUCKET_SIZE)
var _ = require('lodash')
var async = require('async')

/* global describe, it, before */

describe('logs', function () {
  var log

  log = aolog.empty()

  it('got log interface', function () {
    assert(log)
  })

  var log2 = log.append(0)

  it('should have an entry', function () {
    assert.equal(log2.elements[0], 0)
  })

  var log3 = log2
  for (var i = 0 ; i < BUCKET_SIZE ; i++) {
    log3 = log3.append(i + 1)
  }

  it('should have split', function () {
    assert.equal(log3.tail.ref.elements[0], BUCKET_SIZE)
  })

  var log4 = log3.append(BUCKET_SIZE+1)

  it('should have appended in head', function () {
    assert.equal(log4.tail.ref.elements[0], BUCKET_SIZE)
  })

  var log5 = log4
  for (var i = 0 ; i < BUCKET_SIZE-1 ; i++) {
    log5 = log5.append(i + BUCKET_SIZE+2)
  }

  it('should have pushed a bucket down the middle!', function () {
    assert.equal(log5.rest.ref.refs[0].ref.elements[0], BUCKET_SIZE)
  })
})


describe('iterators', function () {

  describe('bucket iterator', function () {
    var log = aolog.empty()

    var expected = []
    for (var i = 0 ; i < BUCKET_SIZE ; i++) {
      log = log.append(i)
      expected.push(i)
    }

    var iter = log.iterator()

    var result = []

    before(function (done) {
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
    var log = aolog.empty()

    var SIZE = BUCKET_SIZE * 8
    var expected = []

    for (var i = 0 ; i < SIZE ; i++) {
      log = log.append(i)
      expected.push(i)
    }

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
      assert.deepEqual(resultAll, result)
    })
  })
})

describe('filters', function () {

  var SIZE = 1000
  var log = aolog.empty()

  var reference = []

  for (var i = 0 ; i < SIZE ; i++) {
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
    log = log.append(val)
    reference.push(val)
  }

  var refcount = 0
  _.forEach(reference, function (val) {
    if (typeof val.msg === 'string' &&
        val.msg.match('buzz')) refcount++
  })

  var count = 0
  before(function (done) {
    var iter = log.iterator({msg: 'buzz'})
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

  var HAYSIZE = 10000

  var haystack = []
  for (let i = 0 ; i < HAYSIZE ; i++) {
    haystack.push({is: "haystrand #" + i})
  }
  haystack.push({is: "needle"})
  haystack = _.shuffle(haystack)

  var haylog = aolog.empty()

  var oldfilter
  for (let i = 0 ; i < HAYSIZE + 1 ; i++) {
    haylog = haylog.append(haystack[i])
    if (oldfilter) {
      // should always contain old filter
      if (!haylog.filter().is.contains(oldfilter)) {
        throw 'new filter should contain old filter'
      }
    }
  }

  it('should have only added to filter', function () {})

  var result = []
  var iter = haylog.iterator({is: 'needle'})

  before(function (done) {
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

describe('persistance', function () {
  describe('persist bucket', function () {

    var log = aolog.empty()

    for (var i = 0 ; i < BUCKET_SIZE ; i++) {
      log = log.append(i)
    }

    var hash

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

    var log = aolog.empty()

    var SIZE = 10000

    for (var i = 0 ; i < SIZE ; i++) {
      log = log.append({is: "i = " + i})
    }

    var hash

    before(function (done) {
      this.timeout(40000)
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

    var log = aolog.empty()

    var SIZE = BUCKET_SIZE * 32 + 1

    for (var i = 0 ; i < SIZE ; i++) {
      log = log.append({is: "i = " + i})
    }

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
})
