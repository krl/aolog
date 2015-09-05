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

      // console.log('res')
      // console.log(res)

      log = res
      next()
    })
  }, function () { cb(null, log) })
}

function range (from, to) {
  if (from > to) return []
  var arr = []
  while (from !== to) {
    arr.push(from++)
  }
  return arr
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
    assert.equal(log2.ref.elements[0], 0)
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
    assert.equal(log3.ref.elements[2].elements[0], BUCKET_SIZE)
  })

  before(function (done) {
    log3.append(BUCKET_SIZE + 1, function (err, res) {
      if (err) throw err
      log4 = res
      done()
    })
  })

  it('should have appended in head', function () {
    assert.equal(log4.ref.elements[2].elements[1], BUCKET_SIZE + 1)
  })

  before(function (done) {
    add_many(log4, BUCKET_SIZE - 1, function (i) { return i + BUCKET_SIZE + 2 },
             function (err, log) {
               if (err) throw err
               log5 = log
               done()
             })
  })

  it('should have pushed a bucket down the middle!', function () {
    assert.deepEqual(log5.ref.elements[1].elements[0].elements[0], BUCKET_SIZE)
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
        iter.next(function (err, res) {
          if (err) throw (err)
          if (res.eof) return next(1)

          result.push(res.element)
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

      // console.log('aolog.empty()')
      // console.log(aolog.empty())

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
        iter.next(function (err, res) {
          if (err) throw (err)
          if (res.eof) return next(1)
          result.push(res.element)
          next()
        })
      }, function () { done() })
    })

    it('should have gotten the right elements', function () {
      assert.deepEqual(expected, result)
    })

    var nr = Math.floor(SIZE / 3)
    var resultPart = []

    before(function (done) {
      var iter = log.iterator()

      iter.take(nr, function (err, array) {
        if (err) throw err
        resultPart = _.map(array, function (x) { return x.element })
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
        resultTakeMore = _.map(array, function (x) { return x.element })
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
        resultAll = _.map(array, function (x) { return x.element })
        done()
      })

    })

    it('should have taken all elements', function () {
      assert.deepEqual(resultAll, expected)
    })

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

    it('should take all from all offsets', function (done) {
      var count = 0

      _.map(range(0, SIZE), function (ofs) {
        var iter = log.iterator({offset: ofs})
        count++
        iter.all(function (err, array) {
          if (err) throw err
          assert.deepEqual(_.map(array, function (x) { return x.element }),
                           reference.slice(ofs))
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
          assert.deepEqual(res.element, reference[ofs])
          if (++count === SIZE) done()
        })
      })
    })

    it('should take all from all offsets in reverse', function (done) {
      var count = 0

      _.map(range(0, SIZE), function (ofs) {
        var iter = log.iterator({offset: ofs, reverse: true})
        count++
        iter.all(function (err, array) {
          if (err) throw err
          assert.deepEqual(_.map(array, function (x) { return x.element }),
                           reference.slice(0, ofs + 1).reverse())
          if (!--count) done()
        })
      })
    })

    it('should take 1 from all offsets in reverse', function (done) {
      var count = 0
      _.map(range(0, SIZE), function (ofs) {
        var iter = log.iterator({offset: ofs, reverse: true})
        iter.next(function (err, res) {
          if (err) throw err
          assert.deepEqual(res.element, reference[ofs])
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
        iter.next(function (err, res) {
          if (err) throw (err)
          if (res.eof) return next(1)

          result.push(res.element)
          next()
        })
      }, function () {
        assert.deepEqual(expected, result)
        done()
      })
    })

    var nr = Math.floor(SIZE / 3)
    var resultPart = []

    before(function (done) {
      var iter = log.iterator({reverse: true})

      iter.take(nr, function (err, array) {
        if (err) throw err
        resultPart = _.map(array, function (x) { return x.element })
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
        resultTakeMore = _.map(array, function (x) { return x.element })
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
        resultAll = _.map(array, function (x) { return x.element })
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
                 var a = i % 3 === 0
                 var b = i % 5 === 0
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
        iter.next(function (err, res) {
          if (err) throw (err)
          if (res.eof) return next(1)
          count++

          if (!res.element.msg.match('buzz')) {
            throw new Error('no buzz!')
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
    for (var i = 0 ; i < HAYSIZE ; i++) {
      haystack.push({is: 'haystrand #' + i})
    }
    var needle = {is: 'needle'}

    haystack.push(needle)
    haystack = _.shuffle(haystack)
    var needleidx = haystack.indexOf(needle)

    var result = []
    var resultB = []
    var log

    before(function (done) {
      this.timeout(40000)
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
        iter.next(function (err, res) {
          if (err) throw (err)
          if (res.eof) return next(1)
          result.push(res)
          next()
        })
      }, function () { done() })
    })

    it('should have found the needle at ' + needleidx, function () {
      assert.equal(result.length, 1)
      assert.deepEqual(result[0], {
        element: {
          is: 'needle'
        },
        index: needleidx
      })
    })

    before(function (done) {
      var iter = log.iterator({filter: {is: 'NeEdle'}})

      async.forever(function (next) {
        iter.next(function (err, res) {
          if (err) throw (err)
          if (res.eof) return next(1)
          resultB.push(res)
          next()
        })
      }, function () { done() })
    })

    it('should have found the nEeDlE at ' + needleidx, function () {
      assert.equal(resultB.length, 1)
      assert.deepEqual(resultB[0], {
        element: {
          is: 'needle'
        },
        index: needleidx
      })
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

describe('index', function () {
  var SIZE = BUCKET_SIZE * 100
  it('should have the correct indicies', function (done) {
    var count = 0
    add_many(aolog.empty(), SIZE,
             function (i, current) {
               current.iterator().all(function (err, res) {
                 assert.deepEqual(range(0, i),
                                  _.map(res, function (x) { return x.index }))
                 if (++count == SIZE) done()
               })
               return i
             },
             function (err, res) {
               if (err) throw err
             })
  })

  it('should have the correct indicies with offset', function (done) {
    var count = 0
    var offset = 2
    add_many(aolog.empty(), SIZE,
             function (i, current) {
               current.iterator({ offset: offset }).all(function (err, res) {
                 assert.deepEqual(range(offset, i),
                                  _.map(res, function (x) { return x.index }))
                 if (++count == SIZE) done()
               })
               return i
             },
             function (err, res) {
               if (err) throw err
             })
  })

  it('should have the correct reverse indicies', function (done) {
    var count = 0
    add_many(aolog.empty(), SIZE,
             function (i, current) {
               return i
             },
             function (err, res) {
               if (err) throw err

               res.iterator({ reverse: true, offset: SIZE - 1 }).all(
                 function (err, res) {
                   if (err) throw err
                   assert.deepEqual(range(0, SIZE).reverse(),
                                    _.map(res, function (x) { return x.index }))
                   done()
                 })

             })
  })
})

describe('search', function () {
  var texts = [
    /* 0 */ "hello worlds",
    /* 1 */ "hello there",
    /* 2 */ "this is a longer string with lots of words in it ok",
    /* 3 */ "this is a longer string with lots of hello",
    /* 4 */ "who would search for this string",
    /* 5 */ "i need more strings to fill this out",
    /* 6 */ "where can you get cheap strings around here?",
    /* 7 */ "we could even search for this #hashtag",
    /* 8 */ "we could even hello again",
    /* 9 */ "i need more sleep"
  ]

  var log

  before(function (done) {
    add_many(aolog.empty(), texts.length,
             function (i, current) {
               return { text: texts[i] }
             },
             function (err, res) {
               if (err) throw err
               log = res
               done()
             })
  })

  it('should search', function (done) {
    log.iterator({ filter: { text: 'hellO' },
                   reverse: true,
                   offset: texts.length - 1
                 }).all(function (err, res) {
                   assert.deepEqual(res, [
                     { element: { text: 'we could even hello again' }, index: 8 },
                     { element: { text: 'this is a longer string with lots of hello' },
                       index: 3 },
                     { element: { text: 'hello there' }, index: 1 },
                     { element: { text: 'hello worlds' }, index: 0 } ])
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
        iterA.next(function (err, res) {
          if (err) throw (err)
          if (res.eof) return next(1)
          resultA.push(res.element)
          next()
        })
      }, iterdone(done))

      async.forever(function (next) {
        iterB.next(function (err, res) {
          if (err) throw (err)
          if (res.eof) return next(1)
          resultB.push(res.element)
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
      this.timeout(10000)
      add_many(aolog.empty(), SIZE, function (i) { return { is: 'i = ' + i } },
               function (err, res) {
                 if (err) throw err
                 log = res
                 done()
               })
    })

    before(function (done) {
      this.timeout(10000)
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
      this.timeout(10000)
      var iterA = log.iterator()
      var iterB = restored.iterator()

      var nrdone = 0
      var iterdone = function (done) {
        return function () {
          if (++nrdone === 2) done()
        }
      }
      async.forever(function (next) {
        iterA.next(function (err, res) {
          if (err) throw (err)
          if (res.eof) return next(1)
          resultA.push(res.element)
          next()
        })
      }, iterdone(done))
      async.forever(function (next) {
        iterB.next(function (err, res) {
          if (err) throw (err)
          if (res.eof) return next(1)
          resultB.push(res.element)
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
      add_many(aolog.empty(), SIZE, function (i) { return { is: 'i = ' + i } },
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
        iter.next(function (err, res) {
          if (err) throw (err)
          if (res.eof) return next(1)
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
    var log, nlog
    before(function (done) {
      add_many(aolog.empty(), SIZE, function (i) { return { is: 'i = ' + i } },
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
                 var val = { is: 'i = ' + i }
                 expected.push(val)
                 return val
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

    var restored
    before(function (done) {
      aolog.restore(hash, function (err, res) {
        if (err) throw err
        restored = res
        done()
      })
    })

    it('should repersist to the same hash', function (done) {
      restored.persist(function (err, repersisted) {
        if (err) throw err
        assert.equal(repersisted.Hash, hash)
        done()
      })
    })

    it('should have added to restored bucket', function (done) {
      var val = {is: 'added after'}
      expected.push(val)

      restored = restored.append(val, function (err, res) {
        if (err) throw err

        res.persist(function (err, repersisted) {
          if (err) throw err

          aolog.restore(repersisted.Hash, function (err, rerestored) {
            if (err) throw err

            rerestored.iterator().all(function (err, res) {
              if (err) throw err
              assert.deepEqual(expected,
                               _.map(res, function (x) { return x.element }))
              done()
            })
          })
        })
      })
    })
  })
})
