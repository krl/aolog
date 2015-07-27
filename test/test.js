'use strict'

var assert = require('assert')
var BUCKET_SIZE = 4
var aolog = require('../log.js')(BUCKET_SIZE)
var _ = require('lodash')

/* global describe, it, before */

var print = function (log) {
  console.log()
  console.log(JSON.stringify(log, null, 2))
}

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
    assert.equal(log3.head.ref.elements[0], BUCKET_SIZE)
  })

  var log4 = log3.append(BUCKET_SIZE+1)

  it('should have appended in head', function () {
    assert.equal(log4.head.ref.elements[0], BUCKET_SIZE)
  })

  var log5 = log4
  for (var i = 0 ; i < BUCKET_SIZE-1 ; i++) {
    log5 = log5.append(i + BUCKET_SIZE+2)
  }

  it('should have pushed a bucket down the middle!', function () {
    assert.equal(log5.rest.ref.elements[0].ref.elements[0], BUCKET_SIZE)
  })
})


describe('iterators', function () {

  describe('bucket iterator', function () {
    var log = aolog.empty()

    for (var i = 0 ; i < 16 ; i++) {
      log = log.append(i)
    }

    var iter = log.iterator()

    it('should yield its elements', function () {
      for (var i = 0 ; i < 16 ; i++) {
        assert.equal(iter.next().value, i)
      }
    })
  })

  describe('finger iterator', function () {
    var SIZE = 2
    var log = aolog.empty()

    for (var i = 0 ; i < SIZE ; i++) {
      log = log.append(i)
    }

    var iter = log.iterator()

    it('should yield its elements', function () {
      for (var i = 0 ; i < SIZE ; i++) {
        var res = iter.next()
        assert.equal(res.value, i)
      }
    })
  })
})

describe('filters', function () {

  var SIZE = 16
  var log = aolog.empty()

  for (var i = 0 ; i < SIZE ; i++) {
    var a = i % 3 == 0
    var b = i % 5 == 0
    if (a && b) {
      log = log.append({msg: 'fizzbuzz'})
    } else if (a) {
      log = log.append({msg: 'fizz'})
    } else if (b) {
      log = log.append({msg: 'buzz'})
    } else {
      log = log.append({msg: i})
    }
  }

  it('should only iterate buzzy things', function () {
    var res
    var iter = log.iterator({msg: 'buzz'})
    while (!(res = iter.next()).done) {
      if (!res.value.msg.match('buzz')) {
        throw 'no buzz!'
      }
    }
  })

  var HAYSIZE = 1000

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

    oldfilter = haylog.filter().is
  }

  it('should have only added to filter', function () {})

  var result = []

  var iter = haylog.iterator({is: 'needle'})

  while (true) {
    var res = iter.next()
    if (res.done) break
    result.push(res.value)
  }

  it('should have found the needle', function () {
    assert.equal(result.length, 1)
    assert.deepEqual(result[0], {is: 'needle'})
  })
})
