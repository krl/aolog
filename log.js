'use strict'

var _ = require('lodash')
var bloom = require('blomma')(4, 3)

require("babelify/polyfill")

var print = function (log) {
  console.log()
  console.log(JSON.stringify(log, null, 2))
}

module.exports = function (BUCKET_SIZE) {

  var Ref = function (pointsto) {
    return {
      filters: pointsto.filter(),
      ref: pointsto,
      append: function (el) {
        return new Ref(this.ref.append(el))
      },
      filter: function () {
        return this.filters
      },
      iterator: function* (filter) {
        if (this.ref.iterator) {
          var iter = this.ref.iterator(filter)
          while (true) {
            var res = iter.next()
            if (res.done === true) {
              break
            } else {
              yield res.value
            }
          }
        }
      }
    }
  }

  var Bucket = function (elements, filters) {
    return {
      elements: elements || [],
      append: function (el) {
        if (this.elements.length === BUCKET_SIZE) {
          return new Finger(new Ref(new Bucket(this.elements)),
                            new Ref(new Bucket([])),
                            new Ref(new Bucket([el])))
        } else {
          var newelements = _.clone(this.elements)
          newelements.push(el)
          return new Bucket(newelements)
        }
      },
      filter: function () {
        // can we combine existing filters?
        if (this.elements[0] &&
            typeof this.elements[0].filter === 'function') {
          return combine(this.elements)
        }

        var filter = {}
        // else create new filters
        _.forEach(this.elements, function (element) {
          _.forEach(element, function (value, key) {
            if (typeof value === 'string') {
              if (!filter[key]) filter[key] = bloom.empty()
              _.forEach(splitWords(value), filter[key].add)
            }
          })
        })
        return filter
      },
      iterator: function* (filter) {
        for (var i = 0 ; i < this.elements.length ; i++) {
          var e = this.elements[i]
          if (e.iterator) {
            var iter = e.iterator(filter)
            while (true) {
              var res = iter.next()
              if (res.done === true) {
                break
              } else {
                yield res.value
              }
            }
          } else {
            if (matches(e, filter))
              yield e
          }
        }
      }
    }
  }

  var Finger = function (tail, rest, head) {
    return {
      head: head,
      rest: rest,
      tail: tail,
      append: function (el) {
        var newhead = head.append(el)
        // did we split the child?
        if (newhead.ref.head) {
          // yep
          return new Finger(tail,
                            rest.append(newhead.ref.tail),
                            newhead.ref.head)
        } else {
          // nope
          return new Finger(tail,
                            rest,
                            newhead)
        }
      },
      filter: function () {
        return combine([tail, rest, head])
      },
      iterator: function* (filter) {
        for (let part of [this.tail, this.rest, this.head]) {
          var iter = part.iterator(filter)
          while (true) {
            var res = iter.next()
            if (res.done === true) {
              break
            } else {
              yield res.value
            }
          }
        }
      }
    }
  }

  var combine = function (tocombine) {
    var filters = {}
    _.forEach(tocombine, function (part) {
      _.forEach(part.filter(), function (value, key) {
        if (!filters[key]) {
          filters[key] = value
        } else {
          filters[key] = bloom.merge(filters[key], value)
        }
      })
    })
    return filters
  }

  var splitWords = function (string) {
    // split into words, # and @ are concidered part
    // of the words
    // TODO: support non-latin alphabets
    return string.split(/[^0-9A-Za-z\u00C0-\u00ff\u00C0-\u024f#@_-]+/)
  }

  var matches = function (element, filter) {
    var matches = true
    _.forEach(filter, function (value, key) {
      if (typeof element[key] !== 'string' ||
          !element[key].match(value)) {
        matches = false
      }
    })
    return matches
  }

  return {
    empty: function () {
      return new Bucket([])
    },

    bloomify: function (elements) {
      var result = _.reduce(elements, function (acc, test) {
        _.map(test, function (value, key) {
          if (typeof value === 'string') {
            var filter = acc[key] ? bloom.clone(acc[key]) : bloom.empty()

            _.forEach(words, function (word) {
              filter.add(word)
            })
            acc[key] = filter
          }
        })
        return acc
      }, {})

      return result
    }
  }
}
