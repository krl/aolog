'use strict'

var _ = require('lodash')
var bloom = require('blomma')(1024, 1)
var async = require('async')
var pako = require('pako')

module.exports = function (ipfs, BUCKET_SIZE) {

  var Ref = function (ref, filters, count) {
    return {
      type: 'Ref',
      filters: filters,
      ref: ref,
      count: count,
      children: 1,
      append: function (el, cb) {
        restore(this.ref.Hash, function (err, restored) {
          if (err) return cb(err)
          restored.append(el, function (err, res) {
            if (err) return cb(err)
            cb(null, res)
          })
        })
      },
      offset: function (ofs) {
        return [0, ofs]
      },
      getOffset: function () {
        return 0
      },
      getChild: function (idx, filter, cb) {
        var self = this
        restore(self.ref.Hash, function (err, res) {
          if (err) return cb(err)
          cb(null, { restored: res })
        })
      },
      persist: function (cb) {
        return cb(null, this.ref)
      }
    }
  }

  var Bucket = function (elements) {
    return {
      type: 'Bucket',
      elements: elements || [],
      count: elements.length,
      children: elements.length,
      filters: elementFilters(elements),
      append: function (el, cb) {
        if (this.elements.length === BUCKET_SIZE) {
          cb(null, { split: [ new Bucket(this.elements),
                              new Bucket([el]) ] })
        } else {
          var newelements = _.clone(this.elements)
          newelements.push(el)
          cb(null, { value: new Bucket(newelements) })
        }
      },
      offset: function (ofs) {
        return [ofs, 0]
      },
      getOffset: function (idx) {
        return idx
      },
      getChild: function (idx, filter, cb) {
        var el = this.elements[idx]
        if (typeof el === 'undefined') return cb(null, { eof: true })

        if (matches(el, filter.words)) {
          return cb(null, { element: el })
        } else {
          return cb(null, { skip: true })
        }
      },
      persist: function (cb) {
        var self = this

        if (self.persisted) return cb(null, self.persisted)

        var buf = new Buffer(JSON.stringify({
          Data: JSON.stringify({
            type: 'Bucket',
            data: this
          }),
          Links: []
        }))

        ipfs.object.put(buf, 'json', function (err, put) {
          if (err) return cb(err)

          ipfs.object.stat(put.Hash, function (err, stat) {
            if (err) return cb(err)
            self.persisted = { Hash: put.Hash,
                               Size: stat.CumulativeSize}
            cb(null, self.persisted)
          })
        })
      }
    }
  }

  var Branch = function (elements) {
    return {
      type: 'Branch',
      elements: elements,
      count: _.reduce(elements, function (a, b) {
        return a + b.count
      }, 0),
      children: elements.length,
      filters: combineFilters(this.elements),
      append: function (el, cb) {
        if (this.elements.length === BUCKET_SIZE) {
          cb(null, { split: [ new Branch(this.elements),
                              new Branch([el]) ]})
        } else {
          var newelements = _.clone(this.elements)
          newelements.push(el)
          cb(null, { value: new Branch(newelements) })
        }
      },
      offset: function (ofs) {
        var idx = 0
        while (this.elements[(idx + 1)] && this.elements[idx].count <= ofs) {
          ofs -= this.elements[idx].count
          idx++
        }
        return [idx, ofs]
      },
      getOffset: function (idx) {
        var count = 0
        for (var i = 0 ; i < idx ; i++) {
          count += this.elements[i].count
        }
        return count
      },
      getChild: function (idx, filter, cb) {
        var element = this.elements[idx]

        if (element) {
          if (!subsetMatches(element.filters, filter.blooms)) {
            cb(null, { skip: true })
          } else {
            cb(null, { push: element })
          }
        } else {
          cb(null, { eof: true })
        }
      },
      persist: function (cb) {
        var self = this

        if (self.persisted) return cb(null, self.persisted)

        var filters = {}
        var counts = {}
        async.series(_.map(self.elements, function (element, idx) {
          var name = zeropad(idx)
          filters[name] = serializeFilters(self.elements[idx].filters)
          counts[name] = self.elements[idx].count
          return function (done) {
            element.persist(function (err, persisted) {
              if (err) return done(err)
              done(null, {
                Name: name,
                Hash: persisted.Hash,
                Size: persisted.Size
              })
            })
          }
        }), function (err, links) {
          if (err) return cb(err)

          var obj = {
            Data: JSON.stringify({
              type: self.type,
              counts: counts,
              filters: filters
            }),
            Links: links
          }

          var buf = new Buffer(JSON.stringify(obj))
          ipfs.object.put(buf, 'json', function (err, put) {
            if (err) return cb(err)
            ipfs.object.stat(put.Hash, function (err, stat) {
              if (err) return cb(err)
              self.persisted = { Hash: put.Hash,
                                 Size: stat.CumulativeSize }
              cb(null, self.persisted)
            })
          })
        })
      }
    }
  }

  var Finger = function (elements) {
    return {
      type: 'Finger',
      elements: elements,
      count: _.reduce(elements, function (a, b) {
        return a + b.count
      }, 0),
      children: 3,
      append: function (el, cb) {
        var self = this
        var tail = 2
        var newelements = _.clone(self.elements)
        elements[tail].append(el, function (err, res) {
          if (err) return cb(err)
          if (res.split) {
            // push first down the middle
            newelements[2] = res.split[1]
            elements[1].append(res.split[0], function (err, pushres) {
              if (err) return cb(err)
              if (pushres.split) {
                newelements[1] = new Finger([ pushres.split[0],
                                              new Branch([]),
                                              pushres.split[1] ])
              } else {
                newelements[1] = pushres.value
              }

              cb(null, { value: new Finger(newelements)})
            })
          } else {
            newelements[2] = res.value
            cb(null, { value: new Finger(newelements)})
          }
        })
      },
      offset: function (ofs) {
        var idx = 0
        while (this.elements[(idx + 1)] && this.elements[idx].count <= ofs) {
          ofs -= this.elements[idx].count
          idx++
        }
        return [idx, ofs]
      },
      getOffset: function (idx) {
        var count = 0
        for (var i = 0 ; i < idx ; i++) {
          count += this.elements[i].count
        }
        return count
      },
      getChild: function (idx, filter, cb) {
        var element = this.elements[idx]
        if (element) {
          if (!subsetMatches(element.filters, filter.blooms)) {
            cb(null, { skip: true })
          } else {
            cb(null, { push: element })
          }
        } else {
          cb(null, { eof: true })
        }
      },
      persist: function (cb) {
        var self = this

        if (self.persisted) return cb(null, self.persisted)

        var filters = {}
        var counts = {}
        var parts = ['head', 'rest', 'tail']
        async.series(_.map(self.elements, function (element, idx) {
          var name = parts[idx]
          filters[name] = serializeFilters(self.elements[idx].filters)
          counts[name] = self.elements[idx].count
          return function (done) {
            self.elements[idx].persist(function (err, persisted) {
              if (err) return done(err)
              done(null, {
                Name: name,
                Hash: persisted.Hash,
                Size: persisted.Size
              })
            })
          }
        }), function (err, links) {
          if (err) return cb(err)

          var obj = {
            Data: JSON.stringify({
              type: 'Finger',
              filters: filters,
              counts: counts
            }),
            Links: links
          }

          var buf = new Buffer(JSON.stringify(obj))

          ipfs.object.put(buf, 'json', function (err, put) {
            if (err) return cb(err)
            ipfs.object.stat(put.Hash, function (err, stat) {
              if (err) return cb(err)
              self.persisted = { Hash: put.Hash,
                                 Size: stat.CumulativeSize }
              cb(null, self.persisted)
            })
          })
        })
      }
    }
  }

  var Root = function (ref) {
    if (!ref) ref = new Bucket([])

    return {
      type: 'Root',
      ref: ref,
      count: ref.count,
      append: function (el, cb) {
        this.ref.append(el, function (err, res) {
          if (err) return cb(err)
          if (res.split) {
            var newelements = []
            newelements[0] = res.split[0]
            newelements[1] = new Branch([])
            newelements[2] = res.split[1]
            cb(null, new Root(new Finger(newelements)))
          } else {
            cb(null, new Root(res.value))
          }
        })
      },
      iterator: function (opts) {
        return new Iterator(this.ref, opts)
      },
      persist: function (cb) {
        ref.persist(cb)
      },
      concat: function (items, cb) {
        var idx = 0
        var log = this
        async.forever(function (next) {
          log.append(items[idx++], function (err, res) {
            if (err) return cb(err)
            if (idx === items.length) return cb(null, res)
            log = res
            next()
          })
        })
      },
      get: function (idx, cb) {
        var self = this
        self.iterator({ offset: idx }).next(function (err, res) {
          if (err) return cb(err)
          cb(null, res)
        })
      }
    }
  }

  var Iterator = function (over, opts) {
    if (!opts) opts = {}
    var reverse = !!opts.reverse
    var fullfilter = makefilter(opts.filter)
    var def = reverse ? over.count - 1 : 0
    var offset = (typeof opts.offset !== 'undefined' ? opts.offset : def)
    var stack = null

    return {
      pushcount: 0,
      next: function (cb) {
        var self = this

        // initialize stack
        if (!stack) {
          stackFromOffset(over, offset, function (err, newstack) {
            if (err) return cb(err)
            stack = newstack
            self.next(cb)
          })
          return
        }

        if (!stack[0]) return cb(null, { eof: true })

        stack[0].obj.getChild(stack[0].idx, fullfilter, function (err, res) {
          if (err) return cb(err)

          if (res.eof) {
            stack.shift()
            if (!stack[0]) return cb(null, { eof: true })
            reverse ? stack[0].idx-- : stack[0].idx++
            self.next(cb)
          } else if (res.skip) {
            reverse ? stack[0].idx-- : stack[0].idx++
            self.next(cb)
          } else if (res.push) {
            self.pushcount++
            stack.unshift({ obj: res.push,
                            idx: reverse ? res.push.children - 1 : 0 })
            self.next(cb)
          } else if (res.restored) {
            stack[0] = { obj: res.restored,
                         idx: reverse ? res.restored.children - 1 : 0 }
            self.next(cb)
          } else if (typeof res.element !== 'undefined') {
            var index = offsetFromStack(stack)

            reverse ? stack[0].idx-- : stack[0].idx++
            cb(null, {
              element: res.element,
              index: index
            })
          } else {
            throw new Error('unhandled case, ' + JSON.stringify(res))
          }
        })
      },
      take: function (nr, cb) {
        var self = this
        var accum = []
        async.forever(function (next) {
          self.next(function (err, res) {
            if (err) return cb(err)
            if (res.eof) return cb(null, accum)
            if (!nr--) return cb(null, accum)
            accum.push(res)
            next()
          })
        })
      },
      all: function (cb) {
        this.take(Infinity, cb)
      }
    }
  }

  var offsetFromStack = function (stack) {
    return _.reduce(stack, function (acc, n) {
      return acc + n.obj.getOffset(n.idx)
    }, 0)
  }

  var stackFromOffset = function (over, offset, acc, cb) {
    if (!cb) {
      cb = acc
      acc = []
    }

    var idxrest = over.offset(offset)

    var idx = idxrest[0]
    var rest = idxrest[1]

    acc.unshift({ obj: over,
                  idx: idx })

    over.getChild(idx, {}, function (err, res) {
      if (err) return cb(err)
      if (res.restored) {
        acc.shift()
        stackFromOffset(res.restored, rest, acc, cb)
      } else if (res.push) {
        stackFromOffset(res.push, rest, acc, cb)
      } else {
        cb(null, acc)
      }
    })
  }

  var elementFilters = function (elements) {
    var filter = {}
    _.forEach(elements, function (element) {
      _.forEach(element, function (value, key) {
        if (typeof value === 'string') {
          if (!filter[key]) filter[key] = bloom.empty()
          _.forEach(splitWords(value), function (word) {
            filter[key].add(word)
          })
        }
      })
    })
    return filter
  }

  var serializeFilters = function (filters) {
    var serialized = {}
    _.forEach(filters, function (value, key) {
      var compressed = new Buffer(pako.deflate(filters[key].buffer)).toString('base64')
      serialized[key] = compressed
    })
    return serialized
  }

  var deserializeFilters = function (filters) {
    var deserialized = {}
    _.forEach(filters, function (value, key) {
      var buffer = new Buffer(
        pako.inflate(new Buffer(filters[key], 'base64')),
        'base64')
      deserialized[key] = bloom.fromBuffer(buffer)
    })
    return deserialized
  }

  var makefilter = function (filter) {
    if (!filter) {
      return {
        words: {},
        blooms: {}}
    }

    var blooms = {}

    _.forEach(filter, function (value, key) {
      blooms[key] = bloom.empty()
      _.forEach(splitWords(value), function (word) {
        blooms[key].add(word)
      })
    })

    return {words: filter,
            blooms: blooms}
  }

  var zeropad = function (nr) {
    var str = ('00' + nr)
    return str.substr(str.length - 3)
  }

  var combineFilters = function (tocombine) {
    var filters = {}
    _.forEach(tocombine, function (part) {
      _.forEach(part.filters, function (value, key) {
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
    return string.toLowerCase().split(/[^0-9a-z\u00C0-\u00ff\u00C0-\u024f#@_-]+/)
  }

  var matches = function (element, filter) {
    var matches = true
    _.forEach(filter, function (value, key) {
      // TODO use pluggable tokenizer
      var regexp = new RegExp('(?:^| )' + value + '(?:$| |[?!,.])', 'i')
      if (typeof element[key] !== 'string' ||
          !element[key].match(regexp)) {
        matches = false
      }
    })

    return matches
  }

  var subsetMatches = function (superset, subset) {
    var matches = true
    if (!superset || !Object.keys(superset).length) return true

    _.forEach(subset, function (value, key) {
      if (!superset[key] ||
          !superset[key].contains(value)) {
        matches = false
      }
    })
    return matches
  }

  var restore = function (hash, cb) {
    ipfs.object.get(hash, function (err, res) {
      if (err) return cb(err)
      var object = JSON.parse(res.Data)

      if (object.type === 'Bucket') {
        cb(null, new Bucket(object.data.elements))
      } else if (object.type === 'Branch') {
        cb(null, new Branch(_.map(res.Links, function (link, idx) {
          return new Ref({ Hash: link.Hash,
                           Size: link.Size },
                         deserializeFilters(object.filters[zeropad(idx)]),
                         object.counts[zeropad(idx)])
        })))
      } else if (object.type === 'Finger') {
        var linkmap = {}
        _.forEach(res.Links, function (link) {
          linkmap[link.Name] = link
        })

        cb(null, new Finger([ new Ref({ Hash: linkmap.head.Hash,
                                        Size: linkmap.head.Size },
                                      deserializeFilters(object.filters.head),
                                      object.counts.head),
                              new Ref({ Hash: linkmap.rest.Hash,
                                        Size: linkmap.rest.Size },
                                      deserializeFilters(object.filters.rest),
                                      object.counts.rest),
                              new Ref({ Hash: linkmap.tail.Hash,
                                        Size: linkmap.tail.Size },
                                      deserializeFilters(object.filters.tail),
                                      object.counts.tail) ]))
      }
    })
  }

  return {
    empty: function () {
      return new Root()
    },
    restore: function (hash, cb) {
      restore(hash, function (err, res) {
        if (err) return cb(err)
        cb(null, new Root(res))
      })
    }
  }
}
