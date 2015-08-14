'use strict'

// var persistances = 0

var _ = require('lodash')
var bloom = require('blomma')(1024, 1)
var async = require('async')
var pako = require('pako')

var EOF = 0
var SKIP = 1

module.exports = function (ipfs, BUCKET_SIZE) {
  var Iterator = function (over, filter, reverse) {
    var stack = [{obj: over, idx: reverse ? -1 : 0}]
    var fullfilter = makefilter(filter)

    return {
      pushcount: 0,
      next: function (cb) {
        var self = this
        // get element from top of stack
        stack[0].obj.get(stack[0].idx, fullfilter, function (err, element, status) {
          if (err) return cb(err)
          if (status === EOF) {
            stack.shift()
            // toplevel eof?
            if (stack.length === 0) return cb(null, null, EOF)
            reverse ? stack[0].idx-- : stack[0].idx++
            self.next(cb)
          } else if (status === SKIP) {
            reverse ? stack[0].idx-- : stack[0].idx++
            self.next(cb)
          } else if (typeof element.get === 'function') {
            self.pushcount++
            stack.unshift({obj: element, idx: reverse ? -1 : 0})
            self.next(cb)
          } else { // leaf
            reverse ? stack[0].idx-- : stack[0].idx++
            cb(null, element)
          }
        })
      },
      take: function (nr, cb) {
        var self = this
        var accum = []
        async.forever(function (next) {
          self.next(function (err, value, status) {
            if (err) return cb(null, err)
            if (status === EOF) return cb(null, accum)
            if (!nr--) return cb(null, accum)
            accum.push(value)
            next()
          })
        })
      },
      all: function (cb) {
        this.take(Infinity, cb)
      }
    }
  }

  var Ref = function (pointsto, hash, filters, count) {
    if (typeof count === 'undefined') {
      count = pointsto.count
    } else {
      count = count
    }

    return {
      type: 'Ref',
      filters: filters || (pointsto && pointsto.filter()),
      ref: pointsto,
      count: count,
      hash: hash,
      append: function (el, cb) {
        if (this.ref) {
          this.ref.append(el, function (err, res) {
            if (err) return cb(err)
            cb(null, new Ref(res))
          })
        } else {
          restore(this.hash, function (err, restored) {
            if (err) return cb(err)
            restored.append(el, function (err, res) {
              if (err) return cb(err)
              cb(null, new Ref(res))
            })
          })
        }
      },
      filter: function () {
        return this.filters
      },
      get: function (idx, filter, cb) {
        var self = this
        if (idx === 0 || idx === -1) {
          if (!subsetMatches(self.filters, filter.blooms)) {
            cb(null, null, SKIP)
          } else if (self.ref) {
            return cb(null, self.ref)
          } else {
            restore(self.hash, function (err, res) {
              if (err) return cb(err)
              self.ref = res
              self.get(idx, filter, cb)
            })
          }
        } else {
          cb(null, null, EOF)
        }
      },
      persist: function (cb) {
        var self = this
        if (self.persisted) {
          cb(null, self.persisted, self.filters)
        } else {
          this.ref.persist(function (err, persisted) {
            if (err) return cb(err)
            self.persisted = persisted.Hash
            cb(null, persisted, self.filters)
          })
        }
      }
    }
  }

  var Bucket = function (elements, filters) {
    return {
      type: 'Bucket',
      elements: elements || [],
      count: elements.length,
      append: function (el, cb) {
        if (this.elements.length === BUCKET_SIZE) {
          cb(null, new Finger(new Ref(new Bucket(this.elements)),
                              new Ref(new Branch([])),
                              new Ref(new Bucket([el]))))
        } else {
          var newelements = _.clone(this.elements)
          newelements.push(el)
          cb(null, new Bucket(newelements))
        }
      },
      filter: function () {
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
      get: function (idx, filter, cb) {
        if (idx < 0) {
          idx += this.elements.length
        }
        var el = this.elements[idx]
        if (typeof el === 'undefined') return cb(null, null, EOF)

        if (matches(el, filter.words)) {
          return cb(null, el)
        } else {
          return cb(null, null, SKIP)
        }
      },
      iterator: function (filter) {
        return new Iterator(this, filter)
      },
      reverseIterator: function (filter) {
        return new Iterator(this, filter, true)
      },
      persist: function (cb) {
        var self = this

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
            cb(null, {Hash: put.Hash,
                      Size: stat.CumulativeSize})
          })
        })
      }
    }
  }

  var Branch = function (refs, filters) {
    return {
      type: 'Branch',
      refs: refs,
      count: _.reduce(refs, function (a, b) {
        return a + b.count;
      }, 0),
      append: function (el, cb) {
        if (this.refs.length === BUCKET_SIZE) {
          cb(null, new Finger(new Ref(new Branch(this.refs)),
                              new Ref(new Branch([])),
                              new Ref(new Branch([el]))))
        } else {
          var newrefs = _.clone(this.refs)
          newrefs.push(el)
          cb(null, new Branch(newrefs))
        }
      },
      filter: function () {
        return combine(this.refs)
      },
      get: function (idx, filter, cb) {
        if (idx < 0) {
          idx += this.refs.length
        }
        var ref = this.refs[idx]
        if (ref) {
          cb(null, ref)
        } else {
          cb(null, null, EOF)
        }
      },
      iterator: function (filter) {
        return new Iterator(this, filter)
      },
      reverseIterator: function (filter) {
        return new Iterator(this, filter, true)
      },
      persist: function (cb) {
        var self = this
        var filters = {}
        var counts = {}
        async.series(_.map(self.refs, function (ref, idx) {
          var name = zeropad(idx)
          filters[name] = serialize_filters(self.refs[idx].filters)
          counts[name] = self.refs[idx].count
          return function (done) {
            ref.persist(function (err, persisted) {
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
              cb(null, {Hash: put.Hash,
                        Size: stat.CumulativeSize})
            })
          })
        })
      }
    }
  }

  var Finger = function (head, rest, tail) {
    return {
      type: 'Finger',
      tail: tail,
      rest: rest,
      head: head,
      count: head.count + rest.count + tail.count,
      append: function (el, cb) {
        tail.append(el, function (err, newtail) {
          if (err) return cb(err)
          // did we split the child?
          if (newtail.ref.tail) {
            // yep
            rest.append(newtail.ref.head, function (err, res) {
              if (err) return cb(err)
              cb(null, new Finger(head,
                                  res,
                                  newtail.ref.tail))
            })
          } else {
            // nope
            cb(null, new Finger(head, rest, newtail))
          }
        })
      },
      filter: function () {
        return combine([head, rest, tail])
      },
      get: function (idx, filter, cb) {
        if (idx === 0 || idx === -3) return cb(null, head)
        if (idx === 1 || idx === -2) return cb(null, rest)
        if (idx === 2 || idx === -1) return cb(null, tail)
        cb(null, null, EOF)
      },
      iterator: function (filter) {
        return new Iterator(this, filter)
      },
      reverseIterator: function (filter) {
        return new Iterator(this, filter, true)
      },
      persist: function (cb) {
        var self = this
        var filters = {}
        var counts = {}
        async.series(_.map(['head', 'rest', 'tail'], function (part) {
          filters[part] = serialize_filters(self[part].filters)
          counts[part] = self[part].count
          return function (done) {
            self[part].persist(function (err, persisted) {
              if (err) return done(err)
              done(null, {
                Name: part,
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
              cb(null, {Hash: put.Hash,
                        Size: stat.CumulativeSize})
            })
          })
        })
      }
    }
  }

  var serialize_filters = function (filters) {
    var serialized = {}

    _.forEach(filters, function (value, key) {
      serialized[key] = new Buffer(pako.deflate(filters[key])).toString('base64')
    })
    return serialized
  }

  var deserialize_filters = function (filters) {
    var deserialized = {}
    _.forEach(filters, function (value, key) {
      deserialized[key] = new Buffer(
        pako.inflate(new Buffer(filters[key], 'base64')),
        'base64')
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
      _.forEach(splitWords(value), blooms[key].add)
    })

    return {words: filter,
            blooms: blooms}
  }

  var zeropad = function (nr) {
    var str = ('00' + nr)
    return str.substr(str.length - 3)
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
          return new Ref(null,
                         link.Hash,
                         deserialize_filters(object.filters[zeropad(idx)]),
                         object.counts[zeropad(idx)])
        })))
      } else if (object.type === 'Finger') {
        var linkmap = {}
        _.forEach(res.Links, function (link) {
          linkmap[link.Name] = link.Hash
        })
        cb(null, new Finger(new Ref(null,
                                    linkmap.head,
                                    deserialize_filters(object.filters.head),
                                    object.counts.head),
                            new Ref(null,
                                    linkmap.rest,
                                    deserialize_filters(object.filters.rest),
                                    object.counts.rest),
                            new Ref(null,
                                    linkmap.tail,
                                    deserialize_filters(object.filters.tail),
                                    object.counts.tail)))
      }
    })
  }

  return {
    empty: function () {
      return new Bucket([])
    },
    restore: restore,
    eof: EOF
  }
}
