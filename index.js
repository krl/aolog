'use strict'

// var persistances = 0

var _ = require('lodash')
var bloom = require('blomma')(1024, 1)
var async = require('async')
var pako = require('pako')

var EOF = 0
var SKIP = 1

module.exports = function (ipfs, BUCKET_SIZE) {

  var Iterator = function (over, opts) {
    if (!opts) opts = {}
    var reverse = !!opts.reverse
    var fullfilter = makefilter(opts.filter)
    var def = reverse ? over.count - 1 : 0
    var offset = (typeof opts.offset !== 'undefined' ? opts.offset : def)
    var index = offset

    var stack = [{obj: over}]

    return {
      pushcount: 0,
      next: function (cb) {
        var self = this

        if (stack.length === 0) {
          return cb(null, { eof: true })
        }

        if (typeof stack[0].idx === 'undefined') {
          if (offset !== 'resolved') {
            var idxRest = stack[0].obj.offset(offset)
            stack[0].idx = idxRest[0]
            offset = idxRest[1]
          } else {
            stack[0].idx = reverse ? stack[0].obj.children - 1 : 0
          }
        }

        stack[0].obj.get(stack[0].idx, fullfilter, function (err, res) {
                           if (err) return cb(err)

                           if (res.eof) {
                             stack.shift()
                             if (!stack[0]) return cb(null, { eof: true })
                             reverse ? stack[0].idx-- : stack[0].idx++
                             self.next(cb)
                           } else if (res.skip) {
                             reverse ? stack[0].idx-- : stack[0].idx++
                             reverse ? index -= res.skip : index += res.skip
                             self.next(cb)
                           } else if (res.push) {
                             self.pushcount++
                             stack.unshift({obj: res.push})
                             self.next(cb)
                           } else if (typeof res.element !== 'undefined') {
                             offset = 'resolved'
                             reverse ? stack[0].idx-- : stack[0].idx++
                             cb(null, { element: res.element, index: index })
                             reverse ? index-- : index++
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

  var Ref = function (pointsto, persisted, filters, count) {
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
      children: 1,
      persisted: persisted,
      append: function (el, cb) {
        if (this.ref) {
          this.ref.append(el, function (err, res) {
            if (err) return cb(err)
            cb(null, new Ref(res))
          })
        } else {
          restore(this.persisted.Hash, function (err, restored) {
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
      offset: function (ofs) {
        return [0, ofs]
      },
      get: function (idx, filter, cb) {
        var self = this

        if (idx === 0) {
          if (!subsetMatches(self.filters, filter.blooms)) {
            cb(null, { skip: self.count })
          } else if (self.ref) {
            return cb(null, { push: self.ref })
          } else {
            restore(self.persisted.Hash, function (err, res) {
              if (err) return cb(err)
              self.ref = res
              self.get(idx, filter, cb)
            })
          }
        } else {
          cb(null, { eof: true })
        }
      },
      persist: function (cb) {
        var self = this
        if (self.persisted) {
          cb(null, self.persisted)
        } else {
          this.ref.persist(function (err, persisted) {
            if (err) return cb(err)
            self.persisted = persisted
            cb(null, persisted)
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
      children: elements.length,
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
      offset: function (ofs) {
        return [ofs, 0]
      },
      filter: function () {
        var filter = {}
        _.forEach(this.elements, function (element) {
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
      },
      get: function (idx, filter, cb) {
        var el = this.elements[idx]
        if (typeof el === 'undefined') return cb(null, { eof: true })

        if (matches(el, filter.words)) {
          return cb(null, { element: el })
        } else {
          return cb(null, { skip: 1 })
        }
      },
      iterator: function (opts) {
        return new Iterator(this, opts)
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
            self.persisted = {Hash: put.Hash,
                              Size: stat.CumulativeSize}
            cb(null, self.persisted)
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
        return a + b.count
      }, 0),
      children: refs.length,
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
      offset: function (ofs) {
        var idx = 0
        while (this.refs[(idx + 1)] && this.refs[idx].count <= ofs) {
          ofs -= this.refs[idx].count
          idx++
        }
        return [idx, ofs]
      },
      get: function (idx, filter, cb) {
        var ref = this.refs[idx]
        if (ref) {
          cb(null, { push: ref })
        } else {
          cb(null, { eof: true })
        }
      },
      iterator: function (opts) {
        return new Iterator(this, opts)
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
              self.persisted = { Hash: put.Hash,
                                 Size: stat.CumulativeSize }
              cb(null, self.persisted)
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
      children: 3,
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
      offset: function (ofs) {
        var idx = 0
        if (this.head.count <= ofs) {
          ofs -= this.head.count
          idx++
        } else {
          return [idx, ofs]
        }

        if (this.rest.count <= ofs) {
          ofs -= this.rest.count
          idx++
        } else {
          return [idx, ofs]
        }

        if (this.tail.count <= ofs) {
          ofs -= this.tail.count
          idx++
        }
        return [idx, ofs]
      },
      get: function (idx, filter, cb) {
        if (idx === 0) return cb(null, { push: head })
        if (idx === 1) return cb(null, { push: rest })
        if (idx === 2) return cb(null, { push: tail })
        cb(null, { eof: true})
      },
      iterator: function (opts) {
        return new Iterator(this, opts)
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
              self.persisted = { Hash: put.Hash,
                                 Size: stat.CumulativeSize }
              cb(null, self.persisted)
            })
          })
        })
      }
    }
  }

  var serialize_filters = function (filters) {
    var serialized = {}
    _.forEach(filters, function (value, key) {
      var compressed = new Buffer(pako.deflate(filters[key].buffer)).toString('base64')
      serialized[key] = compressed
    })
    return serialized
  }

  var deserialize_filters = function (filters) {
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
    return string.toLowerCase().split(/[^0-9a-z\u00C0-\u00ff\u00C0-\u024f#@_-]+/)
  }

  var matches = function (element, filter) {
    var matches = true
    _.forEach(filter, function (value, key) {
      if (typeof element[key] !== 'string' ||
          !element[key].toLowerCase().match(value.toLowerCase())) {
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
                         { Hash: link.Hash,
                           Size: link.Size },
                         deserialize_filters(object.filters[zeropad(idx)]),
                         object.counts[zeropad(idx)])
        })))
      } else if (object.type === 'Finger') {
        var linkmap = {}
        _.forEach(res.Links, function (link) {
          linkmap[link.Name] = link
        })
        cb(null, new Finger(new Ref(null,
                                    { Hash: linkmap.head.Hash,
                                      Size: linkmap.head.Size },
                                    deserialize_filters(object.filters.head),
                                    object.counts.head),
                            new Ref(null,
                                    { Hash: linkmap.rest.Hash,
                                      Size: linkmap.rest.Size },
                                    deserialize_filters(object.filters.rest),
                                    object.counts.rest),
                            new Ref(null,
                                    { Hash: linkmap.tail.Hash,
                                      Size: linkmap.tail.Size },
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
