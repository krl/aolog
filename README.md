
# Append only log

An append only log on IPFS with indexing.

## Rationale

A log structure that you can append to, and search through can be useful for many things. Some properties we want these structures to have are:

- [x] Cheap append
- [x] Cheap random access
- [x] References to old versions of the structure stay valid
- [x] Full text-search through the entries
- [x] Keeping only the parts of the structure in memory that you need to look at

## Implementation

The way this is solved is with a [Finger Tree](https://en.wikipedia.org/wiki/Finger_tree), the tree nodes being annotaded with counts and word-filters.

The word filter is a [Bloom filter](https://en.wikipedia.org/wiki/Bloom_filter) made up of the words in the text of the entry. When searching through the tree, any subtree that reports a negative will be thrown away. Since Bloom-filters only give false positives, this will reduce the size of the search conciderably. Additionally, if you specify a search over two keys in the entries (say, author and message text, like the example below) the search size is further reduced, since both filters have to match.

TODO, run the numbers for this.

## API

```js
var ipfs = require('ipfs-api')()
var aolog = require('aolog')(ipfs, 16)
```

### aolog

The main entry point, takes a ipfs api and a bucket size, the bucket size determines how many entries are stored in each leaf.

#### aolog.empty = function ()

Returns an empty Root.

#### aolog.restore = function (hash, cb)

cb: function (err, res)

Pulls a log out of ipfs, and calls the callback with the resulting Root

### Root

#### Root.append = function (entry, cb)

cb: function (err, res)

Appends an entry to the log, callback provides a new log with the extra element.

#### Root.concat = function (entries, cb)

Like append, but for an array of elements

#### Root.persist = function (cb)

Persists the log to IPFS, and returns 
```js
{ Hash: 'OmAC...',
  Size: 43483 }
```

### Root.iterator = function (opts)

Returns an iterator over the sequence.

opts:
```js
{ 
  offset: 49, // where to start iterating
  filter: { body: "lol", from: "jbenet" }, // filter options
  reverse: true // iterate from latest to oldest 
}
```

## Iterator

### Iterator.next = function (cb)

cb: function (err, res)

Gets the next item in the sequence, matching filters if any

Example return value:

```js
{ element: { from: "jbenet", body: "lol" ... }
  index: 399 }
```

### Iterator.take = function (nr, cb) 

Like next, but returns an array of nr elements, or less if collection is exausted

### Iterator.all = function (nr, cb) 

Return all elements matching the query.

## Example

We'll use an aolog of 36084 entries in the #ipfs channel on freenode with the hash ```QmY5ZfWsSBCCaQ8eb42GcKVyMgDeACsLn2AxGRUjzDWS5y```

Say we want to find out exactly where @jbenet has lol'd in these entries, we would do:

```js
var ipfs = require('ipfs-api')()
var aolog = require('aolog')(ipfs, 16)

aolog.restore('QmY5ZfWsSBCCaQ8eb42GcKVyMgDeACsLn2AxGRUjzDWS5y', function (err, res) {
  if (err) throw err

  res.iterator({filter: {from: "jbenet", body: "lol" }}).all(function (err, res) {
    if (err) throw err
    console.log(res)
    console.log('jbenet lol\'d ' + res.length + ' times.')
  })
})
```

->

```
[ ...
  { element: { date: 1429145165000, from: 'jbenet', body: 'lol' },
    index: 15731 },
  { element: { date: 1429240224000, from: 'jbenet', body: 'lol scheduling' },
    index: 16368 },
  { element: { date: 1429318101000, from: 'jbenet', body: 'lol software' },
    index: 17066 },
... ]

jbenet lol'd 28 times.
```
