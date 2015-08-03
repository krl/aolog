var BUCKET_SIZE = 4

var d3 = require('d3')
var _ = require('lodash')
var aolog = require('../log.js')(null, BUCKET_SIZE)

var WIDTH = document.body.scrollWidth
var HEIGHT = document.documentElement.clientHeight

var container = d3.select('#canvas').append('svg')
  .attr('width', WIDTH)
  .attr('height', HEIGHT)
  .attr('transform', 'translate(' + WIDTH / 2 + ', 10)')

container.append('g').attr('id', 'buckets')
container.append('g').attr('id', 'elements')

var ELHEIGHT = 10
var ELWIDTH = 2
var PADDING = 4

var BUCKET_WIDTH = ELWIDTH * BUCKET_SIZE + (PADDING * (BUCKET_SIZE + 1))
var HALF_BUCKET = BUCKET_WIDTH / 2

var ZOOM_DIST = ELWIDTH * 50

var log = aolog.empty()

var State = {bucketCount: 0,
             elementCount: 0}

var draw = function (tree, offsetx, offsety, format) {
  if (!format) format = {i: 0, fingerdepth: 0, depth: 0}
  // sort

  // console.log('draw ' + tree.type)

  if (tree.type === 'Finger') {
    draw(tree.head.ref,
         offsetx - HALF_BUCKET - PADDING * 4,
         offsety,
         _.assign({}, format, {align: 'left'}))

    draw(tree.rest.ref,
         offsetx,
         offsety + (ELHEIGHT + PADDING * 4) * (format.fingerdepth + 1),
         _.assign({}, format, {align: 'middle', fingerdepth: format.fingerdepth + 1}))

    draw(tree.tail.ref,
         offsetx + HALF_BUCKET + PADDING * 4,
         offsety,
         _.assign({}, format, {align: 'right'}))

  }
  if (tree.type === 'Bucket') {
    drawBucket(tree, offsetx, offsety, format)
  }
}

var drawBucket = function (bucket, offsetx, offsety, format) {
  var id = 'bucket' + State.bucketCount++

  // console.log(offsetx, offsety)

  if (format.i !== 'undefined') {

    // console.log('huh')
    // console.log(format.depth)

    var ofs
    if (format.align === 'middle') {
      ofs = ((format.i - BUCKET_SIZE / 2) + 1 / 2) * format.depth
    } else if (format.align === 'left') {
      ofs = (format.i - BUCKET_SIZE + 1) * format.depth
    } else if (format.align === 'right') {
      ofs = (format.i) * format.depth
    } else {
      ofs = 0
    }

    offsetx += ofs * (BUCKET_WIDTH + PADDING)
  }

  var x = offsetx - HALF_BUCKET
  var y = offsety

  // console.log('bucket x,y')
  // console.log(x,y)

  // bucket background

  var el = State[id]

  if (el) {
    // moved?
    if (el.attr('x') !== x ||
        el.attr('y') !== y) {
      el.transition()
        .duration(700)
        .attr('x', x)
        .attr('y', y)
    }
  } else {
    // zoom in new
    el = container.select('#buckets').append('rect')
      .attr('class', 'bucket')
      .attr('opacity', 0)
      .attr('x', x + ZOOM_DIST)
      .attr('y', y)
      .attr('stroke', '#000')
      .attr('fill', '#eae')
      .attr('width', BUCKET_WIDTH)
      .attr('height', ELHEIGHT + PADDING * 2)

    el
      .transition()
      .duration(700)
      .attr('opacity', 1)
      .attr('x', x)
    State[id] = el
  }

  for (var i = 0 ; i < bucket.elements.length ; i++) {
    drawElement(bucket.elements[i],
                offsetx,
                offsety,
                _.assign({}, format, {i: i, depth: format.depth + 1}))
  }
}

var drawElement = function (element, offsetx, offsety, format) {

  // console.log('offsetx, offsety element')
  // console.log(offsetx, offsety)

  var x = offsetx - HALF_BUCKET + PADDING + (format.i * (ELWIDTH + PADDING))
  var y = offsety + PADDING

  var id
  if (element.ref) {
    drawBucket(element.ref,
               offsetx,
               offsety + ELHEIGHT + PADDING * 4,
               format)
  } else {
    id = element

    var el = State[id]

    if (el) {
      // moved?
      if (el.attr('x') !== x ||
          el.attr('y') !== y) {
        el.transition()
          .duration(700)
          .attr('x', x)
          .attr('y', y)
      }

    } else {
      // new
      el = container.select('#elements').append('rect')
        .attr('class', 'element')
        .attr('opacity', 0)
        .attr('x', x + ZOOM_DIST)
        .attr('y', y)
        .attr('stroke', '#000')
        .attr('fill', '#fcf')
        .attr('width', ELWIDTH)
        .attr('height', ELHEIGHT)

      el
        .transition()
        .duration(700)
        .attr('opacity', 1)
        .attr('x', x)

      State[id] = el
    }
  }
}

var i = 0

for (i = 0 ; i < 0 ; i++) {
  log = log.append(i)
}

var count = 5000

draw(log, 0, 0)

var interval = setInterval(function () {
  log = log.append(i++)
  State.bucketCount = 0
  State.elementCount = 0
  draw(log, 0, 0)
  if (!--count) clearInterval(interval)
}, 1000)
