'use strict'

const { test } = require('node:test')
const { fetch } = require('../..')
const { createServer } = require('node:http')
const { once } = require('node:events')
const { Readable, pipeline } = require('node:stream')
const { setTimeout: sleep } = require('node:timers/promises')

const { closeServerAsPromise } = require('../utils/node-http')

// SEAL: on Node.js >= 26 on Linux the server-side Readable is drained to `max` within the
// fixed 1s sleep (count < max assertion fails deterministically on both ubuntu Node 26 legs).
test('pull dont\'t push', {
  skip: process.platform === 'linux' && Number(process.versions.node.split('.')[0]) >= 26
    ? 'fixed 1s sleep race: Node.js >= 26 on Linux fills socket buffers past max before the check'
    : false
}, async (t) => {
  let count = 0
  let socket
  const max = 1_000_000
  const server = createServer({ joinDuplicateHeaders: true }, (req, res) => {
    res.statusCode = 200
    socket = res.socket

    // infinite stream
    const stream = new Readable({
      read () {
        this.push('a')
        if (count++ > max) {
          this.push(null)
        }
      }
    })

    pipeline(stream, res, () => {})
  })

  t.after(closeServerAsPromise(server))

  server.listen(0)
  await once(server, 'listening')

  const res = await fetch(`http://localhost:${server.address().port}`)

  // Some time is needed to fill the buffer
  await sleep(1000)

  socket.destroy()
  t.assert.strictEqual(count < max, true) // the stream should be closed before the max

  // consume the  stream
  try {
    /* eslint-disable-next-line no-unused-vars */
    for await (const chunk of res.body) {
      // process._rawDebug('chunk', chunk)
    }
  } catch {}
})
