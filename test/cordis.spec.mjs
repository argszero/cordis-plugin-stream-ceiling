/**
 * End-to-end wiring: a real cordis `Context` and a real `llm/stream` waterfall.
 *
 * `ceiling.spec.mjs` drives `wrapStream` directly; this file proves the seam it
 * mounts on — that `apply()` registers on the same event the runtime dispatches,
 * that the listener receives the `(options, next)` signature cordis passes, that
 * our wrapper sits in the chain around the provider call, and that the terminal
 * chunk it yields is what the consumer of `ctx.waterfall` observes.
 */
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { Context } from '@deepseek-ai/cordis'
import { apply, name } from '../lib/index.js'

/** A provider call that delivers one chunk and then never terminates. */
function stalledSource(onReached) {
  return (async function* () {
    onReached()
    yield { type: 'text-delta', index: 0, text: 'hello' }
    await new Promise(() => {})
  })()
}

test('a real cordis waterfall dispatches llm/stream through the ceiling', async () => {
  const ctx = new Context()
  assert.equal(name, 'stream-ceiling')
  apply(ctx, { maxStalledMs: 30 })

  let reached = 0
  const options = { provider: 'p', model: 'm', messages: [] }
  const before = structuredClone(options)
  const stream = ctx.waterfall(null, 'llm/stream', options, () => stalledSource(() => { reached += 1 }))

  const it = stream[Symbol.asyncIterator]()
  assert.deepEqual((await it.next()).value, { type: 'text-delta', index: 0, text: 'hello' }, 'the payload reached the consumer')
  const end = await it.next()
  assert.equal(reached, 1, 'our listener called next() and the provider ran')
  assert.equal(end.value.type, 'finish')
  assert.equal(end.value.reason.kind, 'error')
  assert.equal(end.value.reason.failure.code, 'TIMEOUT')
  assert.match(end.value.reason.failure.message, /no model output for \d+ms \(maxStalledMs 30ms/)
  assert.deepEqual(options, before, 'the plugin never mutates the request it observes')
  await it.return?.(undefined)
})

test('an exempt purpose bypasses the ceiling on the real waterfall', async () => {
  const ctx = new Context()
  apply(ctx, { maxStalledMs: 30, skipPurposes: ['session-title'] })

  const chunks = [{ type: 'text-delta', index: 0, text: 'titled' }, { type: 'finish', reason: { kind: 'stop' } }]
  const source = (async function* () { for (const value of chunks) yield value })()
  const stream = ctx.waterfall(null, 'llm/stream', { provider: 'p', model: 'm', messages: [], purpose: 'session-title' }, () => source)

  const seen = []
  for await (const value of stream) seen.push(value)
  assert.deepEqual(seen, chunks, 'the exempt call keeps its own finish')
})
