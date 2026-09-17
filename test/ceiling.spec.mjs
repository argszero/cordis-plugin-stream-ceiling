/**
 * Stream ceiling behavior — the dsh discussion #6959 shape.
 *
 * #6959 is a `llm-deepseek` call whose payload (reasoning + tool calls) is
 * complete and whose transport keeps emitting SSE keep-alive comments forever.
 * Because `onActivity` → `watchdog.pulse()` re-arms the idle timer on every
 * comment, `streamIdleTimeoutMs: 300000` produced measured waits of 10.0 / 17.3
 * / 17.7 minutes and no configuration value bounded the call at all.
 *
 * The load-bearing assertions here drive `wrapStream`/`apply` rather than the
 * pure arithmetic: a ceiling that computes the right deadline but never emits
 * its terminal `finish` would leave the call running, and a bound that traffic
 * the stream does not deliver could re-arm would be the same bug it claims to
 * fix. `test/*.mjs` runs against the built `lib/index.js`, so these also pin
 * the shipped artifact.
 */
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { apply, remainingBudget, resolveConfig, stallBound, tripMessage, wrapStream } from '../lib/index.js'

/* -------------------------------------------------------------------------- */
/* fixtures                                                                   */
/* -------------------------------------------------------------------------- */

const chunk = {
  blockStart: (index = 0, blockType = 'text') => ({ type: 'block-start', index, blockType }),
  text: (text, index = 0) => ({ type: 'text-delta', index, text }),
  blockEnd: (index = 0, blockType = 'text') => ({ type: 'block-end', index, block: { type: blockType, text: '' } }),
  stop: () => ({ type: 'finish', reason: { kind: 'stop' } }),
}

/** A clock the test drives, and one-shot timers armed against it. */
function fakeTimers() {
  const clock = { t: 0 }
  const queue = []
  let seq = 0
  return {
    clock,
    schedule(delayMs, fire) {
      const entry = { at: clock.t + delayMs, id: ++seq, fire, cancelled: false }
      queue.push(entry)
      return () => { entry.cancelled = true }
    },
    get armed() { return queue.filter(entry => !entry.cancelled).length },
    due() {
      const live = queue.filter(entry => !entry.cancelled).sort((a, b) => a.at - b.at || a.id - b.id)
      const next = live[0]
      assert.ok(next, 'expected an armed deadline')
      queue.splice(queue.indexOf(next), 1)
      next.fire()
      return next
    },
  }
}

/**
 * A source that delivers `chunks`, then holds its transport open.
 *
 * `hold: 'gate'` models a provider that never terminates while its transport is
 * still alive (the #6959 shape); the test releases the gate at the end.
 * `hold: 'never'` models a read that never settles at all.
 */
function source(chunks, hold = 'end', trailing = []) {
  let release
  const gate = new Promise(resolve => { release = resolve })
  const gen = (async function* () {
    for (const value of chunks) yield value
    if (hold === 'gate') await gate
    else if (hold === 'never') await new Promise(() => {})
    for (const value of trailing) yield value
  })()
  return { gen, release: () => release() }
}

const base = () => resolveConfig({ maxStalledMs: 1000 })

async function collect(iterable) {
  const out = []
  for await (const value of iterable) out.push(value)
  return out
}

/* -------------------------------------------------------------------------- */
/* the arithmetic                                                             */
/* -------------------------------------------------------------------------- */

test('remainingBudget measures payload silence against the governing bound', () => {
  const budget = remainingBudget(1234, 0, 0, base())
  assert.equal(budget.kind, 'stall')
  assert.equal(budget.measuredMs, 1234, 'the measured wait is what the timer sees')
  assert.equal(budget.limitMs, 1000)
  assert.equal(budget.remaining, -234, 'a debt, so the caller trips instead of arming a negative delay')
})

test('maxCallMs governs once it becomes the tighter of the two bounds', () => {
  const config = resolveConfig({ maxStalledMs: 1000, maxCallMs: 5000 })
  const early = remainingBudget(0, 0, 0, config)
  assert.equal(early.kind, 'stall', 'at t=0 the 1000ms silence bound is the tighter one')
  assert.equal(early.remaining, 1000)
  const late = remainingBudget(4500, 0, 4400, config)
  assert.equal(late.kind, 'call', '500ms of call budget beats 600ms of silence budget')
  assert.equal(late.remaining, 500)
  assert.equal(late.measuredMs, 4500, 'the call bound measures the whole call, not the silence')
  assert.equal(remainingBudget(0, 0, 0, resolveConfig({ maxCallMs: 0 })).kind, 'stall', '0 disables the absolute ceiling')
})

test('payloadGraceMs tightens the silence bound only once the payload has closed', () => {
  const config = resolveConfig({ maxStalledMs: 10_000, payloadGraceMs: 500 })
  assert.deepEqual(stallBound(config, 1, false), { kind: 'stall', limitMs: 10_000 }, 'an open block is not a closed payload')
  assert.deepEqual(stallBound(config, 1, true), { kind: 'stall', limitMs: 10_000 }, 'a closed block with another still open is not a closed payload')
  assert.deepEqual(stallBound(config, 0, false), { kind: 'stall', limitMs: 10_000 }, 'nothing has closed yet')
  assert.deepEqual(stallBound(config, 0, true), { kind: 'grace', limitMs: 500 })
  assert.deepEqual(stallBound(resolveConfig({ maxStalledMs: 400, payloadGraceMs: 500 }), 0, true), { kind: 'stall', limitMs: 400 }, 'a grace above maxStalledMs can never loosen the deadline')
  assert.deepEqual(stallBound(resolveConfig({ maxStalledMs: 400 }), 0, true), { kind: 'stall', limitMs: 400 }, '0 disables the grace')
})

test('tripMessage names the measured duration beside the bound it breached', () => {
  const stall = tripMessage({ kind: 'stall', limitMs: 300_000, measuredMs: 1_062_000, callElapsedMs: 1_062_400, chunks: 14, lastChunk: 'reasoning-delta' })
  assert.match(stall, /no model output for 1062000ms/)
  assert.match(stall, /maxStalledMs 300000ms/)
  assert.match(stall, /14 chunk\(s\) received/)
  assert.match(stall, /"reasoning-delta"/)
  assert.match(stall, /keep-alives cannot extend this bound/)
  const grace = tripMessage({ kind: 'grace', limitMs: 60_000, measuredMs: 61_500, callElapsedMs: 90_000, chunks: 3, lastChunk: 'block-end' })
  assert.match(grace, /no terminal event for 61500ms after the payload closed/)
  assert.match(grace, /payloadGraceMs 60000ms/)
  assert.match(tripMessage({ kind: 'call', limitMs: 1000, measuredMs: 1000, callElapsedMs: 1000, chunks: 0 }), /call exceeded maxCallMs 1000ms/)
})

/* -------------------------------------------------------------------------- */
/* the wrapper                                                                */
/* -------------------------------------------------------------------------- */

test('a healthy stream passes through untouched and keeps its own finish', async () => {
  const timers = fakeTimers()
  const chunks = [chunk.blockStart(), chunk.text('hello'), chunk.blockEnd(), chunk.stop()]
  const src = source(chunks)
  const observed = []
  const stream = wrapStream({}, () => src.gen, base(), {
    now: () => timers.clock.t,
    schedule: timers.schedule,
    onTrip: report => observed.push(report),
  })
  const it = stream[Symbol.asyncIterator]()
  for (const expected of chunks) {
    const next = await it.next()
    assert.equal(next.done, false)
    assert.deepEqual(next.value, expected)
    assert.equal(timers.armed, 0, 'a chunk that arrives cancels its deadline instead of leaving it armed')
  }
  assert.deepEqual(observed, [], 'a call that keeps producing is never a trip')
  assert.equal((await it.next()).done, true, 'the source ended the call')
})

test('a stalled call is ended at the ceiling with the duration actually measured', async () => {
  const timers = fakeTimers()
  const src = source([chunk.blockStart(), chunk.text('partial')], 'gate', [chunk.text(' never delivered')])
  const observed = []
  const stream = wrapStream({}, () => src.gen, base(), {
    now: () => timers.clock.t,
    schedule: timers.schedule,
    onTrip: report => observed.push(report),
  })
  const it = stream[Symbol.asyncIterator]()
  assert.deepEqual((await it.next()).value, chunk.blockStart())
  assert.deepEqual((await it.next()).value, chunk.text('partial'))

  const pending = it.next()
  assert.equal(timers.armed, 1, 'the stalled read is bounded by exactly one deadline')
  timers.clock.t = 1234 // the deadline fires late, as a real timer would
  const fired = timers.due()
  assert.equal(fired.at, 1000, 'the deadline was armed 1000ms after the last payload chunk')
  const end = await pending

  assert.equal(end.done, false, 'the call ends with a terminal chunk rather than a bare return')
  assert.equal(end.value.type, 'finish')
  assert.equal(end.value.reason.kind, 'error', 'an open block is legal only under error/aborted')
  assert.equal(end.value.reason.failure.code, 'TIMEOUT', 'the default code is the harness\'s own retryable classification')
  assert.match(end.value.reason.failure.message, /no model output for 1234ms/)
  assert.match(end.value.reason.failure.message, /maxStalledMs 1000ms/)
  assert.equal(observed.length, 1)
  assert.equal(observed[0].measuredMs, 1234, 'the trip report carries the measured wait, not the threshold')
  assert.equal(observed[0].chunks, 2)
  assert.equal(observed[0].lastChunk, 'text-delta')

  src.release() // the provider would deliver more payload now
  assert.equal((await it.next()).done, true, 'nothing follows the terminal finish, even after the source resumes')
})

test('keep-alive traffic the stream does not deliver cannot re-arm the bound', async () => {
  // The transport layer of #6959 pulses a watchdog on every SSE comment line,
  // and comments never enter the payload stream. Any number of those pulses must
  // leave this plugin's bound exactly where it was.
  const timers = fakeTimers()
  let pulses = 0
  const pulse = () => { pulses += 1 } // what `onActivity: () => watchdog.pulse()` would call
  const src = source([chunk.text('first')], 'never')
  const stream = wrapStream({}, () => src.gen, base(), { now: () => timers.clock.t, schedule: timers.schedule })
  const it = stream[Symbol.asyncIterator]()
  await it.next()

  const pending = it.next()
  for (let i = 0; i < 500; i++) {
    pulse()
    assert.equal(timers.armed, 1, `pulse #${i} must not arm, cancel, or reset a deadline`)
  }
  assert.equal(pulses, 500)
  timers.clock.t = 1000
  timers.due()
  const end = await pending
  assert.equal(end.value.type, 'finish', '500 keep-alives did not buy the call any time')
  assert.equal(end.value.reason.kind, 'error')
})

test('maxCallMs bounds a call that keeps producing but never terminates', async () => {
  const timers = fakeTimers()
  const config = resolveConfig({ maxStalledMs: 100_000, maxCallMs: 1000 })
  const src = source([chunk.text('a'), chunk.text('b'), chunk.text('c')], 'gate', [chunk.text('d')])
  const observed = []
  const stream = wrapStream({}, () => src.gen, config, {
    now: () => timers.clock.t,
    schedule: timers.schedule,
    onTrip: report => observed.push(report),
  })
  const it = stream[Symbol.asyncIterator]()
  assert.equal((await it.next()).value.text, 'a')
  timers.clock.t = 400
  assert.equal((await it.next()).value.text, 'b')
  timers.clock.t = 800
  assert.equal((await it.next()).value.text, 'c', 'an active call is bounded by time, not by silence')
  const pending = it.next()
  assert.equal(timers.armed, 1)
  const fired = timers.due()
  assert.equal(fired.at, 1000, 'only the remaining 200ms of call budget was armed')
  timers.clock.t = 1000 // a real timer fires at its due time
  const end = await pending
  assert.equal(end.value.type, 'finish')
  assert.equal(observed[0].kind, 'call')
  assert.equal(observed[0].measuredMs, 1000)
  assert.match(end.value.reason.failure.message, /call exceeded maxCallMs 1000ms/)
  src.release()
  assert.equal((await it.next()).done, true)
})

test('payloadGraceMs bounds the #6959 shape: a complete payload and no terminal event', async () => {
  const timers = fakeTimers()
  const config = resolveConfig({ maxStalledMs: 600_000, payloadGraceMs: 100 })
  const src = source([chunk.blockStart(0, 'reasoning'), chunk.blockEnd(0, 'reasoning'), chunk.blockStart(1, 'tool-call'), chunk.blockEnd(1, 'tool-call')], 'gate')
  const observed = []
  const stream = wrapStream({}, () => src.gen, config, {
    now: () => timers.clock.t,
    schedule: timers.schedule,
    onTrip: report => observed.push(report),
  })
  const it = stream[Symbol.asyncIterator]()
  await collectHead(it, 4) // the whole payload: two blocks opened and closed
  const pending = it.next()
  assert.equal(timers.armed, 1)
  const fired = timers.due()
  assert.equal(fired.at, 100, 'the grace, not the 10-minute stall ceiling, bounded this wait')
  timers.clock.t = 100
  const end = await pending
  assert.equal(observed[0].kind, 'grace', 'the payload had closed: every block ended')
  assert.equal(observed[0].limitMs, 100)
  assert.match(end.value.reason.failure.message, /no terminal event for 100ms after the payload closed/)
  src.release()
})

test('the grace waits for the payload to close', async () => {
  const timers = fakeTimers()
  // One block stays open, so the ordinary stall ceiling governs and the short
  // grace must not fire on a provider that is still mid-payload.
  const config = resolveConfig({ maxStalledMs: 1000, payloadGraceMs: 10 })
  const src = source([chunk.blockStart(0, 'text'), chunk.text('still writing')], 'never')
  const observed = []
  const stream = wrapStream({}, () => src.gen, config, {
    now: () => timers.clock.t,
    schedule: timers.schedule,
    onTrip: report => observed.push(report),
  })
  const it = stream[Symbol.asyncIterator]()
  await collectHead(it, 2)
  const pending = it.next()
  assert.equal(timers.due().at, 1000, 'the open block kept the full stall ceiling in force')
  const end = await pending
  assert.equal(observed[0].kind, 'stall')
  assert.equal(end.value.reason.failure.code, 'TIMEOUT')
})

test('a code core does not retry can be selected', async () => {
  const timers = fakeTimers()
  const src = source([], 'never')
  const stream = wrapStream({}, () => src.gen, resolveConfig({ maxStalledMs: 10, code: 'STREAM_CEILING' }), {
    now: () => timers.clock.t,
    schedule: timers.schedule,
  })
  const it = stream[Symbol.asyncIterator]()
  const pending = it.next()
  timers.due()
  assert.equal((await pending).value.reason.failure.code, 'STREAM_CEILING')
})

test('a provider failure still propagates unchanged', async () => {
  const boom = new Error('transport exploded')
  const src = (async function* () { yield chunk.text('a'); throw boom })()
  const stream = wrapStream({}, () => src, base(), { now: () => 0, schedule: () => () => {} })
  const it = stream[Symbol.asyncIterator]()
  assert.equal((await it.next()).value.text, 'a')
  await assert.rejects(it.next(), error => error === boom)
})

test('abandoning the upstream iterator cannot interrupt its pending read', async () => {
  // Documented limitation: `options.signal` is a read-only AbortSignal, and an
  // async generator queues `return()` behind an outstanding read. The plugin
  // ends the *call*; the provider releases the socket when its own read settles
  // or the request context is torn down.
  const timers = fakeTimers()
  let cleanedUp = false
  let openGate
  const gate = new Promise(resolve => { openGate = resolve })
  const src = (async function* () {
    try {
      yield chunk.text('a')
      await gate
    } finally {
      cleanedUp = true
    }
  })()
  const stream = wrapStream({}, () => src, resolveConfig({ maxStalledMs: 10 }), {
    now: () => timers.clock.t,
    schedule: timers.schedule,
  })
  const it = stream[Symbol.asyncIterator]()
  await it.next()
  const pending = it.next()
  timers.due()
  assert.equal((await pending).value.type, 'finish', 'the caller is released immediately')
  await tick()
  assert.equal(cleanedUp, false, 'the parked read is not interruptible from the consumer side')
  openGate()
  await tick()
  assert.equal(cleanedUp, true, 'the provider cleans up as soon as its own read settles')
})

async function collectHead(iterator, count) {
  const out = []
  for (let i = 0; i < count; i++) out.push((await iterator.next()).value)
  return out
}

const tick = () => new Promise(resolve => { setImmediate(resolve) })

/* -------------------------------------------------------------------------- */
/* plugin wiring                                                              */
/* -------------------------------------------------------------------------- */

/** The listener `apply()` registers, plus the log calls it makes. */
function fakeContext() {
  const warnings = []
  let listener
  return {
    ctx: {
      on(name, registered) {
        assert.equal(name, 'llm/stream')
        listener = registered
      },
      logger: { warn: message => warnings.push(message) },
    },
    warnings,
    listener: (...args) => listener(...args),
  }
}

test('skipPurposes delegates an exempt call instead of wrapping it', async () => {
  const { ctx, listener } = fakeContext()
  apply(ctx, { maxStalledMs: 20, skipPurposes: ['compaction'] })
  const raw = (async function* () { yield chunk.stop() })()
  assert.equal(listener({ purpose: 'compaction' }, () => raw), raw, 'the exempt call returns the upstream iterable itself')
  assert.deepEqual(await collect(raw), [chunk.stop()], 'unwrapped, so its finish is the provider\'s own')
})

test('apply() bounds an unexempted call and logs the trip', async () => {
  const { ctx, warnings, listener } = fakeContext()
  apply(ctx, { maxStalledMs: 20 })
  const src = source([chunk.text('x')], 'never')
  const it = listener({ purpose: 'session-title' }, () => src.gen)[Symbol.asyncIterator]()
  await it.next()
  const end = await it.next() // real timers: the plugin default scheduler is in play
  assert.equal(end.value.type, 'finish')
  assert.equal(end.value.reason.kind, 'error')
  assert.equal(warnings.length, 1, 'the trip is logged for the operator')
  assert.match(warnings[0], /stream-ceiling: no model output for/)
})

test('a merely slow call is not tripped', async () => {
  assert.equal(resolveConfig({}).maxStalledMs, 600_000, 'a bare mount waits ten minutes of payload silence')
  const { ctx, warnings, listener } = fakeContext()
  apply(ctx, { maxStalledMs: 200 })
  const src = source([chunk.text('x')], 'never')
  const it = listener({}, () => src.gen)[Symbol.asyncIterator]()
  await it.next()
  const raced = await Promise.race([
    it.next().then(() => 'tripped'),
    new Promise(resolve => { setTimeout(() => resolve('still-waiting'), 60) }),
  ])
  assert.equal(raced, 'still-waiting', 'a 200ms bound must not fire in 60ms of silence')
  assert.deepEqual(warnings, [])
})
