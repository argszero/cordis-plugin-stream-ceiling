/**
 * Stream ceiling — a bound on one model call that transport keep-alives cannot
 * extend, mounted on the public `llm/stream` waterfall.
 *
 * ## The gap this closes (dsh discussion #6959)
 *
 * `llm-deepseek`'s idle watchdog is re-armed by *transport* activity: every SSE
 * comment (keep-alive) line calls `onActivity` → `watchdog.pulse()`, and
 * `packages/util/timeout` documents `pulse()` as "re-arm from now, it does not
 * accumulate". Keep-alive comments never enter the yielded payload stream
 * (`sse.ts`: "comments never enter the yielded payload stream"), so the
 * configured `streamIdleTimeoutMs` bounds **keep-alive silence**, not the call.
 *
 * A provider that keeps commenting after a complete payload holds one attempt
 * open indefinitely; no configuration value bounds it. #6959 measured 10.0 /
 * 17.3 / 17.7 minutes of silence against a configured 300000ms, and the failure
 * text named only the threshold, contradicting the wait it reported.
 *
 * ## What this plugin measures instead
 *
 * A `llm/stream` consumer only ever sees payload chunks. This plugin times the
 * gap between *the chunks it actually receives*, plus two further bounds, and
 * ends the call when one elapses. Because every bound is defined on the payload
 * stream, there is no in-band traffic that can extend it — the re-arming bug
 * cannot be reproduced here by construction.
 *
 * | bound | measures | default |
 * |---|---|---|
 * | `maxStalledMs` | silence between payload chunks | 600000 |
 * | `maxCallMs` | the whole call, deliveries included | off |
 * | `payloadGraceMs` | silence *after the payload has closed* | off |
 *
 * The terminal chunk names the duration **actually measured** beside the bound
 * it breached, which is the second half of what #6959 asks for.
 *
 * ## What it deliberately does not do
 *
 * A plugin cannot abort a transport it does not own. Abandoning the upstream
 * iterator is queued behind the stalled read inside the provider's async
 * generator (see `test/ceiling.spec.mjs`, "abandoning the upstream iterator
 * cannot interrupt its pending read"), so the socket is released when the
 * provider's own read settles or the request context is torn down. The plugin
 * ends the *call*, which is what the loop, the retry policy and the user
 * observe; closing the provider's socket is the adapter's own concern
 * (`options.signal` is a read-only `AbortSignal` — it exposes no `abort()`).
 *
 * @module @argszero/cordis-plugin-stream-ceiling
 */

import z from '@deepseek-ai/schemastery'
import type { Context } from '@deepseek-ai/cordis'
import type { GenerateOptions, LlmFailure, StreamChunk } from '@deepseek-ai/dsh-llm'

/** Largest delay Node schedules without clamping it to one millisecond. */
const MAX_TIMER_DELAY_MS = 2_147_483_647

/** Auxiliary call classifications that can be exempted by `skipPurposes`. */
export type AuxPurpose = 'compaction' | 'session-title'

/** Which bound a trip breached. */
export type BoundKind =
  /** Silence between payload chunks exceeded `maxStalledMs`. */
  | 'stall'
  /** The payload has closed and no terminal event followed within `payloadGraceMs`. */
  | 'grace'
  /** The whole call exceeded `maxCallMs`. */
  | 'call'

/** One ceiling breach. */
export interface TripReport {
  /** Which bound elapsed. */
  readonly kind: BoundKind
  /** The configured bound, in milliseconds. */
  readonly limitMs: number
  /** The duration actually measured against that bound, in milliseconds. */
  readonly measuredMs: number
  /** Total call duration at the moment of the trip, in milliseconds. */
  readonly callElapsedMs: number
  /** Payload chunks received before the trip. */
  readonly chunks: number
  /** Type of the last payload chunk received, when there was one. */
  readonly lastChunk?: string
}

/** Plugin configuration. */
export interface Config {
  /**
   * Ceiling on the gap between payload chunks the stream actually delivers
   * (and, before the first chunk, on time-to-first-chunk). Transport keep-alive
   * lines are invisible here, so they cannot extend it. Default `600000` (10
   * minutes): twice the harness's own `streamIdleTimeoutMs` default.
   */
  maxStalledMs?: number
  /**
   * Absolute ceiling on one call, chunk deliveries included. `0` disables it.
   * Default `0`.
   */
  maxCallMs?: number
  /**
   * Tighter ceiling applied **only once the payload has closed** — every content
   * block ended — while the provider still owes a terminal event. This is the
   * shape #6959 and #6594 report: the answer is complete, and the adapter waits
   * for an event that never comes. `0` disables it. Default `0`, because how
   * long a provider legitimately pauses before its next block is provider
   * behavior, not an error; set it (e.g. `60000`) to bound that shape too.
   */
  payloadGraceMs?: number
  /**
   * Failure code carried by the terminal finish. Default `'TIMEOUT'`, which
   * `RetryPolicySchema`'s default `retryableCodes` retries — the same
   * classification the harness's own idle watchdog produces. Set it to a code
   * outside that list (e.g. `'STREAM_CEILING'`) to fail the call without
   * retrying, at the cost of losing the attempt's output.
   */
  code?: string
  /**
   * Auxiliary call purposes exempted from every bound; ordinary conversation
   * requests have no purpose and cannot be exempted. Default `[]`.
   */
  skipPurposes?: AuxPurpose[]
}

/** Configuration with every default materialized. */
export type ResolvedConfig = Required<Config>

export const Config: z<Config> = z.object({
  maxStalledMs: z.number().step(1).min(1).max(MAX_TIMER_DELAY_MS).default(600_000),
  maxCallMs: z.number().step(1).min(0).max(MAX_TIMER_DELAY_MS).default(0),
  payloadGraceMs: z.number().step(1).min(0).max(MAX_TIMER_DELAY_MS).default(0),
  code: z.string().default('TIMEOUT'),
  skipPurposes: z.array(z.union(['compaction', 'session-title'])).default([]),
})

/** The plugin name used by the mount patch. */
export const name = 'stream-ceiling'

/** Fill every default so the wrapper can be driven without Cordis. */
export function resolveConfig(config: Config = {}): ResolvedConfig {
  return {
    maxStalledMs: config.maxStalledMs ?? 600_000,
    maxCallMs: config.maxCallMs ?? 0,
    payloadGraceMs: config.payloadGraceMs ?? 0,
    code: config.code ?? 'TIMEOUT',
    skipPurposes: config.skipPurposes ?? [],
  }
}

/** The silence bound currently governing a wait. */
export interface StallBound {
  /** `'grace'` when `payloadGraceMs` is the tighter of the two silence bounds. */
  readonly kind: 'stall' | 'grace'
  /** The governing limit, in milliseconds. */
  readonly limitMs: number
}

/**
 * Which silence bound applies to the current wait.
 *
 * `payloadGraceMs` tightens the bound only when it is enabled, when the payload
 * has closed (a block ended and none is open), and when it is genuinely the
 * tighter of the two — so enabling it can never loosen `maxStalledMs`.
 *
 * @param config - resolved configuration.
 * @param openBlocks - content blocks started but not yet ended.
 * @param sawBlockEnd - whether any block has ended during this call.
 * @returns the governing silence bound.
 */
export function stallBound(config: ResolvedConfig, openBlocks: number, sawBlockEnd: boolean): StallBound {
  const grace = config.payloadGraceMs
  if (grace > 0 && grace < config.maxStalledMs && openBlocks === 0 && sawBlockEnd) {
    return { kind: 'grace', limitMs: grace }
  }
  return { kind: 'stall', limitMs: config.maxStalledMs }
}

/**
 * Which bound governs the current wait, and how much of it is left.
 *
 * Pure arithmetic so the trip decisions are testable without a scheduler: the
 * caller only turns `remaining` into a timer.
 *
 * @param nowMs - current clock reading.
 * @param startedAt - when the call began.
 * @param lastChunkAt - when the last payload chunk arrived (equal to `startedAt` before the first).
 * @param config - resolved configuration.
 * @param stall - the governing silence bound; defaults to `maxStalledMs`.
 * @returns the governing bound and its remaining budget (`<= 0` means trip now).
 */
export function remainingBudget(
  nowMs: number,
  startedAt: number,
  lastChunkAt: number,
  config: ResolvedConfig,
  stall: StallBound = { kind: 'stall', limitMs: config.maxStalledMs },
): { kind: BoundKind; limitMs: number; measuredMs: number; remaining: number } {
  const stallMeasured = nowMs - lastChunkAt
  const stallLeft = stall.limitMs - stallMeasured
  if (config.maxCallMs <= 0) {
    return { kind: stall.kind, limitMs: stall.limitMs, measuredMs: stallMeasured, remaining: stallLeft }
  }
  const callMeasured = nowMs - startedAt
  const callLeft = config.maxCallMs - callMeasured
  return callLeft <= stallLeft
    ? { kind: 'call', limitMs: config.maxCallMs, measuredMs: callMeasured, remaining: callLeft }
    : { kind: stall.kind, limitMs: stall.limitMs, measuredMs: stallMeasured, remaining: stallLeft }
}

/**
 * The operator-facing text for one trip. Names the measured duration beside the
 * configured bound — the failure text in #6959 named only the threshold, so a
 * 17.7-minute wait was reported as `after 300000ms`.
 *
 * @param report - the trip.
 * @returns one line fit for a log or an error message.
 */
export function tripMessage(report: TripReport): string {
  const arrived = `${report.chunks} chunk(s) received${report.lastChunk === undefined ? ', none with content' : `, last "${report.lastChunk}"`}`
  if (report.kind === 'call') {
    return `stream-ceiling: call exceeded maxCallMs ${report.limitMs}ms (measured ${report.measuredMs}ms of ${report.callElapsedMs}ms call time; ${arrived})`
  }
  if (report.kind === 'grace') {
    return `stream-ceiling: no terminal event for ${report.measuredMs}ms after the payload closed (payloadGraceMs ${report.limitMs}ms; ${arrived})`
  }
  return `stream-ceiling: no model output for ${report.measuredMs}ms (maxStalledMs ${report.limitMs}ms; ${arrived}) — transport keep-alives cannot extend this bound`
}

/** Build the protocol-legal terminal chunk for one trip. */
export function ceilingFinish(report: TripReport, code: string): StreamChunk {
  const failure: LlmFailure = { message: tripMessage(report), code }
  return { type: 'finish', reason: { kind: 'error', failure } }
}

/** Injectable clock and scheduler, so tests need no wall-clock waiting. */
export interface CeilingHooks {
  /** Monotonic clock in milliseconds. Defaults to `Date.now`. */
  readonly now?: () => number
  /** Arm a one-shot deadline; returns its canceller. Defaults to `setTimeout`. */
  readonly schedule?: (delayMs: number, fire: () => void) => () => void
  /** Observe a trip (used by `apply` to log it). */
  readonly onTrip?: (report: TripReport) => void
}

function defaultSchedule(delayMs: number, fire: () => void): () => void {
  const handle = setTimeout(fire, delayMs)
  return () => { clearTimeout(handle) }
}

/**
 * Wrap one `llm/stream` call with the ceiling.
 *
 * Chunks pass through untouched and in order. On a trip the wrapper emits one
 * terminal `error` finish, abandons the upstream iterator, and returns; the
 * upstream release is fire-and-forget on purpose (awaiting it would re-create
 * the hang this exists to bound).
 *
 * @param options - the call being streamed; only `purpose` is read.
 * @param next - the rest of the `llm/stream` chain.
 * @param config - resolved configuration.
 * @param hooks - injectable clock/scheduler/trip observer.
 * @returns the bounded chunk stream.
 */
export function wrapStream(
  options: GenerateOptions,
  next: () => AsyncIterable<StreamChunk>,
  config: ResolvedConfig,
  hooks: CeilingHooks = {},
): AsyncIterable<StreamChunk> {
  const now = hooks.now ?? (() => Date.now())
  const schedule = hooks.schedule ?? defaultSchedule

  return (async function* ceiling(): AsyncIterable<StreamChunk> {
    const startedAt = now()
    let lastChunkAt = startedAt
    let chunks = 0
    let lastChunk: string | undefined
    // The block grammar is the only signal a consumer has that the payload
    // closed; it mirrors `dsh-llm`'s own stream invariant.
    const open = new Set<number>()
    let sawBlockEnd = false
    const iterator = next()[Symbol.asyncIterator]()
    let released = false

    // Best effort, never awaited: an async generator queues `return()` behind an
    // outstanding `next()`, so blocking on it here would hand the caller the
    // exact unbounded wait the ceiling just refused. Whichever settles first —
    // the abandoned read or the request teardown — runs the provider's cleanup.
    const release = (): void => {
      if (released) return
      released = true
      void Promise.resolve(iterator.return?.()).catch(() => {})
    }

    const report = (kind: BoundKind, limitMs: number, measuredMs: number): TripReport => ({
      kind,
      limitMs,
      measuredMs,
      callElapsedMs: now() - startedAt,
      chunks,
      ...lastChunk === undefined ? {} : { lastChunk },
    })

    try {
      for (;;) {
        const stall = stallBound(config, open.size, sawBlockEnd)
        const budget = remainingBudget(now(), startedAt, lastChunkAt, config, stall)
        if (budget.remaining <= 0) {
          const tripped = report(budget.kind, budget.limitMs, budget.measuredMs)
          hooks.onTrip?.(tripped)
          yield ceilingFinish(tripped, config.code)
          return
        }
        const settled = await raceDeadline(iterator.next(), budget.remaining, schedule)
        if (settled.kind === 'deadline') {
          // Report the duration actually observed at the deadline, not the
          // configured threshold: #6959's first complaint is a failure text
          // ("after 300000ms") that contradicts the wait it measured.
          const measuredMs = budget.kind === 'call' ? now() - startedAt : now() - lastChunkAt
          const tripped = report(budget.kind, budget.limitMs, measuredMs)
          hooks.onTrip?.(tripped)
          yield ceilingFinish(tripped, config.code)
          return
        }
        if (settled.result.done === true) return
        const chunk = settled.result.value
        chunks += 1
        lastChunkAt = now()
        lastChunk = chunk.type
        if (chunk.type === 'block-start') open.add(chunk.index)
        else if (chunk.type === 'block-end') { open.delete(chunk.index); sawBlockEnd = true }
        yield chunk
      }
    } finally {
      release()
    }
  })()
}

type Settled = { kind: 'value'; result: IteratorResult<StreamChunk> } | { kind: 'deadline' }

/**
 * Await one chunk against a deadline.
 *
 * A late rejection from the abandoned read is already handled by the race's own
 * reject callback, so it can never surface as an unhandled rejection.
 */
function raceDeadline(
  pending: Promise<IteratorResult<StreamChunk>>,
  budgetMs: number,
  schedule: (delayMs: number, fire: () => void) => () => void,
): Promise<Settled> {
  let cancel: (() => void) | undefined
  const deadline = new Promise<Settled>((resolve) => {
    cancel = schedule(budgetMs, () => { resolve({ kind: 'deadline' }) })
  })
  const value = pending.then((result): Settled => ({ kind: 'value', result }))
  return Promise.race([value, deadline]).finally(() => { cancel?.() })
}

/**
 * Register the ceiling on every streaming model call.
 *
 * @param ctx - Cordis context.
 * @param config - raw plugin configuration (Cordis applies the schema defaults).
 */
export function apply(ctx: Context, config: Config): void {
  const resolved = resolveConfig(config)
  ctx.on('llm/stream', (options: GenerateOptions, next: () => AsyncIterable<StreamChunk>): AsyncIterable<StreamChunk> => {
    if (options.purpose !== undefined && resolved.skipPurposes.includes(options.purpose)) return next()
    return wrapStream(options, next, resolved, {
      onTrip: (report) => { ctx.logger.warn(tripMessage(report)) },
    })
  })
}
