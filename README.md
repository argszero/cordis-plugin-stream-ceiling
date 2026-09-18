# @argszero/cordis-plugin-stream-ceiling

An absolute bound on one model call that transport keep-alives cannot extend, for
[deepseek-harness](https://github.com/deepseek-ai/deepseek-harness) (`dsh`),
mounted on the public `llm/stream` waterfall.

## The gap

`dsh`'s streaming adapters bound *silence between transport events*
(`streamIdleTimeoutMs`). SSE keep-alive comments count as transport events, so a
provider that keeps commenting without ever sending a terminal event re-arms that
watchdog forever:

- [`util/timeout`](https://github.com/deepseek-ai/deepseek-harness/blob/master/packages/util/timeout/src/index.ts)
  documents `pulse()` as "re-arm from now, it does not accumulate";
- the deepseek adapter passes `() => { watchdog.pulse() }` as `onActivity`;
- `sse.ts` documents that comments "never enter the yielded payload stream".

The result is a call with no upper bound. In
[discussion #6959](https://github.com/deepseek-ai/deepseek-harness/discussions/6959),
a payload that was already complete sat for 10.0 / 17.3 / 17.7 minutes against a
configured `streamIdleTimeoutMs: 300000`, and the failure text named only the
threshold — so a 17-minute wait was reported as `after 300000ms`.

## What this plugin does

A `llm/stream` consumer only ever sees payload chunks, so every bound here is
defined on the payload stream. There is no in-band traffic that can extend it —
the re-arming bug is unreproducible here by construction.

| option | measures | default |
|---|---|---|
| `maxStalledMs` | silence between payload chunks (and time-to-first-chunk) | `600000` |
| `maxCallMs` | the whole call, chunk deliveries included | `0` (off) |
| `payloadGraceMs` | silence **after the payload has closed** (every block ended) while the provider still owes a terminal event | `0` (off) |
| `code` | failure code carried by the terminal `error` finish | `TIMEOUT` |
| `skipPurposes` | auxiliary call purposes exempted from every bound | `[]` |

When one elapses the plugin emits a single terminal `finish` whose message names
the duration **actually measured** beside the bound it breached:

```text
stream-ceiling: no model output for 1062000ms (maxStalledMs 300000ms; 14 chunk(s) received, last "reasoning-delta") — transport keep-alives cannot extend this bound
stream-ceiling: no terminal event for 61500ms after the payload closed (payloadGraceMs 60000ms; 3 chunk(s) received, last "block-end")
stream-ceiling: call exceeded maxCallMs 1000000ms (measured 1002400ms of 1002400ms call time; 812 chunk(s) received, last "text-delta")
```

The provider's own watchdog is left alone: the plugin decides when the *caller*
stops waiting, it never reconfigures an adapter.

## Install

```sh
dsh plugin add @argszero/cordis-plugin-stream-ceiling
```

or mount the bundle patch directly:

```yaml
- insert:
    - id: stream-ceiling
      name: '@argszero/cordis-plugin-stream-ceiling'
```

Tune it from a profile layer:

```yaml
- set:
    - id: stream-ceiling
      config:
        maxStalledMs: 600000
        maxCallMs: 1800000    # optional absolute ceiling (0 = off)
        payloadGraceMs: 60000 # bound the "#6959 shape": payload complete, no terminal event
        code: TIMEOUT
        skipPurposes: [compaction]
```

### Choosing `code`

`TIMEOUT` is in `RetryPolicySchema`'s default `retryableCodes`, so the call is
retried like any other transient failure (at most `maxRetries`, default 5) — a
false trip costs one attempt, not the turn. Set `code: STREAM_CEILING` (or any
code outside that list) to fail the call outright instead; that gives up the
attempt's output but stops a provider that hangs on every attempt.

### Choosing `payloadGraceMs`

This is the shape reported in #6959 and #6594: the answer is complete, and the
adapter waits for an event that never arrives. It is off by default because how
long a provider legitimately pauses *between* blocks is provider behavior, not an
error — a grace well under `maxStalledMs` (a minute, say) turns a seventeen-minute
hang into a retry.

## What it deliberately does not do

A plugin cannot abort a transport it does not own. `GenerateOptions.signal` is a
read-only `AbortSignal` (no `abort()`), and an async generator queues `return()`
behind an outstanding read, so abandoning the upstream iterator cannot interrupt
the provider's parked read either (see `test/ceiling.spec.mjs`, "abandoning the
upstream iterator cannot interrupt its pending read"). The plugin ends the
**call** — which is what the loop, the retry policy and the user observe; the
provider releases its socket when the adapter's own read settles or the request
context is torn down. Bounding the adapter's socket is the adapter's own job.

## Requirements

- `@deepseek-ai/dsh-llm`:

  ```
  >=0.1.2-rc.1 <0.2.0 || >=0.1.3-alpha.2 <0.2.0 || >=0.1.5-alpha.1 <0.2.0 || >=0.1.6-alpha.1 <0.2.0
  ```

  Those four comparators admit exactly eight released versions — `0.1.2-rc.1`,
  `0.1.3-alpha.2`, `0.1.5-alpha.1`, `0.1.5-alpha.2`, `0.1.5-rc.1`, `0.1.5-rc.2`,
  `0.1.6-alpha.1`, `0.1.6-alpha.2` — and each of them has had this suite built and
  run against it with the peer pinned to that single line. The `llm/stream`
  waterfall and its `StreamChunk` grammar are identical across all of them.

  **Write it with one comparator per line, and quote the whole thing.** A semver
  comparator admits a prerelease only when some comparator *in the same group*
  shares that prerelease's `major.minor.patch` tuple, so the upper bound does not
  do what it looks like it does. This all-covering-looking one-liner admits
  **one** version:

  ```jsonc
  // admits only 0.1.2-rc.1 — "<0.2.0" is inert for prereleases, and no other
  // comparator in the group names the 0.1.3 / 0.1.5 / 0.1.6 tuples
  ">=0.1.2-rc.1 <0.2.0"
  ```

  `test/peer-range.spec.mjs` recomputes the admitted set with `semver` and fails
  if it and this section disagree — the quoted range above is asserted against
  the manifest, so the two cannot drift apart.

- `@deepseek-ai/cordis` `^4.0.2`

## Tests

```sh
npm install && npm run build && npm test
```

18 tests: the arithmetic, the wrapper against a driven clock (including that 500
simulated keep-alive pulses cannot re-arm a bound), and an end-to-end run through
a real cordis `Context` and `llm/stream` waterfall. `test/peer-range.spec.mjs`
adds 5 more that guard the peer range and the quoted requirement above.

## License

MIT
