import { test } from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { satisfies } from 'semver'

const pkg = JSON.parse(readFileSync(new URL('../package.json', import.meta.url), 'utf8'))
const readme = readFileSync(new URL('../README.md', import.meta.url), 'utf8')

/**
 * Every `@deepseek-ai/dsh-llm` version published as of 2026-09-18, oldest first;
 * `npm view @deepseek-ai/dsh-llm versions` refreshes it.
 *
 * The list is deliberately frozen: it is a record of what the range was checked
 * against, not a live query. A version published later is not covered by this
 * test — that is what the release checklist is for.
 */
const PUBLISHED = [
  '0.0.1-rc.1', '0.0.1-rc.2', '0.0.1-rc.3', '0.0.1-rc.5',
  '0.1.0-rc.2', '0.1.0-rc.3', '0.1.0-rc.6', '0.1.0-rc.7', '0.1.0-rc.8',
  '0.1.1-rc.1', '0.1.1-rc.2',
  '0.1.2-alpha.2', '0.1.2-alpha.3', '0.1.2-alpha.4', '0.1.2-alpha.5',
  '0.1.2-rc.1',
  '0.1.3-alpha.2',
  '0.1.5-alpha.1', '0.1.5-alpha.2', '0.1.5-rc.1', '0.1.5-rc.2',
  '0.1.6-alpha.1', '0.1.6-alpha.2',
]

/**
 * The versions this plugin is expected to install against. Each one has had the
 * suite built and run against it with `@deepseek-ai/dsh-llm` pinned to that
 * single line (18/18 green on all eight, 2026-09-18).
 */
const SUPPORTED = [
  '0.1.2-rc.1',
  '0.1.3-alpha.2',
  '0.1.5-alpha.1', '0.1.5-alpha.2', '0.1.5-rc.1', '0.1.5-rc.2',
  '0.1.6-alpha.1', '0.1.6-alpha.2',
]

const RANGE = () => pkg.peerDependencies['@deepseek-ai/dsh-llm']

/**
 * Guard the peer range by *computing* admission, not by pattern-matching it.
 *
 * A test that asserts the range string contains the substrings it should contain
 * cannot tell a correct range from an incorrect one — it fails only on a
 * *different-looking* string, so it certifies whatever is there. The two shapes
 * that have actually shipped in this plugin family are:
 *
 *   ">=0.1.2-rc.1 <0.2.0"                                    -> admits 1 version
 *   ">=0.1.2-rc.1 <0.2.0 || >=0.1.5-alpha.1 <0.2.0"          -> admits 5, drops 0.1.3/0.1.6
 *
 * A semver comparator admits a prerelease only when some comparator in the same
 * group shares that prerelease's major.minor.patch tuple, so `<0.2.0` is inert
 * for prereleases and the tuple of each comparator is what does the admitting.
 * This range avoids both mistakes by naming one tuple per comparator.
 *
 * Asserting the admitted set *exactly* makes either drift loud: a line quietly
 * dropped (users on it get ERESOLVE for a plugin that works there) and a line
 * quietly admitted without a run (users on it get a silent breakage instead of a
 * loud refusal).
 */
test('the peer range admits exactly the tested dsh-llm lines', () => {
  const range = RANGE()
  assert.ok(range, 'the dsh-llm peer dependency must be declared')

  const admitted = PUBLISHED.filter((v) => satisfies(v, range))
  assert.deepEqual(
    admitted,
    SUPPORTED,
    `the range "${range}" admits [${admitted.join(', ')}] but the suite has only been ` +
      `run against [${SUPPORTED.join(', ')}]`,
  )
})

test('older, untested prereleases keep getting a loud ERESOLVE', () => {
  const refused = PUBLISHED.filter((v) => !satisfies(v, RANGE()))
  assert.deepEqual(refused, PUBLISHED.filter((v) => !SUPPORTED.includes(v)))
})

test('one comparator per tuple: the inert upper bound is not the only thing', () => {
  // Pins the *reason* the union exists, so a later "simplification" fails here
  // with the explanation attached.
  assert.equal(satisfies('0.1.2-rc.1', '>=0.1.2-rc.1 <0.2.0'), true)
  for (const line of ['0.1.3-alpha.2', '0.1.5-alpha.1', '0.1.6-alpha.2']) {
    assert.equal(
      satisfies(line, '>=0.1.2-rc.1 <0.2.0'),
      false,
      `${line} must NOT be admitted by the single-comparator form`,
    )
    assert.equal(satisfies(line, RANGE()), true, `${line} must be admitted by the shipped range`)
  }
})

test('the README quotes the shipped range verbatim', () => {
  // The documentation is part of the claim. This plugin's README used to state
  // `>=0.1.2-rc.1 <0.2.0` — the first comparator of the union, on its own — for a
  // manifest whose union admits eight versions. Another author copying that line
  // reproduces the broken range, so the quoted text is asserted, not trusted.
  assert.ok(
    readme.includes(RANGE()),
    'README must quote the full peer range verbatim (see the Requirements section)',
  )
})

test('the dev pin stays on a line the range admits', () => {
  // A dev pin outside the peer range means the suite ran against a line the
  // published package refuses — the mismatch this file exists to prevent.
  const pin = pkg.devDependencies['@deepseek-ai/dsh-llm']
  assert.ok(SUPPORTED.includes(pin), `devDependency "@deepseek-ai/dsh-llm": "${pin}" is not tested`)
})
