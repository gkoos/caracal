/**
 * Property tests for the cached-verdict circuit breaker.
 *
 * The generated histories mix clock advances, out-of-band transitions, reads,
 * probe attempts, settlements and refresh cycles, and check Contract 2's
 * invariants against the authority's own state after every step. The deterministic
 * suite resolves one seed per run (`CARACAL_TEST_SEED` replays it).
 */

import * as fc from "fast-check"
import { describe, expect, it } from "vitest"
import { runBreakerHistory } from "../../support/serval/breaker-history.js"
import { createGeneratedSuite } from "../../support/seed.js"

const suite = createGeneratedSuite({
  name: "serval-breaker",
  command: "npm run test:property",
  maxCases: 400,
})

interface HistorySpec {
  readonly seed: number
  readonly steps: number
}

const historyArb: fc.Arbitrary<HistorySpec> = fc.record({
  seed: fc.integer({ min: 0, max: 2 ** 31 - 1 }),
  steps: fc.integer({ min: 4, max: 40 }),
})

describe("serval breaker histories", () => {
  suite.itProperty(
    "never admits from a stale verdict and never fabricates an admission",
    historyArb,
    async (spec) => {
      const violations = await runBreakerHistory({
        seed: spec.seed,
        steps: spec.steps,
      })
      expect(
        violations,
        `seed ${spec.seed} over ${spec.steps} steps\n${suite.replay}`,
      ).toEqual([])
    },
    { runs: 60 },
  )

  it("replays a fixed history set without a violation", async () => {
    for (let index = 0; index < 12; index += 1) {
      const seed = (suite.seed + index * 2_246_822_519) >>> 0
      const violations = await runBreakerHistory({ seed, steps: 24 })
      expect(violations, `seed ${seed}\n${suite.replay}`).toEqual([])
    }
  })

  it("reproduces the same history for the same seed", async () => {
    const first = await runBreakerHistory({ seed: 7_777, steps: 20 })
    const second = await runBreakerHistory({ seed: 7_777, steps: 20 })

    expect(second).toEqual(first)
  })
})
