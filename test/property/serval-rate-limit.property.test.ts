/**
 * Property tests for the delegated rate limiter.
 *
 * The generated histories mix clock advances, concurrent admissions, ambiguous and
 * late replies, outages and several scopes, and check Contract 8's invariants after
 * every step against the reference model's token bucket. The deterministic suite
 * resolves one seed per run (`CARACAL_TEST_SEED` replays it).
 */

import * as fc from "fast-check"
import { describe, expect, it } from "vitest"
import { runRateLimitHistory } from "../support/serval/rate-limit-history.js"
import { createGeneratedSuite } from "../support/seed.js"

const suite = createGeneratedSuite({
  name: "serval-rate-limit",
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

describe("serval rate limit histories", () => {
  suite.itProperty(
    "never admits beyond granted quota and never creates a permit",
    historyArb,
    async (spec) => {
      const violations = await runRateLimitHistory({
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
      const violations = await runRateLimitHistory({ seed, steps: 24 })
      expect(violations, `seed ${seed}\n${suite.replay}`).toEqual([])
    }
  })

  it("reproduces the same history for the same seed", async () => {
    const first = await runRateLimitHistory({ seed: 4_242, steps: 20 })
    const second = await runRateLimitHistory({ seed: 4_242, steps: 20 })

    expect(second).toEqual(first)
  })
})
