/**
 * Property tests for the Serval authority reference model (Phase 0).
 *
 * `docs/serval.md` and `docs/serval-authority.md` are the contract; the model in
 * `test/support/serval/reference-model.ts` is its executable obligation. These
 * properties replay seeded adversarial histories through that model and assert
 * that no contract invariant breaks - delayed, duplicated, reordered and dropped
 * replies, retries, retention sweeps, spend-at-expiry, and an uncertain bulkhead
 * handoff.
 *
 * The suite resolves one seed per run (`CARACAL_TEST_SEED` replays it), scales
 * every case count by the run depth (`CARACAL_TEST_RUNS`), and prints both on
 * failure. `test/unit/serval-authority.test.ts` pins the deterministic acceptance
 * cases, one per mandatory decision.
 */

import * as fc from "fast-check"
import { describe, expect, it } from "vitest"
import { runAdversarialHistories } from "../support/serval/histories.js"
import { createGeneratedSuite } from "../support/seed.js"

const suite = createGeneratedSuite({
  name: "serval-authority",
  command: "npm run test:property",
  maxCases: 600,
})

interface HistorySpec {
  readonly seed: number
  readonly steps: number
}

const historyArb: fc.Arbitrary<HistorySpec> = fc.record({
  seed: fc.integer({ min: 0, max: 2 ** 31 - 1 }),
  steps: fc.integer({ min: 4, max: 40 }),
})

describe("serval authority model", () => {
  suite.itProperty(
    "holds every authority invariant across generated adversarial histories",
    historyArb,
    (spec) => {
      for (const outcome of runAdversarialHistories(spec.seed, spec.steps)) {
        expect(
          outcome.violations,
          `seed ${outcome.seed} over ${outcome.steps} steps\n${suite.replay}`,
        ).toEqual([])
      }
    },
    { runs: 120 },
  )

  it("replays a fixed history set without a violation", () => {
    for (let index = 0; index < 40; index += 1) {
      const seed = (suite.seed + index * 2_654_435_761) >>> 0
      for (const outcome of runAdversarialHistories(seed, 30)) {
        expect(
          outcome.violations,
          `seed ${outcome.seed}\n${suite.replay}`,
        ).toEqual([])
      }
    }
  })

  it("reproduces the same history for the same seed", () => {
    const first = runAdversarialHistories(12_345, 25)
    const second = runAdversarialHistories(12_345, 25)

    expect(second).toEqual(first)
    expect(first.length).toBe(3)
  })
})
