/**
 * Property tests for the Serval authority reference model.
 *
 * `docs/serval/README.md` and `docs/serval/authority.md` are the contract; the model in
 * `test/support/serval/reference-model.ts` is its executable obligation. These
 * properties replay seeded adversarial histories through that model and assert
 * that no contract invariant breaks - delayed, duplicated, reordered and dropped
 * replies, retries, retention sweeps, spend-at-expiry, and an uncertain bulkhead
 * handoff.
 *
 * The suite resolves one seed per run (`CARACAL_TEST_SEED` replays it), scales
 * every case count by the run depth (`CARACAL_TEST_RUNS`), and prints both on
 * failure. `test/unit/serval-authority.test.ts` pins the deterministic acceptance
 * cases, one per contract.
 */

import * as fc from "fast-check"
import { describe, expect, it } from "vitest"
import { runAdversarialHistories } from "../support/serval/histories.js"
import { runSessionHistory } from "../support/serval/session-history.js"
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

describe("serval session histories", () => {
  suite.itProperty(
    "survives reordering, duplication, partitions, reconnects and pressure",
    historyArb,
    async (spec) => {
      const violations = await runSessionHistory({
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

  it("replays a fixed session history set without a violation", async () => {
    for (let index = 0; index < 12; index += 1) {
      const seed = (suite.seed + index * 2_246_822_519) >>> 0
      const violations = await runSessionHistory({ seed, steps: 24 })
      expect(violations, `seed ${seed}\n${suite.replay}`).toEqual([])
    }
  })

  it("reproduces the same session history for the same seed", async () => {
    const first = await runSessionHistory({ seed: 4_242, steps: 20 })
    const second = await runSessionHistory({ seed: 4_242, steps: 20 })

    expect(second).toEqual(first)
  })
})
