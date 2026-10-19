import * as fc from "fast-check"
import { describe, expect } from "vitest"
import type { ExecutionContext } from "../../src/index.js"
import { createScriptedAdapter } from "../harness/index.js"
import { createGeneratedSuite } from "../support/seed.js"

const suite = createGeneratedSuite({
  name: "scripted-adapter",
  command: "npm run test:property",
  maxCases: 500,
})

const capabilities = { abort: "supported", replay: "safe" } as const

function context(index: number): ExecutionContext {
  return {
    operationName: "scripted-property",
    executionId: `execution-${index}`,
    attempt: 1,
    signal: undefined,
    metadata: {},
    capabilities,
    classify: (outcome) =>
      outcome.status === "success" ? "success" : "failure",
  }
}

interface Case {
  readonly entries: number
  readonly calls: number
}

const caseArb: fc.Arbitrary<Case> = fc.record({
  entries: fc.integer({ min: 1, max: 20 }),
  calls: fc.integer({ min: 1, max: 25 }),
})

describe(`scripted adapter - generated queue accounting (seed=${suite.seed} replay="${suite.replay}")`, () => {
  suite.itProperty(
    "consumes exactly one entry per execution, in invocation order",
    caseArb,
    async ({ entries, calls }: Case) => {
      const outcomes = Array.from({ length: entries }, (_, index) => ({
        status: "success" as const,
        value: index,
      }))
      const adapter = createScriptedAdapter<number, number>({
        capabilities,
        outcomes,
      })
      const args = Array.from({ length: calls }, (_, index) => index)
      const results = await Promise.allSettled(
        args.map((index) => adapter.execute(index, context(index))),
      )

      const reserved = Math.min(entries, calls)
      expect(adapter.calls).toHaveLength(reserved)
      expect(adapter.calls.map((call) => call.index)).toEqual(
        Array.from({ length: reserved }, (_, index) => index),
      )
      expect(adapter.calls.map((call) => call.args)).toEqual(
        args.slice(0, reserved),
      )

      for (let index = 0; index < reserved; index++) {
        const result = results[index]
        expect(result?.status).toBe("fulfilled")
        if (result?.status === "fulfilled") {
          expect(result.value).toBe(index)
        }
      }

      // The remaining calls exhaust the script; none repeats the final entry.
      for (let index = reserved; index < calls; index++) {
        const result = results[index]
        expect(result?.status).toBe("rejected")
        if (result?.status === "rejected") {
          expect(result.reason).toBeInstanceOf(Error)
          if (result.reason instanceof Error) {
            expect(result.reason.message).toMatch(/exhausted/)
          }
        }
      }
    },
    { runs: 100 },
  )
})
