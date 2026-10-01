import { describe, expect, it } from "vitest"
import type { Policy } from "../../src/index.js"
import { CircuitOpenError, circuitBreaker, operation } from "../../src/index.js"
import { memoryBreakerCoordinator } from "../support/memory-coordinator/memory-circuit-breaker.js"

/**
 * One `failureThreshold`, one meaning.
 *
 * The threshold is resolved to an integer numerator of thousandths because the
 * distributed coordinator compares that numerator rather than a ratio
 * (`wFail * 1000 >= numerator * wTotal`).  The local breaker compared the raw
 * float instead, so a threshold that is not a multiple of `0.001` meant two
 * different things: `failureThreshold: 0.5004` with one failure and one success
 * left the local breaker closed (`0.5 < 0.5004`) while the distributed breaker
 * opened on the same window, because both resolve to a numerator of 500.  The
 * threshold is now resolved once and compared the same way by both, which these
 * cases pin per trace.
 */

const traits = () => ({
  abort: "unsupported" as const,
  replay: "safe" as const,
})

function subject(breaker: Policy, work: () => Promise<unknown>) {
  return operation({
    name: "work",
    adapter: { capabilities: traits, execute: work },
    policies: [breaker],
  })
}

describe.each(["local", "distributed"] as const)(
  "%s breaker — failureThreshold resolution",
  (coordination) => {
    function makeBreaker(
      failureThreshold: number,
      minimumThroughput: number,
    ): Policy {
      const options = {
        name: "p",
        minimumThroughput,
        failureThreshold,
        openMs: 60_000,
        windowSize: 10,
      }
      return coordination === "local"
        ? circuitBreaker.local(options)
        : circuitBreaker.distributed({
            ...options,
            coordinator: memoryBreakerCoordinator(),
            scope: () => "shared",
          })
    }

    function call(breaker: Policy, failure: boolean): Promise<unknown> {
      return subject(breaker, () =>
        failure ? Promise.reject(new Error("boom")) : Promise.resolve("ok"),
      ).execute(undefined)
    }

    /**
     * Drives `trace` through a fresh breaker and reports whether the call that
     * follows it is shed - the observable difference between the two
     * coordinations.  `minimumThroughput` is the trace length, so the window is
     * complete when the threshold is evaluated.
     */
    async function shedsOn(
      failureThreshold: number,
      trace: readonly boolean[],
    ): Promise<boolean> {
      const breaker = makeBreaker(failureThreshold, trace.length)
      for (const failure of trace) await call(breaker, failure).catch(() => {})
      try {
        await call(breaker, false)
        return false
      } catch (error) {
        if (error instanceof CircuitOpenError) return true
        throw error
      }
    }

    it("compares a threshold as its nearest thousandth", async () => {
      // 0.5004 and 0.4996 both resolve to a numerator of 500, so one failure in
      // a two-observation window - exactly 50% - meets either threshold.
      await expect(shedsOn(0.5004, [true, false])).resolves.toBe(true)
      await expect(shedsOn(0.4996, [true, false])).resolves.toBe(true)
    })

    it("stays closed while the window is below the resolved threshold", async () => {
      // One failure in three (33%) is below the 50% both thresholds resolve to:
      // the resolution must not degrade the comparison into "any failure opens
      // it".
      await expect(shedsOn(0.5004, [true, false, false])).resolves.toBe(false)
      // A threshold that resolves to 999 requires essentially every observation
      // to fail.
      await expect(shedsOn(0.9994, [true, false])).resolves.toBe(false)
    })
  },
)
