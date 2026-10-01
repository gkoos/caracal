import { describe, expect, it } from "vitest"
import type {
  BreakerClassifier,
  OperationEvent,
  RateLimitCoordinator,
} from "../../src/index.js"
import {
  CircuitOpenError,
  RateLimitExceededError,
  circuitBreaker,
  operation,
  rateLimit,
} from "../../src/index.js"
import { memoryBreakerCoordinator } from "../support/memory-coordinator/memory-circuit-breaker.js"
import { memoryRateLimitCoordinator } from "../support/memory-coordinator/memory-rate-limit.js"

/**
 * A rate-limit refusal is not evidence about the dependency.
 *
 * A rate limiter declares `phase: "attempt"`, so it always sits directly around
 * the adapter and its `RateLimitExceededError` always reaches an enclosing
 * breaker, whatever the array order.  The adapter call never started, so the
 * refusal says nothing about the dependency: the default classifier used to
 * record it as a failure, letting a limiter shedding normal traffic open a
 * healthy breaker.  This mirrors the bulkhead admission-refusal handling; the
 * rate limiter had been missed.
 */

const capabilities = () => ({
  abort: "unsupported" as const,
  replay: "safe" as const,
})

type Overrides = {
  readonly classify?: BreakerClassifier
  readonly countAdmissionRejections?: boolean
  readonly minimumThroughput?: number
}

describe.each(["local", "distributed"] as const)(
  "%s breaker and rate-limit refusals",
  (coordination) => {
    function breaker({
      classify,
      countAdmissionRejections,
      minimumThroughput = 2,
    }: Overrides = {}) {
      const options = {
        name: "test",
        minimumThroughput,
        failureThreshold: 0.5,
        openMs: 10,
        halfOpenSuccesses: 1,
        classify,
        countAdmissionRejections,
      }
      return coordination === "local"
        ? circuitBreaker.local(options)
        : circuitBreaker.distributed({
            ...options,
            coordinator: memoryBreakerCoordinator(),
            scope: () => "shared",
          })
    }

    // `rate: 1` resolves to a 1000ms emission interval, so the second call of a
    // run is always rejected deterministically, local or distributed.
    function limiter() {
      return coordination === "local"
        ? rateLimit.local({ name: "gate", rate: 1 })
        : rateLimit.distributed({
            name: "gate",
            rate: 1,
            coordinator: memoryRateLimitCoordinator(),
            scope: () => "shared",
          })
    }

    function observed(events: readonly OperationEvent[]) {
      return events
        .filter((event) => event.type === "breaker.observation")
        .map((event) => (event as { outcome: string }).outcome)
    }

    function stateChanges(events: readonly OperationEvent[]) {
      return events
        .filter((event) => event.type === "breaker.state-changed")
        .map((event) => (event as { state: string }).state)
    }

    function work(overrides: Overrides, onCall: () => void) {
      const events: OperationEvent[] = []
      const op = operation({
        name: "work",
        adapter: {
          capabilities,
          execute: async () => {
            onCall()
            return "ok"
          },
        },
        policies: [breaker(overrides), limiter()],
        events: { emit: (event) => events.push(event) },
      })
      return { op, events }
    }

    it("does not record a rate-limit refusal: the adapter never ran", async () => {
      let calls = 0
      const { op, events } = work({}, () => {
        calls++
      })

      await expect(op.execute(undefined)).resolves.toBe("ok")
      await expect(op.execute(undefined)).rejects.toBeInstanceOf(
        RateLimitExceededError,
      )
      expect(calls).toBe(1)

      // One success only.  A single refusal must not open a healthy breaker, so
      // it is not recorded and the breaker stays closed.
      expect(observed(events)).toEqual(["success"])
      expect(stateChanges(events)).toEqual([])

      // Still closed: the next call is shed by the limiter, not the breaker.
      await expect(op.execute(undefined)).rejects.toBeInstanceOf(
        RateLimitExceededError,
      )
    })

    it("counts a refusal when countAdmissionRejections is on", async () => {
      let calls = 0
      const { op, events } = work({ countAdmissionRejections: true }, () => {
        calls++
      })

      await expect(op.execute(undefined)).resolves.toBe("ok")
      await expect(op.execute(undefined)).rejects.toBeInstanceOf(
        RateLimitExceededError,
      )
      expect(observed(events)).toEqual(["success", "failure"])
      expect(stateChanges(events)).toEqual(["open"])
      await expect(op.execute(undefined)).rejects.toBeInstanceOf(
        CircuitOpenError,
      )
      expect(calls).toBe(1)
    })

    it("lets an explicit classifier own the mapping", async () => {
      // `classify` takes precedence over the built-in refusal default.
      let calls = 0
      const { op, events } = work(
        {
          classify: (_error, isSuccess) => (isSuccess ? "success" : "failure"),
        },
        () => {
          calls++
        },
      )

      await expect(op.execute(undefined)).resolves.toBe("ok")
      await expect(op.execute(undefined)).rejects.toBeInstanceOf(
        RateLimitExceededError,
      )

      // The explicit classifier changes the verdict, not the shed: the second
      // call was still refused before the adapter, so it ran once.
      expect(calls).toBe(1)

      // `classify` wins over the built-in refusal default: the refusal is
      // recorded as a failure even though `countAdmissionRejections` is off.
      expect(observed(events)).toEqual(["success", "failure"])
    })

    it.skipIf(coordination === "local")(
      "still records a coordinator failure: that is not a rate-limit refusal",
      async () => {
        // A coordinator error is rethrown as-is, not as a
        // `RateLimitExceededError`, so it is evidence about the dependency (or
        // the coordination layer) and must still count.
        const events: OperationEvent[] = []
        let calls = 0
        const failing: RateLimitCoordinator = {
          command: async () => {
            throw new Error("coordinator unavailable")
          },
        }
        const op = operation({
          name: "work",
          adapter: {
            capabilities,
            execute: async () => {
              calls++
              return "ok"
            },
          },
          policies: [
            breaker({ minimumThroughput: 1 }),
            rateLimit.distributed({
              name: "gate",
              rate: 1,
              coordinator: failing,
              scope: () => "shared",
            }),
          ],
          events: { emit: (event) => events.push(event) },
        })

        await expect(op.execute(undefined)).rejects.toThrow(
          "coordinator unavailable",
        )
        expect(calls).toBe(0)
        expect(observed(events)).toEqual(["failure"])
      },
    )
  },
)
