import { describe, expect, it } from "vitest"
import type { BreakerClassifier, OperationEvent } from "../../src/index.js"
import {
  BulkheadRejectedError,
  CircuitOpenError,
  TimeoutError,
  bulkhead,
  circuitBreaker,
  operation,
  timeout,
} from "../../src/index.js"
import { Deferred } from "../support/deferred.js"
import { memoryBreakerCoordinator } from "../support/memory-coordinator/memory-circuit-breaker.js"

/**
 * A refusal is not evidence about the dependency.
 *
 * A bulkhead declares `phase: "attempt"`, so it always sits directly around the
 * adapter and its refusal always reaches an enclosing breaker, whatever the
 * array order.  The default classifier used to record that refusal as a
 * failure, so a saturated bulkhead opened a healthy breaker, which then shed
 * the rest of the run under `breaker-open` and reported a failure rate the
 * dependency never produced.  A timeout is the opposite case and still counts:
 * the dependency did not answer.
 */

const capabilities = () => ({
  abort: "unsupported" as const,
  replay: "safe" as const,
})

const refusal = (
  reason: "capacity" | "wait-timeout" | "admission-expired" | "lease-lost",
) => new BulkheadRejectedError("local", "pool", "process", reason)

type Overrides = {
  readonly classify?: BreakerClassifier
  readonly countBulkheadRejections?: boolean
}

describe.each(["local", "distributed"] as const)(
  "%s breaker and bulkhead refusals",
  (coordination) => {
    function breaker({ classify, countBulkheadRejections }: Overrides = {}) {
      const options = {
        name: "test",
        minimumThroughput: 1,
        failureThreshold: 0.5,
        openMs: 10,
        halfOpenSuccesses: 1,
        classify,
        countBulkheadRejections,
      }
      return coordination === "local"
        ? circuitBreaker.local(options)
        : circuitBreaker.distributed({
            ...options,
            coordinator: memoryBreakerCoordinator(),
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

    it("does not record a capacity refusal: the adapter never ran", async () => {
      const gate = new Deferred<string>()
      const events: OperationEvent[] = []
      let calls = 0
      const op = operation({
        name: "work",
        adapter: {
          capabilities,
          execute: async () => {
            calls++
            return gate.promise
          },
        },
        policies: [breaker(), bulkhead.local({ name: "pool", limit: 1 })],
        events: { emit: (event) => events.push(event) },
      })

      const held = op.execute(undefined)
      await Promise.resolve()
      await expect(op.execute(undefined)).rejects.toBeInstanceOf(
        BulkheadRejectedError,
      )
      expect(calls).toBe(1)

      gate.resolve("ok")
      await expect(held).resolves.toBe("ok")

      expect(observed(events)).toEqual(["success"])
      expect(stateChanges(events)).toEqual([])
    })

    it("ignores a queue wait-timeout refusal", async () => {
      const gate = new Deferred<string>()
      const events: OperationEvent[] = []
      let calls = 0
      const op = operation({
        name: "work",
        adapter: {
          capabilities,
          execute: async () => {
            calls++
            return gate.promise
          },
        },
        policies: [
          breaker(),
          bulkhead.local({
            name: "pool",
            limit: 1,
            queue: { limit: 2, timeoutMs: 5 },
          }),
        ],
        events: { emit: (event) => events.push(event) },
      })

      const held = op.execute(undefined)
      await Promise.resolve()
      await expect(op.execute(undefined)).rejects.toMatchObject({
        reason: "wait-timeout",
      })
      expect(calls).toBe(1)

      gate.resolve("ok")
      await expect(held).resolves.toBe("ok")
      expect(observed(events)).toEqual(["success"])
    })

    it("ignores an admission-expired refusal", async () => {
      // The distributed bulkhead raises this when a permit's lease deadline
      // passes before the call starts.  The classifier sees only the error.
      const events: OperationEvent[] = []
      const op = operation({
        name: "work",
        adapter: {
          capabilities,
          execute: async () => {
            throw refusal("admission-expired")
          },
        },
        policies: [breaker()],
        events: { emit: (event) => events.push(event) },
      })

      await expect(op.execute(undefined)).rejects.toBeInstanceOf(
        BulkheadRejectedError,
      )
      expect(observed(events)).toEqual([])
    })

    it("still records a lease-lost refusal: that permit was held", async () => {
      // `lease-lost` arrives as an abort reason after the call started, so the
      // adapter may have run.  It is not a decision to shed, and it counts.
      const events: OperationEvent[] = []
      const op = operation({
        name: "work",
        adapter: {
          capabilities,
          execute: async () => {
            throw refusal("lease-lost")
          },
        },
        policies: [breaker()],
        events: { emit: (event) => events.push(event) },
      })

      await expect(op.execute(undefined)).rejects.toBeInstanceOf(
        BulkheadRejectedError,
      )
      expect(observed(events)).toEqual(["failure"])
      await expect(op.execute(undefined)).rejects.toBeInstanceOf(
        CircuitOpenError,
      )
    })

    it("still records a timeout: the dependency did not answer", async () => {
      const never = new Deferred<string>()
      const events: OperationEvent[] = []
      const op = operation({
        name: "work",
        adapter: { capabilities, execute: () => never.promise },
        policies: [breaker(), timeout({ ms: 5 })],
        events: { emit: (event) => events.push(event) },
      })

      await expect(op.execute(undefined)).rejects.toBeInstanceOf(TimeoutError)
      expect(observed(events)).toEqual(["failure"])
      await expect(op.execute(undefined)).rejects.toBeInstanceOf(
        CircuitOpenError,
      )
    })

    it("counts a refusal when countBulkheadRejections is on", async () => {
      const events: OperationEvent[] = []
      const op = operation({
        name: "work",
        adapter: {
          capabilities,
          execute: async () => {
            throw refusal("capacity")
          },
        },
        policies: [breaker({ countBulkheadRejections: true })],
        events: { emit: (event) => events.push(event) },
      })

      await expect(op.execute(undefined)).rejects.toBeInstanceOf(
        BulkheadRejectedError,
      )
      expect(observed(events)).toEqual(["failure"])
      await expect(op.execute(undefined)).rejects.toBeInstanceOf(
        CircuitOpenError,
      )
    })

    it("lets an explicit classifier own the mapping", async () => {
      // `classify` takes precedence over the built-in default in both
      // directions, which is what the documented workaround relies on.
      const events: OperationEvent[] = []
      const op = operation({
        name: "work",
        adapter: {
          capabilities,
          execute: async () => {
            throw refusal("capacity")
          },
        },
        policies: [
          breaker({
            classify: (_error, isSuccess) =>
              isSuccess ? "success" : "failure",
          }),
        ],
        events: { emit: (event) => events.push(event) },
      })

      await expect(op.execute(undefined)).rejects.toBeInstanceOf(
        BulkheadRejectedError,
      )
      expect(observed(events)).toEqual(["failure"])
    })

    it("releases a refused probe's slot without advancing recovery", async () => {
      // A probe that a saturated bulkhead refuses must release its slot, or the
      // recovery window stalls until `probeLeaseTtlMs` elapses, with every call
      // in between rejected by CircuitOpenError.
      const events: OperationEvent[] = []
      const never = new Deferred<string>()
      const capacity = bulkhead.local({ name: "pool", limit: 2 })

      // Holds both permits.  No breaker in this pipeline, so it keeps holding
      // them once the breaker below opens.
      const holders = operation({
        name: "holders",
        adapter: { capabilities, execute: () => never.promise },
        policies: [capacity],
      })

      let calls = 0
      const work = operation({
        name: "work",
        adapter: {
          capabilities,
          execute: async () => {
            calls++
            throw new Error("dependency failed")
          },
        },
        policies: [breaker(), capacity],
        events: { emit: (event) => events.push(event) },
      })

      // One real dependency failure opens the breaker.
      await expect(work.execute(undefined)).rejects.toThrow("dependency failed")
      expect(stateChanges(events)).toEqual(["open"])
      expect(calls).toBe(1)

      // Saturate the shared bulkhead while the breaker is open.
      const held = [holders.execute(undefined), holders.execute(undefined)]
      await Promise.resolve()
      await Promise.resolve()

      await new Promise((resolve) => setTimeout(resolve, 15))

      // Half-open: the breaker admits a probe, the saturated bulkhead refuses it.
      await expect(work.execute(undefined)).rejects.toMatchObject({
        reason: "capacity",
      })
      expect(observed(events)).toEqual(["failure"])
      expect(stateChanges(events)).not.toContain("closed")
      const probes = events.filter(
        (event) => event.type === "breaker.probe-started",
      ).length
      expect(probes).toBeGreaterThan(0)

      // The slot was released, so the next call is admitted as a probe again
      // instead of being rejected with CircuitOpenError.
      await expect(work.execute(undefined)).rejects.toMatchObject({
        reason: "capacity",
      })
      expect(
        events.filter((event) => event.type === "breaker.probe-started").length,
      ).toBe(probes + 1)
      expect(calls).toBe(1)

      never.resolve("late")
      await Promise.allSettled(held)
    })
  },
)
