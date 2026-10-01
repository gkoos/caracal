import { describe, expect, it } from "vitest"
import type { BreakerCoordinator, OperationEvent } from "../../src/index.js"
import { CircuitOpenError, circuitBreaker, operation } from "../../src/index.js"
import { Deferred } from "../support/deferred.js"
import { memoryBreakerCoordinator } from "../support/memory-coordinator/memory-circuit-breaker.js"

/**
 * A call that arrives already cancelled is not evidence about the dependency.
 *
 * The breaker is an *outer* policy, so it admitted the call before anything
 * downstream checked cancellation.  The adapter boundary then rejected it with
 * the caller's own abort reason and the breaker's `finally` recorded that as a
 * failure - opening a healthy breaker on work the adapter never saw.  The
 * admission signal is now checked before admission, like `bulkhead`, `retry`
 * and the adapter boundary already did.
 */

const capabilities = () => ({
  abort: "supported" as const,
  replay: "safe" as const,
})

describe.each(["local", "distributed"] as const)(
  "%s breaker and pre-aborted calls",
  (coordination) => {
    function breaker(minimumThroughput = 1) {
      const options = {
        name: "test",
        minimumThroughput,
        failureThreshold: 0.5,
        openMs: 10,
        halfOpenSuccesses: 1,
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

    it("does not admit a pre-aborted call: nothing is recorded", async () => {
      let calls = 0
      const events: OperationEvent[] = []
      const policy = breaker()
      const op = operation({
        name: "work",
        adapter: {
          capabilities,
          execute: async () => {
            calls++
            return "ok"
          },
        },
        policies: [policy],
        events: { emit: (event) => events.push(event) },
      })

      const controller = new AbortController()
      const reason = new Error("caller cancelled")
      controller.abort(reason)

      await expect(
        op.execute(undefined, { signal: controller.signal }),
      ).rejects.toBe(reason)

      // The adapter never ran, so there is no outcome to record.
      expect(calls).toBe(0)
      expect(observed(events)).toEqual([])
      expect(stateChanges(events)).toEqual([])
      if (coordination === "local") {
        const snapshot = (
          policy as unknown as { snapshot(): { state: string } }
        ).snapshot()
        expect(snapshot.state).toBe("closed")
      }

      // Not shed by the breaker - this is the regression the suite guards.
      await expect(op.execute(undefined)).resolves.toBe("ok")
      expect(calls).toBe(1)
      expect(observed(events)).toEqual(["success"])
    })

    it("prefers the caller's abort reason over an open breaker", async () => {
      // Pins the check *before* admission: an open breaker rejects with
      // `CircuitOpenError`, but a call that was already cancelled reports its
      // own cancellation and records nothing.
      const events: OperationEvent[] = []
      let calls = 0
      const op = operation({
        name: "work",
        adapter: {
          capabilities,
          execute: async () => {
            calls++
            throw new Error("dependency failed")
          },
        },
        policies: [breaker()],
        events: { emit: (event) => events.push(event) },
      })

      await expect(op.execute(undefined)).rejects.toThrow("dependency failed")
      expect(stateChanges(events)).toEqual(["open"])
      expect(calls).toBe(1)

      const controller = new AbortController()
      const reason = new Error("caller cancelled")
      controller.abort(reason)

      await expect(
        op.execute(undefined, { signal: controller.signal }),
      ).rejects.toBe(reason)
      expect(observed(events)).toEqual(["failure"])
      expect(stateChanges(events)).toEqual(["open"])
      expect(calls).toBe(1)

      // The breaker really is open: an unaborted call is still shed.
      await expect(op.execute(undefined)).rejects.toBeInstanceOf(
        CircuitOpenError,
      )
    })

    it("still records a call that aborts after admission", async () => {
      // The fix must not over-reach: once admitted, an abort is the settlement
      // of work the adapter started, so it counts like any other failure.
      const events: OperationEvent[] = []
      const started = new Deferred<void>()
      let calls = 0
      const op = operation({
        name: "work",
        adapter: {
          capabilities,
          execute: async (_args, context) => {
            calls++
            started.resolve()
            return await new Promise<string>((_resolve, reject) => {
              const signal = context.signal
              const onAbort = () => reject(signal?.reason)
              if (signal?.aborted) {
                onAbort()
                return
              }
              signal?.addEventListener("abort", onAbort, { once: true })
            })
          },
        },
        policies: [breaker()],
        events: { emit: (event) => events.push(event) },
      })

      const controller = new AbortController()
      const reason = new Error("cancelled mid-flight")
      const pending = op.execute(undefined, { signal: controller.signal })
      // The distributed breaker admits through the coordinator first, so the
      // adapter is not reached synchronously: wait until it actually started.
      await started.promise
      controller.abort(reason)

      await expect(pending).rejects.toBe(reason)
      expect(calls).toBe(1)
      expect(observed(events)).toEqual(["failure"])
      expect(stateChanges(events)).toEqual(["open"])
    })

    it.skipIf(coordination === "local")(
      "does not touch the coordinator for a pre-aborted call",
      async () => {
        // Admission is what talks to the coordinator, so a pre-aborted call
        // must not spend a round trip on it either.
        const roundTrips: string[] = []
        const coordinator: BreakerCoordinator = {
          readState: async () => {
            roundTrips.push("readState")
            throw new Error("readState must not be called")
          },
          observe: async () => {
            roundTrips.push("observe")
            throw new Error("observe must not be called")
          },
          admitProbe: async () => {
            roundTrips.push("admitProbe")
            throw new Error("admitProbe must not be called")
          },
          settleProbe: async () => {
            roundTrips.push("settleProbe")
            throw new Error("settleProbe must not be called")
          },
        }
        let calls = 0
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
            circuitBreaker.distributed({
              name: "test",
              minimumThroughput: 1,
              failureThreshold: 0.5,
              openMs: 10,
              coordinator,
              scope: () => "shared",
            }),
          ],
        })

        const controller = new AbortController()
        const reason = new Error("caller cancelled")
        controller.abort(reason)

        await expect(
          op.execute(undefined, { signal: controller.signal }),
        ).rejects.toBe(reason)
        expect(roundTrips).toEqual([])
        expect(calls).toBe(0)
      },
    )

    it.skipIf(coordination === "local")(
      "does not record a call cancelled while admission is in flight",
      async () => {
        // Admission awaits the coordinator, so a cancellation can land after the
        // pre-admission check and before the attempt starts.  The adapter
        // boundary still rejects the call, so it is still not evidence.
        const entered = new Deferred<void>()
        const gate = new Deferred<void>()
        const base = memoryBreakerCoordinator()
        const coordinator: BreakerCoordinator = {
          ...base,
          async readState(identity) {
            entered.resolve()
            await gate.promise
            return base.readState(identity)
          },
        }
        const events: OperationEvent[] = []
        let calls = 0
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
            circuitBreaker.distributed({
              name: "test",
              minimumThroughput: 1,
              failureThreshold: 0.5,
              openMs: 10,
              halfOpenSuccesses: 1,
              coordinator,
              scope: () => "shared",
            }),
          ],
          events: { emit: (event) => events.push(event) },
        })

        const controller = new AbortController()
        const reason = new Error("caller cancelled")
        const pending = op.execute(undefined, { signal: controller.signal })
        await entered.promise
        controller.abort(reason)
        gate.resolve()

        await expect(pending).rejects.toBe(reason)
        expect(calls).toBe(0)
        expect(observed(events)).toEqual([])
        expect(stateChanges(events)).toEqual([])
        const state = await base.readState({
          name: "test",
          operation: "work",
          scope: "shared",
        })
        expect(state?.state ?? "closed").toBe("closed")
      },
    )

    it.skipIf(coordination === "local")(
      "releases a probe claimed by a call cancelled during admission",
      async () => {
        // The half-open probe slot is what recovery runs on.  A cancelled call
        // must give it back, not hold it until the lease expires and not settle
        // it as a failure: that re-opened a breaker that was recovering.
        const entered = new Deferred<void>()
        const gate = new Deferred<void>()
        const base = memoryBreakerCoordinator()
        let gated = false
        const coordinator: BreakerCoordinator = {
          ...base,
          async admitProbe(identity, params) {
            if (!gated) {
              gated = true
              entered.resolve()
              await gate.promise
            }
            return base.admitProbe(identity, params)
          },
        }
        const events: OperationEvent[] = []
        let calls = 0
        let failure = true
        const op = operation({
          name: "work",
          adapter: {
            capabilities,
            execute: async () => {
              calls++
              if (failure) throw new Error("dependency failed")
              return "ok"
            },
          },
          policies: [
            circuitBreaker.distributed({
              name: "test",
              minimumThroughput: 1,
              failureThreshold: 0.5,
              openMs: 5,
              halfOpenProbes: 1,
              probeLeaseTtlMs: 10_000,
              halfOpenSuccesses: 1,
              coordinator,
              scope: () => "shared",
            }),
          ],
          events: { emit: (event) => events.push(event) },
        })

        // One real failure opens the breaker.
        await expect(op.execute(undefined)).rejects.toThrow("dependency failed")
        expect(stateChanges(events)).toEqual(["open"])
        await new Promise((resolve) => setTimeout(resolve, 20))

        const controller = new AbortController()
        const reason = new Error("caller cancelled")
        const pending = op.execute(undefined, { signal: controller.signal })
        await entered.promise
        controller.abort(reason)
        gate.resolve()

        await expect(pending).rejects.toBe(reason)
        // The gated probe was admitted (open → half-open) but never started.
        expect(calls).toBe(1)
        expect(observed(events)).toEqual(["failure"])
        expect(stateChanges(events)).toEqual(["open", "half-open"])
        const state = await base.readState({
          name: "test",
          operation: "work",
          scope: "shared",
        })
        expect(state?.state).toBe("half-open")

        // The slot is free again, so recovery is not stalled behind the
        // cancelled call: the next probe runs and closes the breaker.
        failure = false
        await expect(op.execute(undefined)).resolves.toBe("ok")
        expect(observed(events)).toEqual(["failure", "success"])
        expect(stateChanges(events)).toEqual(["open", "half-open", "closed"])
      },
    )

    it.skipIf(coordination === "local")(
      "reports the caller's cancellation when the state read fails",
      async () => {
        // Admission can reject the call outright - here, fail-closed on a
        // coordinator outage.  A cancellation that landed while that read was
        // in flight must still win: the caller hears its own reason, not a
        // verdict about a dependency the call never reached.
        const entered = new Deferred<void>()
        const gate = new Deferred<void>()
        const base = memoryBreakerCoordinator()
        const coordinator: BreakerCoordinator = {
          ...base,
          async readState() {
            entered.resolve()
            await gate.promise
            throw new Error("coordinator unavailable")
          },
        }
        const events: OperationEvent[] = []
        let calls = 0
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
            circuitBreaker.distributed({
              name: "test",
              minimumThroughput: 1,
              failureThreshold: 0.5,
              openMs: 10,
              onCoordinatorError: "fail-closed",
              coordinator,
              scope: () => "shared",
            }),
          ],
          events: { emit: (event) => events.push(event) },
        })

        const controller = new AbortController()
        const reason = new Error("caller cancelled")
        const pending = op.execute(undefined, { signal: controller.signal })
        await entered.promise
        controller.abort(reason)
        gate.resolve()

        await expect(pending).rejects.toBe(reason)
        expect(calls).toBe(0)
        expect(observed(events)).toEqual([])
        const reported = events.map((event) => event.type)
        // The coordinator failure is still reported: it is a fact about the
        // coordinator, true whether or not the caller is still waiting.
        expect(reported).toContain("breaker.coordinator-error")
        // But nothing claims this call was degraded or shed.
        expect(reported).not.toContain("breaker.degraded")
        expect(reported).not.toContain("breaker.rejected")

        // The protection is intact: a live call still fails closed.
        await expect(op.execute(undefined)).rejects.toBeInstanceOf(
          CircuitOpenError,
        )
        expect(calls).toBe(0)
      },
    )

    it.skipIf(coordination === "local")(
      "reports the caller's cancellation when probe admission is refused",
      async () => {
        // An open breaker refuses the probe with `CircuitOpenError`.  A call
        // cancelled while that refusal was still being decided reports its own
        // cancellation instead, and leaves no trace: no rejection event, no
        // observation, and the breaker stays open with its probe slot untouched.
        const entered = new Deferred<void>()
        const gate = new Deferred<void>()
        const base = memoryBreakerCoordinator()
        let gated = false
        const coordinator: BreakerCoordinator = {
          ...base,
          async admitProbe(identity, params) {
            if (!gated) {
              gated = true
              entered.resolve()
              await gate.promise
            }
            return base.admitProbe(identity, params)
          },
        }
        const events: OperationEvent[] = []
        let calls = 0
        const op = operation({
          name: "work",
          adapter: {
            capabilities,
            execute: async () => {
              calls++
              throw new Error("dependency failed")
            },
          },
          policies: [
            circuitBreaker.distributed({
              name: "test",
              minimumThroughput: 1,
              failureThreshold: 0.5,
              // Long enough that admission still refuses while we abort.
              openMs: 10_000,
              coordinator,
              scope: () => "shared",
            }),
          ],
          events: { emit: (event) => events.push(event) },
        })

        // One real failure opens the breaker, so the next call is the probe
        // attempt - the one cancelled while the coordinator decides.
        await expect(op.execute(undefined)).rejects.toThrow("dependency failed")
        expect(stateChanges(events)).toEqual(["open"])

        const controller = new AbortController()
        const reason = new Error("caller cancelled")
        const pending = op.execute(undefined, { signal: controller.signal })
        await entered.promise
        controller.abort(reason)
        gate.resolve()

        await expect(pending).rejects.toBe(reason)
        expect(calls).toBe(1)
        expect(observed(events)).toEqual(["failure"])
        expect(stateChanges(events)).toEqual(["open"])
        expect(events.map((event) => event.type)).not.toContain(
          "breaker.rejected",
        )

        // The breaker really is open: a live call is still shed.
        await expect(op.execute(undefined)).rejects.toBeInstanceOf(
          CircuitOpenError,
        )
        expect(calls).toBe(1)
      },
    )

    it.skipIf(coordination === "local")(
      "reports the caller's cancellation when probe admission fails",
      async () => {
        // An outage during probe admission always fails closed.  A cancellation
        // that landed during that round trip replaces the verdict, and the call
        // claims no probe - `admitProbe` never granted one, so there is nothing
        // to release either.
        const entered = new Deferred<void>()
        const gate = new Deferred<void>()
        const base = memoryBreakerCoordinator()
        let gated = false
        const coordinator: BreakerCoordinator = {
          ...base,
          async admitProbe(identity, params) {
            if (!gated) {
              gated = true
              entered.resolve()
              await gate.promise
              throw new Error("coordinator unavailable")
            }
            return base.admitProbe(identity, params)
          },
        }
        const events: OperationEvent[] = []
        let calls = 0
        const op = operation({
          name: "work",
          adapter: {
            capabilities,
            execute: async () => {
              calls++
              throw new Error("dependency failed")
            },
          },
          policies: [
            circuitBreaker.distributed({
              name: "test",
              minimumThroughput: 1,
              failureThreshold: 0.5,
              openMs: 10_000,
              coordinator,
              scope: () => "shared",
            }),
          ],
          events: { emit: (event) => events.push(event) },
        })

        await expect(op.execute(undefined)).rejects.toThrow("dependency failed")
        expect(stateChanges(events)).toEqual(["open"])

        const controller = new AbortController()
        const reason = new Error("caller cancelled")
        const pending = op.execute(undefined, { signal: controller.signal })
        await entered.promise
        controller.abort(reason)
        gate.resolve()

        await expect(pending).rejects.toBe(reason)
        expect(calls).toBe(1)
        expect(observed(events)).toEqual(["failure"])
        expect(stateChanges(events)).toEqual(["open"])
        const reported = events.map((event) => event.type)
        expect(reported).toContain("breaker.coordinator-error")
        expect(reported).not.toContain("breaker.degraded")
        expect(reported).not.toContain("breaker.rejected")

        // Fail-closed is untouched for a live call.
        await expect(op.execute(undefined)).rejects.toBeInstanceOf(
          CircuitOpenError,
        )
        expect(calls).toBe(1)
      },
    )
  },
)
