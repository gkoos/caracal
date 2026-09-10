import { afterEach, describe, expect, it, vi } from "vitest"
import type { OperationEvent } from "../../src/index.js"
import { CircuitOpenError, circuitBreaker, operation } from "../../src/index.js"
import { Deferred } from "../support/deferred.js"

function subject() {
  const events: OperationEvent[] = []
  const breaker = circuitBreaker.local({
    name: "test",
    minimumThroughput: 1,
    openMs: 10,
    halfOpenProbes: 2,
    halfOpenSuccesses: 1,
  })
  const op = operation({
    name: "work",
    adapter: {
      capabilities: () => ({ abort: "unsupported", replay: "safe" }),
      execute: (gate: Deferred<string>) => gate.promise,
      classify: (outcome) =>
        outcome.status === "failure"
          ? "failure"
          : outcome.value === "ignored"
            ? "ignored"
            : "success",
    },
    policies: [breaker],
    events: { emit: (e) => events.push(e) },
  })
  function start() {
    const gate = new Deferred<string>()
    return { gate, result: op.execute(gate) }
  }
  return { breaker, events, start }
}

type Call = ReturnType<ReturnType<typeof subject>["start"]>
async function settle(call: Call, outcome: "success" | "failure" | "ignored") {
  if (outcome === "failure") {
    const error = new Error("failure")
    call.gate.reject(error)
    await expect(call.result).rejects.toBe(error)
  } else {
    call.gate.resolve(outcome)
    await expect(call.result).resolves.toBe(outcome)
  }
}

describe("local circuit breaker generations", () => {
  afterEach(() => vi.useRealTimers())

  it.each(["success", "failure"] as const)(
    "ignores a late %s probe after another probe reopens the breaker",
    async (outcome) => {
      vi.useFakeTimers()
      const { breaker, events, start } = subject()
      await settle(start(), "failure")
      await vi.advanceTimersByTimeAsync(10)
      const first = start()
      const late = start()
      await settle(first, "failure")
      const snapshot = breaker.snapshot()
      const eventCount = events.filter((e) =>
        e.type.startsWith("breaker."),
      ).length

      await vi.advanceTimersByTimeAsync(9)
      await settle(late, outcome)
      expect(breaker.snapshot()).toEqual(snapshot)
      expect(events.filter((e) => e.type.startsWith("breaker."))).toHaveLength(
        eventCount,
      )
      await expect(start().result).rejects.toBeInstanceOf(CircuitOpenError)

      // The stale failure must not restart the open interval either.
      await vi.advanceTimersByTimeAsync(1)
      await settle(start(), "success")
      expect(breaker.snapshot().state).toBe("closed")
    },
  )

  describe.each(["open", "half-open", "closed"] as const)(
    "older closed-state work settling in %s",
    (state) => {
      it.each(["success", "failure"] as const)(
        "discards its %s without changing the new window or probe counters",
        async (outcome) => {
          vi.useFakeTimers()
          const { breaker, events, start } = subject()
          const late = start()
          await settle(start(), "failure")
          let probe: Call | undefined
          if (state !== "open") {
            await vi.advanceTimersByTimeAsync(10)
            probe = start()
            if (state === "closed") await settle(probe, "success")
          }
          const snapshot = breaker.snapshot()
          expect(snapshot.state).toBe(state)
          const eventCount = events.filter((e) =>
            e.type.startsWith("breaker."),
          ).length

          await settle(late, outcome)
          expect(breaker.snapshot()).toEqual(snapshot)
          expect(
            events.filter((e) => e.type.startsWith("breaker.")),
          ).toHaveLength(eventCount)
          if (state === "half-open" && probe) await settle(probe, "success")
        },
      )
    },
  )

  it.each(["success", "failure", "ignored"] as const)(
    "does not let an old %s probe alter a later half-open generation",
    async (outcome) => {
      vi.useFakeTimers()
      const { breaker, events, start } = subject()
      await settle(start(), "failure")
      await vi.advanceTimersByTimeAsync(10)
      const first = start()
      const late = start()
      await settle(first, "failure")
      await vi.advanceTimersByTimeAsync(10)
      const currentA = start()
      const currentB = start()
      const snapshot = breaker.snapshot()
      expect(snapshot).toMatchObject({
        state: "half-open",
        probesInFlight: 2,
        halfOpenSuccesses: 0,
      })
      const eventCount = events.filter((e) =>
        e.type.startsWith("breaker."),
      ).length

      await settle(late, outcome)
      expect(breaker.snapshot()).toEqual(snapshot)
      expect(events.filter((e) => e.type.startsWith("breaker."))).toHaveLength(
        eventCount,
      )
      await expect(start().result).rejects.toBeInstanceOf(CircuitOpenError)
      await settle(currentA, "success")
      await settle(currentB, "success")
      expect(breaker.snapshot().state).toBe("closed")
    },
  )

  it("does not reopen a recovered breaker when an old probe later fails", async () => {
    vi.useFakeTimers()
    const { breaker, events, start } = subject()
    await settle(start(), "failure")
    await vi.advanceTimersByTimeAsync(10)
    const first = start()
    const late = start()
    await settle(first, "success")
    const snapshot = breaker.snapshot()
    expect(snapshot.state).toBe("closed")
    const eventCount = events.filter((e) =>
      e.type.startsWith("breaker."),
    ).length

    await settle(late, "failure")
    expect(breaker.snapshot()).toEqual(snapshot)
    expect(events.filter((e) => e.type.startsWith("breaker."))).toHaveLength(
      eventCount,
    )
    await settle(start(), "success")
  })
})
