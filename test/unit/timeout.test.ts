import { describe, expect, it, vi } from "vitest"
import type { Adapter, OperationEvent } from "../../src/index.js"
import { operation, TimeoutError, timeout } from "../../src/index.js"

describe("timeout", () => {
  it("bounds caller completion, requests abort, and still records late underlying settlement", async () => {
    vi.useFakeTimers()
    try {
      let resolveUnderlying!: (value: string) => void
      const underlying = new Promise<string>((resolve) => {
        resolveUnderlying = resolve
      })
      let receivedSignal: AbortSignal | undefined
      const events: OperationEvent[] = []
      const adapter: Adapter<void, string> = {
        capabilities: () => ({ abort: "supported", replay: "safe" }),
        execute: async (_args, context) => {
          receivedSignal = context.signal
          return underlying
        },
      }
      const subject = operation({
        name: "abortable-timeout",
        adapter,
        policies: [timeout({ ms: 10 })],
        events: { emit: (event) => events.push(event) },
      })

      const execution = subject.execute(undefined)
      const rejected = expect(execution).rejects.toBeInstanceOf(TimeoutError)
      await vi.advanceTimersByTimeAsync(10)

      await rejected
      expect(receivedSignal?.aborted).toBe(true)
      expect(
        events.find((event) => event.type === "timeout.triggered"),
      ).toMatchObject({
        abortRequested: true,
        timeoutMs: 10,
      })

      resolveUnderlying("late success")
      await Promise.resolve()
      await Promise.resolve()
      await Promise.resolve()
      await Promise.resolve()
      expect(
        events.filter((event) => event.type === "attempt.settled"),
      ).toHaveLength(1)
    } finally {
      vi.useRealTimers()
    }
  })

  it("does not claim to abort an abort-unsupported adapter", async () => {
    vi.useFakeTimers()
    try {
      let resolveUnderlying: (() => void) | undefined
      let receivedSignal: AbortSignal | undefined
      const events: OperationEvent[] = []
      const adapter: Adapter<void, string> = {
        capabilities: () => ({ abort: "unsupported", replay: "safe" }),
        execute: async (_args, context) => {
          receivedSignal = context.signal
          return new Promise((resolve) => {
            resolveUnderlying = () => resolve("late success")
          })
        },
      }
      const subject = operation({
        name: "unabortable-timeout",
        adapter,
        policies: [timeout({ ms: 10 })],
        events: { emit: (event) => events.push(event) },
      })

      const execution = subject.execute(undefined)
      const rejected = expect(execution).rejects.toBeInstanceOf(TimeoutError)
      await vi.advanceTimersByTimeAsync(10)

      await rejected
      expect(receivedSignal).toBeUndefined()
      expect(
        events.find((event) => event.type === "timeout.triggered"),
      ).toMatchObject({
        abortRequested: false,
      })
      resolveUnderlying?.()
    } finally {
      vi.useRealTimers()
    }
  })
})

// ---------------------------------------------------------------------------
// Duration bounds
// ---------------------------------------------------------------------------

describe("timeout duration bounds", () => {
  it("rejects a duration the platform cannot schedule", () => {
    // setTimeout clamps anything above 2147483647 ms to 1 ms, so this would
    // silently become an immediate timeout instead of a ~24 day one.
    expect(() => timeout({ ms: 2_147_483_648 })).toThrow(RangeError)
    expect(() => timeout({ ms: 2_147_483_648 })).toThrow(/setTimeout/)
  })

  it("accepts the largest schedulable duration", () => {
    expect(() => timeout({ ms: 2_147_483_647 })).not.toThrow()
  })
})
