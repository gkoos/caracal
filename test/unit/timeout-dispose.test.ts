import { describe, expect, it, vi } from "vitest"
import type { Outcome } from "../../src/index.js"
import { operation, retry, timeout, TimeoutError } from "../../src/index.js"
import { Deferred } from "../support/deferred.js"

describe("timeout — dispose of a superseded result", () => {
  it("disposes a value that settles after the deadline", async () => {
    vi.useFakeTimers()
    try {
      const disposed: Array<Outcome<unknown>> = []
      const gate = new Deferred<string>()
      const subject = operation({
        name: "t",
        adapter: {
          capabilities: () => ({
            abort: "unsupported" as const,
            replay: "safe" as const,
          }),
          execute: async () => await gate.promise,
          dispose: (outcome) => {
            disposed.push(outcome)
          },
        },
        policies: [timeout({ ms: 10 })],
      })

      const attempt = subject.execute(undefined)
      const rejected = expect(attempt).rejects.toBeInstanceOf(TimeoutError)
      await vi.advanceTimersByTimeAsync(10)
      await rejected
      expect(disposed).toHaveLength(0)

      gate.resolve("late")
      await vi.advanceTimersByTimeAsync(0)
      expect(disposed).toEqual([{ status: "success", value: "late" }])
    } finally {
      vi.useRealTimers()
    }
  })

  it("does not dispose a value that settles before the deadline", async () => {
    vi.useFakeTimers()
    try {
      const disposed: Array<Outcome<unknown>> = []
      const gate = new Deferred<string>()
      const subject = operation({
        name: "t",
        adapter: {
          capabilities: () => ({
            abort: "unsupported" as const,
            replay: "safe" as const,
          }),
          execute: async () => await gate.promise,
          dispose: (outcome) => {
            disposed.push(outcome)
          },
        },
        policies: [timeout({ ms: 10 })],
      })

      const attempt = subject.execute(undefined)
      gate.resolve("ok")
      await expect(attempt).resolves.toBe("ok")
      expect(disposed).toHaveLength(0)
    } finally {
      vi.useRealTimers()
    }
  })

  it("disposes an error that settles after the deadline", async () => {
    vi.useFakeTimers()
    try {
      const disposed: Array<Outcome<unknown>> = []
      const gate = new Deferred<never>()
      const subject = operation({
        name: "t",
        adapter: {
          capabilities: () => ({
            abort: "unsupported" as const,
            replay: "safe" as const,
          }),
          execute: async () => await gate.promise,
          dispose: (outcome) => {
            disposed.push(outcome)
          },
        },
        policies: [timeout({ ms: 10 })],
      })

      const attempt = subject.execute(undefined)
      const rejected = expect(attempt).rejects.toBeInstanceOf(TimeoutError)
      await vi.advanceTimersByTimeAsync(10)
      await rejected

      gate.reject(new Error("late error"))
      await vi.advanceTimersByTimeAsync(0)
      expect(disposed).toHaveLength(1)
      expect(disposed[0].status).toBe("failure")
    } finally {
      vi.useRealTimers()
    }
  })

  it("does not double-dispose when retry is also present", async () => {
    vi.useFakeTimers()
    try {
      const disposed: Array<Outcome<unknown>> = []
      let calls = 0
      const gate = new Deferred<string>()
      const subject = operation({
        name: "t",
        adapter: {
          capabilities: () => ({
            abort: "unsupported" as const,
            replay: "safe" as const,
          }),
          execute: async () => {
            calls += 1
            if (calls === 1) return "r1"
            return await gate.promise
          },
          classify: () => (calls === 1 ? "retryable" : "success"),
          dispose: (outcome) => {
            disposed.push(outcome)
          },
        },
        policies: [timeout({ ms: 100 }), retry({ maxAttempts: 2 })],
      })

      const attempt = subject.execute(undefined)
      const rejected = expect(attempt).rejects.toBeInstanceOf(TimeoutError)
      await vi.advanceTimersByTimeAsync(100)
      await rejected
      expect(disposed).toEqual([{ status: "success", value: "r1" }])

      gate.resolve("r2")
      await vi.advanceTimersByTimeAsync(0)
      expect(disposed).toEqual([
        { status: "success", value: "r1" },
        { status: "success", value: "r2" },
      ])
    } finally {
      vi.useRealTimers()
    }
  })
})
