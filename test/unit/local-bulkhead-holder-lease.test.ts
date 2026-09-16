import { describe, expect, it, vi } from "vitest"
import type { OperationEvent } from "../../src/index.js"
import { BulkheadRejectedError, bulkhead, operation } from "../../src/index.js"

describe("bulkhead.local — permit lease", () => {
  it("aborts and releases an abort-supported holder when its lease expires", async () => {
    vi.useFakeTimers()
    try {
      const policy = bulkhead.local({ name: "lease", limit: 1, leaseMs: 50 })
      const subject = operation({
        name: "lease",
        adapter: {
          capabilities: () => ({
            abort: "supported" as const,
            replay: "safe" as const,
          }),
          execute: async (_args, context) =>
            await new Promise<string>((_resolve, reject) => {
              const abort = () => reject(context.signal?.reason)
              if (context.signal?.aborted) return abort()
              context.signal?.addEventListener("abort", abort, { once: true })
            }),
        },
        policies: [policy],
      })

      const attempt = subject.execute(undefined)
      expect(policy.snapshot().occupancy).toBe(1)

      vi.advanceTimersByTime(50)
      await expect(attempt).rejects.toBeInstanceOf(BulkheadRejectedError)
      expect(policy.snapshot().occupancy).toBe(0)
    } finally {
      vi.useRealTimers()
    }
  })

  it("keeps an abort-unsupported holder but emits lease-lost", async () => {
    vi.useFakeTimers()
    try {
      const policy = bulkhead.local({ name: "lease", limit: 1, leaseMs: 50 })
      const events: OperationEvent[] = []
      const subject = operation({
        name: "lease",
        adapter: {
          capabilities: () => ({
            abort: "unsupported" as const,
            replay: "safe" as const,
          }),
          execute: async () => await new Promise<never>(() => {}),
        },
        policies: [policy],
        events: { emit: (event) => events.push(event) },
      })

      void subject.execute(undefined)
      expect(policy.snapshot().occupancy).toBe(1)

      vi.advanceTimersByTime(50)
      expect(events.some((event) => event.type === "bulkhead.lease-lost")).toBe(
        true,
      )
      // The holder ignores the abort; the permit cannot be reclaimed.
      expect(policy.snapshot().occupancy).toBe(1)
    } finally {
      vi.useRealTimers()
    }
  })

  it("grants a queued successor after the lease reclaims a permit", async () => {
    vi.useFakeTimers()
    try {
      const policy = bulkhead.local({
        name: "lease",
        limit: 1,
        queue: { limit: 1, timeoutMs: 10_000 },
        leaseMs: 50,
      })
      let calls = 0
      const subject = operation({
        name: "lease",
        adapter: {
          capabilities: () => ({
            abort: "supported" as const,
            replay: "safe" as const,
          }),
          execute: async (_args, context) => {
            calls += 1
            if (calls === 1) {
              return await new Promise<string>((_resolve, reject) => {
                const abort = () => reject(context.signal?.reason)
                if (context.signal?.aborted) return abort()
                context.signal?.addEventListener("abort", abort, { once: true })
              })
            }
            return "ok"
          },
        },
        policies: [policy],
      })

      const first = subject.execute(undefined)
      const second = subject.execute(undefined)
      expect(policy.snapshot().occupancy).toBe(1)
      expect(policy.snapshot().waiting).toBe(1)

      vi.advanceTimersByTime(50)
      await expect(first).rejects.toBeInstanceOf(BulkheadRejectedError)
      await expect(second).resolves.toBe("ok")
      expect(policy.snapshot().occupancy).toBe(0)
    } finally {
      vi.useRealTimers()
    }
  })

  it("rejects leaseMs outside 1..2147483647", () => {
    expect(() => bulkhead.local({ name: "x", limit: 1, leaseMs: 0 })).toThrow()
    expect(() => bulkhead.local({ name: "x", limit: 1, leaseMs: -1 })).toThrow()
    expect(() =>
      bulkhead.local({ name: "x", limit: 1, leaseMs: 1.5 }),
    ).toThrow()
    expect(() =>
      bulkhead.local({ name: "x", limit: 1, leaseMs: 2147483648 }),
    ).toThrow()
    expect(() =>
      bulkhead.local({ name: "x", limit: 1, leaseMs: 50 }),
    ).not.toThrow()
  })
})
