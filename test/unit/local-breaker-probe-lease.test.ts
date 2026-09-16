import { describe, expect, it, vi } from "vitest"
import type { OperationEvent } from "../../src/index.js"
import { CircuitOpenError, circuitBreaker, operation } from "../../src/index.js"
import { Deferred } from "../support/deferred.js"

// ---------------------------------------------------------------------------
// Local breaker probe lease
//
// A half-open probe holds its slot until the attempt settles, but no longer
// indefinitely: the local breaker arms a per-probe lease (`probeLeaseTtlMs`)
// at admission. A hung adapter is reclaimed when the lease expires, and a
// late settle is dropped as stale, so it can neither double-release nor
// transition the breaker.
// ---------------------------------------------------------------------------

const traits = () => ({
  abort: "unsupported" as const,
  replay: "safe" as const,
})

describe("circuitBreaker.local — probe lease", () => {
  it("releases a hung probe's slot when the lease expires", async () => {
    vi.useFakeTimers()
    try {
      const policy = circuitBreaker.local({
        name: "lease",
        minimumThroughput: 1,
        failureThreshold: 0.5,
        openMs: 10,
        halfOpenSuccesses: 1,
        probeLeaseTtlMs: 50,
      })
      const events: OperationEvent[] = []
      let calls = 0
      const subject = operation({
        name: "lease",
        adapter: {
          capabilities: traits,
          execute: async () => {
            calls += 1
            if (calls === 1) throw new Error("boom")
            return await new Promise<never>(() => {})
          },
        },
        policies: [policy],
        events: { emit: (event) => events.push(event) },
      })

      await subject.execute(undefined).catch(() => {})
      expect(policy.snapshot().state).toBe("open")

      vi.advanceTimersByTime(10)
      void subject.execute(undefined)
      expect(policy.snapshot().state).toBe("half-open")
      expect(policy.snapshot().probesInFlight).toBe(1)

      // While the probe is hung the slot is held, so further calls are shed.
      await expect(subject.execute(undefined)).rejects.toBeInstanceOf(
        CircuitOpenError,
      )

      vi.advanceTimersByTime(50)
      expect(policy.snapshot().probesInFlight).toBe(0)
      expect(
        events.some((event) => event.type === "breaker.probe-expired"),
      ).toBe(true)
    } finally {
      vi.useRealTimers()
    }
  })

  it("drops a late settle after the lease has expired", async () => {
    vi.useFakeTimers()
    try {
      const policy = circuitBreaker.local({
        name: "late",
        minimumThroughput: 1,
        failureThreshold: 0.5,
        openMs: 10,
        halfOpenSuccesses: 1,
        probeLeaseTtlMs: 50,
      })
      const events: OperationEvent[] = []
      const gate = new Deferred<"ok">()
      let calls = 0
      const subject = operation({
        name: "late",
        adapter: {
          capabilities: traits,
          execute: async () => {
            calls += 1
            if (calls === 1) throw new Error("boom")
            return await gate.promise
          },
        },
        policies: [policy],
        events: { emit: (event) => events.push(event) },
      })

      await subject.execute(undefined).catch(() => {})
      vi.advanceTimersByTime(10)
      const probe = subject.execute(undefined)
      expect(policy.snapshot().state).toBe("half-open")
      expect(policy.snapshot().probesInFlight).toBe(1)

      vi.advanceTimersByTime(50)
      expect(policy.snapshot().probesInFlight).toBe(0)

      const observationsBefore = events.filter(
        (event) => event.type === "breaker.observation",
      ).length

      // The attempt finally settles, but its slot is already gone.
      gate.resolve("ok")
      await vi.advanceTimersByTimeAsync(0)
      await probe

      // Stale: no observation recorded, no transition, no double-release.
      expect(policy.snapshot().state).toBe("half-open")
      expect(policy.snapshot().probesInFlight).toBe(0)
      expect(
        events.filter((event) => event.type === "breaker.observation").length,
      ).toBe(observationsBefore)
    } finally {
      vi.useRealTimers()
    }
  })

  it("defaults the probe lease to openMs × 2", async () => {
    vi.useFakeTimers()
    try {
      const policy = circuitBreaker.local({
        name: "default-lease",
        minimumThroughput: 1,
        failureThreshold: 0.5,
        openMs: 10,
      })
      let calls = 0
      const subject = operation({
        name: "default-lease",
        adapter: {
          capabilities: traits,
          execute: async () => {
            calls += 1
            if (calls === 1) throw new Error("boom")
            return await new Promise<never>(() => {})
          },
        },
        policies: [policy],
      })

      await subject.execute(undefined).catch(() => {})
      vi.advanceTimersByTime(10)
      void subject.execute(undefined)
      expect(policy.snapshot().probesInFlight).toBe(1)

      // openMs × 2 = 20ms: still held just before, released just after.
      vi.advanceTimersByTime(19)
      expect(policy.snapshot().probesInFlight).toBe(1)
      vi.advanceTimersByTime(2)
      expect(policy.snapshot().probesInFlight).toBe(0)
    } finally {
      vi.useRealTimers()
    }
  })
  it("never admits more than halfOpenProbes, and recovers when they all hang", async () => {
    vi.useFakeTimers()
    try {
      const policy = circuitBreaker.local({
        name: "saturate",
        minimumThroughput: 1,
        failureThreshold: 0.5,
        openMs: 10,
        halfOpenProbes: 3,
        halfOpenSuccesses: 1,
        probeLeaseTtlMs: 50,
      })
      let calls = 0
      const subject = operation({
        name: "saturate",
        adapter: {
          capabilities: traits,
          execute: async () => {
            calls += 1
            if (calls === 1) throw new Error("boom")
            return await new Promise<never>(() => {})
          },
        },
        policies: [policy],
      })

      await subject.execute(undefined).catch(() => {})
      vi.advanceTimersByTime(10)

      void subject.execute(undefined)
      void subject.execute(undefined)
      void subject.execute(undefined)
      expect(policy.snapshot().probesInFlight).toBe(3)

      await expect(subject.execute(undefined)).rejects.toBeInstanceOf(
        CircuitOpenError,
      )

      vi.advanceTimersByTime(50)
      expect(policy.snapshot().probesInFlight).toBe(0)
    } finally {
      vi.useRealTimers()
    }
  })

  it("a transition discards an outstanding probe's lease", async () => {
    vi.useFakeTimers()
    try {
      const policy = circuitBreaker.local({
        name: "clear",
        minimumThroughput: 1,
        failureThreshold: 0.5,
        openMs: 10,
        halfOpenProbes: 2,
        halfOpenSuccesses: 1,
        probeLeaseTtlMs: 50,
      })
      const events: OperationEvent[] = []
      let calls = 0
      const subject = operation({
        name: "clear",
        adapter: {
          capabilities: traits,
          execute: async () => {
            calls += 1
            if (calls === 1) throw new Error("boom")
            if (calls === 2) return await new Promise<never>(() => {})
            throw new Error("probe failure")
          },
        },
        policies: [policy],
        events: { emit: (event) => events.push(event) },
      })

      await subject.execute(undefined).catch(() => {})
      vi.advanceTimersByTime(10)

      // First probe hangs, holding a slot and a lease.
      void subject.execute(undefined)
      expect(policy.snapshot().probesInFlight).toBe(1)

      // Second probe fails: reopening discards the hung probe's lease.
      await subject.execute(undefined).catch(() => {})
      expect(policy.snapshot().state).toBe("open")
      expect(policy.snapshot().probesInFlight).toBe(0)

      // The hung probe's timer was cleared, so nothing expires later.
      vi.advanceTimersByTime(100)
      expect(
        events.filter((event) => event.type === "breaker.probe-expired").length,
      ).toBe(0)
    } finally {
      vi.useRealTimers()
    }
  })

  it("rejects probeLeaseTtlMs outside 1..2147483647", () => {
    expect(() =>
      circuitBreaker.local({ name: "x", probeLeaseTtlMs: 0 }),
    ).toThrow(/probeLeaseTtlMs/)
    expect(() =>
      circuitBreaker.local({ name: "x", probeLeaseTtlMs: -1 }),
    ).toThrow(/probeLeaseTtlMs/)
    expect(() =>
      circuitBreaker.local({ name: "x", probeLeaseTtlMs: 1.5 }),
    ).toThrow(/probeLeaseTtlMs/)
    expect(() =>
      circuitBreaker.local({ name: "x", probeLeaseTtlMs: 2147483648 }),
    ).toThrow(/probeLeaseTtlMs/)
    expect(() =>
      circuitBreaker.local({ name: "x", probeLeaseTtlMs: 50 }),
    ).not.toThrow()
  })
})
