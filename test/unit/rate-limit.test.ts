import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import type { OperationEvent, Policy } from "../../src/index.js"
import {
  operation,
  RateLimitExceededError,
  rateLimit,
} from "../../src/index.js"

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const traits = () => ({
  abort: "unsupported" as const,
  replay: "safe" as const,
})

function subject(
  policy: Policy,
  work: () => Promise<unknown>,
  events: OperationEvent[] = [],
) {
  return operation({
    name: "work",
    adapter: { capabilities: traits, execute: work },
    policies: [policy],
    events: { emit: (e) => events.push(e) },
  })
}

const ok = () => Promise.resolve("ok")

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

describe("rateLimit.local — validation", () => {
  it("rejects an empty name", () => {
    expect(() => rateLimit.local({ name: "", rate: 10 })).toThrow()
    expect(() => rateLimit.local({ name: "   ", rate: 10 })).toThrow()
  })

  it("rejects a non-positive or non-finite rate", () => {
    expect(() => rateLimit.local({ name: "x", rate: 0 })).toThrow()
    expect(() => rateLimit.local({ name: "x", rate: -1 })).toThrow()
    expect(() => rateLimit.local({ name: "x", rate: Number.NaN })).toThrow()
  })

  it("rejects a rate above 1000/s (sub-millisecond emission interval)", () => {
    expect(() => rateLimit.local({ name: "x", rate: 2000 })).toThrow()
  })

  it("rejects a non-positive or non-integer burst", () => {
    expect(() => rateLimit.local({ name: "x", rate: 10, burst: 0 })).toThrow()
    expect(() => rateLimit.local({ name: "x", rate: 10, burst: 1.5 })).toThrow()
  })
})

// ---------------------------------------------------------------------------
// GCRA behaviour
// ---------------------------------------------------------------------------

describe("rateLimit.local — GCRA behaviour", () => {
  beforeEach(() => {
    vi.useFakeTimers()
    vi.setSystemTime(0)
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it("admits then rejects past the sustained rate, with a retry-after hint", async () => {
    const policy = rateLimit.local({ name: "x", rate: 1000 }) // 1ms interval
    const op = subject(policy, ok)

    await expect(op.execute(undefined)).resolves.toBe("ok")
    await expect(op.execute(undefined)).rejects.toMatchObject({
      name: "RateLimitExceededError",
      coordination: "local",
      policyName: "x",
      scope: "process",
      retryAfterMs: 1,
    })

    // Once the emission interval elapses the next request is admissible again.
    vi.advanceTimersByTime(1)
    await expect(op.execute(undefined)).resolves.toBe("ok")
  })

  it("admits a configured burst before enforcing the strict rate", async () => {
    const policy = rateLimit.local({ name: "x", rate: 1000, burst: 2 })
    const op = subject(policy, ok)

    await expect(op.execute(undefined)).resolves.toBe("ok") // t=0, tat=1
    await expect(op.execute(undefined)).resolves.toBe("ok") // t=0, within burst
    await expect(op.execute(undefined)).rejects.toBeInstanceOf(
      RateLimitExceededError,
    ) // burst exhausted
  })

  it("emits admitted and rejected events with a closed reason set", async () => {
    const events: OperationEvent[] = []
    const op = subject(rateLimit.local({ name: "x", rate: 1000 }), ok, events)

    await op.execute(undefined)
    await op.execute(undefined).catch(() => {})

    expect(events.filter((e) => e.type === "ratelimit.admitted")).toHaveLength(
      1,
    )
    const rejected = events.find((e) => e.type === "ratelimit.rejected")
    expect(rejected).toMatchObject({
      coordination: "local",
      policyName: "x",
      scope: "process",
      reason: "rate-exceeded",
      retryAfterMs: 1,
    })
  })

  it("reports nextAllowedAt via snapshot", () => {
    const policy = rateLimit.local({ name: "x", rate: 1000 })
    expect(policy.snapshot()).toEqual({
      coordination: "local",
      nextAllowedAt: 0,
    })
  })
})
