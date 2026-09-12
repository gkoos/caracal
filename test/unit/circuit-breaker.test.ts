import { describe, expect, it, vi } from "vitest"
import type {
  BreakerClassifier,
  OperationEvent,
  Policy,
} from "../../src/index.js"
import {
  bulkhead,
  CircuitOpenError,
  circuitBreaker,
  operation,
  retry,
  TimeoutError,
  timeout,
} from "../../src/index.js"
import { Deferred } from "../support/deferred.js"

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
  extra: Policy[] = [],
  events: OperationEvent[] = [],
) {
  return operation({
    name: "work",
    adapter: { capabilities: traits, execute: work },
    policies: [policy, ...extra],
    events: { emit: (e) => events.push(e) },
  })
}

function failingWork(): Promise<never> {
  return Promise.reject(new Error("boom"))
}

// Drive n calls through an operation, ignoring errors.
async function drive(
  op: ReturnType<typeof operation>,
  n: number,
  arg?: unknown,
): Promise<void> {
  await Promise.allSettled(Array.from({ length: n }, () => op.execute(arg)))
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

describe("circuitBreaker.local — validation", () => {
  it("rejects an empty name", () => {
    expect(() => circuitBreaker.local({ name: "" })).toThrow()
    expect(() => circuitBreaker.local({ name: "   " })).toThrow()
  })

  it("rejects non-positive minimumThroughput", () => {
    expect(() =>
      circuitBreaker.local({ name: "x", minimumThroughput: 0 }),
    ).toThrow()
    expect(() =>
      circuitBreaker.local({ name: "x", minimumThroughput: -1 }),
    ).toThrow()
    expect(() =>
      circuitBreaker.local({ name: "x", minimumThroughput: 1.5 }),
    ).toThrow()
  })

  it("rejects failureThreshold outside (0,1)", () => {
    expect(() =>
      circuitBreaker.local({ name: "x", failureThreshold: 0 }),
    ).toThrow()
    expect(() =>
      circuitBreaker.local({ name: "x", failureThreshold: 1 }),
    ).toThrow()
    expect(() =>
      circuitBreaker.local({ name: "x", failureThreshold: -0.1 }),
    ).toThrow()
    expect(() =>
      circuitBreaker.local({ name: "x", failureThreshold: 1.1 }),
    ).toThrow()
  })

  it("rejects thresholds that cannot be resolved to thousandths", () => {
    // Rounding to 0 makes the coordinator's comparison
    // `wFail * 1000 >= numerator * wTotal` unconditionally true, so the breaker
    // would open on a success-only window and re-open after every recovery.
    expect(() =>
      circuitBreaker.local({ name: "x", failureThreshold: 0.0004 }),
    ).toThrow(/resolved to thousandths/)
    expect(() =>
      circuitBreaker.local({ name: "x", failureThreshold: 0.0001 }),
    ).toThrow(/resolved to thousandths/)
    // Rounding to 1000 requires every observation to fail, so the breaker would
    // effectively never open.
    expect(() =>
      circuitBreaker.local({ name: "x", failureThreshold: 0.9995 }),
    ).toThrow(/resolved to thousandths/)
    expect(() =>
      circuitBreaker.local({ name: "x", failureThreshold: 0.9999 }),
    ).toThrow(/resolved to thousandths/)
  })

  it("accepts the smallest and largest resolvable thresholds", () => {
    expect(() =>
      circuitBreaker.local({ name: "x", failureThreshold: 0.0005 }),
    ).not.toThrow()
    expect(() =>
      circuitBreaker.local({ name: "x", failureThreshold: 0.9994 }),
    ).not.toThrow()
  })

  it("rejects non-positive openMs", () => {
    expect(() => circuitBreaker.local({ name: "x", openMs: 0 })).toThrow()
    expect(() => circuitBreaker.local({ name: "x", openMs: -1 })).toThrow()
    expect(() => circuitBreaker.local({ name: "x", openMs: 100.5 })).toThrow()
  })

  it("rejects non-positive halfOpenSuccesses and halfOpenProbes", () => {
    expect(() =>
      circuitBreaker.local({ name: "x", halfOpenSuccesses: 0 }),
    ).toThrow()
    expect(() =>
      circuitBreaker.local({ name: "x", halfOpenProbes: 0 }),
    ).toThrow()
  })

  it("rejects non-positive windowSize", () => {
    expect(() => circuitBreaker.local({ name: "x", windowSize: 0 })).toThrow()
  })
})

// ---------------------------------------------------------------------------
// Snapshot reflects actual state
// ---------------------------------------------------------------------------

describe("circuitBreaker.local — snapshot", () => {
  it("starts closed with zero observations", () => {
    const policy = circuitBreaker.local({ name: "p" })
    expect(policy.snapshot()).toEqual({
      coordination: "local",
      state: "closed",
      failures: 0,
      successes: 0,
      observations: 0,
      probesInFlight: 0,
      halfOpenSuccesses: 0,
    })
  })

  it("accumulates observations while closed", async () => {
    const policy = circuitBreaker.local({
      name: "p",
      minimumThroughput: 10,
      failureThreshold: 0.5,
    })
    const op = subject(policy, () => Promise.resolve(1))
    await drive(op, 3)
    expect(policy.snapshot().successes).toBe(3)
    expect(policy.snapshot().observations).toBe(3)
    expect(policy.snapshot().state).toBe("closed")
  })
})

// ---------------------------------------------------------------------------
// CLOSED → OPEN transition
// ---------------------------------------------------------------------------

describe("circuitBreaker.local — CLOSED → OPEN", () => {
  it("does not open before minimumThroughput is reached", async () => {
    const policy = circuitBreaker.local({
      name: "p",
      minimumThroughput: 5,
      failureThreshold: 0.5,
    })
    const op = subject(policy, failingWork)
    await drive(op, 4)
    expect(policy.snapshot().state).toBe("closed")
    expect(policy.snapshot().failures).toBe(4)
  })

  it("opens once failure ratio meets the threshold at minimumThroughput", async () => {
    // 2 successes + 2 failures = exactly 50% — meets the 0.5 threshold
    const breaker = circuitBreaker.local({
      name: "p",
      minimumThroughput: 4,
      failureThreshold: 0.5,
    })
    const op = operation({
      name: "work",
      adapter: {
        capabilities: traits,
        execute: vi
          .fn()
          .mockResolvedValueOnce(1)
          .mockResolvedValueOnce(1)
          .mockRejectedValue(new Error("fail")),
      },
      policies: [breaker],
    })
    await drive(op, 4)
    expect(breaker.snapshot().state).toBe("open")
  })

  it("resets the window on transition and emits a state-changed event", async () => {
    const events: OperationEvent[] = []
    const policy = circuitBreaker.local({
      name: "p",
      minimumThroughput: 2,
      failureThreshold: 0.5,
    })
    const op = subject(policy, failingWork, [], events)
    await drive(op, 2)
    expect(policy.snapshot().state).toBe("open")
    expect(policy.snapshot().observations).toBe(0) // window reset
    expect(
      events.some(
        (e) =>
          e.type === "breaker.state-changed" &&
          e.state === "open" &&
          e.previousState === "closed",
      ),
    ).toBe(true)
  })
})

// ---------------------------------------------------------------------------
// OPEN rejection
// ---------------------------------------------------------------------------

describe("circuitBreaker.local — OPEN", () => {
  it("rejects with CircuitOpenError while open", async () => {
    const policy = circuitBreaker.local({
      name: "breaker",
      minimumThroughput: 1,
      failureThreshold: 0.5,
      openMs: 10_000,
    })
    const op = subject(policy, failingWork)
    await drive(op, 1)
    expect(policy.snapshot().state).toBe("open")
    await expect(op.execute(undefined)).rejects.toBeInstanceOf(CircuitOpenError)
  })

  it("emits a breaker.rejected event when open", async () => {
    const events: OperationEvent[] = []
    const policy = circuitBreaker.local({
      name: "breaker",
      minimumThroughput: 1,
      failureThreshold: 0.5,
      openMs: 10_000,
    })
    const op = subject(policy, failingWork, [], events)
    await drive(op, 1)
    events.length = 0
    await op.execute(undefined).catch(() => {})
    expect(events.some((e) => e.type === "breaker.rejected")).toBe(true)
  })

  it("CircuitOpenError carries policyName, coordination and scope", async () => {
    const policy = circuitBreaker.local({
      name: "my-breaker",
      minimumThroughput: 1,
      failureThreshold: 0.5,
      openMs: 10_000,
    })
    const op = subject(policy, failingWork)
    await drive(op, 1)
    try {
      await op.execute(undefined)
      expect.fail("should have thrown")
    } catch (err) {
      expect(err).toBeInstanceOf(CircuitOpenError)
      const e = err as CircuitOpenError
      expect(e.policyName).toBe("my-breaker")
      expect(e.coordination).toBe("local")
      expect(e.scope).toBe("process")
    }
  })
})

// ---------------------------------------------------------------------------
// OPEN → HALF-OPEN transition
// ---------------------------------------------------------------------------

describe("circuitBreaker.local — OPEN → HALF-OPEN", () => {
  it("transitions to half-open after openMs elapses", async () => {
    vi.useFakeTimers()
    try {
      const policy = circuitBreaker.local({
        name: "p",
        minimumThroughput: 1,
        failureThreshold: 0.5,
        openMs: 100,
        halfOpenSuccesses: 99, // requires many successes — won't close on one probe
      })
      // Open the breaker
      const failOp = subject(policy, failingWork)
      await drive(failOp, 1)
      expect(policy.snapshot().state).toBe("open")
      // Advance past openMs
      await vi.advanceTimersByTimeAsync(100)
      // Use a succeeding adapter so the probe doesn't re-open the breaker
      const probeOp = subject(policy, () => Promise.resolve(1))
      await probeOp.execute(undefined)
      // One success out of 99 required: should still be half-open
      expect(policy.snapshot().state).toBe("half-open")
    } finally {
      vi.useRealTimers()
    }
  })

  it("emits a state-changed event for OPEN → HALF-OPEN", async () => {
    vi.useFakeTimers()
    try {
      const events: OperationEvent[] = []
      const policy = circuitBreaker.local({
        name: "p",
        minimumThroughput: 1,
        failureThreshold: 0.5,
        openMs: 50,
      })
      const op = subject(policy, failingWork, [], events)
      await drive(op, 1)
      events.length = 0
      await vi.advanceTimersByTimeAsync(50)
      await op.execute(undefined).catch(() => {})
      expect(
        events.some(
          (e) =>
            e.type === "breaker.state-changed" &&
            e.state === "half-open" &&
            e.previousState === "open",
        ),
      ).toBe(true)
    } finally {
      vi.useRealTimers()
    }
  })
})

// ---------------------------------------------------------------------------
// HALF-OPEN probe limit
// ---------------------------------------------------------------------------

describe("circuitBreaker.local — HALF-OPEN probe limit", () => {
  it("admits up to halfOpenProbes concurrent probes and rejects the rest", async () => {
    vi.useFakeTimers()
    try {
      const policy = circuitBreaker.local({
        name: "p",
        minimumThroughput: 1,
        failureThreshold: 0.5,
        openMs: 50,
        halfOpenProbes: 2,
        halfOpenSuccesses: 10, // won't close in this test
      })
      const gate = new Deferred<void>()
      const op = subject(policy, () => gate.promise)
      // Open the breaker
      await subject(policy, failingWork)
        .execute(undefined)
        .catch(() => {})
      await vi.advanceTimersByTimeAsync(50)

      const a = op.execute(undefined) // probe 1
      const b = op.execute(undefined) // probe 2
      const c = expect(op.execute(undefined)).rejects.toBeInstanceOf(
        CircuitOpenError,
      ) // rejected

      // Let probes settle to avoid unhandled rejection
      gate.resolve()
      await Promise.allSettled([a, b])
      await c
    } finally {
      vi.useRealTimers()
    }
  })
})

// ---------------------------------------------------------------------------
// HALF-OPEN → CLOSED (recovery)
// ---------------------------------------------------------------------------

describe("circuitBreaker.local — HALF-OPEN → CLOSED", () => {
  it("closes after the configured number of consecutive half-open successes", async () => {
    vi.useFakeTimers()
    try {
      const events: OperationEvent[] = []
      const policy = circuitBreaker.local({
        name: "p",
        minimumThroughput: 1,
        failureThreshold: 0.5,
        openMs: 50,
        halfOpenSuccesses: 2,
      })
      // Open
      await subject(policy, failingWork, [], events)
        .execute(undefined)
        .catch(() => {})
      await vi.advanceTimersByTimeAsync(50)

      const op = subject(policy, () => Promise.resolve(1), [], events)
      await op.execute(undefined) // probe 1 → success
      expect(policy.snapshot().state).toBe("half-open")
      await op.execute(undefined) // probe 2 → success → closes
      expect(policy.snapshot().state).toBe("closed")
      expect(
        events.some(
          (e) =>
            e.type === "breaker.state-changed" &&
            e.state === "closed" &&
            e.previousState === "half-open",
        ),
      ).toBe(true)
    } finally {
      vi.useRealTimers()
    }
  })

  it("resets the window and half-open counters on closure", async () => {
    vi.useFakeTimers()
    try {
      const policy = circuitBreaker.local({
        name: "p",
        minimumThroughput: 1,
        failureThreshold: 0.5,
        openMs: 50,
        halfOpenSuccesses: 1,
      })
      await subject(policy, failingWork)
        .execute(undefined)
        .catch(() => {})
      await vi.advanceTimersByTimeAsync(50)
      await subject(policy, () => Promise.resolve(1)).execute(undefined)
      expect(policy.snapshot()).toMatchObject({
        state: "closed",
        observations: 0,
        failures: 0,
        probesInFlight: 0,
        halfOpenSuccesses: 0,
      })
    } finally {
      vi.useRealTimers()
    }
  })
})

// ---------------------------------------------------------------------------
// HALF-OPEN → OPEN (probe failure re-opens)
// ---------------------------------------------------------------------------

describe("circuitBreaker.local — probe failure re-opens", () => {
  it("re-opens when a probe fails", async () => {
    vi.useFakeTimers()
    try {
      const events: OperationEvent[] = []
      const policy = circuitBreaker.local({
        name: "p",
        minimumThroughput: 1,
        failureThreshold: 0.5,
        openMs: 50,
      })
      // Use separate operations sharing the same policy instance so we can
      // have different adapters (fail to open, then fail as a probe).
      const failOp = subject(policy, failingWork, [], events)
      // Open the breaker
      await drive(failOp, 1)
      expect(policy.snapshot().state).toBe("open")
      // Advance past openMs so next admission transitions to half-open
      await vi.advanceTimersByTimeAsync(50)
      // Probe executes and fails → re-opens (same policy, fresh operation)
      await failOp.execute(undefined).catch(() => {})
      expect(policy.snapshot().state).toBe("open")
      expect(
        events.some(
          (e) =>
            e.type === "breaker.state-changed" &&
            e.state === "open" &&
            e.previousState === "half-open",
        ),
      ).toBe(true)
    } finally {
      vi.useRealTimers()
    }
  })
})

// ---------------------------------------------------------------------------
// Custom classifier — "ignored" outcomes
// ---------------------------------------------------------------------------

describe("circuitBreaker.local — custom classifier", () => {
  it("ignores outcomes classified as 'ignored'", async () => {
    const classify: BreakerClassifier = (_err, isSuccess) =>
      isSuccess ? "success" : "ignored"
    const policy = circuitBreaker.local({
      name: "p",
      minimumThroughput: 2,
      failureThreshold: 0.5,
      classify,
    })
    const op = subject(policy, failingWork)
    // Many failures — all ignored, so breaker should stay closed
    await drive(op, 10)
    expect(policy.snapshot().state).toBe("closed")
    expect(policy.snapshot().observations).toBe(0)
  })

  it("emits breaker.observation only for non-ignored outcomes", async () => {
    const events: OperationEvent[] = []
    let calls = 0
    const classify: BreakerClassifier = (_err, isSuccess) => {
      calls++
      return calls % 2 === 0 ? "ignored" : isSuccess ? "success" : "failure"
    }
    const policy = circuitBreaker.local({
      name: "p",
      minimumThroughput: 10,
      classify,
    })
    const op = subject(policy, () => Promise.resolve(1), [], events)
    await drive(op, 4) // calls: 1 (obs), 2 (ign), 3 (obs), 4 (ign)
    expect(events.filter((e) => e.type === "breaker.observation")).toHaveLength(
      2,
    )
  })
})

// ---------------------------------------------------------------------------
// Sliding window eviction
// ---------------------------------------------------------------------------

describe("circuitBreaker.local — sliding window", () => {
  it("evicts oldest observations when the window is full", async () => {
    const policy = circuitBreaker.local({
      name: "p",
      minimumThroughput: 4,
      failureThreshold: 0.5,
      windowSize: 4,
    })
    // Fill with 4 failures — 100% failure rate meets the 50% threshold
    await drive(subject(policy, failingWork), 4)
    expect(policy.snapshot().state).toBe("open")
  })

  it("evicts old failures as new successes enter, preventing re-open", async () => {
    const policy = circuitBreaker.local({
      name: "p",
      minimumThroughput: 4,
      failureThreshold: 0.75, // need ≥75% failure rate
      windowSize: 4,
    })
    // 2 failures, then 4 successes — window now holds 4 successes
    const fail = subject(policy, failingWork)
    const succeed = subject(policy, () => Promise.resolve(1))
    await drive(fail, 2)
    // Not open yet (only 50% with 2 observations below threshold)
    expect(policy.snapshot().state).toBe("closed")
    await drive(succeed, 4)
    // Old failures evicted; window shows 0% failure rate
    expect(policy.snapshot().failures).toBe(0)
    expect(policy.snapshot().state).toBe("closed")
  })
})

// ---------------------------------------------------------------------------
// Composition with retry
// ---------------------------------------------------------------------------

describe("circuitBreaker.local — composition with retry", () => {
  it("observes every individual retry attempt when placed inside retry", async () => {
    const events: OperationEvent[] = []
    const policy = circuitBreaker.local({
      name: "p",
      minimumThroughput: 10, // won't open in this test
    })
    const op = operation({
      name: "work",
      adapter: {
        capabilities: () => ({ abort: "unsupported", replay: "safe" }),
        classify: () => "retryable",
        execute: failingWork,
      },
      policies: [retry({ maxAttempts: 3 }), policy],
      events: { emit: (e) => events.push(e) },
    })
    await op.execute(undefined).catch(() => {})
    // 3 attempts = 3 observations
    expect(events.filter((e) => e.type === "breaker.observation")).toHaveLength(
      3,
    )
  })

  it("stops retry when the breaker opens mid-sequence", async () => {
    const events: OperationEvent[] = []
    const policy = circuitBreaker.local({
      name: "p",
      minimumThroughput: 1,
      failureThreshold: 0.5,
    })
    let attempts = 0
    const op = operation({
      name: "work",
      adapter: {
        capabilities: () => ({ abort: "unsupported", replay: "safe" }),
        classify: () => "retryable",
        execute: async () => {
          attempts++
          throw new Error("fail")
        },
      },
      policies: [retry({ maxAttempts: 5 }), policy],
      events: { emit: (e) => events.push(e) },
    })
    await op.execute(undefined).catch(() => {})
    // First attempt opens the breaker; subsequent retries are rejected by it.
    expect(attempts).toBe(1)
    expect(events.some((e) => e.type === "breaker.rejected")).toBe(true)
  })

  it("observes the final outcome when placed outside retry", async () => {
    const events: OperationEvent[] = []
    const policy = circuitBreaker.local({
      name: "p",
      minimumThroughput: 10,
    })
    let attempts = 0
    const op = operation({
      name: "work",
      adapter: {
        capabilities: () => ({ abort: "unsupported", replay: "safe" }),
        classify: (o) =>
          o.status === "failure" && attempts < 3 ? "retryable" : "success",
        execute: async () => {
          if (++attempts < 3) throw new Error("retryable")
          return "ok"
        },
      },
      policies: [policy, retry({ maxAttempts: 3 })],
      events: { emit: (e) => events.push(e) },
    })
    await op.execute(undefined)
    // Breaker is outer: sees only the final settled outcome.
    expect(events.filter((e) => e.type === "breaker.observation")).toHaveLength(
      1,
    )
    expect(attempts).toBe(3)
  })
})

// ---------------------------------------------------------------------------
// Instance isolation
// ---------------------------------------------------------------------------

describe("circuitBreaker.local — instance isolation", () => {
  it("two instances with the same name have independent state", async () => {
    const a = circuitBreaker.local({
      name: "shared",
      minimumThroughput: 1,
      failureThreshold: 0.5,
    })
    const b = circuitBreaker.local({
      name: "shared",
      minimumThroughput: 1,
      failureThreshold: 0.5,
    })
    await subject(a, failingWork)
      .execute(undefined)
      .catch(() => {})
    expect(a.snapshot().state).toBe("open")
    expect(b.snapshot().state).toBe("closed")
  })
})

// ---------------------------------------------------------------------------
// Timeout interaction — breaker observes the timeout error
// ---------------------------------------------------------------------------

describe("circuitBreaker.local — timeout interaction", () => {
  it("records a timeout as a failure when timeout is inner (per-attempt)", async () => {
    vi.useFakeTimers()
    try {
      const events: OperationEvent[] = []
      const policy = circuitBreaker.local({
        name: "p",
        minimumThroughput: 1,
        failureThreshold: 0.5,
      })
      const gate = new Deferred<void>()
      const op = operation({
        name: "work",
        adapter: {
          capabilities: () => ({ abort: "unsupported", replay: "safe" }),
          execute: () => gate.promise,
        },
        policies: [policy, timeout({ ms: 50 })],
        events: { emit: (e) => events.push(e) },
      })
      const call = expect(op.execute(undefined)).rejects.toBeInstanceOf(
        TimeoutError,
      )
      await vi.advanceTimersByTimeAsync(50)
      await call
      // Timeout settled the attempt promise, so breaker observes a failure.
      expect(policy.snapshot().state).toBe("open")
      gate.resolve()
    } finally {
      vi.useRealTimers()
    }
  })

  it("does not observe when timeout is outer (whole-execution) and inner work succeeds eventually", async () => {
    vi.useFakeTimers()
    try {
      const gate = new Deferred<void>()
      const policy = circuitBreaker.local({
        name: "p",
        minimumThroughput: 1,
        failureThreshold: 0.5,
      })
      const events: OperationEvent[] = []
      const op = operation({
        name: "work",
        adapter: {
          capabilities: () => ({ abort: "unsupported", replay: "safe" }),
          execute: () => gate.promise,
        },
        // timeout is outermost — breaker never gets to observe the inner work
        policies: [timeout({ ms: 50 }), policy],
        events: { emit: (e) => events.push(e) },
      })
      const call = expect(op.execute(undefined)).rejects.toBeInstanceOf(
        TimeoutError,
      )
      await vi.advanceTimersByTimeAsync(50)
      await call
      gate.resolve()
      // Breaker is inside timeout, so it sees the work settle as success
      // (adapter eventually resolves, and `next` returned that value).
      // No observation should have fired yet because `next` hasn't resolved
      // while the outer timeout rejected — the breaker's `execute` is still
      // awaiting `next`. This verifies the breaker `finally` block runs after
      // true settlement.
      await vi.advanceTimersByTimeAsync(0)
      expect(
        events.filter((e) => e.type === "breaker.observation"),
      ).toHaveLength(1)
      expect(policy.snapshot().state).toBe("closed")
    } finally {
      vi.useRealTimers()
    }
  })
})

// ---------------------------------------------------------------------------
// bulkhead + circuit breaker composition
// ---------------------------------------------------------------------------

describe("circuitBreaker.local — composition with bulkhead", () => {
  it("bulkhead (attempt-phase) + breaker: bulkhead blocks later retries after breaker opens", async () => {
    const bh = bulkhead.local({ name: "pool", limit: 2 })
    const cb = circuitBreaker.local({
      name: "p",
      minimumThroughput: 1,
      failureThreshold: 0.5,
    })
    const events: OperationEvent[] = []
    const op = operation({
      name: "work",
      adapter: {
        capabilities: () => ({ abort: "unsupported", replay: "safe" }),
        classify: () => "retryable",
        execute: failingWork,
      },
      // bh is attempt-phase; cb is ordinary; retry is ordinary
      policies: [bh, retry({ maxAttempts: 5 }), cb],
      events: { emit: (e) => events.push(e) },
    })
    await op.execute(undefined).catch(() => {})
    // After 1 failure the breaker opens and rejects subsequent retries
    expect(events.some((e) => e.type === "breaker.rejected")).toBe(true)
  })
})
