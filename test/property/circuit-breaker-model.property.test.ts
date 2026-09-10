/**
 * Property and model tests for the local circuit breaker state machine.
 *
 * All property tests drive outcomes **serially** (one settled attempt per step)
 * to produce a fully-ordered, deterministic trace of state transitions.
 * Concurrent execution would let the breaker transition mid-flight across
 * multiple in-flight attempts, making event-order invariants non-deterministic.
 *
 * These tests validate the local implementation and establish the invariant
 * vocabulary that the distributed breaker must satisfy.
 */

import * as fc from "fast-check"
import { describe, expect, it, vi } from "vitest"
import type { OperationEvent } from "../../src/index.js"
import { circuitBreaker, operation } from "../../src/index.js"
import { replayInstruction, resolveTestSeed } from "../support/seed.js"

// ---------------------------------------------------------------------------
// Helpers — serial execution
// ---------------------------------------------------------------------------

/** Run a list of async thunks one-at-a-time, ignoring errors. */
async function serial(thunks: Array<() => Promise<unknown>>): Promise<void> {
  for (const thunk of thunks) await thunk().catch(() => {})
}

function makeOp(
  policy: ReturnType<typeof circuitBreaker.local>,
  failure: boolean,
  events?: OperationEvent[],
): () => Promise<unknown> {
  const op = operation({
    name: "work",
    adapter: {
      capabilities: () => ({
        abort: "unsupported" as const,
        replay: "safe" as const,
      }),
      execute: async () => {
        if (failure) throw new Error("fail")
        return "ok"
      },
    },
    policies: [policy],
    events: events ? { emit: (e) => events.push(e) } : undefined,
  })
  return () => op.execute(undefined)
}

// ---------------------------------------------------------------------------
// Arbitraries
// ---------------------------------------------------------------------------

const configArb = fc.record({
  minimumThroughput: fc.integer({ min: 1, max: 10 }),
  failureThreshold: fc.float({
    min: Math.fround(0.1),
    max: Math.fround(0.9),
    noNaN: true,
  }),
  openMs: fc.integer({ min: 10, max: 1000 }),
  halfOpenSuccesses: fc.integer({ min: 1, max: 5 }),
  halfOpenProbes: fc.integer({ min: 1, max: 3 }),
  windowSize: fc.integer({ min: 2, max: 20 }),
})

// ---------------------------------------------------------------------------
// Invariant assertions (applied to a fully-settled serial trace)
// ---------------------------------------------------------------------------

/**
 * Each state-change event must use a valid directed edge in the FSM:
 *
 *   closed → open
 *   open → half-open
 *   half-open → closed
 *   half-open → open    (probe failure re-opens)
 */
function assertValidTransitions(
  events: OperationEvent[],
  replay: string,
): void {
  const changes = events.filter((e) => e.type === "breaker.state-changed")
  const validEdges: Record<string, string[]> = {
    closed: ["open"],
    open: ["half-open"],
    "half-open": ["closed", "open"],
  }
  for (const change of changes) {
    if (change.type === "breaker.state-changed") {
      const allowed = validEdges[change.previousState] ?? []
      expect(allowed, replay).toContain(change.state)
    }
  }
}

/** Rejected events must only fire while the state is open or half-open. */
function assertRejectedOnlyWhenNotClosed(
  events: OperationEvent[],
  replay: string,
): void {
  for (const e of events) {
    if (e.type === "breaker.rejected") {
      expect(["open", "half-open"], replay).toContain(e.state)
    }
  }
}

/** failures + successes === observations, and observations ≤ windowSize. */
function assertWindowConsistency(
  policy: ReturnType<typeof circuitBreaker.local>,
  windowSize: number,
  replay: string,
): void {
  const snap = policy.snapshot()
  expect(snap.failures + snap.successes, replay).toBe(snap.observations)
  expect(snap.observations, replay).toBeLessThanOrEqual(windowSize)
  expect(snap.failures, replay).toBeGreaterThanOrEqual(0)
  expect(snap.successes, replay).toBeGreaterThanOrEqual(0)
}

// ---------------------------------------------------------------------------
// Property tests
// ---------------------------------------------------------------------------

const SEED = resolveTestSeed()
const REPLAY = replayInstruction("npm run test:property", SEED)

describe(`circuit breaker model — state machine invariants (seed=${SEED} replay="${REPLAY}")`, () => {
  it("never emits an invalid state transition in a serial failure-only trace", async () => {
    await fc.assert(
      fc.asyncProperty(
        configArb,
        fc.integer({ min: 1, max: 30 }),
        async (cfg, count) => {
          const events: OperationEvent[] = []
          const policy = circuitBreaker.local({
            name: "p",
            minimumThroughput: cfg.minimumThroughput,
            failureThreshold: cfg.failureThreshold,
            openMs: 999_999, // never expires — no half-open transitions
            halfOpenSuccesses: cfg.halfOpenSuccesses,
            halfOpenProbes: cfg.halfOpenProbes,
            windowSize: cfg.windowSize,
          })
          await serial(
            Array.from({ length: count }, () => makeOp(policy, true, events)),
          )
          assertValidTransitions(events, REPLAY)
          assertRejectedOnlyWhenNotClosed(events, REPLAY)
          assertWindowConsistency(policy, cfg.windowSize, REPLAY)
        },
      ),
      { numRuns: 300, seed: SEED },
    )
  })

  it("success-only trace never opens the breaker and has valid window state", async () => {
    await fc.assert(
      fc.asyncProperty(
        configArb,
        fc.integer({ min: 1, max: 30 }),
        async (cfg, count) => {
          const events: OperationEvent[] = []
          const policy = circuitBreaker.local({
            name: "p",
            minimumThroughput: cfg.minimumThroughput,
            failureThreshold: cfg.failureThreshold,
            openMs: 999_999,
            halfOpenSuccesses: cfg.halfOpenSuccesses,
            halfOpenProbes: cfg.halfOpenProbes,
            windowSize: cfg.windowSize,
          })
          await serial(
            Array.from({ length: count }, () => makeOp(policy, false, events)),
          )
          assertValidTransitions(events, REPLAY)
          assertRejectedOnlyWhenNotClosed(events, REPLAY)
          assertWindowConsistency(policy, cfg.windowSize, REPLAY)
          expect(policy.snapshot().state, REPLAY).toBe("closed")
        },
      ),
      { numRuns: 300, seed: SEED },
    )
  })

  it("never emits an invalid state transition in a mixed serial trace", async () => {
    await fc.assert(
      fc.asyncProperty(
        configArb,
        fc.array(fc.boolean(), { minLength: 1, maxLength: 40 }),
        async (cfg, outcomes) => {
          const events: OperationEvent[] = []
          const policy = circuitBreaker.local({
            name: "p",
            minimumThroughput: cfg.minimumThroughput,
            failureThreshold: cfg.failureThreshold,
            openMs: 999_999,
            halfOpenSuccesses: cfg.halfOpenSuccesses,
            halfOpenProbes: cfg.halfOpenProbes,
            windowSize: cfg.windowSize,
          })
          await serial(outcomes.map((f) => makeOp(policy, f, events)))
          assertValidTransitions(events, REPLAY)
          assertRejectedOnlyWhenNotClosed(events, REPLAY)
          assertWindowConsistency(policy, cfg.windowSize, REPLAY)
        },
      ),
      { numRuns: 300, seed: SEED },
    )
  })

  it("window observations never exceed windowSize", async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 2, max: 10 }),
        fc.array(fc.boolean(), { minLength: 1, maxLength: 150 }),
        async (windowSize, outcomes) => {
          // Very high minimumThroughput so the breaker never opens —
          // this lets us observe the window filling and evicting across many calls.
          const policy = circuitBreaker.local({
            name: "p",
            minimumThroughput: windowSize + 100,
            failureThreshold: 0.99,
            windowSize,
            openMs: 999_999,
          })
          await serial(outcomes.map((f) => makeOp(policy, f)))
          expect(policy.snapshot().observations, REPLAY).toBeLessThanOrEqual(
            windowSize,
          )
        },
      ),
      { numRuns: 300, seed: SEED },
    )
  })

  it("failures + successes always equals observations", async () => {
    await fc.assert(
      fc.asyncProperty(
        configArb,
        fc.array(fc.boolean(), { minLength: 1, maxLength: 60 }),
        async (cfg, outcomes) => {
          const policy = circuitBreaker.local({
            name: "p",
            minimumThroughput: cfg.minimumThroughput,
            failureThreshold: cfg.failureThreshold,
            windowSize: cfg.windowSize,
            openMs: 999_999,
          })
          await serial(outcomes.map((f) => makeOp(policy, f)))
          assertWindowConsistency(policy, cfg.windowSize, REPLAY)
        },
      ),
      { numRuns: 300, seed: SEED },
    )
  })

  it("a breaker that is open never emits observations for rejected attempts", async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 2, max: 8 }),
        fc.integer({ min: 1, max: 20 }),
        async (minThroughput, extraAttempts) => {
          const events: OperationEvent[] = []
          const policy = circuitBreaker.local({
            name: "p",
            minimumThroughput: minThroughput,
            failureThreshold: 0.5,
            openMs: 999_999,
            windowSize: minThroughput + 10,
          })
          // Open the breaker with all failures
          await serial(
            Array.from({ length: minThroughput }, () =>
              makeOp(policy, true, events),
            ),
          )
          if (policy.snapshot().state !== "open") return // not open yet: threshold not met

          const obsAfterOpen = events.filter(
            (e) => e.type === "breaker.observation",
          ).length

          await serial(
            Array.from({ length: extraAttempts }, () =>
              makeOp(policy, true, events),
            ),
          )

          expect(
            events.filter((e) => e.type === "breaker.observation").length,
            REPLAY,
          ).toBe(obsAfterOpen)
          expect(
            events.filter((e) => e.type === "breaker.rejected").length,
            REPLAY,
          ).toBe(extraAttempts)
        },
      ),
      { numRuns: 200, seed: SEED },
    )
  })

  it("opening never happens before minimumThroughput observations", async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 3, max: 10 }),
        fc.float({ min: Math.fround(0.1), max: Math.fround(0.9), noNaN: true }),
        fc.integer({ min: 2, max: 20 }),
        async (minThroughput, threshold, windowSize) => {
          const events: OperationEvent[] = []
          const policy = circuitBreaker.local({
            name: "p",
            minimumThroughput: minThroughput,
            failureThreshold: threshold,
            openMs: 999_999,
            windowSize,
          })
          // Drive exactly minimumThroughput - 1 failures
          await serial(
            Array.from({ length: minThroughput - 1 }, () =>
              makeOp(policy, true, events),
            ),
          )
          expect(policy.snapshot().state, REPLAY).toBe("closed")
          expect(
            events.some((e) => e.type === "breaker.state-changed"),
            REPLAY,
          ).toBe(false)
        },
      ),
      { numRuns: 200, seed: SEED },
    )
  })
})

// ---------------------------------------------------------------------------
// Deterministic history replay
// ---------------------------------------------------------------------------

describe("circuit breaker model — deterministic histories", () => {
  it("history: opens at minimumThroughput, not before", async () => {
    const events: OperationEvent[] = []
    const policy = circuitBreaker.local({
      name: "p",
      minimumThroughput: 3,
      failureThreshold: 0.5,
      windowSize: 10,
      openMs: 999_999,
    })
    const fail = makeOp(policy, true, events)
    const succeed = makeOp(policy, false, events)

    await fail().catch(() => {})
    expect(policy.snapshot()).toMatchObject({ state: "closed", failures: 1 })
    await succeed().catch(() => {})
    expect(policy.snapshot()).toMatchObject({
      state: "closed",
      failures: 1,
      successes: 1,
    })
    await fail().catch(() => {})
    // 2 failures / 3 total = 67% ≥ 50% → opens
    expect(policy.snapshot().state).toBe("open")
    expect(policy.snapshot().observations).toBe(0) // window resets on open
    expect(
      events.filter((e) => e.type === "breaker.state-changed"),
    ).toHaveLength(1)
  })

  it("history: stays closed when failure ratio is below threshold", async () => {
    const policy = circuitBreaker.local({
      name: "p",
      minimumThroughput: 4,
      failureThreshold: 0.6,
      windowSize: 10,
    })
    // 2 failures / 4 total = 50% < 60%
    await makeOp(policy, false)().catch(() => {})
    await makeOp(policy, false)().catch(() => {})
    await makeOp(policy, true)().catch(() => {})
    await makeOp(policy, true)().catch(() => {})
    expect(policy.snapshot().state).toBe("closed")
    expect(policy.snapshot().failures).toBe(2)
    expect(policy.snapshot().successes).toBe(2)
  })

  it("history: window eviction caps observation count", async () => {
    const policy = circuitBreaker.local({
      name: "p",
      minimumThroughput: 1000, // never opens
      failureThreshold: 0.99,
      windowSize: 5,
    })
    const fail = makeOp(policy, true)
    for (let i = 0; i < 10; i++) await fail().catch(() => {})
    expect(policy.snapshot().observations).toBe(5)
    expect(policy.snapshot().failures).toBe(5)
    expect(policy.snapshot().state).toBe("closed")
  })

  it("history: probe failure reopens — state sequence is closed→open→half-open→open", async () => {
    vi.useFakeTimers()
    try {
      const events: OperationEvent[] = []
      const policy = circuitBreaker.local({
        name: "p",
        minimumThroughput: 1,
        failureThreshold: 0.5,
        openMs: 100,
        halfOpenSuccesses: 3, // needs 3 successes to close
      })
      const fail = makeOp(policy, true, events)
      const succeed = makeOp(policy, false, events)

      await fail().catch(() => {}) // open
      expect(policy.snapshot().state).toBe("open")
      await vi.advanceTimersByTimeAsync(100)

      await succeed().catch(() => {}) // probe 1 — not enough to close
      expect(policy.snapshot().state).toBe("half-open")
      expect(policy.snapshot().halfOpenSuccesses).toBe(1)

      await fail().catch(() => {}) // probe failure — re-opens
      expect(policy.snapshot().state).toBe("open")
      expect(policy.snapshot().halfOpenSuccesses).toBe(0)

      const seq = events
        .filter((e) => e.type === "breaker.state-changed")
        .map(
          (e) =>
            e.type === "breaker.state-changed" &&
            `${e.previousState}→${e.state}`,
        )
      expect(seq).toEqual(["closed→open", "open→half-open", "half-open→open"])
    } finally {
      vi.useRealTimers()
    }
  })

  it("history: two concurrent successful probes close the breaker", async () => {
    vi.useFakeTimers()
    try {
      const events: OperationEvent[] = []
      const policy = circuitBreaker.local({
        name: "p",
        minimumThroughput: 1,
        failureThreshold: 0.5,
        openMs: 100,
        halfOpenSuccesses: 2,
        halfOpenProbes: 2,
      })
      const failOp = operation({
        name: "w",
        adapter: {
          capabilities: () => ({
            abort: "unsupported" as const,
            replay: "safe" as const,
          }),
          execute: async () => {
            throw new Error("f")
          },
        },
        policies: [policy],
        events: { emit: (e) => events.push(e) },
      })
      const succeedOp = operation({
        name: "w",
        adapter: {
          capabilities: () => ({
            abort: "unsupported" as const,
            replay: "safe" as const,
          }),
          execute: async () => "ok",
        },
        policies: [policy],
        events: { emit: (e) => events.push(e) },
      })

      await failOp.execute(undefined).catch(() => {})
      expect(policy.snapshot().state).toBe("open")
      await vi.advanceTimersByTimeAsync(100)

      // Two concurrent probes — together they satisfy halfOpenSuccesses: 2
      await Promise.all([
        succeedOp.execute(undefined),
        succeedOp.execute(undefined),
      ])
      expect(policy.snapshot().state).toBe("closed")

      const stateSeq = events
        .filter((e) => e.type === "breaker.state-changed")
        .map((e) => e.type === "breaker.state-changed" && e.state)
      expect(stateSeq).toEqual(["open", "half-open", "closed"])
    } finally {
      vi.useRealTimers()
    }
  })

  it("history: instance isolation — two instances with same name are independent", async () => {
    const a = circuitBreaker.local({
      name: "shared-name",
      minimumThroughput: 1,
      failureThreshold: 0.5,
    })
    const b = circuitBreaker.local({
      name: "shared-name",
      minimumThroughput: 1,
      failureThreshold: 0.5,
    })
    await makeOp(a, true)().catch(() => {})
    expect(a.snapshot().state).toBe("open")
    expect(b.snapshot().state).toBe("closed")
  })
})
