/**
 * Fuzz tests for circuit breaker event-history invariants.
 *
 * Generates randomised sequences of attempt outcomes and verifies that the
 * circuit breaker never violates its documented state-machine invariants.
 * All sequences are seeded so a failing history can be replayed exactly:
 *
 *   CARACAL_TEST_SEED=<reported-seed> npm run test:fuzz
 *
 * On PowerShell:
 *   $env:CARACAL_TEST_SEED = "<reported-seed>"; npm run test:fuzz
 */
import { describe, expect, it } from "vitest"
import type { OperationEvent } from "../../src/index.js"
import { circuitBreaker, operation } from "../../src/index.js"
import { memoryBreakerCoordinator } from "../support/memory-coordinator/memory-circuit-breaker.js"
import { replayInstruction, resolveTestSeed } from "../support/seed.js"

// ---------------------------------------------------------------------------
// Seeded PRNG â€” mulberry32, period 2^32, reproducible from any 32-bit seed
// ---------------------------------------------------------------------------

function mulberry32(seed: number): () => number {
  let s = seed >>> 0
  return () => {
    s = (s + 0x6d2b79f5) | 0
    let t = Math.imul(s ^ (s >>> 15), 1 | s)
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
}

function randInt(rng: () => number, lo: number, hi: number): number {
  return lo + Math.floor(rng() * (hi - lo + 1))
}

function randFloat(rng: () => number, lo: number, hi: number): number {
  return lo + rng() * (hi - lo)
}

// ---------------------------------------------------------------------------
// Helpers â€” strictly serial execution preserves deterministic event ordering
// ---------------------------------------------------------------------------

type BinaryOutcome = "success" | "failure"

async function runOutcomes(
  policy: ReturnType<typeof circuitBreaker.local>,
  outcomes: BinaryOutcome[],
  events: OperationEvent[],
): Promise<void> {
  for (const outcome of outcomes) {
    const isFailure = outcome === "failure"
    const op = operation({
      name: "w",
      adapter: {
        capabilities: () => ({
          abort: "unsupported" as const,
          replay: "safe" as const,
        }),
        execute: async () => {
          if (isFailure) throw new Error("boom")
          return "ok"
        },
      },
      policies: [policy],
      events: { emit: (e) => events.push(e) },
    })
    await op.execute(undefined).catch(() => {})
  }
}

// ---------------------------------------------------------------------------
// Invariant assertions
// ---------------------------------------------------------------------------

const VALID_TRANSITIONS: Readonly<Record<string, readonly string[]>> = {
  closed: ["open"],
  open: ["half-open"],
  "half-open": ["closed", "open"],
}

function assertLocalInvariants(
  events: OperationEvent[],
  policy: ReturnType<typeof circuitBreaker.local>,
  windowSize: number,
  ctx: string,
): void {
  // 1. All state-changed edges must be valid FSM transitions.
  for (const e of events) {
    if (e.type === "breaker.state-changed") {
      const allowed = VALID_TRANSITIONS[e.previousState] ?? []
      expect(
        allowed,
        `Invalid transition ${e.previousState}â†’${e.state}\n${ctx}`,
      ).toContain(e.state)
    }
  }

  // 2. Rejected events only carry open or half-open state.
  for (const e of events) {
    if (e.type === "breaker.rejected") {
      expect(
        ["open", "half-open"],
        `breaker.rejected emitted with state=${e.state}\n${ctx}`,
      ).toContain(e.state)
    }
  }

  // 3. failures + successes === observations; observations â‰¤ windowSize.
  const snap = policy.snapshot()
  expect(
    snap.failures + snap.successes,
    `Window inconsistency: ${snap.failures}+${snap.successes}â‰ ${snap.observations}\n${ctx}`,
  ).toBe(snap.observations)
  expect(
    snap.observations,
    `observations ${snap.observations} exceeded windowSize ${windowSize}\n${ctx}`,
  ).toBeLessThanOrEqual(windowSize)
}

// ---------------------------------------------------------------------------
// Shared seed â€” all describe blocks in this file derive from the same seed
// ---------------------------------------------------------------------------

const SEED = resolveTestSeed()
const REPLAY = replayInstruction("npm run test:fuzz", SEED)

// ---------------------------------------------------------------------------
// Local circuit breaker fuzz
// ---------------------------------------------------------------------------

describe(`circuit breaker fuzz â€” local (seed=${SEED} replay="${REPLAY}")`, () => {
  const rng = mulberry32(SEED)
  const RUNS = 200

  it("failure-only histories never produce invalid transitions", async () => {
    for (let i = 0; i < RUNS; i++) {
      const windowSize = randInt(rng, 2, 20)
      const minimumThroughput = randInt(rng, 1, windowSize)
      const failureThreshold = randFloat(rng, 0.1, 0.9)
      const count = randInt(rng, 1, 40)

      const events: OperationEvent[] = []
      const policy = circuitBreaker.local({
        name: "p",
        minimumThroughput,
        failureThreshold,
        windowSize,
        openMs: 999_999,
      })
      const ctx =
        `run=${i} minimumThroughput=${minimumThroughput} ` +
        `failureThreshold=${failureThreshold.toFixed(3)} ` +
        `windowSize=${windowSize} count=${count}\n${REPLAY}`

      await runOutcomes(
        policy,
        Array.from({ length: count }, (): BinaryOutcome => "failure"),
        events,
      )

      assertLocalInvariants(events, policy, windowSize, ctx)
    }
  })

  it("mixed histories never produce invalid transitions", async () => {
    for (let i = 0; i < RUNS; i++) {
      const windowSize = randInt(rng, 2, 20)
      const minimumThroughput = randInt(rng, 1, windowSize)
      const failureThreshold = randFloat(rng, 0.1, 0.9)
      const length = randInt(rng, 2, 40)
      const outcomes: BinaryOutcome[] = Array.from({ length }, () =>
        rng() < 0.6 ? "failure" : "success",
      )

      const events: OperationEvent[] = []
      const policy = circuitBreaker.local({
        name: "p",
        minimumThroughput,
        failureThreshold,
        windowSize,
        openMs: 999_999,
      })
      const ctx =
        `run=${i} minimumThroughput=${minimumThroughput} ` +
        `failureThreshold=${failureThreshold.toFixed(3)} ` +
        `windowSize=${windowSize} outcomes=[${outcomes.join(",")}]\n${REPLAY}`

      await runOutcomes(policy, outcomes, events)
      assertLocalInvariants(events, policy, windowSize, ctx)
    }
  })

  it("success-only histories never open the breaker", async () => {
    for (let i = 0; i < RUNS; i++) {
      const windowSize = randInt(rng, 2, 20)
      const minimumThroughput = randInt(rng, 1, windowSize)
      const length = randInt(rng, 1, 40)

      const events: OperationEvent[] = []
      const policy = circuitBreaker.local({
        name: "p",
        minimumThroughput,
        failureThreshold: 0.5,
        windowSize,
        openMs: 999_999,
      })
      const ctx =
        `run=${i} minimumThroughput=${minimumThroughput} ` +
        `windowSize=${windowSize} count=${length}\n${REPLAY}`

      await runOutcomes(
        policy,
        Array.from({ length }, (): BinaryOutcome => "success"),
        events,
      )

      expect(
        policy.snapshot().state,
        `breaker opened on success-only trace\n${ctx}`,
      ).toBe("closed")
      assertLocalInvariants(events, policy, windowSize, ctx)
    }
  })

  it("rejected attempts never produce observation events", async () => {
    for (let i = 0; i < Math.floor(RUNS / 4); i++) {
      const minimumThroughput = randInt(rng, 2, 8)
      const windowSize = minimumThroughput + randInt(rng, 0, 10)
      const extraAttempts = randInt(rng, 1, 20)

      const events: OperationEvent[] = []
      const policy = circuitBreaker.local({
        name: "p",
        minimumThroughput,
        failureThreshold: 0.5,
        windowSize,
        openMs: 999_999,
      })
      const ctx =
        `run=${i} minimumThroughput=${minimumThroughput} ` +
        `windowSize=${windowSize} extra=${extraAttempts}\n${REPLAY}`

      // Drive to open with all failures
      await runOutcomes(
        policy,
        Array.from(
          { length: minimumThroughput },
          (): BinaryOutcome => "failure",
        ),
        events,
      )

      if (policy.snapshot().state !== "open") continue

      const obsBeforeRejection = events.filter(
        (e) => e.type === "breaker.observation",
      ).length

      // Extra attempts should all be rejected, not observed
      await runOutcomes(
        policy,
        Array.from({ length: extraAttempts }, (): BinaryOutcome => "failure"),
        events,
      )

      expect(
        events.filter((e) => e.type === "breaker.observation").length,
        `Observation count changed after open â€” rejected attempts produced observations\n${ctx}`,
      ).toBe(obsBeforeRejection)

      expect(
        events.filter((e) => e.type === "breaker.rejected").length,
        `Expected ${extraAttempts} breaker.rejected events\n${ctx}`,
      ).toBe(extraAttempts)
    }
  })

  it("probesInFlight never exceeds halfOpenProbes under concurrent admission", async () => {
    // Use a sub-RNG offset to avoid sharing state with earlier tests
    const subRng = mulberry32(SEED ^ 0xabcdef01)

    for (let i = 0; i < 50; i++) {
      const halfOpenProbes = randInt(subRng, 1, 3)
      const halfOpenSuccesses = randInt(subRng, 1, 3)
      const minimumThroughput = randInt(subRng, 1, 5)
      const windowSize = minimumThroughput + randInt(subRng, 0, 5)

      // openMs: 0 so the breaker is immediately eligible for probing after
      // opening — no sleep or fake-timer manipulation required.
      const policy = circuitBreaker.local({
        name: "p",
        minimumThroughput,
        failureThreshold: 0.5,
        windowSize,
        openMs: 1,
        halfOpenProbes,
        halfOpenSuccesses,
      })
      const ctx = `run=${i} halfOpenProbes=${halfOpenProbes} halfOpenSuccesses=${halfOpenSuccesses}\n${REPLAY}`

      // Open the breaker
      const openEvents: OperationEvent[] = []
      await runOutcomes(
        policy,
        Array.from(
          { length: minimumThroughput },
          (): BinaryOutcome => "failure",
        ),
        openEvents,
      )

      if (policy.snapshot().state !== "open") continue

      // Ensure openMs (1 ms) has elapsed so the breaker is eligible for
      // half-open transition on the next admission.
      await new Promise((r) => setTimeout(r, 5))

      // Launch more concurrent probes than the limit permits.
      // Local breaker admission is synchronous (decided before the first
      // await inside execute()), so starting all promises without awaiting
      // gives genuine concurrent admission pressure.
      const concurrentCount = halfOpenProbes + 2
      const events: OperationEvent[] = []
      const op = operation({
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

      // Start all — admissions are decided synchronously during this line
      const attempts = Array.from({ length: concurrentCount }, () =>
        op.execute(undefined).catch(() => {}),
      )

      // Invariant at peak concurrency (before any probe has settled)
      expect(
        policy.snapshot().probesInFlight,
        `probesInFlight exceeded halfOpenProbes at peak concurrency\n${ctx}`,
      ).toBeLessThanOrEqual(halfOpenProbes)

      await Promise.all(attempts)

      // After settlement: admitted probe count must not exceed the limit
      const probeStarted = events.filter(
        (e) => e.type === "breaker.probe-started",
      ).length
      expect(
        probeStarted,
        `More probes admitted than halfOpenProbes allows\n${ctx}`,
      ).toBeLessThanOrEqual(halfOpenProbes)

      // Excess concurrent attempts must have been rejected
      const rejected = events.filter(
        (e) => e.type === "breaker.rejected",
      ).length
      expect(
        rejected,
        `Expected at least ${concurrentCount - halfOpenProbes} probe-limit rejections\n${ctx}`,
      ).toBeGreaterThanOrEqual(concurrentCount - halfOpenProbes)
    }
  })
})
// ---------------------------------------------------------------------------
// Distributed circuit breaker fuzz â€” uses memory coordinator
// ---------------------------------------------------------------------------

describe(`circuit breaker fuzz â€” distributed (seed=${SEED} replay="${REPLAY}")`, () => {
  const rng = mulberry32(SEED ^ 0xdeadbeef)
  const RUNS = 50

  it("never produces invalid transitions in randomised distributed histories", async () => {
    for (let i = 0; i < RUNS; i++) {
      const windowSize = randInt(rng, 5, 20)
      const minimumThroughput = randInt(rng, 2, Math.min(windowSize, 10))
      const failureThreshold = randFloat(rng, 0.3, 0.8)
      const length = randInt(rng, 2, 30)
      const outcomes: BinaryOutcome[] = Array.from({ length }, () =>
        rng() < 0.6 ? "failure" : "success",
      )

      const coordinator = memoryBreakerCoordinator()
      const policy = circuitBreaker.distributed({
        name: "p",
        coordinator,
        scope: () => "shared",
        minimumThroughput,
        failureThreshold,
        windowSize,
        openMs: 999_999,
        halfOpenProbes: 1,
        halfOpenSuccesses: 1,
      })
      const ctx =
        `dist run=${i} minimumThroughput=${minimumThroughput} ` +
        `failureThreshold=${failureThreshold.toFixed(3)} ` +
        `windowSize=${windowSize} outcomes=[${outcomes.join(",")}]\n${REPLAY}`

      const events: OperationEvent[] = []

      for (const outcome of outcomes) {
        const isFailure = outcome === "failure"
        const op = operation({
          name: "w",
          adapter: {
            capabilities: () => ({
              abort: "unsupported" as const,
              replay: "safe" as const,
            }),
            execute: async () => {
              if (isFailure) throw new Error("boom")
              return "ok"
            },
          },
          policies: [policy],
          events: { emit: (e) => events.push(e) },
        })
        await op.execute(undefined).catch(() => {})
      }

      // Validate state-machine transitions
      for (const e of events) {
        if (e.type === "breaker.state-changed") {
          const allowed = VALID_TRANSITIONS[e.previousState] ?? []
          expect(
            allowed,
            `Distributed: invalid transition ${e.previousState}â†’${e.state}\n${ctx}`,
          ).toContain(e.state)
        }
        if (e.type === "breaker.rejected") {
          expect(
            ["open", "half-open"],
            `Distributed: breaker.rejected fired with state=${e.state}\n${ctx}`,
          ).toContain(e.state)
        }
      }
    }
  })

  it("scope isolation holds across randomised multi-scope histories", async () => {
    const scopeRng = mulberry32(SEED ^ 0xcafebabe)

    for (let i = 0; i < RUNS; i++) {
      const minimumThroughput = randInt(scopeRng, 3, 8)
      const windowSize = minimumThroughput + randInt(scopeRng, 0, 5)
      const scopes = ["alpha", "beta", "gamma"]
      // biome-ignore lint/style/noNonNullAssertion: randInt always returns 0-2, scopes.length=3
      const targetScope = scopes[randInt(scopeRng, 0, 2)]!

      const coordinator = memoryBreakerCoordinator()
      let currentScope = targetScope

      const policy = circuitBreaker.distributed({
        name: "p",
        coordinator,
        scope: () => currentScope,
        minimumThroughput,
        failureThreshold: 0.5,
        windowSize,
        openMs: 999_999,
        halfOpenProbes: 1,
        halfOpenSuccesses: 1,
      })

      // Open the target scope with failures
      currentScope = targetScope
      const openOp = operation({
        name: "w",
        adapter: {
          capabilities: () => ({
            abort: "unsupported" as const,
            replay: "safe" as const,
          }),
          execute: async () => {
            throw new Error("boom")
          },
        },
        policies: [policy],
      })
      for (let j = 0; j < minimumThroughput; j++) {
        await openOp.execute(undefined).catch(() => {})
      }

      // Other scopes must remain CLOSED
      const otherScopes = scopes.filter((s) => s !== targetScope)
      for (const other of otherScopes) {
        currentScope = other
        const events: OperationEvent[] = []
        const op = operation({
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
        await op.execute(undefined).catch(() => {})

        const ctx =
          `run=${i} openedScope=${targetScope} checkedScope=${other} ` +
          `minimumThroughput=${minimumThroughput}\n${REPLAY}`

        expect(
          events.filter((e) => e.type === "breaker.rejected"),
          `Scope isolation violated: opening ${targetScope} affected ${other}\n${ctx}`,
        ).toHaveLength(0)
      }
    }
  })
})

// ---------------------------------------------------------------------------
// Distributed coordinator invariants
//
// Mirrors the local window vocabulary against the distributed path via the
// coordinator's inspect() view.
// ---------------------------------------------------------------------------

describe(`circuit breaker fuzz — distributed window invariants (seed=${SEED} replay="${REPLAY}")`, () => {
  const rng = mulberry32(SEED ^ 0x5eed5eed)
  const RUNS = 40

  it("keeps window, generation and rejection invariants across randomised histories", async () => {
    for (let run = 0; run < RUNS; run++) {
      const windowSize = randInt(rng, 3, 12)
      const minimumThroughput = randInt(rng, 1, Math.min(windowSize, 6))
      const failureThreshold = randFloat(rng, 0.2, 0.9)
      const length = randInt(rng, 1, 25)
      const outcomes: BinaryOutcome[] = Array.from({ length }, () =>
        rng() < 0.55 ? "failure" : "success",
      )

      const coordinator = memoryBreakerCoordinator()
      const identity = { name: "p", operation: "op", scope: "shared" }
      const policy = circuitBreaker.distributed({
        name: "p",
        coordinator,
        scope: () => "shared",
        minimumThroughput,
        failureThreshold,
        windowSize,
        openMs: 999_999,
        halfOpenProbes: 1,
        halfOpenSuccesses: 1,
      })

      const ctx =
        `coordinator run=${run} minimumThroughput=${minimumThroughput} ` +
        `failureThreshold=${failureThreshold.toFixed(3)} windowSize=${windowSize} ` +
        `outcomes=[${outcomes.join(",")}]\n${REPLAY}`

      const events: OperationEvent[] = []
      let generation = 0

      for (const outcome of outcomes) {
        const isFailure = outcome === "failure"
        const op = operation({
          name: "op",
          adapter: {
            capabilities: () => ({
              abort: "unsupported" as const,
              replay: "safe" as const,
            }),
            execute: async () => {
              if (isFailure) throw new Error("boom")
              return "ok"
            },
          },
          policies: [policy],
          events: { emit: (e) => events.push(e) },
        })

        const before = events.length
        await op.execute(undefined).catch(() => {})
        const step = events.slice(before)

        // A rejected attempt never reaches the adapter, so it cannot observe.
        if (step.some((e) => e.type === "breaker.rejected")) {
          expect(
            step.some((e) => e.type === "breaker.observation"),
            `rejected attempt produced an observation\n${ctx}`,
          ).toBe(false)
        }

        const record = coordinator.inspect(identity)
        if (record !== undefined) {
          expect(
            record.generation,
            `generation went backwards\n${ctx}`,
          ).toBeGreaterThanOrEqual(generation)
          generation = record.generation

          expect(
            record.observations.length,
            `window retained more than windowSize observations\n${ctx}`,
          ).toBeLessThanOrEqual(windowSize)

          const current = record.observations.filter(
            (observation) => observation.generation === record.generation,
          )
          const failures = current.filter(
            (observation) => observation.outcome === "failure",
          ).length
          expect(
            failures,
            `window failures exceeded the window total\n${ctx}`,
          ).toBeLessThanOrEqual(current.length)
        }
      }
    }
  })
})

// ---------------------------------------------------------------------------
// Distributed probe accounting
// ---------------------------------------------------------------------------

describe(`circuit breaker fuzz — distributed probe accounting (seed=${SEED} replay="${REPLAY}")`, () => {
  const rng = mulberry32(SEED ^ 0x0ddba11)
  const RUNS = 40

  it("never admits more probes than halfOpenProbes and releases one slot per settle", async () => {
    for (let run = 0; run < RUNS; run++) {
      const halfOpenProbes = randInt(rng, 1, 3)
      const coordinator = memoryBreakerCoordinator()
      const identity = { name: "p", operation: "op", scope: `scope-${run}` }

      let opened = false
      for (let attempt = 0; attempt < 4 && !opened; attempt++) {
        const result = await coordinator.observe(identity, {
          generation: 0,
          outcome: "failure",
          uuid: `u-${run}-${attempt}`,
          windowTtlMs: 60_000,
          minimumThroughput: 2,
          failureThresholdNumerator: 500,
          windowSize: 8,
          openMs: 0,
        })
        opened = result.type === "opened"
      }
      expect(opened, `breaker never opened\n${REPLAY}`).toBe(true)

      // openMs: 0 so the first admission transitions OPEN -> HALF_OPEN.
      const attempts = await Promise.all(
        Array.from({ length: halfOpenProbes + 3 }, (_, index) =>
          coordinator.admitProbe(identity, {
            probeToken: `t-${run}-${index}`,
            openMs: 0,
            halfOpenProbes,
            probeLeaseTtlMs: 60_000,
          }),
        ),
      )
      const admitted = attempts.filter((result) => result.type === "admitted")
      expect(
        admitted.length,
        `admitted ${admitted.length} probes with halfOpenProbes=${halfOpenProbes}\n${REPLAY}`,
      ).toBe(halfOpenProbes)

      const record = coordinator.inspect(identity)
      if (record === undefined) throw new Error(`missing record\n${REPLAY}`)
      expect(record.probeTokens.size).toBe(halfOpenProbes)

      let expected = record.probeTokens.size
      for (const token of [...record.probeTokens.keys()]) {
        const settled = await coordinator.settleProbe(identity, {
          probeToken: token,
          outcome: "success",
          generation: record.generation,
          halfOpenSuccesses: 99,
          openMs: 0,
        })
        expect(settled.type).toBe("settled")

        expected -= 1
        const after = coordinator.inspect(identity)
        if (after === undefined) throw new Error(`missing record\n${REPLAY}`)
        expect(after.probeTokens.size).toBe(expected)
      }
    }
  })
})
