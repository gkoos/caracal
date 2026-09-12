import { describe, expect, it } from "vitest"
import type {
  BreakerCoordinator,
  OperationEvent,
  Policy,
} from "../../src/index.js"
import { CircuitOpenError, circuitBreaker, operation } from "../../src/index.js"
import { memoryBreakerCoordinator } from "../support/memory-coordinator/memory-circuit-breaker.js"

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const traits = () => ({
  abort: "unsupported" as const,
  replay: "safe" as const,
})

/** Build an operation backed by a distributed circuit breaker with tight defaults. */
function makeOp(
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

function fail(): Promise<never> {
  return Promise.reject(new Error("boom"))
}
function succeed(): Promise<string> {
  return Promise.resolve("ok")
}

/** Drive n attempts through op, ignoring errors, returns settled events list. */
async function drive(
  op: ReturnType<typeof operation>,
  n: number,
): Promise<void> {
  await Promise.allSettled(
    Array.from({ length: n }, () => op.execute(undefined)),
  )
}

/** Tight distributed breaker for most tests — opens after 5 failures with threshold 0.5. */
function tightBreaker(
  coordinator: BreakerCoordinator,
  overrides: Record<string, unknown> = {},
) {
  return circuitBreaker.distributed({
    name: "test",
    coordinator,
    scope: () => "shared",
    minimumThroughput: 5,
    failureThreshold: 0.5,
    windowSize: 20,
    openMs: 60_000, // long; tests advance time via coordinator directly
    halfOpenProbes: 2,
    halfOpenSuccesses: 1,
    probeLeaseTtlMs: 30_000,
    ...overrides,
  })
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

describe("circuitBreaker.distributed — validation", () => {
  const coordinator = memoryBreakerCoordinator()

  it("rejects empty name", () => {
    expect(() =>
      circuitBreaker.distributed({ name: "", coordinator, scope: () => "x" }),
    ).toThrow()
  })

  it("requires coordinator", () => {
    expect(() =>
      circuitBreaker.distributed({
        name: "x",
        coordinator: null as never,
        scope: () => "x",
      }),
    ).toThrow()
  })

  it("requires scope to be a function", () => {
    expect(() =>
      circuitBreaker.distributed({
        name: "x",
        coordinator,
        scope: "static" as never,
      }),
    ).toThrow()
  })

  it("rejects non-positive minimumThroughput", () => {
    expect(() =>
      circuitBreaker.distributed({
        name: "x",
        coordinator,
        scope: () => "x",
        minimumThroughput: 0,
      }),
    ).toThrow()
  })

  it("rejects failureThreshold outside (0,1)", () => {
    expect(() =>
      circuitBreaker.distributed({
        name: "x",
        coordinator,
        scope: () => "x",
        failureThreshold: 0,
      }),
    ).toThrow()
    expect(() =>
      circuitBreaker.distributed({
        name: "x",
        coordinator,
        scope: () => "x",
        failureThreshold: 1,
      }),
    ).toThrow()
  })

  it("rejects invalid onCoordinatorError", () => {
    expect(() =>
      circuitBreaker.distributed({
        name: "x",
        coordinator,
        scope: () => "x",
        onCoordinatorError: "invalid" as never,
      }),
    ).toThrow()
  })

  it("exposes coordination: 'distributed'", () => {
    const policy = circuitBreaker.distributed({
      name: "x",
      coordinator,
      scope: () => "x",
    })
    expect((policy as { coordination: string }).coordination).toBe(
      "distributed",
    )
  })
})

// ---------------------------------------------------------------------------
// CLOSED state: passes through
// ---------------------------------------------------------------------------

describe("circuitBreaker.distributed — closed state", () => {
  it("lets successful attempts through without emitting rejection", async () => {
    const coordinator = memoryBreakerCoordinator()
    const events: OperationEvent[] = []
    const policy = tightBreaker(coordinator)
    const op = makeOp(policy, succeed, events)

    await op.execute(undefined)

    const breaker = events.filter((e) => e.type.startsWith("breaker."))
    expect(breaker.every((e) => e.type !== "breaker.rejected")).toBe(true)
    expect(breaker.some((e) => e.type === "breaker.observation")).toBe(true)
  })

  it("records failures in the window without opening below threshold", async () => {
    const coordinator = memoryBreakerCoordinator()
    const events: OperationEvent[] = []
    const policy = tightBreaker(coordinator)
    const op = makeOp(policy, fail, events)

    // 4 failures — below minimumThroughput of 5
    await drive(op, 4)
    expect(
      events.filter((e) => e.type === "breaker.state-changed"),
    ).toHaveLength(0)
  })
})

// ---------------------------------------------------------------------------
// CLOSED → OPEN transition
// ---------------------------------------------------------------------------

describe("circuitBreaker.distributed — opening on threshold", () => {
  it("opens after threshold is reached and emits state-changed", async () => {
    const coordinator = memoryBreakerCoordinator()
    const events: OperationEvent[] = []
    const policy = tightBreaker(coordinator)
    const op = makeOp(policy, fail, events)

    // 5 failures hits minimumThroughput=5 at 100% failure rate
    await drive(op, 5)

    const stateChanged = events.filter(
      (e) => e.type === "breaker.state-changed",
    )
    expect(stateChanged).toHaveLength(1)
    expect((stateChanged[0] as { state: string }).state).toBe("open")
    expect((stateChanged[0] as { previousState: string }).previousState).toBe(
      "closed",
    )
    expect((stateChanged[0] as { coordination: string }).coordination).toBe(
      "distributed",
    )
  })

  it("rejects further calls once open", async () => {
    const coordinator = memoryBreakerCoordinator()
    const policy = tightBreaker(coordinator)

    // Open the breaker
    await drive(makeOp(policy, fail), 5)

    const events: OperationEvent[] = []
    const op = makeOp(policy, succeed, events)

    await expect(op.execute(undefined)).rejects.toBeInstanceOf(CircuitOpenError)

    const rejected = events.filter((e) => e.type === "breaker.rejected")
    expect(rejected).toHaveLength(1)
    expect((rejected[0] as { coordination: string }).coordination).toBe(
      "distributed",
    )
  })

  it("emits observation events for each attempt", async () => {
    const coordinator = memoryBreakerCoordinator()
    const events: OperationEvent[] = []
    const policy = tightBreaker(coordinator)
    const op = makeOp(policy, fail, events)

    await drive(op, 3)
    expect(events.filter((e) => e.type === "breaker.observation")).toHaveLength(
      3,
    )
  })
})

// ---------------------------------------------------------------------------
// OPEN → HALF_OPEN: probe admission
// ---------------------------------------------------------------------------

describe("circuitBreaker.distributed — half-open probes", () => {
  it("emits probe-started when admitted to half-open", async () => {
    const coordinator = memoryBreakerCoordinator()
    const events: OperationEvent[] = []

    // Open the breaker
    await drive(makeOp(tightBreaker(coordinator), fail), 5)

    // Force openedAt to be in the past so openMs has elapsed
    const identity = { name: "test", operation: "work", scope: "shared" }
    // biome-ignore lint/style/noNonNullAssertion: state is known to exist after drive()
    const rec = coordinator.inspect(identity)!
    ;(rec as { openedAt: number }).openedAt = Date.now() - 120_000

    const policy = tightBreaker(coordinator)
    const op = makeOp(policy, succeed, events)

    await op.execute(undefined)

    expect(events.some((e) => e.type === "breaker.probe-started")).toBe(true)
    expect(
      events.some(
        (e) =>
          e.type === "breaker.state-changed" &&
          (e as { state: string }).state === "half-open",
      ),
    ).toBe(true)
  })

  it("closes the breaker after enough probe successes", async () => {
    const coordinator = memoryBreakerCoordinator()
    const events: OperationEvent[] = []

    await drive(makeOp(tightBreaker(coordinator), fail), 5)

    const identity = { name: "test", operation: "work", scope: "shared" }
    // biome-ignore lint/style/noNonNullAssertion: state is known to exist after drive()
    const rec = coordinator.inspect(identity)!
    ;(rec as { openedAt: number }).openedAt = Date.now() - 120_000

    const policy = tightBreaker(coordinator, { halfOpenSuccesses: 1 })
    const op = makeOp(policy, succeed, events)

    await op.execute(undefined)

    const closed = events.filter(
      (e) =>
        e.type === "breaker.state-changed" &&
        (e as { state: string }).state === "closed",
    )
    expect(closed).toHaveLength(1)
    expect((closed[0] as { previousState: string }).previousState).toBe(
      "half-open",
    )
  })

  it("re-opens on probe failure", async () => {
    const coordinator = memoryBreakerCoordinator()
    const events: OperationEvent[] = []

    await drive(makeOp(tightBreaker(coordinator), fail), 5)

    const identity = { name: "test", operation: "work", scope: "shared" }
    // biome-ignore lint/style/noNonNullAssertion: state is known to exist after drive()
    const rec = coordinator.inspect(identity)!
    ;(rec as { openedAt: number }).openedAt = Date.now() - 120_000

    const policy = tightBreaker(coordinator)
    const op = makeOp(policy, fail, events)

    await op.execute(undefined).catch(() => {})

    const opened = events.filter(
      (e) =>
        e.type === "breaker.state-changed" &&
        (e as { state: string }).state === "open" &&
        (e as { previousState: string }).previousState === "half-open",
    )
    expect(opened).toHaveLength(1)
  })

  it("rejects when probe limit is reached", async () => {
    const coordinator = memoryBreakerCoordinator()

    await drive(makeOp(tightBreaker(coordinator), fail), 5)

    const identity = { name: "test", operation: "work", scope: "shared" }
    // biome-ignore lint/style/noNonNullAssertion: state is known to exist after drive()
    const rec = coordinator.inspect(identity)!
    ;(rec as { openedAt: number }).openedAt = Date.now() - 120_000

    // First probe — holds in flight (Deferred)
    const policy = tightBreaker(coordinator, { halfOpenProbes: 1 })
    let resolveProbe!: () => void
    const inFlightPromise = new Promise<void>((res) => {
      resolveProbe = res
    })

    const op1 = makeOp(policy, () => inFlightPromise.then(succeed))
    const in1 = op1.execute(undefined) // probe acquired, not yet settled

    // Second attempt should be rejected (probe limit = 1)
    const events: OperationEvent[] = []
    const op2 = makeOp(policy, succeed, events)
    await expect(op2.execute(undefined)).rejects.toBeInstanceOf(
      CircuitOpenError,
    )

    resolveProbe()
    await in1.catch(() => {})

    const rejected = events.filter((e) => e.type === "breaker.rejected")
    expect(rejected).toHaveLength(1)
    expect((rejected[0] as { state: string }).state).toBe("half-open")
  })
})

// ---------------------------------------------------------------------------
// Stale observation (generation mismatch)
// ---------------------------------------------------------------------------

describe("circuitBreaker.distributed — stale observations", () => {
  it("emits observation-stale when generation does not match", async () => {
    const _coordinator = memoryBreakerCoordinator()
    const events: OperationEvent[] = []

    // A forged coordinator that opens immediately on observe then returns stale
    const _identity = { name: "test", operation: "work", scope: "shared" }
    const openGen = 1

    // Pre-seed coordinator state as OPEN generation=1 so our readState returns it
    const realCoordinator = memoryBreakerCoordinator()

    // Fake coordinator: readState returns gen=0 (closed), observe always returns stale
    const fakeCoordinator: BreakerCoordinator = {
      readState: async () => ({ state: "closed", generation: 0 }),
      observe: async () => ({ type: "stale", currentGeneration: openGen }),
      admitProbe: realCoordinator.admitProbe.bind(realCoordinator),
      settleProbe: realCoordinator.settleProbe.bind(realCoordinator),
    }

    const policy = circuitBreaker.distributed({
      name: "test",
      coordinator: fakeCoordinator,
      scope: () => "shared",
      minimumThroughput: 5,
      failureThreshold: 0.5,
    })

    const op = makeOp(policy, succeed, events)
    await op.execute(undefined)

    const stale = events.filter((e) => e.type === "breaker.observation-stale")
    expect(stale).toHaveLength(1)
    expect((stale[0] as { attemptGeneration: number }).attemptGeneration).toBe(
      0,
    )
    expect((stale[0] as { currentGeneration: number }).currentGeneration).toBe(
      1,
    )
  })
})

// ---------------------------------------------------------------------------
// Coordinator failure handling
// ---------------------------------------------------------------------------

describe("circuitBreaker.distributed — coordinator failures", () => {
  it("fail-open: coordinator error at CLOSED admission lets attempt through", async () => {
    const events: OperationEvent[] = []
    const badCoordinator: BreakerCoordinator = {
      readState: async () => {
        throw new Error("redis down")
      },
      observe: async () => ({
        type: "observed",
        generation: 0,
        windowTotal: 1,
        windowFailures: 0,
      }),
      admitProbe: async () => {
        throw new Error("redis down")
      },
      settleProbe: async () => ({ type: "stale", generation: 0 }),
    }

    const policy = circuitBreaker.distributed({
      name: "test",
      coordinator: badCoordinator,
      scope: () => "shared",
      onCoordinatorError: "fail-open",
    })
    const op = makeOp(policy, succeed, events)

    await expect(op.execute(undefined)).resolves.toBeDefined()

    expect(events.some((e) => e.type === "breaker.coordinator-error")).toBe(
      true,
    )
    expect(
      events.some(
        (e) =>
          e.type === "breaker.degraded" &&
          (e as { behavior: string }).behavior === "fail-open",
      ),
    ).toBe(true)
  })

  it("fail-closed: coordinator error at CLOSED admission rejects", async () => {
    const events: OperationEvent[] = []
    const badCoordinator: BreakerCoordinator = {
      readState: async () => {
        throw new Error("redis down")
      },
      observe: async () => ({
        type: "observed",
        generation: 0,
        windowTotal: 1,
        windowFailures: 0,
      }),
      admitProbe: async () => {
        throw new Error("redis down")
      },
      settleProbe: async () => ({ type: "stale", generation: 0 }),
    }

    const policy = circuitBreaker.distributed({
      name: "test",
      coordinator: badCoordinator,
      scope: () => "shared",
      onCoordinatorError: "fail-closed",
    })
    const op = makeOp(policy, succeed, events)

    await expect(op.execute(undefined)).rejects.toBeInstanceOf(CircuitOpenError)
    expect(
      events.some(
        (e) =>
          e.type === "breaker.degraded" &&
          (e as { behavior: string }).behavior === "fail-closed",
      ),
    ).toBe(true)
  })

  // ---- last-known-state cache tests ----------------------------------------

  it("readState failure fails-closed when last known state was OPEN (even with fail-open setting)", async () => {
    const events: OperationEvent[] = []
    const coordinator = memoryBreakerCoordinator()
    // Drive the breaker open so the policy sees OPEN in a successful read
    const policy = tightBreaker(coordinator, {
      onCoordinatorError: "fail-open",
    })
    await drive(makeOp(policy, fail), 5)

    // Patch coordinator so readState now throws; the policy instance already
    // has "open" in its last-known-state cache from the drive() calls above.
    const origRead = coordinator.readState.bind(coordinator)
    ;(coordinator as unknown as Record<string, unknown>).readState =
      async () => {
        throw new Error("redis down")
      }

    await expect(
      makeOp(policy, succeed, events).execute(undefined),
    ).rejects.toBeInstanceOf(CircuitOpenError)

    ;(coordinator as unknown as Record<string, unknown>).readState = origRead

    expect(
      events.some(
        (e) =>
          e.type === "breaker.degraded" &&
          (e as { behavior: string }).behavior === "fail-closed",
      ),
    ).toBe(true)
  })

  it("readState failure respects onCoordinatorError: fail-open when last known state was CLOSED", async () => {
    const events: OperationEvent[] = []
    const coordinator = memoryBreakerCoordinator()

    // Seed a successful CLOSED read into the cache
    const policy = tightBreaker(coordinator, {
      onCoordinatorError: "fail-open",
    })
    await makeOp(policy, succeed).execute(undefined)

    // Now make readState throw; cache holds "closed" → onCoordinatorError applies
    const origRead = coordinator.readState.bind(coordinator)
    ;(coordinator as unknown as Record<string, unknown>).readState =
      async () => {
        throw new Error("redis down")
      }

    await expect(
      makeOp(policy, succeed, events).execute(undefined),
    ).resolves.toBeDefined()

    ;(coordinator as unknown as Record<string, unknown>).readState = origRead

    expect(
      events.some(
        (e) =>
          e.type === "breaker.degraded" &&
          (e as { behavior: string }).behavior === "fail-open",
      ),
    ).toBe(true)
  })

  it("readState failure respects onCoordinatorError: fail-closed when last known state was CLOSED", async () => {
    const events: OperationEvent[] = []
    const coordinator = memoryBreakerCoordinator()

    const policy = tightBreaker(coordinator, {
      onCoordinatorError: "fail-closed",
    })
    await makeOp(policy, succeed).execute(undefined) // seed CLOSED cache

    const origRead = coordinator.readState.bind(coordinator)
    ;(coordinator as unknown as Record<string, unknown>).readState =
      async () => {
        throw new Error("redis down")
      }

    await expect(
      makeOp(policy, succeed, events).execute(undefined),
    ).rejects.toBeInstanceOf(CircuitOpenError)

    ;(coordinator as unknown as Record<string, unknown>).readState = origRead

    expect(
      events.some(
        (e) =>
          e.type === "breaker.degraded" &&
          (e as { behavior: string }).behavior === "fail-closed",
      ),
    ).toBe(true)
  })

  it("readState failure with no prior read respects onCoordinatorError: fail-open (empty cache)", async () => {
    const events: OperationEvent[] = []
    const badCoordinator: BreakerCoordinator = {
      readState: async () => {
        throw new Error("redis down")
      },
      observe: async () => ({
        type: "observed",
        generation: 0,
        windowTotal: 1,
        windowFailures: 0,
      }),
      admitProbe: async () => {
        throw new Error("redis down")
      },
      settleProbe: async () => ({ type: "stale", generation: 0 }),
    }
    const policy = circuitBreaker.distributed({
      name: "test",
      coordinator: badCoordinator,
      scope: () => "shared",
      onCoordinatorError: "fail-open",
    })
    // Empty cache → onCoordinatorError applies → fail-open → admit
    await expect(
      makeOp(policy, succeed, events).execute(undefined),
    ).resolves.toBeDefined()
    expect(
      events.some(
        (e) =>
          e.type === "breaker.degraded" &&
          (e as { behavior: string }).behavior === "fail-open",
      ),
    ).toBe(true)
  })

  it("cache updates to CLOSED after recovery; subsequent readState failure uses onCoordinatorError", async () => {
    const events: OperationEvent[] = []
    const coordinator = memoryBreakerCoordinator()
    const policy = tightBreaker(coordinator, {
      onCoordinatorError: "fail-open",
    })

    // Open the breaker (cache → "open")
    await drive(makeOp(policy, fail), 5)

    // Force past openMs so probe can be admitted
    // biome-ignore lint/style/noNonNullAssertion: state is known to exist after drive()
    const rec = coordinator.inspect({
      name: "test",
      operation: "work",
      scope: "shared",
    })!
    ;(rec as { openedAt: number }).openedAt = Date.now() - 120_000

    // Successful probe closes the breaker (cache → "closed")
    await makeOp(policy, succeed).execute(undefined)

    // Now make readState throw; cache now holds "closed" → fail-open applies
    const origRead = coordinator.readState.bind(coordinator)
    ;(coordinator as unknown as Record<string, unknown>).readState =
      async () => {
        throw new Error("redis down")
      }

    await expect(
      makeOp(policy, succeed, events).execute(undefined),
    ).resolves.toBeDefined()

    ;(coordinator as unknown as Record<string, unknown>).readState = origRead

    expect(
      events.some(
        (e) =>
          e.type === "breaker.degraded" &&
          (e as { behavior: string }).behavior === "fail-open",
      ),
    ).toBe(true)
    expect(events.filter((e) => e.type === "breaker.rejected")).toHaveLength(0)
  })

  it("coordinator error during admitProbe always rejects (OPEN state)", async () => {
    const events: OperationEvent[] = []
    const coordinator = memoryBreakerCoordinator()
    // Drive the breaker open
    await drive(makeOp(tightBreaker(coordinator), fail), 5)

    // Wrap coordinator: readState works, admitProbe throws
    const wrapped: BreakerCoordinator = {
      readState: coordinator.readState.bind(coordinator),
      observe: coordinator.observe.bind(coordinator),
      admitProbe: async () => {
        throw new Error("redis down")
      },
      settleProbe: coordinator.settleProbe.bind(coordinator),
    }

    const policy = circuitBreaker.distributed({
      name: "test",
      coordinator: wrapped,
      scope: () => "shared",
      minimumThroughput: 5,
      failureThreshold: 0.5,
      onCoordinatorError: "fail-open", // should NOT apply here
    })
    const op = makeOp(policy, succeed, events)

    await expect(op.execute(undefined)).rejects.toBeInstanceOf(CircuitOpenError)
    expect(
      events.some(
        (e) =>
          e.type === "breaker.degraded" &&
          (e as { behavior: string }).behavior === "fail-closed",
      ),
    ).toBe(true)
  })

  it("coordinator error during observe does not throw but emits breaker.coordinator-error", async () => {
    const events: OperationEvent[] = []
    const badObserve: BreakerCoordinator = {
      readState: async () => null,
      observe: async () => {
        throw new Error("redis down")
      },
      admitProbe: async () => ({
        type: "rejected",
        reason: "closed",
        generation: 0,
      }),
      settleProbe: async () => ({ type: "stale", generation: 0 }),
    }

    const policy = circuitBreaker.distributed({
      name: "test",
      coordinator: badObserve,
      scope: () => "shared",
    })
    const op = makeOp(policy, succeed, events)

    // Should NOT throw — caller is not affected by a lost observation
    await expect(op.execute(undefined)).resolves.toBeDefined()

    // But the diagnostic event MUST be emitted
    const coordErrors = events.filter(
      (e) => e.type === "breaker.coordinator-error",
    )
    expect(coordErrors).toHaveLength(1)
    expect((coordErrors[0] as { operation: string }).operation).toBe("observe")
  })

  it("coordinator error during settleProbe does not throw but emits breaker.coordinator-error", async () => {
    const events: OperationEvent[] = []
    const coordinator = memoryBreakerCoordinator()

    // Drive breaker open
    await drive(makeOp(tightBreaker(coordinator), fail), 5)

    // Force openMs elapsed
    const identity = { name: "test", operation: "work", scope: "shared" }
    // biome-ignore lint/style/noNonNullAssertion: state is known to exist after drive()
    const rec = coordinator.inspect(identity)!
    ;(rec as { openedAt: number }).openedAt = Date.now() - 120_000

    // Wrap: settleProbe throws
    const wrapped: BreakerCoordinator = {
      readState: coordinator.readState.bind(coordinator),
      observe: coordinator.observe.bind(coordinator),
      admitProbe: coordinator.admitProbe.bind(coordinator),
      settleProbe: async () => {
        throw new Error("redis down")
      },
    }

    const policy = circuitBreaker.distributed({
      name: "test",
      coordinator: wrapped,
      scope: () => "shared",
      minimumThroughput: 5,
      failureThreshold: 0.5,
    })
    const op = makeOp(policy, succeed, events)

    // Should resolve — caller is not affected by a lost probe settlement
    await expect(op.execute(undefined)).resolves.toBeDefined()

    // But the diagnostic event MUST be emitted
    const coordErrors = events.filter(
      (e) => e.type === "breaker.coordinator-error",
    )
    expect(coordErrors).toHaveLength(1)
    expect((coordErrors[0] as { operation: string }).operation).toBe(
      "settle-probe",
    )
  })
})

// ---------------------------------------------------------------------------
// Outcome classifier
// ---------------------------------------------------------------------------

describe("circuitBreaker.distributed — classify", () => {
  it("does not record observation for 'ignored' outcome", async () => {
    const coordinator = memoryBreakerCoordinator()
    const events: OperationEvent[] = []

    const policy = circuitBreaker.distributed({
      name: "test",
      coordinator,
      scope: () => "shared",
      classify: () => "ignored",
    })
    const op = makeOp(policy, fail, events)

    await op.execute(undefined).catch(() => {})

    expect(events.filter((e) => e.type === "breaker.observation")).toHaveLength(
      0,
    )
  })

  it("does not open breaker when all outcomes are 'ignored'", async () => {
    const coordinator = memoryBreakerCoordinator()
    const policy = circuitBreaker.distributed({
      name: "test",
      coordinator,
      scope: () => "shared",
      minimumThroughput: 3,
      classify: () => "ignored",
    })
    const op = makeOp(policy, fail)

    await drive(op, 10)

    // Subsequent call must NOT be rejected
    const events: OperationEvent[] = []
    await makeOp(policy, succeed, events).execute(undefined)
    expect(events.filter((e) => e.type === "breaker.rejected")).toHaveLength(0)
  })
})

// ---------------------------------------------------------------------------
// Multiple independent scopes
// ---------------------------------------------------------------------------

describe("circuitBreaker.distributed — scope isolation", () => {
  it("scope A open does not affect scope B", async () => {
    const coordinator = memoryBreakerCoordinator()
    let currentScope = "A"

    const policy = circuitBreaker.distributed({
      name: "test",
      coordinator,
      scope: () => currentScope,
      minimumThroughput: 5,
      failureThreshold: 0.5,
    })

    // Open scope A
    currentScope = "A"
    await drive(makeOp(policy, fail), 5)

    // Scope B should still be CLOSED
    currentScope = "B"
    const events: OperationEvent[] = []
    await makeOp(policy, succeed, events).execute(undefined)

    expect(events.filter((e) => e.type === "breaker.rejected")).toHaveLength(0)
  })
})

// ---------------------------------------------------------------------------
// Cross-instance state sharing (same coordinator, different policy instances)
// ---------------------------------------------------------------------------

describe("circuitBreaker.distributed — cross-instance sharing", () => {
  it("two policy instances sharing the same coordinator share state", async () => {
    const coordinator = memoryBreakerCoordinator()

    const policy1 = tightBreaker(coordinator)
    const policy2 = tightBreaker(coordinator) // same coordinator

    // Drive policy1 to OPEN
    await drive(makeOp(policy1, fail), 5)

    // policy2 should observe OPEN state
    await expect(
      makeOp(policy2, succeed).execute(undefined),
    ).rejects.toBeInstanceOf(CircuitOpenError)
  })
})

// ---------------------------------------------------------------------------
// Expiry / TTL-safety guarantees (coordinator contract)
//
// These tests exercise the three failure modes identified in the Redis expiry
// audit against the in-memory coordinator, ensuring the contract is upheld
// regardless of which backend is used.  Corresponding Redis-level assertions
// for the Lua PERSIST calls live in the integration test suite.
// ---------------------------------------------------------------------------

describe("circuitBreaker.distributed — expiry / TTL-safety guarantees", () => {
  // 1. An open breaker must remain open even when idle (no traffic).
  //    Without traffic the Redis key would receive no TTL refreshes under the
  //    old scheme; the memory coordinator must mirror that invariant.
  it("an open breaker continues to reject calls when idle (no interim traffic)", async () => {
    const coordinator = memoryBreakerCoordinator()
    const policy = tightBreaker(coordinator)
    const op = makeOp(policy, fail)

    // Drive to OPEN
    await drive(op, 5)

    // No further calls are made (simulates an idle period).
    // The breaker must still be OPEN for the next caller.
    const events: OperationEvent[] = []
    await expect(
      makeOp(policy, succeed, events).execute(undefined),
    ).rejects.toBeInstanceOf(CircuitOpenError)
    expect(events.filter((e) => e.type === "breaker.rejected")).toHaveLength(1)
  })

  // 2. A generation-zero result from a pre-open cycle must be rejected as
  //    stale after the breaker has opened (generation incremented to 1+).
  //    The stale-result guard must work even when expectGen === 0.
  it("rejects a gen-0 observation that arrives after the breaker has opened", async () => {
    const coordinator = memoryBreakerCoordinator()
    const policy = tightBreaker(coordinator)

    // Capture a gen-0 admission by starting an attempt that will settle late
    const gate = new (
      await import("../support/deferred.js").then((m) => m.Deferred)
    )<string>()
    const lateAttempt = makeOp(policy, () => gate.promise).execute(undefined)

    // Drive breaker to OPEN (generation now ≥ 1)
    await drive(makeOp(policy, fail), 5)

    // Settle the stale gen-0 attempt as a failure — must not affect state
    const identity = { name: "test", operation: "work", scope: "shared" }
    const stateBefore = await coordinator.readState(identity)
    gate.reject(new Error("stale"))
    await lateAttempt.catch(() => {})

    // State must be unchanged: still OPEN at the same generation
    const stateAfter = await coordinator.readState(identity)
    expect(stateAfter).toEqual(stateBefore)
  })

  // 3. An open breaker reached via admitProbe must still be OPEN after the
  //    probe lease would have expired (i.e. probeKey outliving stateHash is
  //    not possible if PERSIST is used; the state remains fully recoverable).
  it("an open breaker is still observable for probe admission after a long idle period", async () => {
    const coordinator = memoryBreakerCoordinator()
    const policy = circuitBreaker.distributed({
      name: "test",
      coordinator,
      scope: () => "shared",
      minimumThroughput: 5,
      failureThreshold: 0.5,
      openMs: 50, // very short so we can transition to half-open
      halfOpenProbes: 1,
      halfOpenSuccesses: 1,
      probeLeaseTtlMs: 1, // immediately-expiring lease to simulate orphan token
    })

    // Open the breaker
    await drive(makeOp(policy, fail), 5)

    // Wait past openMs so half-open transition can occur
    await new Promise((r) => setTimeout(r, 60))

    // Probe should be admitted (breaker state still intact, no expiry)
    const events: OperationEvent[] = []
    await makeOp(policy, succeed, events).execute(undefined)
    expect(
      events.filter((e) => e.type === "breaker.probe-started"),
    ).toHaveLength(1)
  })
})

// ---------------------------------------------------------------------------
// Scope function validation
// ---------------------------------------------------------------------------

describe("circuitBreaker.distributed — scope validation at runtime", () => {
  it("throws TypeError if scope returns non-string", async () => {
    const policy = circuitBreaker.distributed({
      name: "test",
      coordinator: memoryBreakerCoordinator(),
      scope: () => "" as string, // empty string
    })
    await expect(
      operation({
        name: "work",
        adapter: { capabilities: traits, execute: succeed },
        policies: [policy],
      }).execute(undefined),
    ).rejects.toBeInstanceOf(TypeError)
  })
})

// ---------------------------------------------------------------------------
// Threshold resolution
// ---------------------------------------------------------------------------

describe("circuitBreaker.distributed — threshold resolution", () => {
  const identity = { name: "test", operation: "work", scope: "shared" }

  it("rejects thresholds that cannot be resolved to thousandths", () => {
    const coordinator = memoryBreakerCoordinator()
    // Below 0.0005 the numerator rounds to 0, which makes the coordinator's
    // comparison unconditionally true; 0.9995 and above rounds to 1000, which
    // requires every observation to fail.
    for (const failureThreshold of [0.0004, 0.0001, 0.9995, 0.9999]) {
      expect(() =>
        circuitBreaker.distributed({
          name: "x",
          coordinator,
          scope: () => "x",
          failureThreshold,
        }),
      ).toThrow(/resolved to thousandths/)
    }
  })

  it("accepts the smallest and largest resolvable thresholds", () => {
    const coordinator = memoryBreakerCoordinator()
    for (const failureThreshold of [0.0005, 0.9994]) {
      expect(() =>
        circuitBreaker.distributed({
          name: "x",
          coordinator,
          scope: () => "x",
          failureThreshold,
        }),
      ).not.toThrow()
    }
  })

  it("never opens on a success-only trace at the smallest resolvable threshold", async () => {
    const coordinator = memoryBreakerCoordinator()
    const events: OperationEvent[] = []
    const policy = tightBreaker(coordinator, {
      minimumThroughput: 5,
      failureThreshold: 0.0005,
      windowSize: 20,
    })

    await drive(makeOp(policy, succeed, events), 20)

    expect(
      events.filter((e) => e.type === "breaker.state-changed"),
    ).toHaveLength(0)
    expect((await coordinator.readState(identity))?.state).toBe("closed")
  })
})
