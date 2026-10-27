import { randomUUID } from "node:crypto"
import { afterAll, beforeAll, describe, expect, it } from "vitest"
import { coordinationKey } from "../../src/coordination/redis/keys.js"
import type { OperationEvent } from "../../src/index.js"
import { CircuitOpenError, circuitBreaker, operation } from "../../src/index.js"
import {
  createCoordinationClient,
  redisCircuitBreakerCoordinator,
  redisConfigurationManager,
} from "../../src/redis.js"
import { redisProxy } from "../support/redis-proxy.js"
import { BreakerWorker } from "../support/worker-process/harness.js"

const url = process.env.CARACAL_REDIS_URL

describe.skipIf(!url)("distributed circuit breaker — Redis integration", () => {
  const client = createCoordinationClient(url ?? "redis://127.0.0.1:6379")
  const cleanupKeys: string[] = []

  function track(namespace: string): string {
    // Track the three keys the circuit breaker creates for this namespace
    for (const suffix of ["breaker", "observations", "probes"] as const) {
      cleanupKeys.push(
        coordinationKey(namespace, "breaker:breaker", "work", "shared", suffix),
      )
    }
    return namespace
  }

  function makeCoordinator(namespace: string) {
    return redisCircuitBreakerCoordinator(client, { namespace })
  }

  const traits = () => ({
    abort: "unsupported" as const,
    replay: "safe" as const,
  })

  function makePolicy(
    namespace: string,
    overrides: Record<string, unknown> = {},
  ) {
    return circuitBreaker.distributed({
      name: "breaker",
      coordinator: makeCoordinator(namespace),
      scope: () => "shared",
      minimumThroughput: 5,
      failureThreshold: 0.5,
      windowSize: 20,
      openMs: 10_000,
      halfOpenProbes: 2,
      halfOpenSuccesses: 1,
      probeLeaseTtlMs: 3_000,
      ...overrides,
    })
  }

  function makeOp(
    policy: ReturnType<typeof makePolicy>,
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

  async function drive(
    op: ReturnType<typeof operation>,
    n: number,
  ): Promise<void> {
    await Promise.allSettled(
      Array.from({ length: n }, () => op.execute(undefined)),
    )
  }

  beforeAll(() => client.connect())

  it("applies a stored configuration threshold to real observations", async () => {
    const namespace = `test-${randomUUID()}`
    track(namespace)
    cleanupKeys.push(
      coordinationKey(
        namespace,
        "breaker:breaker",
        "work",
        "shared",
        "configuration",
      ),
    )
    const manager = redisConfigurationManager(client, { namespace })
    await manager.update(
      {
        policy: "circuitBreaker",
        name: "breaker",
        operation: "work",
        scope: "shared",
      },
      0,
      {
        policy: "circuitBreaker",
        failureThreshold: 0.5,
        minimumThroughput: 1,
        windowSize: 20,
        openMs: 10_000,
        halfOpenProbes: 2,
        halfOpenSuccesses: 1,
      },
    )
    // The constructor baseline is minimumThroughput 5, so one failure would not
    // open it. The stored record lowers the floor to a single observation.
    const op = operation({
      name: "work",
      adapter: {
        capabilities: traits,
        classify: () => "failure",
        execute: async () => {
          throw new Error("down")
        },
      },
      policies: [makePolicy(namespace)],
    })
    await expect(op.execute(undefined)).rejects.toThrow("down")
    await expect(op.execute(undefined)).rejects.toBeInstanceOf(CircuitOpenError)
  })

  afterAll(async () => {
    try {
      if (cleanupKeys.length) await client.del(...cleanupKeys)
    } finally {
      client.disconnect()
    }
  })

  // -----------------------------------------------------------------------
  // 1. Opens on threshold
  // -----------------------------------------------------------------------
  it("opens the breaker in Redis when failure threshold is reached", async () => {
    const namespace = track(`test-${randomUUID()}`)
    const events: OperationEvent[] = []
    const policy = makePolicy(namespace)
    const op = makeOp(policy, () => Promise.reject(new Error("boom")), events)

    await drive(op, 5) // 5 failures at 100% → opens

    const opened = events.filter(
      (e) =>
        e.type === "breaker.state-changed" &&
        (e as { state: string }).state === "open",
    )
    expect(opened).toHaveLength(1)
  })

  // -----------------------------------------------------------------------
  // 2. Rejects when open
  // -----------------------------------------------------------------------
  it("rejects subsequent attempts when open", async () => {
    const namespace = track(`test-${randomUUID()}`)
    const policy = makePolicy(namespace)

    // Open the breaker
    await drive(
      makeOp(policy, () => Promise.reject(new Error("boom"))),
      5,
    )

    // Now attempts should be rejected
    const events: OperationEvent[] = []
    const op = makeOp(policy, () => Promise.resolve("ok"), events)

    await expect(op.execute(undefined)).rejects.toBeInstanceOf(CircuitOpenError)

    const rejected = events.filter((e) => e.type === "breaker.rejected")
    expect(rejected).toHaveLength(1)
    expect((rejected[0] as { coordination: string }).coordination).toBe(
      "distributed",
    )
  })

  // -----------------------------------------------------------------------
  // 3. Probe success closes the breaker
  // -----------------------------------------------------------------------
  it("closes the breaker after a probe success", async () => {
    const namespace = track(`test-${randomUUID()}`)
    // openMs=200 → hashTtl=400ms; we wait 250ms so state survives while openMs elapses
    const policy = makePolicy(namespace, {
      openMs: 200,
      probeLeaseTtlMs: 5_000,
      halfOpenSuccesses: 1,
    })

    // Open the breaker
    await drive(
      makeOp(policy, () => Promise.reject(new Error("boom"))),
      5,
    )

    // Wait for openMs to elapse (hashTtl=400ms so state is still alive)
    await new Promise((r) => setTimeout(r, 250))

    // Probe with a success
    const events: OperationEvent[] = []
    const op = makeOp(policy, () => Promise.resolve("ok"), events)
    await op.execute(undefined)

    const closedEvts = events.filter(
      (e) =>
        e.type === "breaker.state-changed" &&
        (e as { state: string }).state === "closed",
    )
    expect(closedEvts).toHaveLength(1)
  })

  // -----------------------------------------------------------------------
  // 4. Cross-process: state is shared between workers
  // -----------------------------------------------------------------------
  it("shares open state across worker processes", async () => {
    const namespace = track(`test-${randomUUID()}`)

    const workers = [
      new BreakerWorker(url as string),
      new BreakerWorker(url as string),
    ]
    try {
      await Promise.all(workers.map((w) => w.ready()))

      const cfg = {
        namespace,
        minimumThroughput: 5,
        failureThreshold: 0.5,
        openMs: 30_000,
      }

      // Worker A sends 5 failures → opens the breaker
      await Promise.allSettled(
        Array.from({ length: 5 }, () =>
          // biome-ignore lint/style/noNonNullAssertion: workers array is fully populated
          workers[0]!.execute({ outcome: "failure", ...cfg }).catch(() => {}),
        ),
      )

      // Worker B should now see the open breaker and be rejected
      // biome-ignore lint/style/noNonNullAssertion: workers array is fully populated
      const result = await workers[1]!
        .execute({ outcome: "success", ...cfg })
        .catch((e: Error) => e.message)

      expect(result).toContain("CircuitOpenError")

      // Worker B should have received a breaker.rejected event
      expect(
        // biome-ignore lint/style/noNonNullAssertion: workers array is fully populated
        workers[1]!.events.some((e) => e.type === "breaker.rejected"),
      ).toBe(true)
    } finally {
      await Promise.all(workers.map((w) => w.stop()))
    }
  })

  // -----------------------------------------------------------------------
  // 5. Cross-process: probe limit is enforced across workers
  //
  // The limit bounds *concurrent* probes, so a second probe is legitimately
  // admitted once the first settles.  Racing two workers therefore proves
  // nothing (an instantaneous probe frees its slot before the other worker even
  // reads state) - the slot is held explicitly here instead.
  // -----------------------------------------------------------------------
  it("enforces the probe limit across processes and releases the slot on settle", async () => {
    const namespace = track(`test-${randomUUID()}`)
    const hashKey = coordinationKey(
      namespace,
      "breaker:breaker",
      "work",
      "shared",
      "breaker",
    )
    const probesKey = coordinationKey(
      namespace,
      "breaker:breaker",
      "work",
      "shared",
      "probes",
    )

    const workers = [
      new BreakerWorker(url as string),
      new BreakerWorker(url as string),
    ]
    try {
      await Promise.all(workers.map((w) => w.ready()))

      const cfg = {
        namespace,
        minimumThroughput: 5,
        failureThreshold: 0.5,
        openMs: 200,
        halfOpenProbes: 1, // only 1 concurrent probe
        probeLeaseTtlMs: 5_000,
      }

      // Open the breaker with failures from another process
      await Promise.allSettled(
        Array.from({ length: 5 }, () =>
          // biome-ignore lint/style/noNonNullAssertion: workers array is fully populated
          workers[0]!.execute({ outcome: "failure", ...cfg }).catch(() => {}),
        ),
      )
      expect(await client.hmget(hashKey, "state")).toEqual(["open"])

      // Hold the single probe slot from this process
      const coordinator = redisCircuitBreakerCoordinator(client, { namespace })
      const identity = { name: "breaker", operation: "work", scope: "shared" }
      const heldToken = randomUUID()
      const held = await coordinator.admitProbe(identity, {
        probeToken: heldToken,
        openMs: 0, // the OPEN window has elapsed: transition to HALF_OPEN now
        halfOpenProbes: 1,
        probeLeaseTtlMs: 5_000,
      })
      expect(held).toMatchObject({ type: "admitted", stateChanged: true })
      expect(await client.zcard(probesKey)).toBe(1)

      // A worker process must be rejected while the slot is held
      // biome-ignore lint/style/noNonNullAssertion: workers array is fully populated
      const blocked = await workers[1]!
        .execute({ outcome: "success", ...cfg })
        .catch((e: Error) => e.message)
      expect(blocked).toContain("CircuitOpenError")
      expect(
        // biome-ignore lint/style/noNonNullAssertion: workers array is fully populated
        workers[1]!.events.some((e) => e.type === "breaker.rejected"),
      ).toBe(true)

      // Releasing the slot admits the next worker probe
      await expect(
        coordinator.settleProbe(identity, {
          probeToken: heldToken,
          outcome: "success",
          generation: held.type === "admitted" ? held.generation : 0,
          halfOpenSuccesses: 99, // stay HALF_OPEN: the slot is the only change
          openMs: 200,
          windowTtlMs: 60_000,
        }),
      ).resolves.toMatchObject({ type: "settled" })
      expect(await client.zcard(probesKey)).toBe(0)

      // biome-ignore lint/style/noNonNullAssertion: workers array is fully populated
      const admitted = await workers[1]!
        .execute({ outcome: "success", ...cfg })
        .catch((e: Error) => e.message)
      expect(admitted).toBe("ok")
      expect(await client.hmget(hashKey, "state")).toEqual(["closed"])
    } finally {
      await Promise.all(workers.map((w) => w.stop()))
    }
  })

  // -----------------------------------------------------------------------
  // 6. Dead probe expiry: stale probe token pruned, allowing fresh probe
  // -----------------------------------------------------------------------
  it("allows a fresh probe after a dead worker's probe token expires", async () => {
    const namespace = `test-${randomUUID()}`
    const hashKey = coordinationKey(
      namespace,
      "breaker:breaker",
      "work",
      "shared",
      "breaker",
    )
    const probeKey = coordinationKey(
      namespace,
      "breaker:breaker",
      "work",
      "shared",
      "probes",
    )
    cleanupKeys.push(hashKey, probeKey)

    // Seed breaker state directly: HALF_OPEN, generation=1, one stale probe token
    const staleToken = randomUUID()
    const expiredAt = Date.now() - 1000 // already 1 second in the past
    await client.hset(
      hashKey,
      "state",
      "half-open",
      "generation",
      "1",
      "openedAt",
      String(Date.now() - 60_000),
      "probeCount",
      "1",
      "probeSuccesses",
      "0",
    )
    await client.pexpire(hashKey, 60_000)
    await client.zadd(probeKey, expiredAt, staleToken)
    await client.pexpire(probeKey, 30_000)

    // Worker executes: admitProbe prunes the stale token and grants a fresh probe
    const worker = new BreakerWorker(url as string)
    try {
      await worker.ready()
      const result = await worker
        .execute({
          outcome: "success",
          namespace,
          openMs: 30_000,
          halfOpenProbes: 1, // limit of 1 — only works if stale token is pruned
          halfOpenSuccesses: 1,
          probeLeaseTtlMs: 5_000,
        })
        .catch((e: Error) => e.message)

      expect(result).toBe("ok")
      // Worker should have emitted probe-started, not rejected
      expect(
        worker.events.some((e) => e.type === "breaker.probe-started"),
      ).toBe(true)
    } finally {
      await worker.stop()
    }
  })

  // -----------------------------------------------------------------------
  // 7. Coordinator proxy failure — fail-open and fail-closed
  // -----------------------------------------------------------------------
  it("fails open when coordinator is unreachable (default)", async () => {
    const proxy = await redisProxy(new URL(url as string))
    try {
      const proxyClient = createCoordinationClient(proxy.url)
      await proxyClient.connect()

      const policy = circuitBreaker.distributed({
        name: "breaker",
        coordinator: redisCircuitBreakerCoordinator(proxyClient, {
          namespace: `test-${randomUUID()}`,
        }),
        scope: () => "shared",
        onCoordinatorError: "fail-open",
      })

      // Disconnect the proxy so all subsequent Redis commands fail
      proxy.disconnect()

      const events: OperationEvent[] = []
      const op = makeOp(policy, () => Promise.resolve("ok"), events)

      // Should succeed despite coordinator being down (fail-open)
      await expect(op.execute(undefined)).resolves.toBeDefined()
      expect(
        events.some(
          (e) =>
            e.type === "breaker.degraded" &&
            (e as { behavior: string }).behavior === "fail-open",
        ),
      ).toBe(true)

      proxyClient.disconnect()
    } finally {
      await proxy.close()
    }
  })

  it("fails closed when coordinator is unreachable (onCoordinatorError: fail-closed)", async () => {
    const proxy = await redisProxy(new URL(url as string))
    try {
      const proxyClient = createCoordinationClient(proxy.url)
      await proxyClient.connect()

      const policy = circuitBreaker.distributed({
        name: "breaker",
        coordinator: redisCircuitBreakerCoordinator(proxyClient, {
          namespace: `test-${randomUUID()}`,
        }),
        scope: () => "shared",
        onCoordinatorError: "fail-closed",
      })

      proxy.disconnect()

      const events: OperationEvent[] = []
      const op = makeOp(policy, () => Promise.resolve("ok"), events)

      await expect(op.execute(undefined)).rejects.toBeInstanceOf(
        CircuitOpenError,
      )
      expect(
        events.some(
          (e) =>
            e.type === "breaker.degraded" &&
            (e as { behavior: string }).behavior === "fail-closed",
        ),
      ).toBe(true)

      proxyClient.disconnect()
    } finally {
      await proxy.close()
    }
  })

  // -----------------------------------------------------------------------
  // 8. TTL-safety: OPEN state hash must have no expiry (PERSIST)
  // -----------------------------------------------------------------------
  it("open breaker state hash has no TTL after transition to OPEN", async () => {
    const namespace = track(`test-${randomUUID()}`)
    const hashKey = coordinationKey(
      namespace,
      "breaker:breaker",
      "work",
      "shared",
      "breaker",
    )

    const policy = makePolicy(namespace)
    const op = makeOp(policy, () => Promise.reject(new Error("boom")))
    await drive(op, 5)

    // -1 means the key exists with no TTL (persistent)
    const ttl = await client.pttl(hashKey)
    expect(ttl).toBe(-1)
  })

  // -----------------------------------------------------------------------
  // 9. TTL-safety: re-opened breaker (probe failure) state hash has no TTL
  // -----------------------------------------------------------------------
  it("re-opened breaker state hash has no TTL after probe failure", async () => {
    const namespace = track(`test-${randomUUID()}`)
    const hashKey = coordinationKey(
      namespace,
      "breaker:breaker",
      "work",
      "shared",
      "breaker",
    )

    const policy = makePolicy(namespace, { openMs: 200, halfOpenSuccesses: 2 })
    await drive(
      makeOp(policy, () => Promise.reject(new Error("boom"))),
      5,
    )

    // Wait for openMs to elapse so the next attempt triggers OPEN→HALF_OPEN
    await new Promise((r) => setTimeout(r, 250))

    // Probe fails → HALF_OPEN → OPEN (re-open)
    const failPolicy = makePolicy(namespace, {
      openMs: 200,
      halfOpenSuccesses: 2,
    })
    await makeOp(failPolicy, () => Promise.reject(new Error("probe-fail")))
      .execute(undefined)
      .catch(() => {})

    const ttl = await client.pttl(hashKey)
    expect(ttl).toBe(-1)
  })

  // -----------------------------------------------------------------------
  // 10. TTL-safety: idle open breaker remains OPEN and rejects traffic
  // -----------------------------------------------------------------------
  it("open breaker seeded without TTL continues to reject traffic", async () => {
    const namespace = `test-${randomUUID()}`
    const hashKey = coordinationKey(
      namespace,
      "breaker:breaker",
      "work",
      "shared",
      "breaker",
    )
    cleanupKeys.push(hashKey)

    // Seed OPEN state with no TTL (replicates what PERSIST produces)
    await client.hset(
      hashKey,
      "state",
      "open",
      "generation",
      "3",
      "openedAt",
      String(Date.now() - 5_000),
      "probeCount",
      "0",
      "probeSuccesses",
      "0",
    )

    await new Promise((r) => setTimeout(r, 100))

    const policy = makePolicy(namespace, { openMs: 60_000 })
    const events: OperationEvent[] = []
    await expect(
      makeOp(policy, () => Promise.resolve("ok"), events).execute(undefined),
    ).rejects.toBeInstanceOf(CircuitOpenError)
    expect(events.filter((e) => e.type === "breaker.rejected")).toHaveLength(1)
  })

  // -----------------------------------------------------------------------
  // 11. TTL-safety: closed breaker state hash expires for cleanup, but never
  //     before the observation window it governs.
  // -----------------------------------------------------------------------
  it("closed breaker state hash outlives the observation window", async () => {
    const namespace = track(`test-${randomUUID()}`)
    const hashKey = coordinationKey(
      namespace,
      "breaker:breaker",
      "work",
      "shared",
      "breaker",
    )

    const openMs = 200
    const policy = makePolicy(namespace, {
      openMs,
      halfOpenSuccesses: 1,
      halfOpenProbes: 1,
    })
    await drive(
      makeOp(policy, () => Promise.reject(new Error("boom"))),
      5,
    )
    await new Promise((r) => setTimeout(r, 250))
    await makeOp(policy, () => Promise.resolve("ok")).execute(undefined)

    // A closed hash expires, but not before the members it governs: losing the
    // generation while its observations survived would let them be counted
    // again.  The default windowTtlMs is max(openMs x 3, 60_000).
    const ttl = await client.pttl(hashKey)
    expect(ttl).toBeGreaterThan(openMs * 2)
    expect(ttl).toBeLessThanOrEqual(60_000)
  })

  // -----------------------------------------------------------------------
  // 12. Generation counter must not reset to 0 while breaker is OPEN
  // -----------------------------------------------------------------------
  it("generation counter stays above 0 while breaker is open", async () => {
    const namespace = track(`test-${randomUUID()}`)
    const hashKey = coordinationKey(
      namespace,
      "breaker:breaker",
      "work",
      "shared",
      "breaker",
    )

    const policy = makePolicy(namespace)
    await drive(
      makeOp(policy, () => Promise.reject(new Error("boom"))),
      5,
    )

    const fields = await client.hmget(hashKey, "state", "generation")
    expect(fields[0]).toBe("open")
    expect(Number(fields[1])).toBeGreaterThan(0)

    // A new attempt must be rejected, not fall through as gen-0 CLOSED
    const events: OperationEvent[] = []
    await expect(
      makeOp(policy, () => Promise.resolve("ok"), events).execute(undefined),
    ).rejects.toBeInstanceOf(CircuitOpenError)
    expect(events.filter((e) => e.type === "breaker.rejected")).toHaveLength(1)
  })

  it("never opens on a success-only trace at the smallest resolvable threshold", async () => {
    // 0.0005 resolves to a numerator of 1; the old encoding resolved 0.0004 to
    // a numerator of 0 and opened the breaker on a success-only window.
    const namespace = track(`test-${randomUUID()}`)
    const policy = makePolicy(namespace, {
      minimumThroughput: 20,
      failureThreshold: 0.0005,
      windowSize: 100,
    })
    const events: OperationEvent[] = []

    await drive(
      makeOp(policy, () => Promise.resolve("ok"), events),
      25,
    )

    const fields = await client.hmget(
      coordinationKey(
        namespace,
        "breaker:breaker",
        "work",
        "shared",
        "breaker",
      ),
      "state",
    )
    expect(fields[0]).toBe("closed")
    expect(
      events.filter((e) => e.type === "breaker.state-changed"),
    ).toHaveLength(0)
  })

  it("releases the probe slot when the classifier ignores the probe result", async () => {
    const namespace = track(`test-${randomUUID()}`)
    const hashKey = coordinationKey(
      namespace,
      "breaker:breaker",
      "work",
      "shared",
      "breaker",
    )
    const probesKey = coordinationKey(
      namespace,
      "breaker:breaker",
      "work",
      "shared",
      "probes",
    )

    // Seed a live HALF_OPEN recovery window: the OPEN window has long elapsed, and
    // the window itself still has time left, so the ignored result is the only
    // thing under test.
    await client.hset(
      hashKey,
      "state",
      "half-open",
      "generation",
      "1",
      "openedAt",
      String(Date.now() - 60_000),
      "probeCount",
      "0",
      "probeSuccesses",
      "0",
      "windowProbes",
      "0",
      "windowOpenedAt",
      String(Date.now()),
    )

    // The probe is admitted, runs, and its result is ignored.
    const ignoring = makePolicy(namespace, {
      minimumThroughput: 5,
      openMs: 30_000,
      classify: () => "ignored" as const,
    })
    const events: OperationEvent[] = []
    let ran = 0
    const work = () => {
      ran++
      return Promise.resolve("ok")
    }

    await drive(makeOp(ignoring, work, events), 1)

    expect(ran).toBe(1) // the probe ran
    expect(await client.hget(hashKey, "state")).toBe("half-open")
    expect(Number(await client.hget(hashKey, "probeCount"))).toBe(0)
    expect(Number(await client.hget(hashKey, "probeSuccesses"))).toBe(0)
    expect(await client.zcard(probesKey)).toBe(0)
    // The in-flight slot is released, but the window's probe budget is not: an
    // inconclusive result must not be recycled into another probe.
    expect(Number(await client.hget(hashKey, "windowProbes"))).toBe(1)
    expect(events.filter((e) => e.type === "breaker.observation")).toHaveLength(
      0,
    )

    // The next probe is admitted because the window still has budget - the default
    // halfOpenProbes is 2 - rather than because the slot came back.
    ran = 0
    await makeOp(ignoring, work, events).execute(undefined)
    expect(ran).toBe(1)
    expect(Number(await client.hget(hashKey, "windowProbes"))).toBe(2)
  })

  it("admits at most halfOpenProbes probes per recovery window", async () => {
    // Desired behaviour. `halfOpenProbes` currently bounds probes *in flight*:
    // every settle removes its token, so a probe that produces no result frees its
    // slot instantly - without re-opening the breaker and without starting a
    // cooldown. An "ignored" result is exactly that (the classifier ignores
    // admission refusals, cancellations and superseded attempts), so the recovery
    // window admits one probe per settlement rather than three.
    //
    // That is invisible while the breaker is OPEN, because the cooldown refuses
    // every admission. It is a storm while it is HALF_OPEN - which is where the
    // delegated topology sits during a failure, because a replica holding a cached
    // HALF_OPEN verdict forwards every admission to the authority. The fleet turns
    // every request into a probe (study 23: 512 probes against a configured 3, and
    // no CLOSED transition), while the strict path stays OPEN and never spends the
    // budget: 3 probes, then closed.
    //
    // The recovery window, not the in-flight count, is the budget: three probes are
    // admitted while the window lasts, and an inconclusive result does not return a
    // slot to it.
    const namespace = `test-${randomUUID()}`
    track(namespace)
    const coordinator = makeCoordinator(namespace)
    const identity = { name: "breaker", operation: "work", scope: "shared" }
    const probeLeaseTtlMs = 60_000
    const halfOpenProbes = 3
    const openMs = 5_000

    // Open it: one failure against a threshold of one.
    const opened = await coordinator.observe(identity, {
      generation: 0,
      outcome: "failure",
      uuid: randomUUID(),
      windowTtlMs: 60_000,
      minimumThroughput: 1,
      failureThresholdNumerator: 500,
      windowSize: 20,
      openMs: 60_000,
      closedCleanupMs: 60_000,
    })
    expect(opened).toMatchObject({ type: "opened" })

    // Phase A - OPEN and cooling. The same loop admits nothing, which is what keeps
    // the strict path safe: it stays OPEN for the whole failure.
    let coolingReason = ""
    for (let index = 0; index < 20; index += 1) {
      const refused = await coordinator.admitProbe(identity, {
        probeToken: randomUUID(),
        openMs: 60_000,
        halfOpenProbes,
        probeLeaseTtlMs,
      })
      if (refused.type !== "rejected") break
      coolingReason = refused.reason
    }
    expect(coolingReason).toBe("open")

    // Phase B - the cooldown has elapsed, so this attempt enters the recovery
    // window. Every later attempt is fed an inconclusive result: the probe runs,
    // reports nothing usable, and settles as "ignored" while the breaker stays
    // HALF_OPEN.
    const first = await coordinator.admitProbe(identity, {
      probeToken: randomUUID(),
      openMs: 0,
      halfOpenProbes,
      probeLeaseTtlMs,
    })
    expect(first).toMatchObject({ type: "admitted" })

    let admissions = 0
    for (let index = 0; index < 200; index += 1) {
      const token = randomUUID()
      const admitted = await coordinator.admitProbe(identity, {
        probeToken: token,
        openMs,
        halfOpenProbes,
        probeLeaseTtlMs,
      })
      if (admitted.type !== "admitted") break
      admissions += 1
      const settled = await coordinator.settleProbe(identity, {
        probeToken: token,
        outcome: "ignored",
        generation: admitted.generation,
        halfOpenSuccesses: 1,
        openMs,
        closedCleanupMs: 60_000,
        windowTtlMs: 60_000,
      })
      if (settled.type !== "settled") break
    }

    // Three probes per recovery window - not one per settlement.
    expect(admissions).toBeLessThanOrEqual(halfOpenProbes)
  })

  it("refills the probe budget on the cooldown, not on an outcome", async () => {
    // Liveness for the window budget. A window whose probes all report nothing
    // spends its budget without reaching a verdict, so it must not stay HALF_OPEN
    // forever: after its own cooldown it re-opens, and the next window starts with
    // full capacity.
    const namespace = `test-${randomUUID()}`
    track(namespace)
    const coordinator = makeCoordinator(namespace)
    const identity = { name: "breaker", operation: "work", scope: "shared" }
    const hashKey = coordinationKey(
      namespace,
      "breaker:breaker",
      "work",
      "shared",
      "breaker",
    )
    const openMs = 300
    const halfOpenProbes = 1
    const probeLeaseTtlMs = 300

    const opened = await coordinator.observe(identity, {
      generation: 0,
      outcome: "failure",
      uuid: randomUUID(),
      windowTtlMs: 60_000,
      minimumThroughput: 1,
      failureThresholdNumerator: 500,
      windowSize: 20,
      openMs: 60_000,
      closedCleanupMs: 60_000,
    })
    expect(opened).toMatchObject({ type: "opened" })

    // Enter the window and spend its single probe on an inconclusive result.
    const token = randomUUID()
    const admitted = await coordinator.admitProbe(identity, {
      probeToken: token,
      openMs: 0,
      halfOpenProbes,
      probeLeaseTtlMs,
    })
    expect(admitted).toMatchObject({ type: "admitted" })
    await coordinator.settleProbe(identity, {
      probeToken: token,
      outcome: "ignored",
      generation: admitted.type === "admitted" ? admitted.generation : 0,
      halfOpenSuccesses: 1,
      openMs,
      closedCleanupMs: 60_000,
      windowTtlMs: 60_000,
    })
    expect(await client.hget(hashKey, "state")).toBe("half-open")

    // Budget spent, and it does not come back because the result was inconclusive:
    // the next admission inside the cooldown is refused.
    const spent = await coordinator.admitProbe(identity, {
      probeToken: randomUUID(),
      openMs,
      halfOpenProbes,
      probeLeaseTtlMs,
    })
    expect(spent).toMatchObject({ type: "rejected", reason: "probe-limit" })

    await new Promise((resolve) => setTimeout(resolve, openMs + 100))

    // The cooldown has passed: the window refills on time, so recovery is not
    // stuck behind an inconclusive result.
    const refilled = await coordinator.admitProbe(identity, {
      probeToken: randomUUID(),
      openMs,
      halfOpenProbes,
      probeLeaseTtlMs,
    })
    expect(refilled).toMatchObject({ type: "admitted" })
    expect(Number(await client.hget(hashKey, "windowProbes"))).toBe(1)
    expect(await client.hget(hashKey, "state")).toBe("half-open")
  })
})
