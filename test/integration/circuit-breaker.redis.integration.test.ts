import { randomUUID } from "node:crypto"
import { afterAll, beforeAll, describe, expect, it } from "vitest"
import { coordinationKey } from "../../src/coordination/redis/keys.js"
import type { OperationEvent } from "../../src/index.js"
import { CircuitOpenError, circuitBreaker, operation } from "../../src/index.js"
import {
  createCoordinationClient,
  redisCircuitBreakerCoordinator,
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
  // -----------------------------------------------------------------------
  it("enforces probe limit across worker processes", async () => {
    const namespace = track(`test-${randomUUID()}`)

    const workers = [
      new BreakerWorker(url as string),
      new BreakerWorker(url as string),
      new BreakerWorker(url as string),
    ]
    try {
      await Promise.all(workers.map((w) => w.ready()))

      const cfg = {
        namespace,
        minimumThroughput: 5,
        failureThreshold: 0.5,
        openMs: 200, // hashTtl=400ms; state stays alive while we wait
        halfOpenProbes: 1, // only 1 concurrent probe
        probeLeaseTtlMs: 5_000,
      }

      // Open the breaker
      await Promise.allSettled(
        Array.from({ length: 5 }, () =>
          // biome-ignore lint/style/noNonNullAssertion: workers array is fully populated
          workers[0]!.execute({ outcome: "failure", ...cfg }).catch(() => {}),
        ),
      )

      // Wait for openMs to elapse (hash still alive at 250ms, expires at 400ms)
      await new Promise((r) => setTimeout(r, 250))

      // Two workers compete for the probe slot
      const [r1, r2] = await Promise.all([
        // biome-ignore lint/style/noNonNullAssertion: workers array is fully populated
        workers[1]!
          .execute({ outcome: "success", ...cfg })
          .catch((e: Error) => e.message),
        // biome-ignore lint/style/noNonNullAssertion: workers array is fully populated
        workers[2]!
          .execute({ outcome: "success", ...cfg })
          .catch((e: Error) => e.message),
      ])

      // Exactly one should succeed, one should be rejected with "probe-limit"
      const results = [r1, r2]
      const rejected = results.filter(
        (r) => typeof r === "string" && r.includes("CircuitOpenError"),
      )
      const admitted = results.filter((r) => r === "ok")

      expect(rejected).toHaveLength(1)
      expect(admitted).toHaveLength(1)
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
})
