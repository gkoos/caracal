import { randomUUID } from "node:crypto"
import { afterAll, beforeAll, describe, expect, it } from "vitest"
import {
  bulkhead,
  BulkheadRejectedError,
  circuitBreaker,
  operation,
  rateLimit,
  RateLimitExceededError,
  retry,
  timeout,
} from "../../../src/index.js"
import { redisBulkheadAllocationIssuer } from "../../../src/coordination/redis/bulkhead.js"
import { redisRateLimitGrantIssuer } from "../../../src/coordination/redis/rate-limit.js"
import { createServalBreaker } from "../../../src/serval/breaker.js"
import { createServalBulkheadCoordinator } from "../../../src/serval/bulkhead.js"
import { createServalRateLimitCoordinator } from "../../../src/serval/rate-limit.js"
import {
  createCoordinationClient,
  redisCircuitBreakerCoordinator,
} from "../../../src/redis.js"
import { redisProxy } from "../../support/redis-proxy.js"

/**
 * All three Serval policies composed in one operation, against a real Redis.
 *
 * The point of composing them is the absence of surprises: no cross-policy
 * interference, no stuck recovery, no unbounded growth and no unreported degradation.
 * Each case here exercises a composition requirement from `docs/serval/authority.md` contract 6
 * with the real coordinators, so a policy that quietly depends on another's state, or
 * that keeps accounting after shutdown, fails rather than passing plausibly.
 *
 *   npm run test:integration:redis
 */

const url = process.env.CARACAL_REDIS_URL
const IDENTITY = { name: "composed", operation: "charge", scope: "t1" }

/** Waits until a coordinator call succeeds again, e.g. after a client reconnect. */
async function untilReady(
  check: () => Promise<unknown>,
  timeoutMs = 10_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs
  for (;;) {
    try {
      await check()
      return
    } catch (error) {
      if (Date.now() > deadline) throw error
      await new Promise((resolve) => setTimeout(resolve, 50))
    }
  }
}

describe.skipIf(!url)("serval composition", () => {
  const client = url ? createCoordinationClient(url) : undefined

  beforeAll(async () => {
    await client?.connect()
  })

  afterAll(() => {
    client?.disconnect()
  })

  /**
   * One replica's three delegated policies over one identity. Each policy keeps its
   * own authority, as the deployment chooses; nothing is shared between them except
   * the Redis server.
   */
  function replica(options: {
    readonly namespace: string
    readonly replicaId?: string
    readonly grantSize?: number
    readonly slots?: number
    readonly freshnessMs?: number
    readonly staleClosed?: "refresh" | "fail-open" | "fail-closed"
    /** A different client, e.g. one behind a fault-injecting proxy. */
    readonly client?: ReturnType<typeof createCoordinationClient>
  }) {
    const replicaId = options.replicaId ?? "replica-1"
    const connection = options.client ?? (client as never)
    const breaker = createServalBreaker({
      namespace: options.namespace,
      authority: redisCircuitBreakerCoordinator(connection, {
        namespace: options.namespace,
      }),
      replicaId,
      freshnessMs: options.freshnessMs ?? 30_000,
      staleClosed: options.staleClosed,
      reconciliationIntervalMs: 60_000,
    })
    const rate = createServalRateLimitCoordinator({
      authority: redisRateLimitGrantIssuer(connection, {
        namespace: options.namespace,
      }),
      grantSize: options.grantSize ?? 4,
      validForMs: 5_000,
    })
    const capacity = createServalBulkheadCoordinator({
      authority: redisBulkheadAllocationIssuer(connection, {
        namespace: options.namespace,
      }),
      replicaId,
      validityMs: 30_000,
      graceMs: 0,
      initialSlots: options.slots ?? 2,
      maximumSlots: options.slots ?? 2,
      redistributionIntervalMs: 0,
    })
    return { breaker, rate, capacity }
  }

  /** The composed pipeline: breaker, rate limiter and bulkhead over one attempt. */
  function composedOperation(options: {
    readonly namespace: string
    readonly replica: ReturnType<typeof replica>
    readonly execute: () => Promise<string>
    readonly abort?: "supported" | "unsupported"
    readonly limit?: number
    readonly rate?: number
    readonly burst?: number
    readonly policies?: readonly unknown[]
    /** The adapter's own outcome classifier, when a case needs one. */
    readonly classify?: (outcome: {
      readonly status: string
    }) => "success" | "failure" | "retryable" | "ignored"
  }) {
    const { breaker, rate, capacity } = options.replica
    return operation({
      name: IDENTITY.operation,
      adapter: {
        capabilities: () =>
          ({ abort: options.abort ?? "supported", replay: "safe" }) as const,
        execute: options.execute,
        ...(options.classify === undefined
          ? {}
          : { classify: options.classify as never }),
      },
      policies: [
        circuitBreaker.distributed({
          name: IDENTITY.name,
          coordinator: breaker.coordinator,
          scope: () => IDENTITY.scope,
          minimumThroughput: 2,
          failureThreshold: 0.5,
          openMs: 500,
        }),
        rateLimit.distributed({
          name: IDENTITY.name,
          rate: options.rate ?? 200,
          burst: options.burst ?? 4,
          coordinator: rate,
          scope: () => IDENTITY.scope,
        }),
        bulkhead.distributed({
          name: IDENTITY.name,
          limit: options.limit ?? 2,
          coordinator: capacity,
          scope: () => IDENTITY.scope,
        }),
        ...(options.policies ?? []),
      ] as never,
    })
  }

  it("runs a three-policy pipeline end to end", async () => {
    const namespace = `serval-composed-${randomUUID()}`
    const replicaSet = replica({ namespace })
    replicaSet.breaker.session.registerScope({
      policyKind: "circuit-breaker",
      policyName: IDENTITY.name,
      operation: IDENTITY.operation,
      scope: IDENTITY.scope,
    })
    await replicaSet.breaker.session.start()
    await replicaSet.breaker.session.whenIdle()

    const op = composedOperation({
      namespace,
      replica: replicaSet,
      // Headroom on the burst, deliberately. The issuer's contract is that "a grant
      // may be smaller, or refused": four calls against a burst of exactly four sit
      // on the tolerance boundary, so the first grant can legitimately come back as
      // three and a second issuance is then needed - which can be refused. That is
      // correct behaviour, and the assertions below are about the composition rather
      // than about that boundary. CI caught it; it did not reproduce on a faster
      // local machine in twelve runs.
      burst: 8,
      execute: async () => "ok",
    })

    for (let index = 0; index < 4; index += 1) {
      await expect(op.execute(undefined)).resolves.toBe("ok")
    }

    // Every policy took its own decision, and each stayed inside its own budget.
    expect(replicaSet.rate.metrics()).toMatchObject({
      admissions: 4,
      refused: 0,
    })
    expect(replicaSet.capacity.metrics()).toMatchObject({
      admissions: 4,
      activeHolders: 0,
    })
    expect(replicaSet.breaker.coordinator.cached(IDENTITY)).toMatchObject({
      state: "closed",
    })

    await Promise.all([
      replicaSet.breaker.session.close(),
      replicaSet.capacity.close(),
    ])
  })

  it("keeps a local guardrail binding without interfering with the delegated layer", async () => {
    const namespace = `serval-composed-${randomUUID()}`
    const replicaSet = replica({ namespace, slots: 4 })
    await replicaSet.breaker.session.start()
    await replicaSet.breaker.session.whenIdle()

    let release: (() => void) | undefined
    const held = new Promise<void>((resolve) => {
      release = resolve
    })
    let entered: (() => void) | undefined
    const reachedAdapter = new Promise<void>((resolve) => {
      entered = resolve
    })
    // A local bulkhead of one is the tighter bound; the delegated allocation is four.
    const op = composedOperation({
      namespace,
      replica: replicaSet,
      execute: async () => {
        // Reaching the adapter means every layer's permit is held. Waiting for that is what
        // makes the second execution a deterministic refusal instead of a race: it has to
        // pass the delegated layers before it can meet the local guardrail, so starting it
        // earlier can let it take the only local permit and deadlock the case.
        entered?.()
        await held
        return "ok"
      },
      policies: [
        bulkhead.local({ name: "local-cap", limit: 1 }),
        rateLimit.local({ name: "local-rate", rate: 1_000, burst: 2 }),
      ],
    })

    const first = op.execute(undefined)
    await reachedAdapter
    const second = op.execute(undefined)

    // The local guardrail refuses what the delegated layer would have admitted.
    const rejected = await second.catch((error) => error)
    expect(rejected).toBeInstanceOf(BulkheadRejectedError)
    expect((rejected as BulkheadRejectedError).coordination).toBe("local")

    release?.()
    await expect(first).resolves.toBe("ok")
    await replicaSet.breaker.session.whenIdle()
    await replicaSet.capacity.whenIdle()

    // The refusal is not interference: the delegated layer kept its own accounting and
    // released every permit it granted, including the one the local guardrail refused.
    expect(replicaSet.capacity.metrics()).toMatchObject({
      admissions: 2,
      activeHolders: 0,
    })
    expect(replicaSet.rate.metrics().admissions).toBe(2)

    await Promise.all([
      replicaSet.breaker.session.close(),
      replicaSet.capacity.close(),
    ])
  })

  it("admits each retry attempt separately and releases between them", async () => {
    const namespace = `serval-composed-${randomUUID()}`
    const replicaSet = replica({ namespace, slots: 2 })
    await replicaSet.breaker.session.start()
    await replicaSet.breaker.session.whenIdle()

    let attempts = 0
    const op = composedOperation({
      namespace,
      replica: replicaSet,
      execute: async () => {
        attempts += 1
        if (attempts < 3) throw new Error("downstream unavailable")
        return "ok"
      },
      classify: (outcome) =>
        outcome.status === "failure" ? "retryable" : "success",
      policies: [
        retry({ maxAttempts: 3, delay: () => 0 }),
        // A higher floor keeps two failures of three attempts from opening the
        // breaker, so the case observes admission rather than protection.
        circuitBreaker.distributed({
          name: "retry-observer",
          coordinator: replicaSet.breaker.coordinator,
          scope: () => IDENTITY.scope,
          minimumThroughput: 8,
          failureThreshold: 0.5,
          openMs: 500,
        }),
      ],
    })

    await expect(op.execute(undefined)).resolves.toBe("ok")
    await replicaSet.capacity.whenIdle()

    // Each attempt paid its own admission and released its own permit.
    expect(attempts).toBe(3)
    expect(replicaSet.rate.metrics().admissions).toBe(3)
    expect(replicaSet.capacity.metrics()).toMatchObject({
      admissions: 3,
      activeHolders: 0,
    })

    await Promise.all([
      replicaSet.breaker.session.close(),
      replicaSet.capacity.close(),
    ])
  })

  it("does not turn an admission refusal into breaker evidence", async () => {
    const namespace = `serval-composed-${randomUUID()}`
    const replicaSet = replica({ namespace, slots: 2 })
    await replicaSet.breaker.session.start()
    await replicaSet.breaker.session.whenIdle()

    const op = composedOperation({
      namespace,
      replica: replicaSet,
      execute: async () => "ok",
      // One permit at a time: the second call is refused before it reaches the adapter.
      rate: 1,
      burst: 1,
    })

    await expect(op.execute(undefined)).resolves.toBe("ok")
    const rejected = await op.execute(undefined).catch((error) => error)
    expect(rejected).toBeInstanceOf(RateLimitExceededError)

    await replicaSet.breaker.session.refresh("periodic")
    await replicaSet.breaker.session.whenIdle()

    // A refusal is not evidence about the dependency, so the breaker stays closed and
    // the adapter was never asked twice.
    expect(replicaSet.breaker.coordinator.cached(IDENTITY)).toMatchObject({
      state: "closed",
      generation: 0,
    })
    expect(replicaSet.rate.metrics().admissions).toBe(1)

    await Promise.all([
      replicaSet.breaker.session.close(),
      replicaSet.capacity.close(),
    ])
  })

  it("releases the permit of a cancelled attempt even when the adapter ignores it", async () => {
    const namespace = `serval-composed-${randomUUID()}`
    const replicaSet = replica({ namespace, slots: 2 })
    await replicaSet.breaker.session.start()
    await replicaSet.breaker.session.whenIdle()

    let finished = 0
    const op = composedOperation({
      namespace,
      replica: replicaSet,
      // The adapter ignores cancellation: the attempt is abandoned by the policy, but
      // the work it started keeps running.
      abort: "unsupported",
      execute: async () => {
        await new Promise((resolve) => setTimeout(resolve, 150))
        finished += 1
        return "late"
      },
      policies: [timeout({ ms: 20 })],
    })

    await expect(op.execute(undefined)).rejects.toThrow()
    await replicaSet.capacity.whenIdle()

    // The downstream work the attempt started keeps running, so its permit is held
    // until that work settles - an abandoned admission is not evidence that the
    // dependency stopped, and the permit is not released early on its behalf.
    expect(replicaSet.rate.metrics().admissions).toBe(1)
    // Wait for the abandoned work to settle instead of assuming a fixed delay is
    // enough on a loaded machine.
    const deadline = Date.now() + 2_000
    while (finished === 0 && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 10))
    }
    expect(finished).toBe(1)
    await replicaSet.capacity.whenIdle()

    // Once it settles, the permit is released: a cancelled attempt leaks nothing.
    expect(replicaSet.capacity.metrics().activeHolders).toBe(0)
    expect(replicaSet.capacity.metrics().returns).toBe(0)

    await Promise.all([
      replicaSet.breaker.session.close(),
      replicaSet.capacity.close(),
    ])
  })

  it("keeps local authority through a Redis outage and converges after recovery", async () => {
    const namespace = `serval-composed-${randomUUID()}`
    const proxy = await redisProxy(new URL(url as string))
    const proxied = createCoordinationClient(proxy.url)
    await proxied.connect()

    try {
      const replicaSet = replica({ namespace, client: proxied, slots: 2 })
      await replicaSet.breaker.session.start()
      await replicaSet.breaker.session.whenIdle()
      const strict = redisCircuitBreakerCoordinator(proxied, { namespace })

      // Warm: one call so every layer holds local authority.
      const op = composedOperation({
        namespace,
        replica: replicaSet,
        execute: async () => "ok",
      })
      await expect(op.execute(undefined)).resolves.toBe("ok")

      proxy.disconnect()

      // The strict coordinator, which has no local authority, fails closed.
      await expect(strict.readState(IDENTITY)).rejects.toThrow()

      // Rate and bulkhead keep admitting from local authority: neither reads Redis on
      // the admission path, and neither invents capacity it does not hold.
      expect(
        (
          await replicaSet.rate.command(IDENTITY, {
            emissionIntervalMs: 5,
            burstDelayMs: 15,
          })
        ).allowed,
      ).toBe(true)
      expect(
        (
          await replicaSet.capacity.command(
            IDENTITY,
            "acquire",
            "outage-token",
            60_000,
            2,
          )
        ).allowed,
      ).toBe(true)

      // Degradation is reported rather than hidden: a return cannot be committed, so
      // its slots stay frozen and the failure is counted.
      await expect(
        replicaSet.capacity.relinquish(IDENTITY, 1),
      ).resolves.toEqual({
        type: "frozen",
        slots: 1,
        reason: "unavailable",
      })
      expect(replicaSet.capacity.metrics().modificationFailures).toBe(1)

      // The cached verdict is unchanged: an outage never fabricates a transition.
      expect(replicaSet.breaker.coordinator.cached(IDENTITY)).toMatchObject({
        state: "closed",
      })

      proxy.restore()
      // The client reconnects on its own schedule; wait for it rather than assuming.
      await untilReady(() => strict.readState(IDENTITY))
      await replicaSet.breaker.session.refresh("periodic")
      await replicaSet.breaker.session.whenIdle()

      // Release the permit the outage case took, so the recovered pipeline has a slot.
      await replicaSet.capacity.command(
        IDENTITY,
        "release",
        "outage-token",
        60_000,
        2,
      )
      await replicaSet.capacity.whenIdle()

      // No stuck state: the pipeline works again, and the strict coordinator too.
      await expect(op.execute(undefined)).resolves.toBe("ok")
      await expect(strict.readState(IDENTITY)).resolves.toMatchObject({
        state: "closed",
      })

      await Promise.all([
        replicaSet.breaker.session.close(),
        replicaSet.capacity.close(),
      ])
    } finally {
      await proxy.close()
      proxied.disconnect()
    }
  })

  it("stays bounded across many operations and shuts down clean", async () => {
    const namespace = `serval-composed-${randomUUID()}`
    const replicaSet = replica({ namespace, slots: 2 })
    replicaSet.breaker.session.registerScope({
      policyKind: "circuit-breaker",
      policyName: IDENTITY.name,
      operation: IDENTITY.operation,
      scope: IDENTITY.scope,
    })
    await replicaSet.breaker.session.start()
    await replicaSet.breaker.session.whenIdle()

    const op = composedOperation({
      namespace,
      replica: replicaSet,
      execute: async () => "ok",
      // Headroom for ten rapid calls, so the case observes composition rather than
      // the rate limiter's own ceiling.
      rate: 1_000,
      burst: 20,
    })

    for (let index = 0; index < 10; index += 1) {
      await expect(op.execute(undefined)).resolves.toBe("ok")
    }

    // One scope per identity, whatever the traffic: nothing grows per request.
    expect(replicaSet.breaker.session.scopeCount).toBe(1)
    expect(replicaSet.rate.metrics().scopes).toBe(1)
    expect(replicaSet.capacity.metrics().scopes).toBe(1)

    await Promise.all([
      replicaSet.breaker.session.close(),
      replicaSet.capacity.close(),
    ])
    await replicaSet.rate.whenIdle()

    // Shutdown releases every registration and leaves nothing pending.
    expect(replicaSet.breaker.session.scopeCount).toBe(0)
    expect(replicaSet.breaker.session.metrics().registrySize).toBe(0)
    expect(replicaSet.capacity.metrics().activeHolders).toBe(0)
    await expect(replicaSet.breaker.session.whenIdle()).resolves.toBeUndefined()
  })
})
