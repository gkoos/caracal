import { randomUUID } from "node:crypto"
import { afterAll, beforeAll, describe, expect, it } from "vitest"
import type {
  BreakerCoordinator,
  BreakerIdentity,
} from "../../src/core/circuit-breaker.js"
import { circuitBreaker, operation } from "../../src/index.js"
import {
  breakerScopeIdentity,
  createServalBreaker,
} from "../../src/coordination/serval/breaker.js"
import {
  createCoordinationClient,
  redisCircuitBreakerCoordinator,
} from "../../src/redis.js"

/**
 * The cached-verdict breaker against a real Redis.
 *
 * The unit suite proves the paths with a scriptable authority; this proves the two
 * claims only a real server can settle: that the admission read really is removed
 * while a cached verdict is valid, and that a cached OPEN still recovers through
 * the strict probe path after its cooldown.
 *
 *   npm run test:integration:redis
 */

const url = process.env.CARACAL_REDIS_URL
const IDENTITY: BreakerIdentity = {
  name: "serval-breaker",
  operation: "charge",
  scope: "tenant-a",
}
const OPEN_MS = 150

/** Counts the round trips the policy would otherwise have to pay for. */
function counting(coordinator: BreakerCoordinator) {
  const calls = { readState: 0, observe: 0, admitProbe: 0, settleProbe: 0 }
  const wrapped: BreakerCoordinator = {
    async readState(identity) {
      calls.readState += 1
      return coordinator.readState(identity)
    },
    async observe(identity, params) {
      calls.observe += 1
      return coordinator.observe(identity, params)
    },
    async admitProbe(identity, params) {
      calls.admitProbe += 1
      return coordinator.admitProbe(identity, params)
    },
    async settleProbe(identity, params) {
      calls.settleProbe += 1
      return coordinator.settleProbe(identity, params)
    },
  }
  return { calls, coordinator: wrapped }
}

describe.skipIf(!url)("serval breaker against Redis", () => {
  const client = url ? createCoordinationClient(url) : undefined

  beforeAll(async () => {
    await client?.connect()
  })

  afterAll(() => {
    client?.disconnect()
  })

  it("removes the admission read while the cached verdict is valid", async () => {
    const namespace = `serval-breaker-${randomUUID()}`
    const strict = redisCircuitBreakerCoordinator(client as never, {
      namespace,
    })
    const counted = counting(strict)
    const breaker = createServalBreaker({
      namespace,
      authority: counted.coordinator,
      replicaId: "replica-1",
      freshnessMs: 30_000,
      reconciliationIntervalMs: 60_000,
    })
    breaker.session.registerScope(breakerScopeIdentity(IDENTITY))
    await breaker.session.start()
    await breaker.session.whenIdle()

    const policy = circuitBreaker.distributed({
      name: IDENTITY.name,
      coordinator: breaker.coordinator,
      scope: () => IDENTITY.scope,
      minimumThroughput: 2,
      failureThreshold: 0.5,
      openMs: OPEN_MS,
      halfOpenProbes: 1,
      halfOpenSuccesses: 1,
    })
    const op = operation({
      name: IDENTITY.operation,
      adapter: {
        capabilities: () => ({ abort: "supported", replay: "safe" }) as const,
        execute: async () => "ok",
      },
      policies: [policy],
    })

    // The cold start has already read the verdict once, so the cache is warm.
    counted.calls.readState = 0
    for (let index = 0; index < 25; index += 1) {
      await expect(op.execute(undefined)).resolves.toBe("ok")
    }

    // The gate's cost claim, measured against a real server: no admission reads,
    // and every settlement still went.
    expect(counted.calls.readState).toBe(0)
    expect(counted.calls.observe).toBe(25)

    await breaker.session.close()
  })

  it("protects locally while OPEN and recovers through the strict probe path", async () => {
    const namespace = `serval-breaker-${randomUUID()}`
    const strict = redisCircuitBreakerCoordinator(client as never, {
      namespace,
    })
    const counted = counting(strict)
    const breaker = createServalBreaker({
      namespace,
      authority: counted.coordinator,
      replicaId: "replica-1",
      freshnessMs: 30_000,
      reconciliationIntervalMs: 60_000,
    })
    breaker.session.registerScope(breakerScopeIdentity(IDENTITY))
    await breaker.session.start()
    await breaker.session.whenIdle()

    // Open the breaker authoritatively, through the strict path.
    for (let attempt = 0; attempt < 2; attempt += 1) {
      await strict.observe(IDENTITY, {
        generation: 0,
        outcome: "failure",
        uuid: randomUUID(),
        windowTtlMs: 60_000,
        minimumThroughput: 2,
        failureThresholdNumerator: 500,
        windowSize: 20,
        openMs: OPEN_MS,
      })
    }
    await breaker.session.refresh("periodic")
    await breaker.session.whenIdle()
    expect(breaker.coordinator.cached(IDENTITY)).toMatchObject({
      state: "open",
    })

    const probeParams = {
      probeToken: randomUUID(),
      openMs: OPEN_MS,
      halfOpenProbes: 1,
      probeLeaseTtlMs: 1_000,
    }

    // Before the cooldown: refused locally, with no Redis call at all.
    counted.calls.readState = 0
    counted.calls.admitProbe = 0
    expect(await breaker.coordinator.readState(IDENTITY)).toMatchObject({
      state: "open",
    })
    expect(
      await breaker.coordinator.admitProbe(IDENTITY, probeParams),
    ).toMatchObject({ type: "rejected", reason: "open" })
    expect(counted.calls.readState).toBe(0)
    expect(counted.calls.admitProbe).toBe(0)

    // After the cooldown: one authoritative probe is admitted, and a successful
    // settlement closes the breaker.
    await new Promise((resolve) => setTimeout(resolve, OPEN_MS + 50))
    const admitted = await breaker.coordinator.admitProbe(IDENTITY, probeParams)
    expect(admitted).toMatchObject({ type: "admitted" })
    expect(counted.calls.admitProbe).toBe(1)

    const settled = await breaker.coordinator.settleProbe(IDENTITY, {
      probeToken: probeParams.probeToken,
      outcome: "success",
      generation: (admitted as { generation: number }).generation,
      halfOpenSuccesses: 1,
      openMs: OPEN_MS,
      closedCleanupMs: 60_000,
      windowTtlMs: 60_000,
    })
    expect(settled).toMatchObject({ type: "transitioned", newState: "closed" })
    expect(breaker.coordinator.cached(IDENTITY)).toMatchObject({
      state: "closed",
    })

    await breaker.session.close()
  })
})
