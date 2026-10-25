import { randomUUID } from "node:crypto"
import { afterAll, beforeAll, describe, expect, it } from "vitest"
import { circuitBreaker, operation, rateLimit } from "../../src/index.js"
import {
  createCoordinationClient,
  redisCircuitBreakerCoordinator,
} from "../../src/redis.js"
import {
  createServalBreaker,
  createServalRateLimitCoordinator,
  redisRateLimitGrantIssuer,
} from "../../src/serval.js"

/**
 * The public entry point, against a real Redis.
 *
 * The unit surface test pins the shape; this proves the shape works: a session, a
 * cached-verdict breaker and a delegated rate limiter, built only from
 * `caracal/serval`, admit a real operation and converge. If an export were wired to
 * the wrong module, this is where it would show.
 *
 *   npm run test:integration:redis
 */

const url = process.env.CARACAL_REDIS_URL
const IDENTITY = { name: "public-api", operation: "charge", scope: "tenant-a" }

describe.skipIf(!url)("serval public entry point", () => {
  const client = url ? createCoordinationClient(url) : undefined

  beforeAll(async () => {
    await client?.connect()
  })

  afterAll(() => {
    client?.disconnect()
  })

  it("runs a pipeline built only from the published exports", async () => {
    const namespace = `serval-public-${randomUUID()}`
    const breaker = createServalBreaker({
      namespace,
      authority: redisCircuitBreakerCoordinator(client as never, { namespace }),
      replicaId: "public-api-replica",
      freshnessMs: 30_000,
      reconciliationIntervalMs: 60_000,
    })
    breaker.session.registerScope({
      policyKind: "circuit-breaker",
      policyName: IDENTITY.name,
      operation: IDENTITY.operation,
      scope: IDENTITY.scope,
    })
    await breaker.session.start()
    await breaker.session.whenIdle()

    const rate = createServalRateLimitCoordinator({
      authority: redisRateLimitGrantIssuer(client as never, { namespace }),
      grantSize: 4,
      validForMs: 5_000,
    })

    const op = operation({
      name: IDENTITY.operation,
      adapter: {
        capabilities: () => ({ abort: "supported", replay: "safe" }) as const,
        execute: async () => "ok",
      },
      policies: [
        circuitBreaker.distributed({
          name: IDENTITY.name,
          coordinator: breaker.coordinator,
          scope: () => IDENTITY.scope,
        }),
        rateLimit.distributed({
          name: IDENTITY.name,
          rate: 200,
          burst: 4,
          coordinator: rate,
          scope: () => IDENTITY.scope,
        }),
      ],
    })

    for (let index = 0; index < 4; index += 1) {
      await expect(op.execute(undefined)).resolves.toBe("ok")
    }
    await rate.whenIdle()

    // One grant served four admissions, and the session reports a bounded registry.
    expect(rate.metrics().admissions).toBe(4)
    expect(rate.metrics().permitsGranted).toBeGreaterThanOrEqual(4)
    expect(breaker.session.scopeCount).toBe(1)
    expect(breaker.coordinator.cached(IDENTITY)).toMatchObject({
      state: "closed",
    })

    await breaker.session.close()
    expect(breaker.session.scopeCount).toBe(0)
  })
})
