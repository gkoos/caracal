import { randomUUID } from "node:crypto"
import { afterAll, beforeAll, describe, expect, it } from "vitest"
import { operation, rateLimit } from "../../src/index.js"
import { redisRateLimitGrantIssuer } from "../../src/coordination/redis/rate-limit.js"
import { createServalRateLimitCoordinator } from "../../src/coordination/serval/rate-limit.js"
import {
  createCoordinationClient,
  redisRateLimitCoordinator,
} from "../../src/redis.js"

/**
 * The delegated rate limiter against a real Redis.
 *
 * The unit suite proves the local protocol with the reference-model authority; this
 * proves the two claims only a real server can settle: that a grant is taken from
 * the same bucket the strict path uses - so the delegated path is not a second
 * budget - and that the round trips really are removed while quota is in hand.
 *
 *   npm run test:integration:redis
 */

const url = process.env.CARACAL_REDIS_URL

/** The knobs every case here admits with, unless it says otherwise. */
function knobs(emissionIntervalMs: number, burstDelayMs: number) {
  return { emissionIntervalMs, burstDelayMs }
}

describe.skipIf(!url)("serval rate limiter against Redis", () => {
  const client = url ? createCoordinationClient(url) : undefined

  beforeAll(async () => {
    await client?.connect()
  })

  afterAll(() => {
    client?.disconnect()
  })

  /** One replica's delegated quota, with the parameters the case needs. */
  function replica(options: {
    readonly namespace: string
    readonly grantSize: number
    readonly validForMs: number
    readonly maximumOutstandingPermits?: number
  }) {
    return createServalRateLimitCoordinator({
      authority: redisRateLimitGrantIssuer(client as never, {
        namespace: options.namespace,
      }),
      grantSize: options.grantSize,
      validForMs: options.validForMs,
      ...(options.maximumOutstandingPermits === undefined
        ? {}
        : { maximumOutstandingPermits: options.maximumOutstandingPermits }),
    })
  }

  it("takes a grant from the strict bucket and deduplicates a retry", async () => {
    const namespace = `serval-rate-${randomUUID()}`
    const identity = { name: "shared-bucket", operation: "charge", scope: "t1" }
    const strict = redisRateLimitCoordinator(client as never, { namespace })
    const issuer = redisRateLimitGrantIssuer(client as never, { namespace })

    // rate 10/s with a burst of 8: an emission interval of 100ms, so the case runs
    // far inside one interval and the bucket's arithmetic is exact.
    const params = knobs(100, 700)
    const request = {
      identity,
      operationId: randomUUID(),
      ...params,
      permits: 4,
      validForMs: 5_000,
    }

    const granted = await issuer.issue(request)
    expect(granted).toMatchObject({ granted: 4, duplicate: false })

    // A retry of the same operation is served from the ledger, not the bucket.
    const retried = await issuer.issue(request)
    expect(retried).toMatchObject({ granted: 4, duplicate: true })

    // A fresh request gets what the bucket still holds: four of the eight. Had the
    // retry deducted again, this would be zero.
    const afterRetry = await issuer.issue({
      ...request,
      operationId: randomUUID(),
      permits: 8,
    })
    expect(afterRetry.granted).toBe(4)

    // The strict coordinator draws on that same bucket, and it is now empty: the
    // delegated grants spent it, so the strict path refuses with a wait instead of
    // admitting against a second budget.
    const strictDecision = await strict.command(identity, params)
    expect(strictDecision.allowed).toBe(false)
    expect(strictDecision.retryAfterMs).toBeGreaterThan(0)
  })

  it("removes the per-admission round trip while quota is in hand", async () => {
    const namespace = `serval-rate-${randomUUID()}`
    const identity = { name: "payments-rate", operation: "charge", scope: "t1" }
    const coordinator = replica({
      namespace,
      grantSize: 8,
      validForMs: 10_000,
    })

    const policy = rateLimit.distributed({
      name: identity.name,
      rate: 200,
      burst: 8,
      coordinator,
      scope: () => identity.scope,
    })
    const op = operation({
      name: identity.operation,
      adapter: {
        capabilities: () => ({ abort: "supported", replay: "safe" }) as const,
        execute: async () => "ok",
      },
      policies: [policy],
    })

    let admitted = 0
    for (let index = 0; index < 20; index += 1) {
      const ok = await op.execute(undefined).then(
        () => true,
        () => false,
      )
      if (ok) admitted += 1
    }
    await coordinator.whenIdle()

    const metrics = coordinator.metrics()
    // The gate's cost claim, measured: fewer round trips than admissions, no
    // admission the server did not grant, and stranded capacity bounded by the one
    // grant a replica holds.
    expect(admitted).toBeGreaterThan(0)
    expect(admitted).toBe(metrics.admissions)
    expect(metrics.issuances).toBeLessThan(admitted)
    expect(metrics.admissions).toBeLessThanOrEqual(metrics.permitsGranted)
    expect(metrics.permitsGranted - metrics.permitsSpent).toBeLessThanOrEqual(8)
  })

  it("bounds stranded quota when a replica hoards", async () => {
    const namespace = `serval-rate-${randomUUID()}`
    const identity = { name: "hoarding", operation: "charge", scope: "t1" }
    const options = {
      namespace,
      grantSize: 4,
      validForMs: 200,
      // One live grant of four permits, and the cap counts permits: the hoarder's grant
      // is exactly the outstanding stock the cap allows.
      maximumOutstandingPermits: 4,
    }
    const hoarder = replica(options)
    const hot = replica(options)

    // The hoarder takes the only grant the cap allows and does not spend it.
    expect((await hoarder.command(identity, knobs(50, 350))).allowed).toBe(true)
    await hoarder.whenIdle()

    const refused = await hot.command(identity, knobs(50, 350))
    expect(refused.allowed).toBe(false)
    expect(refused.retryAfterMs).toBeGreaterThan(0)
    expect(hot.metrics().refused).toBe(1)

    // Once the hoarded grant expires, capacity is free again - the deviation the
    // cap bounds is temporary, not permanent.
    await new Promise((resolve) => setTimeout(resolve, 250))
    expect((await hot.command(identity, knobs(50, 350))).allowed).toBe(true)

    await Promise.all([hoarder.whenIdle(), hot.whenIdle()])
  })

  it("bounds live permits when replicas request different grant sizes", async () => {
    const namespace = `serval-rate-${randomUUID()}`
    const identity = { name: "mixed-sizes", operation: "charge", scope: "t1" }
    const bulk = replica({
      namespace,
      grantSize: 8,
      validForMs: 5_000,
      maximumOutstandingPermits: 8,
    })
    const single = replica({
      namespace,
      grantSize: 1,
      validForMs: 5_000,
      maximumOutstandingPermits: 8,
    })

    // One replica takes the whole cap in a single grant of eight permits.
    expect((await bulk.command(identity, knobs(50, 500))).allowed).toBe(true)
    await bulk.whenIdle()

    // The cap counts permits, not grants. A caller asking for single permits cannot add
    // more on top of the eight already outstanding, which a grant count derived from its
    // own request size would have allowed.
    const refused = await single.command(identity, knobs(50, 500))
    expect(refused.allowed).toBe(false)
    expect(refused.retryAfterMs).toBeGreaterThan(0)

    await Promise.all([bulk.whenIdle(), single.whenIdle()])
  })

  it("admits no more than the fleet was granted under contention", async () => {
    const namespace = `serval-rate-${randomUUID()}`
    const identity = { name: "contended", operation: "charge", scope: "t1" }
    const replicas = Array.from({ length: 5 }, () =>
      replica({ namespace, grantSize: 2, validForMs: 5_000 }),
    )

    const decisions = await Promise.all(
      replicas.map((coordinator) =>
        coordinator.command(identity, knobs(50, 150)),
      ),
    )
    await Promise.all(replicas.map((coordinator) => coordinator.whenIdle()))

    const admitted = decisions.filter((decision) => decision.allowed).length
    const metrics = replicas.map((coordinator) => coordinator.metrics())
    const spent = metrics.reduce(
      (total, entry) => total + entry.permitsSpent,
      0,
    )
    const granted = metrics.reduce(
      (total, entry) => total + entry.permitsGranted,
      0,
    )

    // Five replicas asked at once; the bucket's burst is the only capacity there is,
    // and every admission was covered by a permit the server actually granted.
    expect(spent).toBe(admitted)
    expect(spent).toBeLessThanOrEqual(granted)
    expect(granted).toBeLessThanOrEqual(4)
    for (const entry of metrics) {
      expect(entry.admissions).toBeLessThanOrEqual(entry.permitsGranted)
    }
  })
})
