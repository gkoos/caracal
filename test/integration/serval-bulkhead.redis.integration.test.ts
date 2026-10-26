import { randomUUID } from "node:crypto"
import { afterAll, beforeAll, describe, expect, it } from "vitest"
import { coordinationKey } from "../../src/coordination/redis/keys.js"
import { redisBulkheadAllocationIssuer } from "../../src/coordination/redis/bulkhead.js"
import { createServalBulkheadCoordinator } from "../../src/coordination/serval/bulkhead.js"
import { createCoordinationClient } from "../../src/redis.js"

/**
 * The delegated bulkhead against a real Redis.
 *
 * The unit suite proves the local accounting with the server model; this proves what
 * only a real server can settle: that the fleet limit is a sum of allocations, that a
 * renewal is not capacity, that a returned slot is reallocatable only after the commit
 * that followed the local freeze, and how much tracked overshoot a reclaimed
 * allocation permits.
 *
 *   npm run test:integration:redis
 */

const url = process.env.CARACAL_REDIS_URL

describe.skipIf(!url)("serval bulkhead against Redis", () => {
  const client = url ? createCoordinationClient(url) : undefined

  beforeAll(async () => {
    await client?.connect()
  })

  afterAll(() => {
    client?.disconnect()
  })

  function replica(options: {
    readonly namespace: string
    readonly replicaId: string
    readonly validityMs: number
    readonly graceMs?: number
    readonly initialSlots?: number
    readonly maximumSlots?: number
    readonly renewLeadMs?: number
  }) {
    return createServalBulkheadCoordinator({
      authority: redisBulkheadAllocationIssuer(client as never, {
        namespace: options.namespace,
      }),
      replicaId: options.replicaId,
      validityMs: options.validityMs,
      graceMs: options.graceMs ?? 0,
      initialSlots: options.initialSlots ?? 1,
      maximumSlots: options.maximumSlots,
      renewLeadMs: options.renewLeadMs,
      redistributionIntervalMs: 0,
    })
  }

  const identity = {
    name: "payments-capacity",
    operation: "charge",
    scope: "t1",
  }
  const leaseMs = 60_000

  it("sums allocations across replicas and refuses the rest", async () => {
    const namespace = `serval-bulkhead-${randomUUID()}`
    const limit = 6
    const first = replica({
      namespace,
      replicaId: "replica-1",
      validityMs: 30_000,
      initialSlots: 4,
      maximumSlots: 4,
    })
    const second = replica({
      namespace,
      replicaId: "replica-2",
      validityMs: 30_000,
      initialSlots: 4,
      maximumSlots: 4,
    })

    // The first replica takes four of the six.
    for (let index = 0; index < 4; index += 1) {
      await expect(
        first.command(identity, "acquire", `t-${index}`, leaseMs, limit),
      ).resolves.toMatchObject({ allowed: true })
    }
    expect(first.diagnostics(identity)).toMatchObject({ allocated: 4 })

    // The second gets the remainder, and no more: it asks for four and is granted two.
    for (let index = 0; index < 2; index += 1) {
      await expect(
        second.command(identity, "acquire", `s-${index}`, leaseMs, limit),
      ).resolves.toMatchObject({ allowed: true })
    }
    await expect(
      second.command(identity, "acquire", "s-2", leaseMs, limit),
    ).resolves.toMatchObject({ allowed: false })

    expect(second.diagnostics(identity)).toMatchObject({ allocated: 2 })
    const total =
      (first.diagnostics(identity)?.allocated ?? 0) +
      (second.diagnostics(identity)?.allocated ?? 0)
    expect(total).toBeLessThanOrEqual(limit)

    await Promise.all([first.close(), second.close()])
  })

  it("does not count a renewal as capacity", async () => {
    const namespace = `serval-bulkhead-${randomUUID()}`
    const limit = 4
    const holder = replica({
      namespace,
      replicaId: "replica-1",
      // Always due for renewal, so the case needs no sleeps and cannot race a clock.
      validityMs: 4_000,
      renewLeadMs: 4_000,
      initialSlots: 4,
      maximumSlots: 4,
    })
    const latecomer = replica({
      namespace,
      replicaId: "replica-2",
      validityMs: 30_000,
      initialSlots: 1,
    })

    await expect(
      holder.command(identity, "acquire", "holder", leaseMs, limit),
    ).resolves.toMatchObject({ allowed: true })
    for (let index = 0; index < 3; index += 1) {
      await expect(
        holder.command(identity, "acquire", `holder-${index}`, leaseMs, limit),
      ).resolves.toMatchObject({ allowed: true })
    }
    await holder.whenIdle()

    // Every further admission finds the pool empty, so it consults the authority, and
    // the only thing it can do is renew.
    for (let index = 0; index < 3; index += 1) {
      await expect(
        holder.command(identity, "acquire", `extra-${index}`, leaseMs, limit),
      ).resolves.toMatchObject({ allowed: false })
      await holder.whenIdle()
    }
    expect(holder.metrics().renewals).toBeGreaterThan(0)

    // The second replica gets nothing while the first replica holds all four: renewing
    // consumed no capacity, so the holder's four slots are still the whole fleet budget.
    await expect(
      latecomer.command(identity, "acquire", "late", leaseMs, limit),
    ).resolves.toMatchObject({ allowed: false })
    expect(latecomer.metrics().refusals).toBeGreaterThan(0)

    await Promise.all([holder.close(), latecomer.close()])
  })

  it("freezes returned capacity before the server can reallocate it", async () => {
    const namespace = `serval-bulkhead-${randomUUID()}`
    const limit = 2
    const holder = replica({
      namespace,
      replicaId: "replica-1",
      validityMs: 30_000,
      initialSlots: 2,
      maximumSlots: 2,
    })
    const waiter = replica({
      namespace,
      replicaId: "replica-2",
      validityMs: 30_000,
      initialSlots: 1,
    })

    await expect(
      holder.command(identity, "acquire", "holder", leaseMs, limit),
    ).resolves.toMatchObject({ allowed: true })
    await holder.whenIdle()
    // One holder and one free slot: the fleet is fully allocated.
    await expect(
      waiter.command(identity, "acquire", "waiting", leaseMs, limit),
    ).resolves.toMatchObject({ allowed: false })

    // The reduction is sent only after the slot was frozen locally, and the commit
    // makes it reallocatable.
    await expect(holder.relinquish(identity, 1)).resolves.toMatchObject({
      type: "returned",
      slots: 1,
    })
    expect(holder.diagnostics(identity)).toMatchObject({
      allocated: 1,
      frozenPendingReturn: 0,
    })

    await expect(
      waiter.command(identity, "acquire", "waiting", leaseMs, limit),
    ).resolves.toMatchObject({ allowed: true })
    expect(waiter.diagnostics(identity)).toMatchObject({ allocated: 1 })

    await Promise.all([holder.close(), waiter.close()])
  })

  it("quantifies the overshoot a reclaimed allocation permits", async () => {
    const namespace = `serval-bulkhead-${randomUUID()}`
    const limit = 2
    const first = replica({
      namespace,
      replicaId: "replica-1",
      // Short enough to lapse inside the test, long enough that a clock difference
      // between this process and the server cannot change the outcome.
      validityMs: 300,
      graceMs: 0,
      initialSlots: 2,
      maximumSlots: 2,
    })
    const second = replica({
      namespace,
      replicaId: "replica-2",
      validityMs: 30_000,
      initialSlots: 2,
      maximumSlots: 2,
    })

    // Two long-running holders under the first allocation. They do not settle: this is
    // the physical work that tracked occupancy cannot bound.
    await expect(
      first.command(identity, "acquire", "a", leaseMs, limit),
    ).resolves.toMatchObject({ allowed: true })
    await expect(
      first.command(identity, "acquire", "b", leaseMs, limit),
    ).resolves.toMatchObject({ allowed: true })
    await first.whenIdle()
    expect(first.diagnostics(identity)).toMatchObject({
      allocated: 2,
      active: 2,
    })

    // Its validity lapses, the reservation is reclaimed, and the second replica takes
    // the whole budget while the first replica's holders are still running.
    await new Promise((resolve) => setTimeout(resolve, 700))
    const decision = await second.command(
      identity,
      "acquire",
      "c",
      leaseMs,
      limit,
    )
    expect(decision).toMatchObject({ allowed: true })
    await expect(
      second.command(identity, "acquire", "d", leaseMs, limit),
    ).resolves.toMatchObject({ allowed: true })
    await Promise.all([first.whenIdle(), second.whenIdle()])

    const tracked =
      (first.diagnostics(identity)?.active ?? 0) +
      (second.diagnostics(identity)?.active ?? 0)
    // The measured deviation: tracked occupancy exceeds the fleet's ceiling by the work
    // that outlived its allocation. It is reported, not denied.
    expect(second.diagnostics(identity)?.allocated).toBe(2)
    expect(tracked).toBe(4)
    expect(tracked - limit).toBe(2)

    await Promise.all([first.close(), second.close()])
  })

  it("prunes a lapsed-reservation backlog in bounded batches", async () => {
    const namespace = `serval-bulkhead-${randomUUID()}`
    const redis = client as NonNullable<typeof client>
    const policy = `bulkhead:${identity.name}`
    const suffix = (name: string) =>
      coordinationKey(
        namespace,
        policy,
        identity.operation,
        identity.scope,
        name,
      )
    const keys = [
      "allocation",
      "allocation-expiry",
      "allocated",
      "allocation-ops",
      "configuration",
    ].map(suffix)
    const expiryKey = suffix("allocation-expiry")
    const counterKey = suffix("allocated")
    const issuer = redisBulkheadAllocationIssuer(client as never, { namespace })

    // A lapsed backlog far past one call's prune budget, plus one lapsed allocation whose
    // slots the counter still holds. Seeded directly: the set carries no TTL, so this drain
    // is what returns capacity.
    const backlog = 40_000
    const lapsedAt = String(Date.now() - 60_000)
    for (let offset = 0; offset < backlog; offset += 1_024) {
      const scores: string[] = []
      for (
        let index = offset;
        index < Math.min(offset + 1_024, backlog);
        index += 1
      ) {
        scores.push(lapsedAt, `o|lapsed-${index}`)
      }
      await redis.zadd(expiryKey, ...scores)
    }
    await redis.zadd(expiryKey, lapsedAt, "a|replica-gone|grant-gone|4")
    await redis.hset(
      suffix("allocation"),
      "replica-gone",
      `grant-gone|1|4|${Number(lapsedAt)}`,
    )
    await redis.incrby(counterKey, 4)

    const limit = 6
    const live = replica({
      namespace,
      replicaId: "replica-live",
      validityMs: 30_000,
      initialSlots: 1,
      maximumSlots: 1,
    })

    try {
      // One call, and only one: a read is a single pass of the script, so what it prunes is
      // what one call prunes.
      await issuer.modify({
        identity,
        replicaId: "replica-live",
        operationId: randomUUID(),
        action: "read",
        limit,
      })

      // Bounded: one call prunes a slice of the backlog rather than all of it, so no call
      // pays a cost proportional to everything that has lapsed.
      const afterFirst = await redis.zcount(
        expiryKey,
        "-inf",
        String(Date.now() - 1),
      )
      expect(afterFirst).toBeGreaterThan(0)
      expect(afterFirst).toBeLessThan(backlog + 1)

      // The live replica takes its slot, so the counter below has something of its own in
      // it once the lapsed allocation is gone.
      await live.command(identity, "acquire", "t-0", leaseMs, limit)

      // It converges over the calls that follow, and the capacity the lapsed allocation held
      // comes back with it: only the live replica's single slot is still counted.
      for (let index = 0; index < 12; index += 1) {
        const lapsed = await redis.zcount(
          expiryKey,
          "-inf",
          String(Date.now() - 1),
        )
        if (lapsed === 0) break
        await issuer.modify({
          identity,
          replicaId: "replica-live",
          operationId: randomUUID(),
          action: "read",
          limit,
        })
      }
      expect(
        await redis.zcount(expiryKey, "-inf", String(Date.now() - 1)),
      ).toBe(0)
      expect(Number(await redis.get(counterKey))).toBe(1)
    } finally {
      await live.close()
      await redis.del(...keys)
    }
  })
})
