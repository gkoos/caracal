import { randomUUID } from "node:crypto"
import { afterAll, beforeAll, describe, expect, it } from "vitest"
import { createCoordinationClient } from "../../src/coordination/redis/client.js"
import { coordinationKey } from "../../src/coordination/redis/keys.js"
import { leaseCommand } from "../../src/coordination/redis/leases.js"
import { redisProxy } from "../support/redis-proxy.js"
import { LeaseWorker } from "../support/worker-process/harness.js"

const url = process.env.CARACAL_REDIS_URL
describe.skipIf(!url)("Redis foundation", () => {
  const client = createCoordinationClient(url ?? "redis://127.0.0.1:6379")
  const keys: string[] = []
  function key(scope = randomUUID()) {
    const value = coordinationKey(`test-${run}`, "bulkhead", "test", scope)
    keys.push(value)
    return value
  }
  const run = randomUUID()
  beforeAll(async () => {
    await client.connect()
  })
  afterAll(async () => {
    try {
      if (keys.length) await client.del(...keys)
    } finally {
      client.disconnect()
    }
  })
  it("atomically arbitrates repeated cross-process races after a readiness barrier", async () => {
    const workers = Array.from(
      { length: 4 },
      () => new LeaseWorker(url as string),
    )
    try {
      await Promise.all(workers.map((worker) => worker.ready()))
      for (let round = 0; round < 3; round++) {
        const identity = key()
        const results = await Promise.all(
          workers.map((worker, i) =>
            worker.command("acquire", identity, String(i), 5000, 2),
          ),
        )
        expect(results.filter(Boolean)).toHaveLength(2)
        expect(await client.zcard(identity)).toBe(2)
        await Promise.all(
          workers.map((worker, i) =>
            worker.command("release", identity, String(i)),
          ),
        )
        expect(await client.zcard(identity)).toBe(0)
      }
    } finally {
      await Promise.all(workers.map((worker) => worker.stop()))
    }
  }, 20000)
  it("expires permits of killed and frozen owners without stale release affecting replacements", async () => {
    for (const mode of ["kill", "freeze"]) {
      const worker = new LeaseWorker(url as string)
      const identity = key()
      try {
        await worker.ready()
        expect(await worker.command("acquire", identity, "old", 250)).toBe(true)
        if (mode === "kill") await worker.stop()
        else await worker.command("freeze", identity, "old", 2000)
        await expect
          .poll(() => client.exists(identity), { timeout: 4000 })
          .toBe(0)
        expect(
          await leaseCommand(client, identity, "acquire", "new", 2000, 1),
        ).toBe(true)
        expect(
          await leaseCommand(client, identity, "release", "old", 2000, 1),
        ).toBe(false)
        expect(await client.zcard(identity)).toBe(1)
      } finally {
        await worker.stop()
      }
    }
  }, 15000)
  it("renews live leases, rejects expired renewal and isolates cleanup", async () => {
    const first = key()
    const other = key()
    await leaseCommand(client, first, "acquire", "a", 100, 1)
    await leaseCommand(client, first, "renew", "a", 2000, 1)
    expect(await client.pttl(first)).toBeGreaterThan(100)
    await leaseCommand(client, other, "acquire", "b", 100, 1)
    await expect.poll(() => client.exists(other)).toBe(0)
    expect(await leaseCommand(client, other, "renew", "b", 100, 1)).toBe(false)
    expect(await client.zcard(first)).toBe(1)
  })
  it("bounds delayed commands, surfaces disconnects, and reconnects without local fallback", async () => {
    const proxy = await redisProxy(new URL(url as string))
    const remote = createCoordinationClient(proxy.url, 100)
    try {
      await remote.connect()
      proxy.delay(300)
      await expect(
        leaseCommand(remote, key(), "acquire", "delayed", 100, 1),
      ).rejects.toMatchObject({ name: "CoordinatorUnavailableError" })
      proxy.disconnect()
      await expect(
        leaseCommand(remote, key(), "acquire", "offline", 100, 1),
      ).rejects.toMatchObject({ name: "CoordinatorUnavailableError" })
      proxy.delay(0)
      proxy.restore()
      await expect.poll(() => remote.status, { timeout: 5000 }).toBe("ready")
      expect(
        await leaseCommand(remote, key(), "acquire", "reconnected", 100, 1),
      ).toBe(true)
    } finally {
      remote.disconnect()
      await proxy.close()
    }
  }, 10000)
})
