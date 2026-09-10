import { randomUUID } from "node:crypto"
import { afterAll, beforeAll, describe, expect, it } from "vitest"
import { coordinationKey } from "../../src/coordination/redis/keys.js"
import { fetchAdapter } from "../../src/fetch.js"
import type { OperationEvent } from "../../src/index.js"
import { bulkhead, operation, timeout } from "../../src/index.js"
import { createCoordinationClient, redisCoordinator } from "../../src/redis.js"
import { Deferred } from "../support/deferred.js"
import { dependencyServer } from "../support/dependency-server.js"
import { redisProxy } from "../support/redis-proxy.js"
import { LeaseWorker } from "../support/worker-process/harness.js"

const url = process.env.CARACAL_REDIS_URL
describe.skipIf(!url)("distributed bulkhead public API", () => {
  it("protects the real fetch adapter with the same distributed policy", async () => {
    const dependency = await dependencyServer()
    const namespace = `test-${randomUUID()}`
    const key = identity(namespace)
    const policy = bulkhead.distributed({
      name: "pool",
      limit: 1,
      scope: () => "shared",
      coordinator: redisCoordinator(client, { namespace }),
    })
    const op = operation({
      name: "work",
      adapter: fetchAdapter(),
      policies: [policy],
    })
    const call = op.execute({ url: dependency.url })
    try {
      await expect.poll(() => dependency.active).toBe(1)
      await expect(op.execute({ url: dependency.url })).rejects.toMatchObject({
        name: "BulkheadRejectedError",
      })
      dependency.release()
      expect(await (await call).text()).toBe("done")
      expect(await client.zcard(key)).toBe(0)
    } finally {
      dependency.release()
      await call.catch(() => {})
      await dependency.close()
    }
  })
  const client = createCoordinationClient(url ?? "redis://127.0.0.1:6379")
  const keys: string[] = []
  function identity(namespace: string, operation = "work", scope = "shared") {
    const key = coordinationKey(namespace, "bulkhead:pool", operation, scope)
    keys.push(key)
    return key
  }
  beforeAll(() => client.connect())
  afterAll(async () => {
    try {
      if (keys.length) await client.del(...keys)
    } finally {
      client.disconnect()
    }
  })
  it("bounds independently measured underlying concurrency across processes and renews past the initial lease", async () => {
    const dependency = await dependencyServer()
    const workers = Array.from(
      { length: 4 },
      () => new LeaseWorker(url as string),
    )
    try {
      await Promise.all(workers.map((w) => w.ready()))
      for (let round = 0; round < 3; round++) {
        const namespace = `test-${randomUUID()}`
        const key = identity(namespace)
        const calls = workers.map((w) =>
          w
            .command("execute", dependency.url, namespace, 900, 2)
            .catch(() => "rejected"),
        )
        await expect.poll(() => dependency.active).toBe(2)
        await new Promise((resolve) => setTimeout(resolve, 1200))
        expect(await client.zcard(key)).toBe(2)
        expect(dependency.maximum).toBe(2)
        dependency.release()
        const results = await Promise.all(calls)
        expect(results.filter((r) => r === "done")).toHaveLength(2)
        expect(results.filter((r) => r === "rejected")).toHaveLength(2)
        expect(await client.zcard(key)).toBe(0)
      }
    } finally {
      await Promise.all(workers.map((w) => w.stop()))
      await dependency.close()
    }
  }, 20000)
  it("keeps renewing after caller timeout until late underlying settlement", async () => {
    const dependency = await dependencyServer()
    const worker = new LeaseWorker(url as string)
    const namespace = `test-${randomUUID()}`
    const key = identity(namespace)
    try {
      await worker.ready()
      await expect(
        worker.command("execute-timeout", dependency.url, namespace, 600),
      ).rejects.toThrow("timed out")
      expect(dependency.active).toBe(1)
      await new Promise((resolve) => setTimeout(resolve, 900))
      expect(await client.zcard(key)).toBe(1)
      await expect(
        worker.command("execute", dependency.url, namespace, 600),
      ).rejects.toThrow("capacity")
      dependency.release()
      await expect.poll(() => client.zcard(key)).toBe(0)
    } finally {
      await worker.stop()
      await dependency.close()
    }
  }, 10000)
  it("reclaims a killed or frozen owner's lease and exposes the orphan-work boundary", async () => {
    for (const mode of ["kill", "freeze"]) {
      const dependency = await dependencyServer()
      const owner = new LeaseWorker(url as string)
      const successor = new LeaseWorker(url as string)
      const namespace = `test-${randomUUID()}`
      const key = identity(namespace)
      try {
        await Promise.all([owner.ready(), successor.ready()])
        const old = owner
          .command("execute", dependency.url, namespace, 600)
          .catch(() => "exited")
        await expect.poll(() => dependency.active).toBe(1)
        if (mode === "kill") await owner.stop()
        else await owner.command("freeze", key, "unused", 4000)
        await expect.poll(() => client.exists(key), { timeout: 3000 }).toBe(0)
        const replacement = successor.command(
          "execute",
          dependency.url,
          namespace,
          600,
        )
        await expect.poll(() => dependency.active).toBe(2)
        expect(await client.zcard(key)).toBe(1)
        // Dead/partitioned owners can leave real work running: expiry is not fencing.
        dependency.release()
        await replacement
        await owner.stop()
        await old
      } finally {
        await Promise.all([owner.stop(), successor.stop()])
        await dependency.close()
      }
    }
  }, 15000)
  it("isolates operation and scope identities and does not release on unsupported timeout", async () => {
    const namespace = `test-${randomUUID()}`
    const coordinator = redisCoordinator(client, { namespace })
    const gate = new Deferred<void>()
    const policy = bulkhead.distributed({
      name: "pool",
      limit: 1,
      leaseMs: 1000,
      coordinator,
      scope: (ctx) => String(ctx.metadata.scope),
    })
    const make = (name: string) =>
      operation({
        name,
        adapter: {
          capabilities: () => ({ abort: "unsupported", replay: "safe" }),
          execute: () => gate.promise,
        },
        policies: [policy, timeout({ ms: 100 })],
      })
    const one = make("one"),
      two = make("two")
    const tracked = [
      identity(namespace, "one", "a"),
      identity(namespace, "one", "b"),
      identity(namespace, "two", "a"),
    ]
    try {
      await Promise.all(
        [
          one.execute(undefined, { metadata: { scope: "a" } }),
          one.execute(undefined, { metadata: { scope: "b" } }),
          two.execute(undefined, { metadata: { scope: "a" } }),
        ].map((call) =>
          expect(call).rejects.toMatchObject({ name: "TimeoutError" }),
        ),
      )
      for (const key of tracked) expect(await client.zcard(key)).toBe(1)
      await expect(
        one.execute(undefined, { metadata: { scope: "a" } }),
      ).rejects.toMatchObject({ name: "BulkheadRejectedError", scope: "a" })
    } finally {
      gate.resolve()
      for (const key of tracked)
        await expect.poll(() => client.zcard(key)).toBe(0)
    }
  })
  it("fails closed on disconnect and reports lost renewal without silently releasing live work", async () => {
    const proxy = await redisProxy(new URL(url as string))
    const remote = createCoordinationClient(proxy.url, 100)
    const namespace = `test-${randomUUID()}`
    const key = identity(namespace)
    const gate = new Deferred<void>()
    const events: OperationEvent[] = []
    let starts = 0
    try {
      await remote.connect()
      const policy = bulkhead.distributed({
        name: "pool",
        limit: 1,
        leaseMs: 600,
        scope: () => "shared",
        coordinator: redisCoordinator(remote, { namespace }),
      })
      const op = operation({
        name: "work",
        adapter: {
          capabilities: () => ({ abort: "unsupported", replay: "safe" }),
          execute: () => {
            starts++
            return gate.promise
          },
        },
        policies: [policy],
        events: { emit: (e) => events.push(e) },
      })
      const call = op.execute(undefined)
      await expect.poll(() => starts).toBe(1)
      proxy.disconnect()
      await expect(op.execute(undefined)).rejects.toMatchObject({
        name: "CoordinatorUnavailableError",
      })
      await expect
        .poll(() => events.some((e) => e.type === "bulkhead.lease-lost"))
        .toBe(true)
      await expect.poll(() => client.exists(key)).toBe(0)
      expect(starts).toBe(1)
      proxy.restore()
      await expect.poll(() => remote.status).toBe("ready")
      gate.resolve()
      await call
      await op.execute(undefined)
      expect(starts).toBe(2)
    } finally {
      gate.resolve()
      remote.disconnect()
      await proxy.close()
    }
  }, 10000)
})
