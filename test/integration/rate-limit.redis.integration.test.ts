import { randomUUID } from "node:crypto"
import { afterAll, beforeAll, describe, expect, it } from "vitest"
import { coordinationKey } from "../../src/coordination/redis/keys.js"
import { operation, rateLimit } from "../../src/index.js"
import {
  createCoordinationClient,
  redisRateLimitCoordinator,
} from "../../src/redis.js"
import { redisProxy } from "../support/redis-proxy.js"

const url = process.env.CARACAL_REDIS_URL
describe.skipIf(!url)("distributed rate limit public API", () => {
  const client = createCoordinationClient(url ?? "redis://127.0.0.1:6379")
  const keys: string[] = []
  function identity(namespace: string, operation = "work", scope = "shared") {
    const key = coordinationKey(
      namespace,
      "ratelimit:pool",
      operation,
      scope,
      "rate",
    )
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

  it("enforces a shared rate with retry-after and cleans up its key", async () => {
    const namespace = `test-${randomUUID()}`
    const key = identity(namespace)
    const policy = rateLimit.distributed({
      name: "pool",
      rate: 100, // 10 ms emission interval, no burst
      scope: () => "shared",
      coordinator: redisRateLimitCoordinator(client, { namespace }),
    })
    const op = operation({
      name: "work",
      adapter: {
        capabilities: () => ({ abort: "unsupported", replay: "safe" }),
        execute: async () => "ok",
      },
      policies: [policy],
    })

    await expect(op.execute(undefined)).resolves.toBe("ok")
    await expect(op.execute(undefined)).rejects.toMatchObject({
      name: "RateLimitExceededError",
      retryAfterMs: expect.any(Number),
    })

    // Once the emission interval elapses the next call is admissible and the
    // GCRA key self-cleans, so the scope leaves no state behind.
    await new Promise((resolve) => setTimeout(resolve, 15))
    await expect(op.execute(undefined)).resolves.toBe("ok")
    await expect.poll(() => client.exists(key)).toBe(0)
  })

  it("fails closed on disconnect", async () => {
    const proxy = await redisProxy(new URL(url as string))
    const remote = createCoordinationClient(proxy.url, 100)
    const namespace = `test-${randomUUID()}`
    identity(namespace)
    try {
      await remote.connect()
      const policy = rateLimit.distributed({
        name: "pool",
        rate: 100,
        scope: () => "shared",
        coordinator: redisRateLimitCoordinator(remote, { namespace }),
      })
      const op = operation({
        name: "work",
        adapter: {
          capabilities: () => ({ abort: "unsupported", replay: "safe" }),
          execute: async () => "ok",
        },
        policies: [policy],
      })

      await expect(op.execute(undefined)).resolves.toBe("ok")
      proxy.disconnect()
      await expect(op.execute(undefined)).rejects.toMatchObject({
        name: "CoordinatorUnavailableError",
      })
    } finally {
      remote.disconnect()
      await proxy.close()
    }
  }, 10000)
})
