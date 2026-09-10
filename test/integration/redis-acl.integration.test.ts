import { randomUUID } from "node:crypto"
import { afterAll, beforeAll, describe, expect, it } from "vitest"
import { evalScript } from "../../src/coordination/redis/eval-script.js"
import { coordinationKey } from "../../src/coordination/redis/keys.js"
import type { BreakerIdentity } from "../../src/index.js"
import {
  createCoordinationClient,
  redisCircuitBreakerCoordinator,
  redisCoordinator,
} from "../../src/redis.js"

const url = process.env.CARACAL_REDIS_URL

/**
 * The grant from docs/redis.md ("ACL recommendations"), spelled out in full.
 * Keep the two lists identical: this suite is what proves the documented set is
 * complete, so a command the implementation starts using without a new row (or
 * a row without a real need) shows up here.
 */
const GRANTED_COMMANDS = [
  "EVAL",
  "EVALSHA",
  "TIME",
  "HMGET",
  "HSET",
  "ZADD",
  "ZREM",
  "ZCARD",
  "ZSCORE",
  "ZREVRANGE",
  "ZRANGE",
  "ZREMRANGEBYSCORE",
  "ZREMRANGEBYRANK",
  "PEXPIRE",
  "PEXPIREAT",
  "PERSIST",
  "EXISTS",
  "DEL",
  "PING",
]

const KEY_PATTERN = "caracal:v1:*"

type CoordinationClient = ReturnType<typeof createCoordinationClient>

const IDENTITY: BreakerIdentity = {
  name: "b",
  operation: "op",
  scope: "shared",
}

const BREAKER_PARAMS = {
  windowTtlMs: 60_000,
  minimumThroughput: 2,
  failureThresholdNumerator: 500,
  windowSize: 10,
  openMs: 50,
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

describe.skipIf(!url)("distributed breaker/bulkhead — ACL integration", () => {
  const admin = createCoordinationClient(url ?? "redis://127.0.0.1:6379")
  const username = `caracal-acl-${randomUUID().slice(0, 8)}`
  const password = randomUUID().replace(/-/g, "")
  const cleanupKeys: string[] = []
  let restrictedUrl = ""
  let setupError: unknown = null

  beforeAll(async () => {
    const parsed = new URL(url ?? "")
    restrictedUrl = `redis://${username}:${password}@${parsed.host}`
    await admin.connect()
    try {
      await admin.call("ACL", "SETUSER", username, "reset")
      await admin.call(
        "ACL",
        "SETUSER",
        username,
        "on",
        `>${password}`,
        `~${KEY_PATTERN}`,
        ...GRANTED_COMMANDS.map((command) => `+${command}`),
      )
    } catch (error) {
      setupError = error
    }
  })

  afterAll(async () => {
    if (setupError === null) {
      await admin.call("ACL", "DELUSER", username).catch(() => {})
    }
    if (cleanupKeys.length > 0) {
      await admin.del(...cleanupKeys).catch(() => {})
    }
    admin.disconnect()
  })

  /** Skips when this Redis cannot manage ACL users (e.g. a managed instance). */
  function skipWithoutAcl(ctx: { skip: (reason?: string) => void }): boolean {
    if (setupError === null) return false
    const detail =
      setupError instanceof Error ? setupError.message : String(setupError)
    ctx.skip(`ACL management unavailable on this server: ${detail}`)
    return true
  }

  /** A namespace whose keys are cleaned up after the run. */
  function namespace(label: string): string {
    const value = `acl-${label}-${randomUUID()}`
    const keys: [string, string][] = [
      ["breaker:b", "breaker"],
      ["breaker:b", "observations"],
      ["breaker:b", "probes"],
      ["bulkhead:b", "leases"],
    ]
    for (const [policy, suffix] of keys) {
      cleanupKeys.push(coordinationKey(value, policy, "op", "shared", suffix))
    }
    return value
  }

  async function withRestrictedClient<Result>(
    run: (client: CoordinationClient) => Promise<Result>,
  ): Promise<Result> {
    const client = createCoordinationClient(restrictedUrl)
    client.on("error", () => {})
    await client.connect()
    try {
      return await run(client)
    } finally {
      client.disconnect()
    }
  }

  it("admits, renews and releases a bulkhead lease", async (ctx) => {
    if (skipWithoutAcl(ctx)) return
    await withRestrictedClient(async (client) => {
      const coordinator = redisCoordinator(client, {
        namespace: namespace("bulkhead"),
      })
      const token = randomUUID()

      await expect(
        coordinator.command(IDENTITY, "acquire", token, 5_000, 5),
      ).resolves.toMatchObject({ allowed: true, occupancy: 1 })
      await expect(
        coordinator.command(IDENTITY, "renew", token, 5_000, 5),
      ).resolves.toMatchObject({ allowed: true })
      await expect(
        coordinator.command(IDENTITY, "release", token, 5_000, 5),
      ).resolves.toMatchObject({ allowed: true })
    })
  })

  it("runs the complete breaker lifecycle", async (ctx) => {
    if (skipWithoutAcl(ctx)) return
    await withRestrictedClient(async (client) => {
      const coordinator = redisCircuitBreakerCoordinator(client, {
        namespace: namespace("breaker"),
      })
      let generation = 0
      const observe = async (outcome: "success" | "failure") => {
        const result = await coordinator.observe(IDENTITY, {
          ...BREAKER_PARAMS,
          generation,
          outcome,
          uuid: randomUUID(),
        })
        if (result.type === "opened") generation = result.newGeneration
        else if (result.type === "observed") generation = result.generation
        return result
      }

      // CLOSED -> OPEN
      for (let index = 0; index < BREAKER_PARAMS.minimumThroughput; index++) {
        await observe("failure")
      }
      expect(generation).toBeGreaterThan(0)

      // OPEN -> HALF_OPEN: the transition clears the probe set, which needs DEL
      await sleep(BREAKER_PARAMS.openMs + 30)
      const closingToken = randomUUID()
      const admitted = await coordinator.admitProbe(IDENTITY, {
        probeToken: closingToken,
        openMs: BREAKER_PARAMS.openMs,
        halfOpenProbes: 1,
        probeLeaseTtlMs: 5_000,
      })
      expect(admitted).toMatchObject({ type: "admitted", stateChanged: true })
      if (admitted.type !== "admitted") return

      // HALF_OPEN -> CLOSED: also needs DEL
      const closed = await coordinator.settleProbe(IDENTITY, {
        probeToken: closingToken,
        outcome: "success",
        generation: admitted.generation,
        halfOpenSuccesses: 1,
        openMs: BREAKER_PARAMS.openMs,
        windowTtlMs: BREAKER_PARAMS.windowTtlMs,
      })
      expect(closed).toMatchObject({ type: "transitioned", newState: "closed" })
      if (closed.type !== "transitioned") return
      generation = closed.newGeneration

      // CLOSED -> OPEN again, then a failing probe re-opens (third DEL path)
      for (let index = 0; index < BREAKER_PARAMS.minimumThroughput; index++) {
        await observe("failure")
      }
      await sleep(BREAKER_PARAMS.openMs + 30)
      const failingToken = randomUUID()
      const failing = await coordinator.admitProbe(IDENTITY, {
        probeToken: failingToken,
        openMs: BREAKER_PARAMS.openMs,
        halfOpenProbes: 1,
        probeLeaseTtlMs: 5_000,
      })
      if (failing.type !== "admitted") {
        throw new Error(`probe not admitted: ${failing.type}`)
      }
      await expect(
        coordinator.settleProbe(IDENTITY, {
          probeToken: failingToken,
          outcome: "failure",
          generation: failing.generation,
          halfOpenSuccesses: 2,
          openMs: BREAKER_PARAMS.openMs,
          windowTtlMs: BREAKER_PARAMS.windowTtlMs,
        }),
      ).resolves.toMatchObject({ type: "transitioned", newState: "open" })
    })
  })

  it("mints a new epoch after an out-of-band state loss", async (ctx) => {
    if (skipWithoutAcl(ctx)) return
    const value = namespace("epoch")
    const hashKey = coordinationKey(
      value,
      "breaker:b",
      "op",
      "shared",
      "breaker",
    )
    await withRestrictedClient(async (client) => {
      const coordinator = redisCircuitBreakerCoordinator(client, {
        namespace: value,
      })
      let generation = 0
      for (let index = 0; index < BREAKER_PARAMS.minimumThroughput; index++) {
        const result = await coordinator.observe(IDENTITY, {
          ...BREAKER_PARAMS,
          generation,
          outcome: "failure",
          uuid: randomUUID(),
        })
        if (result.type === "opened") generation = result.newGeneration
      }
      expect(generation).toBeGreaterThan(0)

      // Only an admin removes the hash; the next observation has to mint an
      // epoch from inside the script.
      await admin.del(hashKey)

      const fresh = await coordinator.observe(IDENTITY, {
        ...BREAKER_PARAMS,
        generation: 0,
        outcome: "failure",
        uuid: randomUUID(),
      })
      expect(fresh).toMatchObject({ type: "observed", windowTotal: 1 })
      if (fresh.type !== "observed") return
      expect(fresh.generation).not.toBe(generation)
    })
  })

  it("falls back to EVAL for a script the server has never seen", async (ctx) => {
    if (skipWithoutAcl(ctx)) return
    await withRestrictedClient(async (client) => {
      // A body no other process can have cached, so EVALSHA alone cannot
      // succeed: this only passes because EVAL is granted as well.
      const result = await evalScript(client, `return ${Date.now()}`, 0)
      expect(Number(result)).toBeGreaterThan(0)
    })
  })

  it("keeps the grant narrow and allows a health check", async (ctx) => {
    if (skipWithoutAcl(ctx)) return
    await withRestrictedClient(async (client) => {
      await expect(client.ping()).resolves.toBe("PONG")
      await expect(client.keys("*")).rejects.toThrow(/NOPERM/)
      await expect(client.flushall()).rejects.toThrow(/NOPERM/)
    })
  })
})
