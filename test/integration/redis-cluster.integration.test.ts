import { randomUUID } from "node:crypto"
import { afterAll, beforeAll, describe, expect, it } from "vitest"
import { redisCoordinator } from "../../src/coordination/redis/bulkhead.js"
import { redisCircuitBreakerCoordinator } from "../../src/coordination/redis/circuit-breaker.js"
import { createCoordinationClusterClient } from "../../src/coordination/redis/client.js"
import { coordinationKey } from "../../src/coordination/redis/keys.js"
import type { OperationEvent } from "../../src/index.js"
import {
  bulkhead,
  circuitBreaker,
  operation,
  timeout,
} from "../../src/index.js"
import { Deferred } from "../support/deferred.js"

const clusterUrls = process.env.CARACAL_REDIS_CLUSTER_URLS

function parseNodes(urls: string) {
  return urls.split(",").map((entry) => {
    const [host, portStr] = entry.trim().split(":")
    const port = Number(portStr)
    if (!host || !Number.isInteger(port) || port < 1 || port > 65535)
      throw new Error(`Invalid cluster node: ${entry}`)
    return { host, port }
  })
}

describe.skipIf(!clusterUrls)("Redis Cluster coordination", () => {
  // All setup is deferred inside describe so the factory is never called
  // when CARACAL_REDIS_CLUSTER_URLS is unset and the suite is skipped.
  let client: ReturnType<typeof createCoordinationClusterClient>
  const ns = `test-${randomUUID()}`

  beforeAll(async () => {
    // clusterUrls is non-null: describe.skipIf(!clusterUrls) guards this block
    client = createCoordinationClusterClient(parseNodes(clusterUrls as string))
    await client.connect()
  })

  afterAll(async () => {
    client?.disconnect()
  })

  it("configures the coordinators' safeguards on the node connections ioredis builds", () => {
    // This is the only place the configuration the coordinators actually use can
    // be observed: `client.options` describes the cluster, while the work runs on
    // the node connections the pool derives from `redisOptions`.
    type NodeClient = {
      options: {
        maxRetriesPerRequest?: number
        autoResendUnfulfilledCommands?: boolean
        commandTimeout?: number
        connectTimeout?: number
        enableOfflineQueue?: boolean
      }
    }
    const pool = (
      client as unknown as {
        connectionPool: { nodes: { all: Record<string, NodeClient> } }
      }
    ).connectionPool
    const nodes = Object.values(pool.nodes.all)
    expect(nodes.length).toBeGreaterThan(0)

    for (const node of nodes) {
      expect(node.options.maxRetriesPerRequest).toBe(0)
      expect(node.options.autoResendUnfulfilledCommands).toBe(false)
      expect(node.options.commandTimeout).toBe(1_000)
      expect(node.options.connectTimeout).toBe(1_000)
      // Known gap, documented in redis.md: ioredis 6 does not let this flag be
      // set for cluster node connections, so they keep its default. Pinned so a
      // future ioredis release that changes it is noticed here.
      expect(node.options.enableOfflineQueue).toBe(true)
    }
  })

  async function keySlot(key: string): Promise<string> {
    return String(await client.call("CLUSTER", "KEYSLOT", key))
  }

  it("places every key of one breaker identity on the same cluster slot", async () => {
    const keys = (["breaker", "observations", "probes"] as const).map(
      (suffix) =>
        coordinationKey(
          ns,
          "breaker:cluster-breaker",
          "cluster-cb",
          "cluster-scope",
          suffix,
        ),
    )

    const slots = await Promise.all(keys.map((key) => keySlot(key)))
    expect(new Set(slots).size).toBe(1)
  })

  it("distributed bulkhead admits and rejects correctly across a cluster", async () => {
    const capacity = bulkhead.distributed({
      name: "cluster-test",
      coordinator: redisCoordinator(client, { namespace: ns }),
      scope: () => "slot-a",
      limit: 1,
      leaseMs: 5_000,
    })

    const adapter = {
      capabilities: () => ({
        abort: "unsupported" as const,
        replay: "safe" as const,
      }),
      execute: async () => "ok",
    }

    const op = operation({
      name: "cluster-bulkhead",
      adapter,
      policies: [timeout({ ms: 2_000 }), capacity],
    })

    // First call acquires the permit and completes — limit 1, so second
    // concurrent call would be rejected, but sequential is fine
    await expect(op.execute(undefined)).resolves.toBe("ok")
    await expect(op.execute(undefined)).resolves.toBe("ok")
  })

  it("enforces the distributed limit under contention through a cluster", async () => {
    const capacity = bulkhead.distributed({
      name: "cluster-contention",
      coordinator: redisCoordinator(client, { namespace: ns }),
      scope: () => "contention",
      limit: 1,
      leaseMs: 5_000,
    })

    const entered = new Deferred<void>()
    const release = new Deferred<void>()
    const adapter = {
      capabilities: () => ({
        abort: "unsupported" as const,
        replay: "safe" as const,
      }),
      execute: async () => {
        entered.resolve()
        await release.promise
        return "ok"
      },
    }

    const op = operation({
      name: "cluster-contention",
      adapter,
      policies: [capacity],
    })

    const first = op.execute(undefined)
    await entered.promise

    await expect(op.execute(undefined)).rejects.toMatchObject({
      name: "BulkheadRejectedError",
    })

    release.resolve()
    await expect(first).resolves.toBe("ok")
  }, 10_000)

  it("distributed circuit breaker tracks state across a cluster", async () => {
    const events: OperationEvent[] = []
    const breaker = circuitBreaker.distributed({
      name: "cluster-breaker",
      coordinator: redisCircuitBreakerCoordinator(client, { namespace: ns }),
      scope: () => "cluster-scope",
      minimumThroughput: 3,
      failureThreshold: 0.99,
      openMs: 500,
      halfOpenSuccesses: 1,
      onCoordinatorError: "fail-open",
    })

    let shouldFail = true
    const adapter = {
      capabilities: () => ({
        abort: "unsupported" as const,
        replay: "safe" as const,
      }),
      execute: async () => {
        if (shouldFail) throw new Error("injected failure")
        return "ok"
      },
    }

    const op = operation({
      name: "cluster-cb",
      adapter,
      policies: [breaker],
      events: { emit: (event) => events.push(event) },
    })

    // Drive 3 failures to open the breaker
    for (let i = 0; i < 3; i++) {
      await expect(op.execute(undefined)).rejects.toThrow("injected failure")
    }

    // Breaker should now be open — next call rejected
    await expect(op.execute(undefined)).rejects.toMatchObject({
      name: "CircuitOpenError",
    })

    // Wait for openMs to elapse and allow a probe
    await new Promise((resolve) => setTimeout(resolve, 600))
    shouldFail = false

    // Probe should succeed and close the breaker
    await expect(op.execute(undefined)).resolves.toBe("ok")
    expect(
      events.some(
        (event) =>
          event.type === "breaker.state-changed" &&
          event.state === "closed" &&
          event.previousState === "half-open",
      ),
    ).toBe(true)
  }, 10_000)

  it("both coordinators share the same client connection", () => {
    // Verify both factories accept the cluster client — no second connection needed
    expect(() => redisCoordinator(client, { namespace: ns })).not.toThrow()
    expect(() =>
      redisCircuitBreakerCoordinator(client, { namespace: ns }),
    ).not.toThrow()
  })
})
