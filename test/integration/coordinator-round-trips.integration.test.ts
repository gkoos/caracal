import { randomUUID } from "node:crypto"
import { describe, expect, it } from "vitest"
import {
  bulkhead,
  circuitBreaker,
  operation,
  rateLimit,
} from "../../src/index.js"
import { redisCoordinator } from "../../src/coordination/redis/bulkhead.js"
import { redisCircuitBreakerCoordinator } from "../../src/coordination/redis/circuit-breaker.js"
import { redisRateLimitCoordinator } from "../../src/coordination/redis/rate-limit.js"
import { createCoordinationClient } from "../../src/coordination/redis/client.js"
import { scriptSha } from "../../src/coordination/redis/eval-script.js"
import {
  breakerAdmitProbeV1,
  breakerObserveV1,
  breakerSettleProbeV1,
  bulkheadLeaseV1,
  rateLimitV1,
} from "../../src/coordination/redis/scripts.js"

const url = process.env.CARACAL_REDIS_URL

/**
 * The counts this test asserts are the "Round trips per execution" table in
 * docs/redis.md. Driving each configuration and counting the commands the
 * coordinator actually sends makes the doc an executable claim: a policy that
 * adds a coordinator call fails here instead of silently raising the cost.
 */
describe.skipIf(!url)("coordinator round trips per execution", () => {
  it("issues the documented number of coordinator calls", async () => {
    if (!url) return
    const namespace = `rt-${randomUUID()}`
    const client = createCoordinationClient(url)

    let calls = 0
    // SHA -> body for every script the coordinators ship. The round-trip table
    // counts logical coordinator calls, not transport retries: when the server
    // evicts a script from its cache (a restart, a concurrent SCRIPT FLUSH, or
    // Valkey 8's LRU eviction), EVALSHA answers NOSCRIPT and the coordinator
    // re-sends the body with EVAL. Resolving the SHA here - from the shipped
    // bodies, so it never depends on having seen a prior EVAL - keeps that
    // fallback from counting as two calls.
    const scriptBodies = new Map<string, string>(
      [
        breakerAdmitProbeV1,
        breakerObserveV1,
        breakerSettleProbeV1,
        bulkheadLeaseV1,
        rateLimitV1,
      ].map((script) => [scriptSha(script), script]),
    )
    const counting = {
      eval(script: string, numberOfKeys: number, ...args: (string | number)[]) {
        calls += 1
        return client.eval(script, numberOfKeys, ...args)
      },
      evalsha(sha: string, numberOfKeys: number, ...args: (string | number)[]) {
        calls += 1
        const body = scriptBodies.get(sha)
        if (body !== undefined) return client.eval(body, numberOfKeys, ...args)
        return client.evalsha(sha, numberOfKeys, ...args)
      },
      hmget(key: string, ...fields: string[]) {
        calls += 1
        return client.hmget(key, ...fields)
      },
    }

    await client.connect()

    try {
      async function callsPerExecution(
        drive: () => Promise<unknown>,
        executions: number,
      ): Promise<number> {
        for (let index = 0; index < 20; index++) await drive()
        calls = 0
        for (let index = 0; index < executions; index++) await drive()
        return calls / executions
      }

      const adapter = {
        capabilities: () => ({
          abort: "unsupported" as const,
          replay: "safe" as const,
        }),
        execute: async () => "ok",
      }

      // bulkhead.distributed: acquire + release (a fast attempt never renews).
      const bulkheadPolicy = bulkhead.distributed({
        name: "rt",
        coordinator: redisCoordinator(counting, { namespace }),
        scope: () => "shared",
        limit: 100,
        leaseMs: 30_000,
      })
      const bulkheadOp = operation({
        name: "rt",
        adapter,
        policies: [bulkheadPolicy],
      })
      expect(
        await callsPerExecution(() => bulkheadOp.execute(undefined), 200),
      ).toBe(2)

      // circuitBreaker.distributed, CLOSED: readState + observe.
      const closedPolicy = circuitBreaker.distributed({
        name: "rt",
        coordinator: redisCircuitBreakerCoordinator(counting, { namespace }),
        scope: () => "shared",
        minimumThroughput: 1_000_000,
        failureThreshold: 0.5,
        openMs: 30_000,
      })
      const closedOp = operation({
        name: "rt",
        adapter,
        policies: [closedPolicy],
      })
      expect(
        await callsPerExecution(() => closedOp.execute(undefined), 200),
      ).toBe(2)

      // circuitBreaker.distributed, HALF_OPEN: readState + admitProbe + settleProbe.
      // halfOpenSuccesses is unreachable, so every probe stays half-open and
      // costs the full three calls.
      let shouldFail = true
      const halfOpenPolicy = circuitBreaker.distributed({
        name: "rt-half",
        coordinator: redisCircuitBreakerCoordinator(counting, { namespace }),
        scope: () => "shared",
        minimumThroughput: 1,
        failureThreshold: 0.5,
        openMs: 1,
        halfOpenProbes: 1,
        halfOpenSuccesses: 100_000,
      })
      const halfOpenOp = operation({
        name: "rt-half",
        adapter: {
          capabilities: () => ({
            abort: "unsupported" as const,
            replay: "safe" as const,
          }),
          execute: async () => {
            if (shouldFail) throw new Error("boom")
            return "ok"
          },
        },
        policies: [halfOpenPolicy],
      })
      await halfOpenOp.execute(undefined).catch(() => {})
      shouldFail = false
      await new Promise((resolve) => setTimeout(resolve, 2))
      expect(
        await callsPerExecution(() => halfOpenOp.execute(undefined), 200),
      ).toBe(3)

      // Both policies on one operation.
      const combinedOp = operation({
        name: "rt",
        adapter,
        policies: [
          circuitBreaker.distributed({
            name: "rt",
            coordinator: redisCircuitBreakerCoordinator(counting, {
              namespace,
            }),
            scope: () => "shared",
            minimumThroughput: 1_000_000,
            failureThreshold: 0.5,
            openMs: 30_000,
          }),
          bulkhead.distributed({
            name: "rt",
            coordinator: redisCoordinator(counting, { namespace }),
            scope: () => "shared",
            limit: 100,
            leaseMs: 30_000,
          }),
        ],
      })
      expect(
        await callsPerExecution(() => combinedOp.execute(undefined), 200),
      ).toBe(4)

      // rateLimit.distributed: one atomic GCRA admission. The burst covers the
      // whole run so every call admits and the count is a pure admission path.
      const rateOp = operation({
        name: "rt",
        adapter,
        policies: [
          rateLimit.distributed({
            name: "rt",
            rate: 1000,
            burst: 1000,
            coordinator: redisRateLimitCoordinator(counting, { namespace }),
            scope: () => "shared",
          }),
        ],
      })
      expect(
        await callsPerExecution(() => rateOp.execute(undefined), 200),
      ).toBe(1)
    } finally {
      client.disconnect()
    }
  })
})
