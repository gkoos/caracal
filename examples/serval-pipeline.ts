/**
 * A runnable Serval pipeline.
 *
 *   npm run redis:up
 *   npm run example:serval
 *
 * It builds the three delegated policies over one operation, runs a few calls through
 * a local guardrail, and prints what each layer decided. The imports go through the
 * package's own entry points, so the example is also a smoke test of the public
 * surface: if an export moves, this stops compiling.
 */

import { bulkhead, circuitBreaker, operation, rateLimit } from "../src/index.js"
import {
  createCoordinationClient,
  redisCircuitBreakerCoordinator,
} from "../src/redis.js"
import {
  createServalBreaker,
  createServalBulkheadCoordinator,
  createServalRateLimitCoordinator,
  redisBulkheadAllocationIssuer,
  redisRateLimitGrantIssuer,
} from "../src/serval.js"

const url = process.env.CARACAL_REDIS_URL ?? "redis://127.0.0.1:6379"
const namespace = `example:${Date.now()}`
const replicaId = process.env.HOSTNAME ?? "example-replica"

const client = createCoordinationClient(url, 500)
await client.connect()

const breaker = createServalBreaker({
  namespace,
  authority: redisCircuitBreakerCoordinator(client, { namespace }),
  freshnessMs: 1_000,
  reconciliationIntervalMs: 60_000,
})
await breaker.session.start()

const rate = createServalRateLimitCoordinator({
  authority: redisRateLimitGrantIssuer(client, { namespace }),
  grantSize: 4,
  validForMs: 1_000,
})

const capacity = createServalBulkheadCoordinator({
  authority: redisBulkheadAllocationIssuer(client, { namespace }),
  replicaId,
  validityMs: 10_000,
  initialSlots: 1,
  maximumSlots: 4,
})

let calls = 0
const charge = operation({
  name: "charge",
  adapter: {
    capabilities: () => ({ abort: "supported", replay: "safe" }) as const,
    execute: async () => {
      calls += 1
      return `charged-${calls}`
    },
  },
  policies: [
    circuitBreaker.distributed({
      name: "payments",
      coordinator: breaker.coordinator,
      scope: () => "tenant-a",
    }),
    rateLimit.distributed({
      name: "payments-rate",
      rate: 100,
      burst: 4,
      coordinator: rate,
      scope: () => "tenant-a",
    }),
    bulkhead.distributed({
      name: "payments-capacity",
      limit: 4,
      coordinator: capacity,
      scope: () => "tenant-a",
    }),
    // A local guardrail is composed independently of the delegated ceiling.
    bulkhead.local({ name: "payments-local", limit: 2 }),
  ],
})

const outcomes: string[] = []
for (let index = 0; index < 6; index += 1) {
  try {
    outcomes.push(await charge.execute(undefined))
  } catch (error) {
    outcomes.push(`${(error as Error).name}: ${(error as Error).message}`)
  }
}

console.log(`namespace ${namespace}, replica ${replicaId}`)
console.log(`calls     ${outcomes.join(", ")}`)
console.log(
  `breaker   ${JSON.stringify(breaker.coordinator.cached({ name: "payments", operation: "charge", scope: "tenant-a" }))}`,
)
console.log(`rate      ${JSON.stringify(rate.metrics())}`)
console.log(`bulkhead  ${JSON.stringify(capacity.metrics())}`)
console.log(`session   ${JSON.stringify(breaker.session.metrics().counters)}`)

await Promise.all([breaker.session.close(), capacity.close()])
await rate.whenIdle()
client.disconnect()
