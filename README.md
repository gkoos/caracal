<p align="center">
  <img src="docs/caracal.svg" alt="Caracal" width="140">
</p>

![npm](https://img.shields.io/npm/v/@gkoos/caracal)
![Downloads](https://img.shields.io/npm/dm/@gkoos/caracal)
![GitHub stars](https://img.shields.io/github/stars/gkoos/caracal?style=social)

![Build](https://github.com/gkoos/caracal/actions/workflows/ci.yml/badge.svg)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/gkoos/caracal/badge)](https://scorecard.dev/viewer/?uri=github.com/gkoos/caracal)

![MIT](https://img.shields.io/npm/l/@gkoos/caracal)
![Types](https://img.shields.io/npm/types/@gkoos/caracal)

# Caracal

**Scoped distributed resilience for asynchronous operations.**

Caracal is a library for wrapping asynchronous operations (like HTTP requests or database queries) with resilience policies. It provides a composable model for applying timeouts, retries, circuit breakers, and bulkheads to any operation that can be expressed as an adapter. The library is designed to work in distributed environments, allowing multiple instances of an application to **share and coordinate** their resilience policies through a shared Redis backend.

### The problem

There are many libraries for applying resilience policies to operations, but most of them are designed for a single process. In a distributed system, each replica of an application may have its own local concurrency limits and circuit breakers, which can lead to uncoordinated failures and resource exhaustion.

If 40 replicas each enforce a local concurrency limit of 20, the downstream can still receive 800 concurrent requests. If each replica maintains its own circuit breaker, you get 40 independent failure windows and 40 independent recovery probes. Caracal uses Redis to coordinate those constraints across whichever scope actually matches your failure domain: region, shard, tenant, credential, workload class, or any grouping that makes sense for your application.

## Quick start

```sh
npm install @gkoos/caracal
# Redis coordination (optional peer dependency for distributed policies):
npm install ioredis
# PostgreSQL adapter (optional peer dependency for database operations):
npm install pg
```

```ts
import { bulkhead, circuitBreaker, operation, retry, timeout } from "@gkoos/caracal"
import { createCoordinationClient, redisCoordinator, redisCircuitBreakerCoordinator } from "@gkoos/caracal/redis"
import { fetchAdapter } from "@gkoos/caracal/fetch"
import { Pool } from "pg"
import { postgresAdapter } from "@gkoos/caracal/postgres"

// Redis coordination - connect once, share across all policies
const redis = createCoordinationClient(process.env.REDIS_URL!)
await redis.connect()

// Circuit breaker shared across all replicas, tracked per region.
// Use circuitBreaker.local({ name, minimumThroughput, failureThreshold, openMs }) if you only need in-process tracking.
const breaker = circuitBreaker.distributed({
  name: "partner-api",
  coordinator: redisCircuitBreakerCoordinator(redis, { namespace: "svc:prod" }),
  scope: (ctx) => `region:${String(ctx.metadata.region)}`,
  minimumThroughput: 20,
  failureThreshold: 0.5,
  openMs: 30_000,
  halfOpenProbes: 3,
  onCoordinatorError: "fail-open",
})

// Concurrency limit enforced across all replicas, per region.
// Use bulkhead.local({ name, limit, queue }) if you only need a per-process limit.
const capacity = bulkhead.distributed({
  name: "partner-api",
  coordinator: redisCoordinator(redis, { namespace: "svc:prod" }),
  scope: (ctx) => `region:${String(ctx.metadata.region)}`,
  limit: 20,
  leaseMs: 30_000,
})

// Fetch operation: breaker outermost, bulkhead innermost around the adapter.
// timeout bounds the whole retry sequence; retry drives individual attempts.
const fetchOp = operation({
  name: "partner-api",
  adapter: fetchAdapter(),
  policies: [
    breaker,
    timeout({ ms: 10_000 }),
    retry({ maxAttempts: 3, delay: (n) => 100 * 2 ** (n - 1) }),
    capacity,
  ],
  events: { emit: (e) => console.log(e.type, e) },
})

const response = await fetchOp.execute(
  { url: "https://api.partner.com/orders/42" },
  { metadata: { region: "eu-west-2" } },
)

// PostgreSQL operation reusing the same policy instances.
// A distributed policy is identified by (namespace, policy name, operation
// name, scope), so `orders-db` gets its own breaker window and capacity
// budget. Reuse the same operation name to share them deliberately.
const pool = new Pool({ connectionString: process.env.DATABASE_URL })
const dbOp = operation({
  name: "orders-db",
  adapter: postgresAdapter(pool),
  policies: [breaker, retry({ maxAttempts: 2 }), timeout({ ms: 2_000 }), capacity],
})

const row = await dbOp.execute(
  { sql: "select * from orders where id = $1", values: ["42"], replay: "safe" },
  { metadata: { region: "eu-west-2" } },
)

// At shutdown
await pool.end()
redis.disconnect()
```

Distributed policies coordinate by `(namespace, policy name, operation name, scope)`. Two operations with different names get independent budgets and breaker windows even when they reuse the same policy instances. To share a budget or breaker deliberately, give the operations the same name - see [bulkheads](docs/bulkhead.md) and the [Redis key scheme](docs/redis.md).

## How it works

### Operations and adapters

An `operation` wraps an `adapter`: an object that declares its cancellation and replay capabilities before executing underlying work:

```ts
import { operation, type Adapter } from "@gkoos/caracal"

const adapter: Adapter<{ id: string }, Order> = {
  capabilities: () => ({ abort: "supported", replay: "safe" }),
  execute: async ({ id }, context) => fetchOrder(id, { signal: context.signal }),
}

const orders = operation({ name: "orders", adapter })
const order = await orders.execute({ id: "42" }, { metadata: { region: "eu-west-2" } })
```

`abort` tells Caracal whether it can signal cancellation to the underlying work. `replay` tells retry whether a repeated attempt is safe. Both are declared explicitly by the adapter, Caracal never infers them.

### Policies

Policies are applied in array order, outermost first. The recommended ordering for a fully-configured operation is:

```ts
// Schematic - order only; each entry is a constructed policy instance.
policies: [breaker, timeout, retry, capacity]
```

Each policy is described below.

#### Timeout

Bounds how long the wrapped work may take - not the whole call: an outer distributed policy's coordinator calls are bounded by the client's `commandTimeout`, not by this timer. Local only, there is no distributed timeout.

```ts
import { timeout } from "@gkoos/caracal"

timeout({ ms: 5_000 })
```

If the adapter declares `abort: "supported"`, Caracal cancels the underlying work when the deadline expires. If not, the caller still receives `TimeoutError` on time, but the underlying work may continue until it naturally settles.

#### Retry

Retries adapter-classified failures. Local only, there is no distributed retry.

```ts
import { retry } from "@gkoos/caracal"

retry({
  maxAttempts: 3,
  delay: (attempt) => 100 * 2 ** (attempt - 1) * (0.5 + Math.random() * 0.5),
})
```

`maxAttempts` includes the initial attempt. An attempt is only retried if the adapter classifies the outcome as `retryable` and declares `replay: "safe"`. The delay function receives the attempt number (1 = first retry) and a context carrying the settled outcome, so a protocol-specific helper can pace retries from server feedback. The fetch adapter ships an opt-in `Retry-After` implementation: `retry({ maxAttempts: 3, delay: retryAfterDelay })`. See [fetch](docs/fetch.md#retry-after).

#### Bulkhead

Limits concurrent underlying adapter calls. Available as local (per-process) or distributed (shared across replicas via Redis).

```ts
import { bulkhead } from "@gkoos/caracal"
import { createCoordinationClient, redisCoordinator } from "@gkoos/caracal/redis"

// Local - each process enforces its own limit independently
const localCapacity = bulkhead.local({
  name: "partner-api",
  limit: 10,
  queue: { limit: 50, timeoutMs: 2_000 }, // optional; default is immediate rejection
})

// Distributed - limit is shared across all replicas in the same scope
const sharedCapacity = bulkhead.distributed({
  name: "partner-api",
  coordinator: redisCoordinator(redis, { namespace: "svc:prod" }),
  scope: (ctx) => `region:${String(ctx.metadata.region)}`,
  limit: 20,
  leaseMs: 30_000,
})
```

Permits are held for the duration of the underlying adapter call only, not for the caller's wait or the retry loop. **A distributed bulkhead rejects immediately when full, there is no distributed queue**. On coordinator loss, admission always fails closed - with the Redis coordinator that is a `CoordinatorUnavailableError`, exported from `@gkoos/caracal/redis`.

#### Circuit breaker

Opens when the failure rate in a sliding window exceeds a threshold, blocking further attempts until a probe succeeds. Available as local (per-process) or distributed (shared across replicas via Redis).

```ts
import { circuitBreaker } from "@gkoos/caracal"
import { redisCircuitBreakerCoordinator } from "@gkoos/caracal/redis"

// Local - each process tracks its own failure window independently
const localBreaker = circuitBreaker.local({
  name: "partner-api",
  minimumThroughput: 10,  // minimum observations before the breaker may open
  failureThreshold: 0.5,  // open when ≥ 50% of the window are failures
  openMs: 10_000,         // stay open for 10s before allowing a probe
})

// Distributed - failure window and probe budget are shared across all replicas in the same scope
const sharedBreaker = circuitBreaker.distributed({
  name: "partner-api",
  coordinator: redisCircuitBreakerCoordinator(redis, { namespace: "svc:prod" }),
  scope: (ctx) => `region:${String(ctx.metadata.region)}`,
  minimumThroughput: 20,
  failureThreshold: 0.5,
  openMs: 30_000,
  halfOpenProbes: 3,       // globally bounded concurrent recovery probes per scope
  onCoordinatorError: "fail-open",  // or "fail-closed"
})
```

Place the local circuit breaker outside retry. The breaker then observes one outcome per logical execution - a transient failure that a retry recovers from never counts against the breaker. **A distributed breaker never silently falls back to local state on coordinator loss.**

The `scope` function maps each execution context to a coordination key. Everything sharing that key shares the same failure window and state:

```ts
scope: (ctx) => `region:${String(ctx.metadata.region)}`   // one breaker per region
scope: (ctx) => `tenant:${String(ctx.metadata.tenantId)}` // one breaker per tenant
scope: () => "global"                                     // one breaker for all replicas
```

Use stable, non-secret values, scope keys are observable in the Redis keyspace.

### Events and observability

Every operation accepts an `events` sink - or an array of sinks - that receives structured events from every policy decision. Sinks are output-only and isolated: a synchronous throw and a rejected promise are both dropped, so a failing sink cannot affect execution and an `async` sink is allowed (though never awaited).

```ts
const op = operation({
  name: "partner-api",
  adapter: fetchAdapter(),
  policies: [sharedBreaker, timeout({ ms: 5_000 }), retry({ maxAttempts: 3 }), sharedCapacity],
  events: { emit: (event) => metrics.record(event) },
})
```

The full set can be found in the [Events and observability](docs/events-and-observability.md) section.

## Why not just use X, bro?

There are many libraries for applying resilience policies to operations, but most of them are designed for a single process. Caracal is built for the case where the constraint - capacity, health - belongs to a shared downstream rather than to one replica.

The comparisons below summarise each project's documented scope at the time of writing: they are a snapshot, not a benchmark or a judgement of quality. Check the projects themselves before choosing.

| Library / category | What it covers | Distributed aspect | Caracal difference |
|---|---|---|---|
| Polly-TS | Retry, breaker, timeout, bulkhead, rate limiting, cache, hedge, fallback; HTTP/framework integrations | Redis-backed distributed circuit breaker | Polly-TS adds Redis to one policy in an otherwise local-first, HTTP-centric library. In Caracal the policies coordinate across a user-defined scope - per region, per tenant, or any grouping - and the same model works for HTTP, PostgreSQL, or any async operation. |
| Breakwater | TS resilience pipeline: breaker, retry, timeout, fallback, bulkhead, rate limiter, cache, telemetry | Redis distributed circuit breaker; shared rate quota | Same pattern: Redis is an add-on to a local pipeline. Caracal makes scoped coordination the foundation, not an afterthought. |
| resilience4ts | Broad functional TS fault-tolerance patterns; distributed-first positioning | Distributed locks/cache and planned distributed context; project is small/early | Caracal's scope is narrower and more precise: explicit abort/replay traits, per-scope breaker and bulkhead with documented failure guarantees, and property/fuzz/multi-process integration testing. |
| Cockatiel / Resilience4j / Polly (.NET) | Mature, well-tested composable resilience patterns | Per-process state only | These are excellent libraries for single-process resilience. Caracal exists for the case where the constraint - capacity, health - belongs to a shared downstream, not to one replica. |
| Envoy / Envoy Gateway | Network-level circuit breaking, retries, concurrency limits; global rate limiting as a separate service | Circuit-breaker counters are not synchronised across Envoy processes | Envoy operates at the network layer without application semantics. Caracal works at the call site: it understands abort capability, replay safety, and lets you scope coordination to any application-defined group without a sidecar. |
| Redis semaphore/rate-limit libraries | Individual distributed primitives (semaphores, token buckets) | Shared Redis state | Assembling raw primitives means wiring up admission, leasing, renewal, failure handling, and observability yourself, for each policy. Caracal provides a tested, composed model with consistent semantics across policies. |

## Development

Requires Node.js 20.3+.

```sh
npm install
npm run check     # format, lint, typecheck, build, unit tests
```

### Tests

```sh
npm test                    # unit suite (test/unit) - fast, no external dependencies
npm run test:property       # property suite - fast-check, CARACAL_TEST_SEED for replay
npm run test:fuzz           # fuzz suite - seeded event-history generator
npm run test:integration    # integration suite - requires services and CARACAL_* URLs
npm run test:integration:cluster  # cluster suite - local three-master Valkey cluster (not run in CI)
npm run test:all            # check, then the property, fuzz, and integration suites
```

Integration tests use real Valkey and PostgreSQL via Docker Compose. Each suite skips itself unless its URL is set, so starting the containers alone is not enough:

```sh
npm run redis:up
CARACAL_REDIS_URL=redis://127.0.0.1:6379 npm run test:integration
npm run redis:down
```

See [Testing](docs/testing.md) and [Local development](docs/development.md) for the full test environment setup, seeded replay, and benchmarks.

## Documentation

### Core

- [Core API](docs/core-api.md) - Operation, adapter, policy composition, events
- [Redis foundation](docs/redis.md) - Standalone and cluster clients, key scheme, security, tuning

### Policies

- [Timeout and retry](docs/timeout-and-retry.md) - Cancellation semantics, backoff, replay safety
- [Bulkheads](docs/bulkhead.md) - Local and distributed, lease guarantees, failure modes
- [Circuit breaker](docs/circuit-breaker.md) - State machine, sliding window, distributed coordination
- [Events and observability](docs/events-and-observability.md) - Full event reference, metrics, alerting

### Adapters

- [Fetch adapter](docs/fetch.md) - Cancellation, replay safety, response classification, streaming
- [PostgreSQL adapter](docs/postgres.md) - Cancellation, replay safety, SQLSTATE classification

### Contributing

- [Writing your own adapter](docs/adapter-contracts.md) - The `Adapter` interface, capabilities, and the contract test harness
- [Testing](docs/testing.md) - Test suites, property/fuzz testing, multi-process harness
- [Architecture](docs/architecture.md) - Package structure, subpath exports, tree-shaking rules
- [Local development](docs/development.md) - Setup, scripts, integration environments, benchmarks
