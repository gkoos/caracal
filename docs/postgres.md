# PostgreSQL adapter

`@gkoos/caracal/postgres` wraps node-postgres (`pg`) 8.x query clients and pools. Install `pg` alongside `@gkoos/caracal` if you haven't already.

```ts
import { Pool } from "pg"
import { circuitBreaker, operation, retry, timeout } from "@gkoos/caracal"
import { postgresAdapter } from "@gkoos/caracal/postgres"

const pool = new Pool({ connectionString: process.env.DATABASE_URL })

const breaker = circuitBreaker.local({
  name: "orders-db",
  minimumThroughput: 10,
  failureThreshold: 0.5,
  openMs: 10_000,
})

const orders = operation({
  name: "orders-db",
  adapter: postgresAdapter(pool),
  policies: [breaker, timeout({ ms: 2_000 }), retry({ maxAttempts: 2 })],
})

const result = await orders.execute({
  sql: "select * from orders where id = $1",
  values: [orderId],
  replay: "safe",
})
```

## Adapter options

```ts
postgresAdapter(pool, {
  replay: "safe",                        // adapter-wide default; overridden per-query
  classifyError: (err) => "retryable",   // override the default SQLSTATE classification
})
```

Both options are optional. `replay` can be a fixed value or a function `(args) => replay` that varies per query.

## Cancellation

The `pg` query contract does not expose a portable `AbortSignal` path, so the adapter declares `abort: "unsupported"`. A timeout still bounds the caller's wait and delivers `TimeoutError` on schedule, but the underlying query may continue running and consuming a pool slot or database resources after the caller has moved on.

When using a `Pool`, the slot is returned to the pool when the query eventually settles. When using a single `Client`, the connection is not released until the query settles, timeouts do not free it.

## Replay safety

The adapter never infers SQL replay safety. It reports `unknown` unless the application explicitly declares `replay` per query or via the adapter-wide option. State-changing SQL should remain `unknown` or `unsafe`.

Retry requires both a `retryable` outcome classification and `replay: "safe"`. A transient SQLSTATE alone is not enough to authorise repeating a query:

- Within an explicit transaction, serialization or deadlock errors require replaying the entire transaction, not just the failed statement.
- A lost connection leaves a write's outcome unknown - the server may have committed before the connection dropped.

## Classification

| Outcome | Classification |
|---|---|
| Successful result | `success` |
| Connection exception SQLSTATEs (`08xxx`) | `retryable` |
| Serialization failure (`40001`) | `retryable` |
| Deadlock (`40P01`) | `retryable` |
| Lock not available (`55P03`) | `retryable` |
| Too many connections (`53300`) | `retryable` |
| Shutdown / recovery (`57P01`–`57P03`) | `retryable` |
| All other errors | `failure` |

`failure` outcomes are visible to the circuit breaker and recorded in events, but do not trigger retry. Override `classifyError` when your application has more specific semantics.

## Running the integration tests

```sh
npm run test:integration:postgres
```

Requires Docker. Starts the PostgreSQL service from `compose.yaml`, runs the suite against `CARACAL_POSTGRES_URL`, and leaves the service running for inspection. Run `npm run postgres:down` to stop it. See [testing](testing.md) for details on what the suite covers.

