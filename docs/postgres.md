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

The adapter never infers SQL replay safety. It reports `unknown` unless the application explicitly declares `replay` per query or via the adapter-wide option. State-changing SQL should remain `unknown` or `unsafe` unless the application has established that repeating the specific write is safe, as in the example below.

Retry requires both a `retryable` outcome classification and `replay: "safe"`. A transient SQLSTATE alone is not enough to authorise repeating a query:

- Within an explicit transaction, serialization or deadlock errors require replaying the entire transaction, not just the failed statement.
- A lost connection leaves a write's outcome unknown - the server may have committed before the connection dropped.

### Writes protected by a unique request-id

A database constraint can make a specific write safe to repeat. Here the request ID identifies an immutable order intent, and the order row itself is the deduplication record:

```sql
create table order_requests (
  request_id text primary key,
  sku text not null,
  quantity integer not null check (quantity > 0)
);
```

Use a single autocommit statement with `ON CONFLICT DO NOTHING`. The constraint prevents duplicate order rows, including when the first insert committed but its response was lost. This example assumes rows are retained and immutable and there are no additional effects in triggers or functions.

```ts
import { Pool } from "pg"
import { operation, retry } from "@gkoos/caracal"
import { postgresAdapter } from "@gkoos/caracal/postgres"

const pool = new Pool({ connectionString: process.env.DATABASE_URL })
const orders = operation({
  name: "order-requests-db",
  adapter: postgresAdapter(pool),
  policies: [retry({ maxAttempts: 3, delay: 100 })],
})

export async function ensureOrder(request: {
  requestId: string
  sku: string
  quantity: number
}) {
  // Persist this identity and payload before the first call; reuse on retries.
  await orders.execute({
    sql: `insert into order_requests (request_id, sku, quantity)
          values ($1, $2, $3)
          on conflict (request_id) do nothing`,
    values: [request.requestId, request.sku, request.quantity],
    replay: "safe", // This statement is protected by the primary key.
  })

  // Read in a separate statement on the primary after the insert settles.
  // Declare replay for the read too: the adapter never infers it from SQL.
  const result = await orders.execute({
    sql: "select request_id, sku, quantity from order_requests where request_id = $1",
    values: [request.requestId],
    replay: "safe",
  })
  const stored = result.rows[0]
  if (stored === undefined) throw new Error("Order outcome could not be recovered")
  if (stored.sku !== request.sku || stored.quantity !== request.quantity) {
    throw new Error("Request ID was already used for a different order")
  }
  return stored
}
```

`DO NOTHING` can succeed without inserting a row; adding `RETURNING` would not return the existing row on that path. The separate read recovers the order and checks that the key was not reused with a different payload. See PostgreSQL's [`INSERT` reference](https://www.postgresql.org/docs/current/sql-insert.html) and [Read Committed behaviour](https://www.postgresql.org/docs/current/transaction-iso.html#XACT-READ-COMMITTED).

The guarantee covers this row creation, not later actions such as sending an email or charging a customer. Additional database changes must be committed atomically with their deduplication record. If a logical write spans multiple statements, use an application-managed transaction and a custom adapter that retries the whole transaction where safe. A Redis key checked separately from the SQL write does not provide that atomicity. Keep `replay` conservative for other writes, retain the request record for the retry lifetime, and recover by the same request ID if retries exhaust. See [who provides idempotency](timeout-and-retry.md#who-provides-idempotency).

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

