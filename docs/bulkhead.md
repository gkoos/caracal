# Bulkheads

A bulkhead limits the number of concurrent underlying adapter calls. Permits are held for the duration of the adapter promise, not for the caller's wait or the retry loop. A permit is released only after the adapter promise settles, regardless of whether the caller has already received a timeout.

This means if an adapter does not support abort, a caller can receive `TimeoutError` while the adapter call continues running and its permit remains held. The adapter promise boundary is the accounting unit, not proof that the remote server has stopped processing. For the fetch adapter specifically, settlement means response headers have arrived - streaming body consumption is outside the adapter promise. Use a custom adapter if body consumption is the capacity boundary you need.

## Local

Each process enforces its own limit independently. No coordinator or Redis connection is required.

```ts
import { bulkhead, operation } from "@gkoos/caracal"

const capacity = bulkhead.local({
  name: "partner-api",
  limit: 12,
  queue: { limit: 24, timeoutMs: 250 }, // optional; default is immediate rejection
})

const op = operation({ name: "orders", adapter, policies: [capacity] })

capacity.snapshot() // { coordination: "local", occupancy, waiting }
```

Reusing the same policy instance across multiple operations shares the budget between them. Constructing a second instance with the same name creates a completely independent budget.

The optional queue holds waiters in FIFO order up to `queue.limit` waiters (24 in the example above, independent of the `limit` permits). A waiter that times out or is cancelled is removed from the queue. A full queue or elapsed wait rejects with `BulkheadRejectedError`. Waiting consumes no permit.

## Distributed

The concurrency limit is shared across all replicas that share the same coordination identity: `(namespace, policy name, operation name, scope)`.

Unlike the local policy, the distributed policy has no `snapshot()`: occupancy lives in Redis, so reading it would take a coordinator round trip. Use the `bulkhead.admitted` / `bulkhead.released` events, or the scope's coordination key, for visibility.

```ts
import { bulkhead, operation, timeout, retry } from "@gkoos/caracal"
import { createCoordinationClient, redisCoordinator } from "@gkoos/caracal/redis"
import { fetchAdapter } from "@gkoos/caracal/fetch"

const redis = createCoordinationClient(process.env.REDIS_URL!)
await redis.connect()

const capacity = bulkhead.distributed({
  name: "partner-api",
  coordinator: redisCoordinator(redis, { namespace: "svc:prod" }),
  scope: (ctx) => `region:${String(ctx.metadata.region)}`,
  limit: 40,
  leaseMs: 30_000,
})

const op = operation({
  name: "orders",
  adapter: fetchAdapter(),
  policies: [timeout({ ms: 5_000 }), retry({ maxAttempts: 2 }), capacity],
})

await op.execute(
  { url: "https://api.partner.com/orders" },
  { metadata: { region: "eu-west-2" } },
)

redis.disconnect()
```

Two operations sharing the same `capacity` instance have **separate** budgets because their operation names differ. To share a budget across operations, use the same operation name and policy name.

Distributed admission is **immediate-reject only**, there is no distributed queue. All configuration (`limit`, `leaseMs`) must be consistent across all processes using the same identity. Use stable, non-secret names and scopes, they are observable in the Redis keyspace. Empty values and components over 1024 UTF-8 bytes are rejected.

### Scope

The `scope` function maps each execution context to a string key. Everything sharing a key shares the same budget:

```ts
scope: (ctx) => `region:${String(ctx.metadata.region)}`   // per-region limit
scope: (ctx) => `tenant:${String(ctx.metadata.tenantId)}` // per-tenant limit
scope: () => "global"                                     // one shared limit
```

### Lease semantics and failure guarantees

Each admitted permit holds a renewable lease. Caracal renews it at approximately one-third of `leaseMs`. Choose `leaseMs` comfortably larger than the expected p99 latency of the underlying operation. A good starting point is `max(p99 × 3, 30_000)`.

- Under healthy Redis, timely renewal, and consistent configuration, admitted concurrency stays within the configured limit per scope.
- Failed or uncertain admission fails closed with `CoordinatorUnavailableError`. No adapter call is started and no local fallback occurs.
- If lease renewal fails, the lease is marked lost: renewal stops, a `bulkhead.lease-lost` event is emitted, and abort is requested if the adapter supports it. The runtime does not claim the work stopped and does not attempt to reacquire the lease.
- Release is token-checked: an expired or replaced token cannot free a successor's slot.
- Worker death, long GC pauses, network partitions, and Redis restarts can let leases expire while real work continues. A successor may then admit overlapping work. **These leases are not downstream fencing or exactly-once execution. Strict concurrency is not guaranteed through arbitrary failures.**

### Combining multiple bulkheads

When using more than one bulkhead, permits are acquired in array order, not atomically. An earlier permit can be held while a later bulkhead waits or rejects. Keep a consistent acquisition order across all operations to avoid contention, and prefer immediate rejection when combining multiple budgets.

## Events

See [events and observability](events-and-observability.md) for the full event reference. Relevant events: `bulkhead.admitted`, `bulkhead.rejected`, `bulkhead.waited`, `bulkhead.released`, `bulkhead.lease-lost`, `bulkhead.degraded`.
