# Redis foundation

Distributed bulkheads and circuit breakers require a Redis coordinator. If you only use local policies, no Redis connection is needed.

You create **one** Redis client and pass it to both coordinator factories. The factories just wrap the same client with different operation contracts suited to each policy.

```ts
import { createCoordinationClient, redisCoordinator, redisCircuitBreakerCoordinator } from "@gkoos/caracal/redis"

const client = createCoordinationClient(process.env.REDIS_URL!) // commandTimeoutMs defaults to 1000
await client.connect()

// Same client, two coordinator wrappers
const bulkheadCoord = redisCoordinator(client, { namespace: "svc:prod" })
const breakerCoord  = redisCircuitBreakerCoordinator(client, { namespace: "svc:prod" })

// Then pass each to its policy
const capacity = bulkhead.distributed({ coordinator: bulkheadCoord, /* ... */ })
const breaker  = circuitBreaker.distributed({ coordinator: breakerCoord, /* ... */ })

// At shutdown
client.disconnect()
```

The two factories exist because the policies need different things from Redis. The bulkhead coordinator handles acquire/renew/release against a single sorted-set key. The circuit breaker coordinator handles state reads, windowed observations, and probe arbitration across three key types. They implement separate internal interfaces, so they have separate factories, but they share the connection.

## Standalone vs Cluster

Use `createCoordinationClient` for a standalone Redis or Valkey server, and `createCoordinationClusterClient` for a Redis Cluster. Both accept the same coordinator factories and are otherwise interchangeable.

```ts
import {
  createCoordinationClient,
  createCoordinationClusterClient,
} from "@gkoos/caracal/redis"

// Standalone
const client = createCoordinationClient("redis://redis.internal:6379")

// Cluster - provide at least one seed node; ioredis discovers the rest
const client = createCoordinationClusterClient([
  { host: "redis-node-1.internal", port: 7000 },
  { host: "redis-node-2.internal", port: 7001 },
  { host: "redis-node-3.internal", port: 7002 },
])
```

All coordination keys use a hash-tag (`{identity}`) so that every key for a given policy+operation+scope lands on the same cluster slot. The multi-key Lua scripts used by the circuit breaker (which accesses the state hash, observations sorted set, and probe sorted set in a single script) are therefore cluster-safe: all three keys share the same hash-tag and are guaranteed to be on the same slot.

Sentinel failover is not supported. Both clients disable offline queueing and command replay. A command timeout is an **unknown** outcome - the command may have executed on the server before the timeout was observed locally. Never interpret a coordinator error as confirmation that an operation was denied or admitted.

Supported servers: **Redis 7+** and **Valkey 8+** for both standalone and cluster topologies.

## Key scheme

Every coordination slot is keyed by `caracal:v1:{sha256(JSON([namespace, policy, operation, scope]))}:suffix`. The SHA-256 hash provides collision-resistant isolation; the Redis hash-tag `{...}` keeps related keys on the same cluster slot. Suffix distinguishes the per-policy key types:

| Suffix | Policy | Purpose |
|--------|--------|---------|
| `leases` | bulkhead | Active permit sorted set |
| `breaker` | circuit breaker | State hash (`state`, `generation`, `openedAt`, …) |
| `observations` | circuit breaker | Sliding observation window sorted set |
| `probes` | circuit breaker | Half-open probe token sorted set |

Empty identities and components over 1 024 UTF-8 bytes are rejected. The SHA-256 pre-image includes the raw namespace/policy/operation/scope strings; **these are not anonymised**, treat them as potentially observable in Redis keyspace.

## Lua scripts and atomicity

All state transitions are single Lua scripts using `redis.call('TIME')` for server-side timestamps.

Scripts are sent with `EVALSHA` - a 40-byte SHA1 - once the server has the body cached, and fall back to `EVAL` with the full body otherwise. The body therefore goes out only on a cache miss: a server restart, a `SCRIPT FLUSH`, or (Redis 7.4 and later) LRU eviction of the script cache under memory pressure. The first call after a miss pays one `NOSCRIPT` rejection and re-registers the script.

That retry is safe. Redis rejects an unknown SHA1 before executing anything, so the outcome of the failed command is known - unlike a command timeout, which is never replayed.

The saving is worth having because the bodies are not small. Measured with `npm run bench` against a local Valkey, per call:

| Script | Lua body | Request bytes | Server cost |
|--------|----------|---------------|-------------|
| `breakerObserveV1` | 1 937 B | 2 272 B with `EVAL` → 376 B with `EVALSHA` (-83 %) | 30.6 µs → 29.2 µs |
| `bulkheadLeaseV1` | 868 B | 1 040 B with `EVAL` → 214 B with `EVALSHA` (-79 %) | 18.2 µs → 17.5 µs |

`breakerAdmitProbeV1` (~1.7 kB) and `breakerSettleProbeV1` (~1.8 kB) sit between those two. Client latency is unchanged on a same-host connection - the win is bandwidth (a breaker doing 20 000 observations/s per client drops from roughly 42 MB/s to 8 MB/s of request traffic) plus the server CPU spent hashing and copying the body on every `EVAL`. Server cost above includes the script's own work, so the transport share is small but non-zero. Run `node scripts/bench.mjs` with a local Valkey to reproduce, or to check a different deployment.

## Security guidance

### Authentication and encryption

Always configure Redis authentication (`requirepass` / ACLs) and TLS in production.

```ts
const client = createCoordinationClient("rediss://username:password@redis.internal:6380")
```

Use `rediss://` (double-s) for TLS. Verify the server certificate; disable `rejectUnauthorized` only in controlled environments. Prefer ACL-based authentication over `requirepass` in Redis 6+.

### ACL recommendations

Grant the application user the minimum permissions needed. The namespace prefix provides logical isolation, but ACLs provide hard isolation.

The full set of commands required by the coordination scripts is:

| Command | Used by | Notes |
|---|---|---|
| `EVALSHA` | all coordinators | **the steady-state call**: every coordination operation sends the script by SHA1 |
| `EVAL` | all coordinators | fallback that sends the full Lua body when the server has no cached copy (restart, `SCRIPT FLUSH`, eviction) |
| `TIME` | all Lua scripts | server-side timestamp; **first call in every script** — blocking it aborts all coordination |
| `HMGET` | breaker scripts; `readState` direct call | reads state hash fields |
| `HSET` | breaker scripts | writes state hash fields |
| `ZADD` | bulkhead and breaker scripts | adds lease/observation/probe entries |
| `ZREM` | bulkhead and breaker scripts | removes lease/probe entries |
| `ZCARD` | bulkhead and breaker scripts | counts active entries |
| `ZSCORE` | bulkhead `leaseV1`; breaker `breakerSettleProbeV1` | detects an existing lease token; reads a probe token's deadline |
| `ZREVRANGE` | bulkhead `leaseV1` | reads max-score entry to set key expiry |
| `ZREMRANGEBYSCORE` | all Lua scripts | prunes expired entries from sorted sets |
| `ZRANGE` | breaker `breakerObserveV1` | reads window entries for threshold calculation |
| `ZREMRANGEBYRANK` | breaker `breakerObserveV1` | caps observation window at `windowSize` |
| `PEXPIRE` | breaker scripts | sets cleanup TTL on CLOSED-state hash |
| `PEXPIREAT` | bulkhead and breaker scripts | sets absolute expiry on lease/probe sorted sets |
| `PERSIST` | breaker scripts | removes TTL from OPEN/HALF_OPEN state hash so it is never silently expired |
| `EXISTS` | breaker `breakerObserveV1` | checks whether the state hash and observation set exist |
| `DEL` | breaker `breakerAdmitProbeV1` / `breakerSettleProbeV1` | clears the probe set when a recovery window ends |
| `PING` | client health checks only | `client.ping()`; not used by the coordinators themselves |

```
ACL SETUSER caracal-prod on >strongpassword ~caracal:v1:* \
  +EVAL +EVALSHA \
  +TIME \
  +HMGET +HSET \
  +ZADD +ZREM +ZCARD +ZSCORE \
  +ZREVRANGE +ZRANGE +ZREMRANGEBYSCORE +ZREMRANGEBYRANK \
  +PEXPIRE +PEXPIREAT +PERSIST \
  +EXISTS +DEL \
  +PING
```

> **Note:** every command above is load-bearing. `EVALSHA` carries the script and `TIME` is its first inner call, so denying either turns every coordinator operation into a permission error. With the default `onCoordinatorError: "fail-open"` that makes both the bulkhead and the circuit breaker silently inoperative — all attempts are admitted regardless of state. For a scope last seen OPEN or HALF_OPEN it is worse: those failures fail closed, and the state hash is persistent, so the scope keeps rejecting traffic.
>
> A permission error inside a script does **not** roll back what the script already did. Deny `DEL` and the OPEN→HALF_OPEN or →CLOSED transition is still applied while the call itself fails: the caller sees a coordinator error and that request is rejected, the probe set is never cleared, and the recovery window that should have started clean inherits stale tokens.
>
> Client libraries issue a few more commands around the connection. ioredis sends `INFO` for its ready check and `CLIENT SETINFO` on connect; both are denied by the grant above and both are non-fatal (the ready check is skipped with a warning). Grant `+INFO`/`+CLIENT` to silence that, or set `enableReadyCheck: false`. `PING` is listed because readiness probes and `client.ping()` are common; drop it if nothing health-checks the connection.
>
> The ACL integration suite (`test/integration/redis-acl.integration.test.ts`) runs the complete lifecycle with exactly this grant, so a command the implementation starts using without a matching row here fails that test by name.

### Key namespace isolation

Use a namespace that includes the service name and environment to prevent cross-deployment state collisions:

```ts
redisCoordinator(client, { namespace: "orders-svc:prod" })
redisCircuitBreakerCoordinator(client, { namespace: "orders-svc:prod" })
```

Separate namespaces for separate environments (`prod`, `staging`, `dev`) are strongly recommended. If multiple services share a Redis instance, use distinct namespaces per service.

## Key cardinality

Each unique combination of `(namespace, policyName, operationName, scope)` creates up to 4 Redis keys. Key count grows linearly with scope cardinality. Guidelines:

- **Bulkhead:** One `leases` sorted-set key per active `(policy, operation, scope)` triple. Keys expire after the last live lease deadline. Low cardinality is safe.
- **Circuit breaker:** Three keys per active scope: the state hash, the observations sorted set, and the probe set. The hash is created by the first observation and records which epoch the window belongs to; a missing hash = CLOSED. While the scope is OPEN or HALF_OPEN the hash is kept without a TTL; once CLOSED it expires after `max(openMs × 2, windowTtlMs)`, so cleanup cannot outlive the observations that reference its epoch. Observations are pruned by `windowTtlMs` and capped at `windowSize` entries; the probe set is discarded whenever a recovery window ends. High-cardinality scopes (e.g., one scope per user) create proportionally many Redis keys - design scope functions with bounded cardinality.

Use stable, bounded scope values. Avoid high-cardinality identifiers (user IDs, request IDs, trace IDs) as scope keys unless you have explicitly bounded the number of active scopes. The SHA-256 hashing does not reduce key count; it only prevents key collisions.

Redis is not the only place scope cardinality shows up. Each process also retains, in memory, the last **non-closed** state per `(operation, scope)` so it can decide fail-open versus fail-closed during a coordinator outage. CLOSED scopes are discarded, so that memory is proportional to the scopes currently believed OPEN or HALF_OPEN - not to every scope ever observed.

## Lease tuning

### Bulkhead `leaseMs`

`leaseMs` must be longer than the expected p99 underlying operation duration. The lease is renewed every `leaseMs / 3`. A renewal that receives no reply (command timeout) triggers lease-loss and requests supported abort.

- Too short: spurious lease-loss under slow operations or Redis latency.
- Too long: dead workers hold permits for longer before TTL recovery.

A conservative starting point: `leaseMs = max(expected_p99 × 3, 30_000)`.

### Circuit breaker `openMs`, `probeLeaseTtlMs`, `windowTtlMs`

| Parameter | Purpose | Conservative default |
|-----------|---------|---------------------|
| `openMs` | How long the breaker stays OPEN before allowing probes; also a floor for the CLOSED cleanup TTL | `30_000` ms |
| `probeLeaseTtlMs` | How long a probe token lives before expiring (allows dead-worker recovery) | `openMs × 2` |
| `windowTtlMs` | How long individual observations are retained; also a floor for the CLOSED cleanup TTL | `max(openMs × 3, 60_000)` |

`probeLeaseTtlMs` must exceed the time a slow probe might take to settle. If probes run behind a `timeout()` policy, `probeLeaseTtlMs > timeoutMs` is a safe bound. It is enforced, not just advisory: the token's deadline is checked atomically when the probe settles, and a result that arrives late is dropped as stale - its slot had already been recoverable, so counting it would let a dead probe close or re-open the breaker.

The state hash is what remembers which epoch the retained observations belong to, so its CLOSED cleanup TTL is `max(openMs × 2, windowTtlMs)` rather than `openMs × 2`. You do not normally see this: it only matters that the state is never expired while its window can still be counted. If it is lost anyway (eviction policy, admin cleanup), the next observation starts a fresh epoch and the retained members stop counting - see [circuit breaker state storage](circuit-breaker.md#generations-epochs-and-stale-result-rejection).

### Keeping the breaker knobs consistent

The defaults are a consistent set, but every override has to stay in line with the others: the window counters decide *when* the breaker opens, and the probe lease decides how the recovery window behaves. Caracal validates each value on its own (positive integers, threshold in range) and deliberately does **not** reject inconsistent combinations - the right values depend on your traffic rate and your downstream latency.

| Constraint | Why it matters |
|---|---|
| `windowSize >= minimumThroughput` | the count cap trims the window, so a smaller `windowSize` keeps the observed total below the opening threshold and the breaker **never opens** |
| `windowTtlMs` long enough to accumulate `minimumThroughput` observations | time-based pruning is a backstop for idle scopes; if it fires first the window never fills and the breaker again **never opens**. The `max(openMs × 3, 60_000)` default is a proxy for this, not a measurement |
| `failureThreshold >= 1 / minimumThroughput` | below that, a single failure inside a full window already satisfies the ratio, so the breaker can open on one bad call |
| `probeLeaseTtlMs > timeoutMs`, and above the slowest probe settle | the lease must outlive the probe. Too short and a live token expires mid-probe: the slot is re-issued, more than `halfOpenProbes` probes run concurrently, and the settle from the first one is dropped as stale |
| `probeLeaseTtlMs` as an upper bound | it is the worst case a HALF_OPEN window stalls when crashed workers hold every slot. Do not make it arbitrarily large |
| `halfOpenSuccesses` small relative to the probe rate | any probe failure resets progress back to OPEN, so a large target with rare probes keeps traffic throttled long after the downstream recovered |

`probeLeaseTtlMs <= openMs` is *not* required: a recovery window discards its probe tokens when it ends, so leases never leak into the next window. The `openMs × 2` default simply means "comfortably longer than a probe".

Symptoms point at the constraint that is violated:

| Symptom | Usually means | Fix |
|---|---|---|
| Never opens despite steady failures | `windowSize < minimumThroughput`, or `windowTtlMs` too short for the traffic rate | raise `windowSize` and/or `windowTtlMs` |
| Opens on a handful of failures | `minimumThroughput` too low, or `failureThreshold < 1 / minimumThroughput` | raise both |
| Opens → HALF_OPEN → opens again, repeatedly | `halfOpenSuccesses` too high for the probe rate, or `probeLeaseTtlMs` too short so probes overlap | lower `halfOpenSuccesses`, raise `probeLeaseTtlMs` |
| Traffic stays rejected after the downstream recovered | every probe slot held by a crashed worker with a long lease, or `halfOpenSuccesses` unreachable at the current probe rate | shorten `probeLeaseTtlMs` (stay above `timeoutMs`), lower `halfOpenSuccesses`, consider more `halfOpenProbes` |
| More concurrent probes than `halfOpenProbes` | `probeLeaseTtlMs` shorter than the probe duration | raise `probeLeaseTtlMs` |

A consistent starting point for a scope doing ~500 calls/s behind a 5 s `timeout()` with a 2 s p99 downstream:

```ts
circuitBreaker.distributed({
  name: "payments",
  coordinator,
  scope: (ctx) => ctx.tenant,
  minimumThroughput: 20, // ~40 ms of traffic at 500/s: the window fills quickly
  failureThreshold: 0.5, // 10+ failures inside the window opens it
  windowSize: 100, // >= minimumThroughput
  windowTtlMs: 90_000, // >= the time needed to accumulate 20 observations
  openMs: 30_000, // probe once 30 s have passed
  probeLeaseTtlMs: 60_000, // > 5 s timeout and > 2 s p99: a probe cannot outlive its lease
  halfOpenProbes: 1, // one probe at a time is the easiest to reason about
  halfOpenSuccesses: 2, // two successes close it again
})
```

Closing the breaker starts a new epoch for the observation window, so a breaker that just closed does not immediately reopen from the failures that opened it, and cleanup cannot bring those failures back: the state hash outlives the window it governs, and if it is lost anyway the next observation mints a fresh epoch instead of reusing one the retained members would match.

## Production monitoring

### Metrics

See [events and observability](events-and-observability.md) for the full event reference and suggested metric names. Avoid using `scope` as a metric label if it has unbounded cardinality, aggregate or drop it in that case.

### Alerts

- **Sustained `bulkhead.rejected`** with reason `capacity`: the limit is too low for current load, or underlying work is slower than expected.
- **`bulkhead.lease-lost` or `bulkhead.degraded`**: Redis latency or a network partition is affecting renewal. Check Redis latency and the `commandTimeoutMs` setting.
- **`breaker.state-changed` to `open`**: the dependency's failure rate breached `failureThreshold`. Investigate the dependency before assuming the breaker will recover.
- **Repeated `breaker.state-changed` open↔half-open** without closing: probes are consistently failing. The dependency may not have recovered.
- **`breaker.coordinator-error`**: Redis is unreachable or responding slowly. `fail-open` behavior is in effect if configured; verify the Redis connection.

## Local verification

Run `npm run test:integration:redis`. It starts the disposable Valkey service, sets `CARACAL_REDIS_URL`, then runs the integration suite. The service remains running after the suite for inspection.

```sh
npm run test:integration:redis   # starts Valkey, sets CARACAL_REDIS_URL
# or, against an existing test server:
CARACAL_REDIS_URL=redis://127.0.0.1:6379 npm run test:integration
```

Integration suites skip themselves unless `CARACAL_REDIS_URL` is set, so starting the container alone is not enough.

The cluster client has its own suite: `npm run test:integration:cluster` starts a three-master Valkey cluster, sets `CARACAL_REDIS_CLUSTER_URLS`, and runs it. It is a local gate and is not part of CI - see [testing](testing.md).

The multi-process harness forks independent Node processes with IPC barriers, worker kill and freeze controls, and a test-only TCP proxy for Redis traffic manipulation. See [testing](testing.md) for full details.

References: [ioredis connection and command settings](https://github.com/redis/ioredis), [Redis Lua atomicity](https://redis.io/docs/latest/develop/programmability/eval-intro/), [Redis ACL documentation](https://redis.io/docs/latest/operate/oss_and_stack/management/security/acl/).
