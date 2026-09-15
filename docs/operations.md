# Operations

Each `(namespace, policy name, operation name, scope)` is a coordination identity: it has its own Redis keys, coordinator round trips, and its own entry in every process that has seen it. This page collects what that costs and what outlives what. The mechanism is in [Redis foundation](redis.md), per-policy semantics are in [bulkheads](bulkhead.md) and [circuit breaker](circuit-breaker.md).

## What one scope costs

- One identity, up to four Redis keys: `leases` for the bulkhead, and `breaker`, `observations`, `probes` for the circuit breaker. A bulkhead and a breaker sharing a policy name, operation and scope are one identity and share all four.
- Up to `windowSize` members in the window (default 100), plus one probe token per in-flight probe.
- One retained entry per process, per operation, for every scope that has ever been non-closed - each process keeps the last state it saw so it can choose between fail-open and fail-closed during a coordinator outage.
- A key name is `caracal:v1:{<64 hex>}:<suffix>`, 85 to 91 bytes before Redis's own per-key overhead.

That is per scope, and it multiplies: hashing the identity does not reduce key count, it only prevents collisions. Every event also carries `scope`, so a consumer that turns it into a metrics label inherits the same cardinality - keep the label off, or bound the scope.

## Retention

| Key | TTL |
|---|---|
| `leases` | until the last live lease deadline |
| `breaker` (state hash) | none while OPEN or HALF_OPEN; `max(openMs × 2, windowTtlMs)` once CLOSED |
| `observations` | `windowTtlMs` from the last write |
| `probes` | `probeLeaseTtlMs + 1 s` from the last admission |

Leases, observation windows and probe tokens clean themselves up. The state hash is the one that can outlive its traffic indefinitely, and deliberately so: a missing hash reads as CLOSED, so expiring an open breaker would silently admit unrestricted traffic. Removal is traffic-dependent - the key goes away when that same scope is next settled as CLOSED - so a scope that opens once and then goes quiet leaves its hash in Redis, and an entry in the memory of every process that saw it, until something touches that scope again.

Scopes that are one-shot by construction (a per-request key, a tenant that never comes back, a test run) are what accumulate. Deleting a state hash is a manual override: a missing hash reads as CLOSED, and the next observation mints a new epoch, so surviving window members are ignored rather than resurrected.

## Namespace lifetime

The namespace is part of every identity, and breaker state outlives the processes that wrote it. A deployment that reuses a namespace starts by reading the previous deployment's state: a scope left OPEN rejects traffic until a probe succeeds, and the events show `breaker.rejected` with `state: "open"` while this process emitted no `breaker.state-changed` of its own. That reads as a broken dashboard rather than as inherited state. A rejection caused by a coordinator outage reports the same `state: "open"`, so check for a `breaker.coordinator-error` or `breaker.degraded` alongside it before concluding the state was inherited.

Keep the namespace stable across releases of the same service and environment. It is also the coordination boundary: replicas using different namespaces do not share budgets or breaker windows, so a per-release namespace splits the fleet's limit exactly the way per-replica limits do.

Where a clean slate is what you want - test suites, demos, one-shot jobs - scope the namespace to the run (`caracal-demo:<runId>`) and give every participant of that run the same value. [Key namespace isolation](redis.md#key-namespace-isolation) shows the service-and-environment form.

## Per-group coordinators

A coordinator is bound to the policy instance, and a policy instance is fixed when the operation is built. Routing groups to separate Redis instances therefore means one policy instance and one operation per group, with the group chosen by your dispatch:

```ts
// One instance per group, each coordinating against that group's Redis.
const euBreaker = circuitBreaker.distributed({
  name: "orders",
  coordinator: redisCircuitBreakerCoordinator(euClient, { namespace: "orders-svc:prod" }),
  scope: () => "global", // one budget per group; subdivide further if the group needs it
  minimumThroughput: 20,
  failureThreshold: 0.5,
  openMs: 30_000,
})

// ...and the same for usClient. Both feed their own operation.
```

What that buys and what it costs:

- **State never crosses groups**, even with an identical namespace, policy name, operation name and scope - it lives in a different Redis instance. A budget that spans the fleet needs one Redis that every replica can reach.
- **An outage is contained to the group.** Calls routed elsewhere keep coordinating; nothing gives a group a view of the others.
- **Coordination latency is the group's latency**, paid on every attempt: `readState` and `admitProbe` before the wrapped work, `observe` or `settleProbe` after it, each bounded by that client's `commandTimeout`. A coordinator in another region is a WAN round trip around every attempt, and a `timeout()` policy does not cover it - see [what a timeout does not cover](timeout-and-retry.md#what-a-timeout-does-not-cover).
- **The fail-closed memory is per process.** `onCoordinatorError` governs only a failed read with no prior read establishing the scope as non-closed; once a process has seen a scope OPEN or HALF_OPEN, a coordinator outage for that scope fails closed whatever the setting, and a restart empties that memory. Bulkhead admission has no such knob: failed or uncertain admission always fails closed.
- **Events identify a decision by `coordination`, `policyName` and `scope`**, not by the namespace or which coordinator answered, so two groups can emit identical-looking events. Distinguish them by the sink that received them, or by a `metadata` value you pass through.

## Tuning

- `leaseMs`, and the constraints between `openMs`, `probeLeaseTtlMs`, `windowTtlMs`, `minimumThroughput` and `windowSize`: [lease tuning](redis.md#lease-tuning) and [keeping the breaker knobs consistent](redis.md#keeping-the-breaker-knobs-consistent).
- What to watch: [production monitoring](redis.md#production-monitoring) lists the metrics and the alerts worth having.
- Still open by design: leases are not fencing tokens, and a scope is not a rate limit. See [lease semantics and failure guarantees](bulkhead.md#lease-semantics-and-failure-guarantees).

