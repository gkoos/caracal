# Serval API: configuration, lifecycle and migration

`@gkoos/caracal/serval` is the **opt-in, experimental** entry point for the delegated
coordination strategy described in [Serval](README.md). Nothing in the root package,
`caracal/redis`, or the policy factories changes when you use it: a delegated
coordinator is passed to the policy you already call.

```ts
import { circuitBreaker, operation, rateLimit, bulkhead } from "@gkoos/caracal"
import { createCoordinationClient, redisCircuitBreakerCoordinator } from "@gkoos/caracal/redis"
import {
  createServalBreaker,
  createServalBulkheadCoordinator,
  createServalRateLimitCoordinator,
  redisBulkheadAllocationIssuer,
  redisRateLimitGrantIssuer,
} from "@gkoos/caracal/serval"

const url = process.env.CARACAL_REDIS_URL as string
const namespace = "orders:prod"
const client = createCoordinationClient(url)
await client.connect()

const breaker = createServalBreaker({
  namespace,
  authority: redisCircuitBreakerCoordinator(client, { namespace }),
  freshnessMs: 1_000,
})
await breaker.session.start()

const rate = createServalRateLimitCoordinator({
  authority: redisRateLimitGrantIssuer(client, { namespace }),
  grantSize: 4,
  validForMs: 1_000,
})

const capacity = createServalBulkheadCoordinator({
  authority: redisBulkheadAllocationIssuer(client, { namespace }),
  replicaId: process.env.HOSTNAME ?? "local",
  validityMs: 10_000,
  initialSlots: 1,
})

const charge = operation({
  name: "charge",
  adapter: paymentAdapter,
  policies: [
    circuitBreaker.distributed({
      name: "payments",
      coordinator: breaker.coordinator,
      scope: (ctx) => String(ctx.metadata.tenantId),
    }),
    rateLimit.distributed({
      name: "payments-rate",
      rate: 100,
      burst: 20,
      coordinator: rate,
      scope: (ctx) => String(ctx.metadata.tenantId),
    }),
    bulkhead.distributed({
      name: "payments-capacity",
      limit: 80,
      coordinator: capacity,
      scope: (ctx) => String(ctx.metadata.tenantId),
    }),
    // A local guardrail is composed independently, not implied by the delegated one.
    bulkhead.local({ name: "payments-local", limit: 12 }),
  ],
})
```

A runnable version of that pipeline is `examples/serval-pipeline.ts`:

```bash
npm run redis:up
npm run example:serval
```

## Exports

| Export | Kind | Purpose |
|---|---|---|
| `createServalSession` | function | The per-process session: replica identity, scope registry, bounded authority cache, invalidation subscription and reconciliation |
| `createServalBreaker` | function | Wires a session, its snapshot reader and the strict breaker coordinator into a cached-verdict coordinator, and returns `{ session, coordinator }` |
| `createServalRateLimitCoordinator` | function | A `RateLimitCoordinator` that spends locally held quota |
| `createServalBulkheadCoordinator` | function | A `BulkheadCoordinator` that admits within a locally held slot allocation |
| `redisRateLimitGrantIssuer` | function | The Redis authority a delegated rate limiter draws grants from |
| `redisBulkheadAllocationIssuer` | function | The Redis authority a delegated bulkhead draws allocations from |
| `ServalSessionClosedError` | class | Thrown when a session is used after `close()` |
| `ServalRegistryPressureError` | class | Thrown when the scope registry is full and nothing reclaimable can make room |
| `ServalAuthorityRefusalError` | class | Thrown when the delegated breaker refuses locally because the authority it holds is no longer usable; it extends `CoordinatorRefusalError`, so the policy rejects the call |

Types exported for annotations: `ServalSession`, `ServalSessionOptions`,
`ServalSessionState`, `ServalScopeRegistration`, `ServalBreakerCoordinator`,
`ServalBreakerCoordinatorOptions`, `ServalBreakerOptions`, `ServalBreakerPayload`,
`ServalStaleClosedBehavior`, `ServalRateLimitCoordinator`,
`ServalRateLimitCoordinatorOptions`, `ServalRateLimitMetrics`,
`ServalRateLimitDiagnostics`, `ServalRateGrantState`, `ServalBulkheadCoordinator`,
`ServalBulkheadCoordinatorOptions`, `ServalBulkheadMetrics`,
`ServalBulkheadDiagnostics`, `ServalBulkheadState`, `ServalRelinquishResult`,
`RateLimitGrantIssuer`, `RateGrantRequest`, `RateGrantReply`,
`BulkheadAllocationIssuer`, `BulkheadAllocationRequest`, `BulkheadAllocationReply`,
`ServalAuthorityReader`, `ServalNotificationTransport`,
`ServalInvalidationPublisher` and `ServalMetricsSnapshot`.

## Configuration

Every option has a default; the ones that change behaviour are listed here.

| Option | Where | Default | Effect |
|---|---|---|---|
| `namespace` | all three | required | Key prefix and the isolation boundary between deployments |
| `freshnessMs` | breaker | `1000` | How long a cached CLOSED verdict admits without an authority read |
| `staleClosed` | breaker | `"refresh"` | `"refresh"` reads the authority (and propagates its failure; a reply that cannot be adopted — late, or superseded by a newer verdict the replica holds — is a refusal, not a fresh verdict), `"fail-open"` admits from the cache, `"fail-closed"` refuses |
| `reconciliationIntervalMs` | session | `10000` | Periodic convergence, independent of notifications |
| `registryCapacity` | session | `1024` | Bounded scope registry; a full registry refuses rather than evicting live authority |
| `grantSize` | rate limiter | the identity's burst | Permits per issuance: larger means fewer round trips and more stranded capacity |
| `validForMs` | rate limiter | `1000` | The spend horizon `V`; admissions in `L` are bounded by `R * (L + V) + B` |
| `maximumOutstandingPermits` | rate limiter | unset | Fleet-wide cap on issued-but-unspent permits, for high-replica fleets |
| `validityMs` | bulkhead | `10000` | How long an allocation stays valid before it must be renewed |
| `graceMs` | bulkhead | `10000` | How long the server holds an allocation's slots after its validity ends, so a lost reply is reconcilable |
| `initialSlots` / `maximumSlots` | bulkhead | `1` / the policy limit | Conservative start, demand-driven growth. The ceiling bounds every allocation, including the first, so a start above a configured ceiling is refused at construction |
| `redistributionIntervalMs` | bulkhead | `100` | Shortest gap between two redistribution attempts for one scope |

## Lifecycle

```text
create -> start() -> serving -> close()
                        |            +-- registrations released, allocations returned
                        +-- reconciliation, invalidations, renewal and redistribution
```

- `session.start()` subscribes to invalidations **before** the cold-start read, then
  reads a snapshot. Until it adopts an authority incarnation, a delegated coordinator
  reads the authority per admission rather than caching. Only the breaker holds a session, so only it
  reconciles a snapshot; the rate limiter and bulkhead converge through expiry and their own
  renewals and allocation replies.
- An allocation is renewed before its validity ends, on the admission path, so traffic that never
  empties the pool still renews. The nudge is a background modification: the admission itself is
  served from the local pool, and repeated nudges join the renewal already in flight.
- Serving never blocks on a notification: hints trigger a coalesced refresh, and
  periodic reconciliation, expiry and reconnect snapshots provide convergence without
  them.
- `capacity.close()` stops admitting, returns every *free* slot (holders keep theirs)
  and awaits in-flight modifications. Closure wins over an admission that was already in flight, and
  a second pass returns capacity that arrived while the first was closing. `session.close()` releases
  the registry and the cache, and a closed session refuses every admission from then on.
  A rate limiter needs no relinquishment: unused permits simply expire.
- Shutdown never depends on a clean handoff. A slot that could not be returned is
  reclaimed by the server when its reservation lapses.

## Approximate guarantees

| Guarantee | Bound |
|---|---|
| No double allocation while authority history is intact | The server reserves outstanding quotas and slots atomically, and deduplicates ambiguous retries by `operationId` |
| No admission beyond granted authority | An admission is served only from locally held, unexpired, unfrozen authority; the lab measures tracked occupancy against the sum of the local ceilings |
| Rolling rate envelope | `admissions(L) <= R * (L + V) + B` under intact authority history |
| Bounded local state | One registry entry per scope per policy; the registry refuses rather than evicting live accounting |
| Convergence | A cached verdict converges through reconciliation, invalidation or expiry, with or without notifications; an allocation converges through expiry and its own modifications. Only the breaker reconciles a snapshot |

**Explicit deviations**, all measured in [the fault lab](benchmarks.md):

- Delegated rate limiting is not exact GCRA, and a slow dependency exhausts local stock
  earlier than the strict coordinator would refuse.
- Tracked occupancy is not physical concurrency: an abandoned attempt whose adapter
  ignores cancellation keeps its permit until it settles, and capacity reclaimed after
  an allocation lapses can be reallocated while old holders still run.
- A cold start without Redis admits nothing, because no replica holds a verdict, a grant
  or an allocation yet and every path has to read before it can serve. A warm process
  keeps admitting from the authority it holds, and a failed observation or settlement is
  reported as degradation rather than failing the call.
- A rollback or restore that loses acknowledged issuance can permit overlapping grants;
  that is a documented failure-model deviation, not a bounded guarantee.


## Failure behaviour

| Situation | Breaker | Rate limiter | Bulkhead |
|---|---|---|---|
| Authority unreachable, authority held | Admits from a valid cached verdict; settlement reports degradation | Spends remaining stock within its horizon | Admits within its valid allocation |
| Authority unreachable, no authority | `"refresh"` propagates the failure, `"fail-open"` admits, `"fail-closed"` refuses | Rejects: no permit is invented | Rejects: no slot is invented |
| Cached verdict stale and CLOSED | Follows `staleClosed` | - | - |
| Reply the session refused (late, or superseded by a newer verdict) | Refuses the call: the cached verdict answers instead, so a protective OPEN or HALF_OPEN stands | - | - |
| Allocation or grant expired | Cached OPEN stays protective | Refuses new admissions, keeps holders | Refuses new admissions, keeps holders |
| Ambiguous modification | - | Retries the same `operationId` | Retries the same `operationId`; frozen slots stay frozen |
| Registry full | Refuses the call: the coordinator reports the registry's refusal as its own | Refuses the new scope | Refuses the new scope |
| Session closed | Refuses the call | - | - |

## Errors

| Error | Thrown by | Meaning |
|---|---|---|
| `ServalSessionClosedError` | session and the policies using it | The session was closed, so no new authority may be acquired |
| `ServalRegistryPressureError` | the rate-limit and bulkhead coordinators; the breaker reports the same refusal as `ServalAuthorityRefusalError` with this as its `cause` | The scope registry is full and no entry could be reclaimed without dropping live accounting |
| `ServalAuthorityRefusalError` | breaker | A local refusal because no usable authority exists: `staleClosed: "fail-closed"`, a reply the session could not adopt (late, or superseded by a newer verdict), a closed session, or a registry that cannot hold the scope. It extends `CoordinatorRefusalError`, so the policy rejects the call instead of applying `onCoordinatorError` |

Admission refusals are not errors from this entry point: a rate or bulkhead refusal is
reported by the policy as `RateLimitExceededError` or `BulkheadRejectedError`, exactly
as it is for a strict coordinator, and a breaker refusal is `CircuitOpenError`.

## Metrics

`session.metrics()` returns counters plus a bounded per-scope view.
`rate.metrics()` and `capacity.metrics()` return their own counters, and
`capacity.diagnostics(identity)` and `breaker.coordinator.cached(identity)` expose the
local state for diagnostics and tests.

The numbers worth alerting on:

| Signal | Source | Why |
|---|---|---|
| `registryRefusals` | session, rate, bulkhead | Scope cardinality outgrew the bound; accounting is being refused, not dropped |
| `invalidationsMalformed` | session | A publisher and a subscriber disagree about the protocol |
| `reconciliationFailures` | session | The authority is unreachable; cached authority ages while it persists |
| `permitsLost` / `modificationFailures` | rate, bulkhead | Replies arrived too late to adopt, or a modification outcome is unknown |
| `frozenPendingReturn` | bulkhead diagnostics | Capacity is frozen for a return that has not been confirmed |
| `contended` / `localRefusals` | rate, bulkhead | Callers are losing the race for local capacity: the earliest sign of an undersized grant |

## Migration from the strict coordinators

1. **Keep the strict coordinators.** They remain the right choice when approximation is
   unacceptable; nothing here deprecates them.
2. **Add the delegated layers per policy**, one at a time, starting with the one whose
   read cost dominates. The composition suite in `test/serval/integration/` is the model:
   local guardrail plus delegated layer, then all three.
3. **Do not mix paths for one identity.** A delegated rate limiter draws on the same
   bucket the strict one uses, so mixing is safe but not additive; a delegated bulkhead
   uses its own keys and must not share an identity with a strict one.
4. **Keep a local guardrail on paths that must hold a per-process ceiling.** Local
   limits can strand capacity, and autoscaling changes the aggregate bound.
5. **Re-measure.** `npm run bench:serval` compares the profiles on your machine; the
   published numbers are a starting point, not a promise.

## Known limitations

- The strategy is experimental: this API may change in a minor release until multi-seed,
  real-process evidence supports stability.
- It is approximate by construction. Read [the fault lab](benchmarks.md) before
  relying on it for a hard guarantee.
- The lab runs on one machine with a loopback dependency, a closed loop per replica and
  short cells: it compares profiles, it does not certify steady-state behaviour.
- Async or batched outcome settlement is not implemented. The awaited observation write
  is why the cost reduction is bounded rather than total.

