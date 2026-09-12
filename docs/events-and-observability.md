# Events and observability

Operations emit structured events from every policy decision. Every event carries `at` (Unix timestamp ms), `context` (`ExecutionContext` with `operationName`, `executionId`, `attempt`, `metadata`), and a `type` discriminant.

Local policies report `scope: "process"` on every event: their state is per-process rather than per-caller-scope, so the label is constant. Distributed policies report the resolved scope key.

```ts
const op = operation({
  name: "partner-api",
  adapter: fetchAdapter(),
  policies: [sharedBreaker, timeout({ ms: 5_000 }), retry({ maxAttempts: 3 }), sharedCapacity],
  events: { emit: (event) => metrics.record(event) },
})
```

## Execution lifecycle

| Event | When | Key fields |
|---|---|---|
| `execution.started` | Before the policy pipeline runs | - |
| `execution.settled` | After the pipeline resolves or rejects | `outcome` |
| `attempt.started` | Before each adapter call | - |
| `attempt.settled` | After each adapter call | `outcome`, `classification` |

## Timeout

| Event | When | Key fields |
|---|---|---|
| `timeout.triggered` | When the deadline expires | `timeoutMs`, `abortRequested` |

## Retry

| Event | When | Key fields |
|---|---|---|
| `retry.scheduled` | When a retry is queued | `nextAttempt`, `delayMs`, `outcome`, `classification` |
| `retry.exhausted` | When `maxAttempts` is reached | `outcome`, `classification` |

## Bulkhead

| Event | When | Coordination | Key fields |
|---|---|---|---|
| `bulkhead.admitted` | Permit granted | local | `policyName`, `scope`, `occupancy` |
| `bulkhead.waited` | Request entered the local queue | local | `policyName`, `scope`, `occupancy` |
| `bulkhead.released` | Permit returned after the adapter settles | local | `policyName`, `scope`, `occupancy` |
| `bulkhead.rejected` | Permit denied | both | `policyName`, `scope`, `reason`, `occupancy` |
| `bulkhead.lease-lost` | Distributed lease could not be renewed | distributed | `policyName`, `scope` |
| `bulkhead.degraded` | Coordinator error during an in-flight permit | distributed | `policyName`, `scope`, `reason` |

The distributed policy does not emit `bulkhead.admitted`, `bulkhead.waited` or `bulkhead.released`: occupancy is owned by the coordinator, not by the process.

`reason` values by event:

| Event | `reason` | Meaning |
|---|---|---|
| `bulkhead.rejected` (local) | `capacity` | No permit available and either no queue is configured or it is full |
| `bulkhead.rejected` (local) | `wait-timeout` | Waited in the queue for longer than `queue.timeoutMs` |
| `bulkhead.rejected` (local) | `cancelled` | The caller's signal aborted while waiting |
| `bulkhead.rejected` (distributed) | `capacity` | The coordinator refused a lease; the shared limit is reached |
| `bulkhead.rejected` (distributed) | `coordinator-unavailable` | The lease request failed, so the attempt fails closed |
| `bulkhead.degraded` | `admission-unknown` | The admission result is unknown (the request failed) |
| `bulkhead.degraded` | `lease-uncertain` | A renewal failed while the permit was in flight |
| `bulkhead.degraded` | `release-unknown` | The release of a permit failed |

## Circuit breaker

| Event | When | Key fields |
|---|---|---|
| `breaker.state-changed` | Any state transition | `coordination`, `policyName`, `scope`, `state`, `previousState` |
| `breaker.rejected` | Admission blocked (open or half-open probe limit reached) | `coordination`, `policyName`, `scope`, `state` |
| `breaker.observation` | After each non-ignored settled attempt | `coordination`, `policyName`, `scope`, `outcome` |
| `breaker.probe-started` | A half-open probe is admitted | `coordination`, `policyName`, `scope` |
| `breaker.observation-stale` | Distributed: observation arrived for a superseded generation | `policyName`, `scope`, `attemptGeneration`, `currentGeneration` |
| `breaker.coordinator-error` | Redis command failed during admit/observe/settle-probe | `policyName`, `scope`, `operation`, `error` |
| `breaker.degraded` | Coordinator unavailable; fail-open/closed behaviour applied | `policyName`, `scope`, `reason`, `behavior` |

Distributed breaker events additionally carry `generation`, the epoch the decision belongs to (`breaker.observation-stale` reports `attemptGeneration` and `currentGeneration` instead). Local events do not: the local breaker's generation is process-internal and every result settles in-process, so there is nothing to correlate.

`breaker.degraded` carries `reason: "coordinator-unavailable"` plus `behavior: "fail-open" | "fail-closed"`. `breaker.coordinator-error` carries `operation: "admit" | "observe" | "settle-probe"`.
