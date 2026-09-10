# Events and observability

Operations emit structured events from every policy decision. Every event carries `at` (Unix timestamp ms), `context` (`ExecutionContext` with `operationName`, `executionId`, `attempt`, `metadata`), and a `type` discriminant.

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

| Event | When | Key fields |
|---|---|---|
| `bulkhead.admitted` | Permit granted | `coordination`, `policyName`, `scope`, `occupancy` |
| `bulkhead.rejected` | Permit denied (full or coordinator unavailable) | `coordination`, `policyName`, `scope`, `reason`, `occupancy` |
| `bulkhead.waited` | Request entered the local queue | `coordination`, `policyName`, `scope` |
| `bulkhead.released` | Permit returned after adapter settles | `coordination`, `policyName`, `scope`, `occupancy` |
| `bulkhead.lease-lost` | Distributed lease could not be renewed | `policyName`, `scope` |
| `bulkhead.degraded` | Coordinator error during an in-flight permit | `policyName`, `scope`, `reason` |

## Circuit breaker

| Event | When | Key fields |
|---|---|---|
| `breaker.state-changed` | Any state transition | `coordination`, `policyName`, `scope`, `state`, `previousState` |
| `breaker.rejected` | Admission blocked (open or half-open probe limit reached) | `coordination`, `policyName`, `scope`, `state` |
| `breaker.observation` | After each non-ignored settled attempt | `coordination`, `policyName`, `scope`, `outcome` |
| `breaker.probe-started` | A half-open probe is admitted | `coordination`, `policyName`, `scope` |
| `breaker.observation-stale` | Distributed: observation arrived for a superseded generation | `policyName`, `scope`, `attemptGeneration`, `currentGeneration` |
| `breaker.coordinator-error` | Redis command failed during admit/observe/settle-probe | `policyName`, `scope`, `operation`, `error` |
| `breaker.degraded` | Coordinator unavailable; fail-open/closed behaviour applied | `policyName`, `scope`, `behavior` |
