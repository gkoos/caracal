# Core API

## Operation and adapter

An `operation` is a named, typed execution unit. It wraps an `adapter` and applies a policy pipeline to every call.

```ts
import { operation, type Adapter } from "@gkoos/caracal"

const adapter: Adapter<{ id: string }, Order> = {
  capabilities: () => ({ abort: "supported", replay: "safe" }),
  execute: async ({ id }, context) => fetchOrder(id, { signal: context.signal }),
}

const orders = operation({ name: "orders", adapter })

const order = await orders.execute({ id: "42" }, {
  metadata: { region: "eu-west-2" },
})
```

### Adapter capabilities

Capabilities are declared per invocation before any policy runs:

| Capability | Values | Meaning |
|---|---|---|
| `abort` | `"supported"` \| `"unsupported"` | Whether Caracal may *generate* cancellation (a timeout firing, a lost distributed lease). The caller's `signal` is always propagated on `context.signal` and can abort the attempt either way |
| `replay` | `"safe"` \| `"unsafe"` \| `"unknown"` | Whether the operation can safely be repeated; must be `"safe"` for retry to issue more than one attempt |

Caracal never infers capabilities. The adapter must declare them accurately.

### ExecutionContext

Every adapter call receives an immutable `ExecutionContext`:

| Field | Type | Description |
|---|---|---|
| `operationName` | `string` | The name passed to `operation()` |
| `executionId` | `string` | UUID identifying this logical execution |
| `attempt` | `number` | Starts at 1; incremented by retry for each subsequent attempt |
| `signal` | `AbortSignal \| undefined` | Combined cancellation signal from timeout and/or the caller |
| `metadata` | `Record<string, unknown>` | Passed through from `execute()` options; used by policy `scope` functions |
| `capabilities` | `OperationCapabilities` | The capabilities declared for this invocation |
| `classify` | `OutcomeClassifier` | The classifier in force for this invocation: the adapter's `classify`, or the default. Call it to classify an outcome exactly as the retry and breaker policies will |

### execute() options

`operation.execute(args, options?)` takes caller-supplied inputs:

| Option | Type | Description |
|---|---|---|
| `metadata` | `Record<string, unknown>` | Passed through to `context.metadata`; the usual input to policy `scope` functions |
| `signal` | `AbortSignal` | Caller cancellation, combined with the timeout signal into `context.signal` |
| `executionId` | `string` | Overrides the generated execution id (the identifier every attempt of this call shares) |

### Outcome classification

After each attempt the adapter's `classify` method (or the default) returns one of:

| Classification | Meaning |
|---|---|
| `"success"` | The attempt succeeded |
| `"failure"` | A non-transient failure; recorded by the breaker, not retried |
| `"retryable"` | A transient failure; retry will attempt again if `replay: "safe"` and attempts remain |
| `"ignored"` | Not recorded by the breaker, does not trigger retry, and releases the half-open probe slot it was admitted with |

Caracal rethrows the adapter's original error unchanged. It does not wrap errors.

## Policy composition

Policies are applied in array order, outermost first. The recommended ordering is:

```ts
const breaker = circuitBreaker.local({
  name: "orders",
  failureThreshold: 0.5,
  openMs: 10_000,
})
const capacity = bulkhead.local({ name: "orders", limit: 10 })

operation({
  name: "orders",
  adapter,
  policies: [breaker, timeout({ ms: 5_000 }), retry({ maxAttempts: 3 }), capacity],
})
```

Bulkheads are always placed directly around the adapter regardless of their position in the array. This ensures permits are held only for the duration of the underlying work - not for the caller's wait or inter-attempt delays - and that every retry attempt is independently admitted. See [bulkheads](bulkhead.md).

## Policies

| Policy | Local | Distributed | Import |
|---|---|---|---|
| `timeout` | ✓ | - | `@gkoos/caracal` |
| `retry` | ✓ | - | `@gkoos/caracal` |
| `bulkhead` | ✓ | ✓ | `@gkoos/caracal` |
| `circuitBreaker` | ✓ | ✓ | `@gkoos/caracal` |

**Timeout**: bounds how long the wrapped work may take; coordinator calls made by an outer distributed policy are bounded separately by the client's `commandTimeout`. Passes a cancelled `AbortSignal` to the adapter if `abort: "supported"`; delivers `TimeoutError` to the caller regardless. See [timeout and retry](timeout-and-retry.md).

```ts
timeout({ ms: 5_000 })
```

**Retry**: retries `retryable` outcomes when `replay: "safe"`. `maxAttempts` includes the initial attempt. `delay` is a number or a function `(attempt, context) => number`, where the context carries the settled outcome (`result`, `error`) plus capabilities and metadata. See [timeout and retry](timeout-and-retry.md).

```ts
retry({
  maxAttempts: 3,
  delay: (attempt) => 100 * 2 ** (attempt - 1) * (0.5 + Math.random() * 0.5),
})
```

**Bulkhead**: limits concurrent underlying adapter calls. Local variant uses an in-process semaphore; distributed variant enforces a shared limit across replicas per scope via Redis. Permits are held for the duration of the adapter call only, not the caller's wait. See [bulkheads](bulkhead.md).

```ts
bulkhead.local({ name: "partner-api", limit: 10 })
bulkhead.distributed({ name: "partner-api", coordinator, scope, limit: 20, leaseMs: 30_000 })
```

**Circuit breaker**: opens when the failure rate in a sliding window exceeds a threshold, blocking further attempts until a probe succeeds. Local variant tracks state per process; distributed variant shares state across replicas per scope via Redis. Place outermost so it observes final outcomes after retries. See [circuit breaker](circuit-breaker.md).

```ts
circuitBreaker.local({ name: "partner-api", failureThreshold: 0.5, openMs: 10_000 })
circuitBreaker.distributed({ name: "partner-api", coordinator, scope, failureThreshold: 0.5, openMs: 30_000, onCoordinatorError: "fail-open" })
```

## Writing a custom policy

A policy is an object with a `name` and an `execute(context, next)` method; calling `next` continues the chain. The type is exported as `Policy`:

```ts
import type { ExecutionContext, Next, Policy } from "@gkoos/caracal"

const tagging: Policy = {
  name: "tagging",
  async execute<Result>(context: ExecutionContext, next: Next<Result>) {
    return await next(context)
  },
}
```

Set `phase: "attempt"` to declare an **attempt-phase** policy. Two independent dimensions decide where a policy sits:

- **`phase`** decides placement relative to the adapter. An attempt-phase policy wraps each individual adapter call and must await the underlying settlement. That is the mechanism behind the rule above - a bulkhead declares it, so it always sits directly around the adapter, whatever its position in the array.
- **Array order** decides nesting between ordinary policies, applied outermost first. A policy without `phase` listed *after* `retry` runs inside it, once per attempt; the same policy listed *before* `retry` wraps the whole retry sequence and sees only its final outcome.

## Policy options

Defaults, bounds and coordination. Options marked *distributed* exist only on distributed policies and *local* only on local ones; unmarked options behave identically in both.

### timeout

| Option | Default | Bounds |
|---|---|---|
| `ms` | required | finite, `> 0`, at most `2147483647` (the largest delay `setTimeout` honours) |

Local only: there is no distributed timeout.

### retry

| Option | Default | Bounds |
|---|---|---|
| `maxAttempts` | required | integer `>= 1` |
| `delay` | `0` (retry immediately) | ms as a number, or `(attempt, context) => number` returning a finite value within `0..2147483647` |

### circuit breaker

| Option | Default (local / distributed) | Bounds | Coordination |
|---|---|---|---|
| `name` | required | non-empty | both |
| `minimumThroughput` | `5` / `20` | integer `>= 1` | both |
| `failureThreshold` | `0.5` | `0.0005 <= t < 0.9995`, resolved to thousandths | both |
| `openMs` | `10_000` / `30_000` | integer `>= 1` | both |
| `halfOpenSuccesses` | `1` / `2` | integer `>= 1` | both |
| `halfOpenProbes` | `1` / `3` | integer `>= 1` | both |
| `windowSize` | `100` | integer `>= 1` | both |
| `classify` | adapter classification | `(error, isSuccess) => BreakerOutcome` (`"success" \| "failure" \| "ignored"`) | both |
| `coordinator` | — | coordinator object | distributed |
| `scope` | — | `(context) => string` | distributed |
| `windowTtlMs` | `max(openMs × 3, 60_000)` | integer `>= 1` | distributed |
| `probeLeaseTtlMs` | `openMs × 2` | integer `>= 1` | distributed |
| `onCoordinatorError` | `"fail-open"` | `"fail-open"` \| `"fail-closed"` | distributed |

### bulkhead

| Option | Default | Bounds | Coordination |
|---|---|---|---|
| `name` | required | non-empty | both |
| `limit` | required | integer `>= 1` | both |
| `queue` | none (reject immediately) | `{ limit: integer >= 1, timeoutMs: 1..2147483647 }` | local |
| `coordinator` | — | coordinator object | distributed |
| `scope` | — | `(context) => string` | distributed |
| `leaseMs` | `30_000` | `100..86400000` | distributed |

Every returned policy carries a `coordination` property (`"local"` or `"distributed"`) alongside the `Policy` members, so a policy can be introspected without knowing which factory built it. `snapshot()` is available on local policies only: `{ coordination, occupancy, waiting }` for the bulkhead, `{ coordination, state, failures, successes, observations, probesInFlight, halfOpenSuccesses }` for the breaker. Distributed occupancy and breaker state live in Redis.

## Errors

| Error | Thrown by | Fields |
|---|---|---|
| `TimeoutError` | `timeout`, when the deadline expires | `timeoutMs` |
| `CircuitOpenError` | `circuitBreaker`, when an attempt is rejected | `policyName`, `coordination`, `scope` |
| `BulkheadRejectedError` | `bulkhead`, when a permit is refused or a wait times out | `coordination`, `policyName`, `scope`, `reason` |
| `CoordinatorUnavailableError` | the Redis coordinators, exported from `@gkoos/caracal/redis` | `coordination` (`"distributed"`), `cause` |

**`BulkheadRejectedError.reason`** is one of `capacity`, `wait-timeout`, `admission-expired`, `lease-lost`. A caller that aborts while queued receives its own abort reason and the `bulkhead.rejected` event reports `cancelled`, so that value only ever appears on events. A failed lease request fails closed by rethrowing the coordinator's own `CoordinatorUnavailableError` instead of constructing this error, so `coordinator-unavailable` is an event reason with no error counterpart. `lease-lost` is delivered as the abort reason when a renewal fails mid-flight, so a caller only observes it when the adapter honours `context.signal`. [Events and observability](events-and-observability.md#bulkhead) lists the `reason` each bulkhead event reports.

## Events

See [events and observability](events-and-observability.md).
