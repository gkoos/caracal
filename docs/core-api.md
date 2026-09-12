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
| `abort` | `"supported"` \| `"unsupported"` | Whether Caracal can cancel the underlying work via `AbortSignal` |
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

**Timeout**: bounds how long the caller waits. Passes a cancelled `AbortSignal` to the adapter if `abort: "supported"`; delivers `TimeoutError` to the caller regardless. See [timeout and retry](timeout-and-retry.md).

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

## Events

See [events and observability](events-and-observability.md).
