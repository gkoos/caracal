# Timeout and retry

Timeout and retry are local-only policies, neither has a distributed variant or a coordinator.

## Timeout

```ts
import { timeout } from "@gkoos/caracal"

timeout({ ms: 5_000 })
```

Bounds how long the wrapped work may take: the timer starts when the policy runs and the caller is rejected when it expires, whether or not the adapter honours the abort signal. It does not cover coordinator calls an outer distributed policy makes - see [composition](#composition).

If the adapter declares `abort: "unsupported"`, Caracal does not invent cancellation. The caller receives `TimeoutError` on schedule, but the underlying work may continue. Its eventual settlement is still visible through `attempt.settled` events, and any bulkhead permit it holds remains held until the adapter promise actually resolves or rejects.

An internal admission signal separately cancels queued bulkhead waiters and prevents new retry attempts from starting once the timed section has expired, without falsely signaling cancellation to the adapter.

## Retry

```ts
import { retry } from "@gkoos/caracal"

retry({
  maxAttempts: 3,
  delay: (attempt) => 100 * 2 ** (attempt - 1) * (0.5 + Math.random() * 0.5),
})
```

`maxAttempts` includes the initial attempt. The `delay` function receives the attempt number where 1 is the first retry (after the first failed attempt). The function can incorporate jitter directly, as shown above.

`delay` may also be a fixed number. When it is a function it receives a second argument, `RetryContext`, carrying the settled attempt:

| Field | Description |
|---|---|
| `outcome` | The settled attempt as `{ status: "success", value }` or `{ status: "failure", error }` |
| `result` | Convenience: the resolved value, or `undefined` on failure |
| `error` | Convenience: the thrown error, or `undefined` on success |
| `capabilities` | The capabilities declared for this invocation |
| `metadata` | The metadata passed to `execute()` |

The core never interprets `result` or `error`, so a protocol-specific helper can pace retries from server feedback without the core knowing the protocol. The fetch adapter ships one for the HTTP `Retry-After` header: `retry({ maxAttempts: 3, delay: retryAfterDelay })`. See [Retry-After](fetch.md#retry-after).

Retry uses the adapter's `classify` result for both thrown errors and returned values. Only `retryable` outcomes trigger another attempt, and only when `replay: "safe"`. A `failure` classification is visible to the circuit breaker but does not trigger retry. Once `maxAttempts` is reached, the final value or error is returned or rethrown unchanged, Caracal does not wrap it.

Retry stops before starting a new attempt if the caller's signal has been aborted (e.g. timeout expired).

## Composition

Policies are outermost first. Timeout and retry placement determines their relationship:

```ts
// `breaker` and `capacity` are constructed policy instances:
// circuitBreaker.local({ ... }) and bulkhead.local({ ... }).

// Recommended: one budget covers the entire retry sequence
policies: [breaker, timeout({ ms: 10_000 }), retry({ maxAttempts: 3 }), capacity]

// Alternative: a fresh timeout around each individual attempt
policies: [breaker, retry({ maxAttempts: 3 }), timeout({ ms: 3_000 }), capacity]
```

The first is usually what you want: the total time is bounded, and retries consume from the same budget.

### What a timeout does not cover

A `timeout` bounds the work it wraps, not the whole call. With a *distributed* breaker outside it, the caller also waits for the coordinator:

- **before** the timer starts: `readState` and `admitProbe`, each bounded by the client's `commandTimeout` (default 1000 ms);
- **after** the wrapped work settles: `observe`, or `settleProbe` for a half-open probe, same bound.

A 10 ms timeout behind a coordinator with 60 ms round trips therefore reaches the caller in roughly 130 ms. That is the nesting model working as intended - the tightest budget innermost, the shared state machine outside it - but if you need a *total* deadline, add a second `timeout` outermost:

```ts
// Outer timer: the caller's deadline.  Inner timer: one attempt stays short.
policies: [timeout({ ms: 1_000 }), breaker, timeout({ ms: 250 }), retry({ maxAttempts: 3 }), capacity]
```

The outer timer rejects the caller with `TimeoutError` at its deadline even when a coordinator call is what is taking the time. It cannot *cancel* that call: only the caller's own signal is aborted, and only when the adapter declares `abort: "supported"`. The bound on a stuck coordinator call is `commandTimeout`.

## Replay safety

Retry will not issue a second attempt unless the adapter declares `replay: "safe"` for that invocation. An adapter that cannot guarantee idempotency should return `"unsafe"` or `"unknown"`. These are declared per-invocation, so an adapter can vary them per request. For example, the fetch adapter returns `"safe"` for GET and `"unsafe"` for POST.

## Events

See [events and observability](events-and-observability.md) for the full event reference. Relevant events: `timeout.triggered`, `retry.scheduled`, `retry.exhausted`.
