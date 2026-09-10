# Fetch adapter

`caracal/fetch` wraps the Web Fetch API and targets server-side runtimes - Node.js 20+, Deno, and Bun. It works with any spec-compliant `fetch` implementation, which you can inject via the adapter options. **Caracal is not a browser library.** The distributed policies coordinate through Redis, which must stay server-side, and per-tab local policies would not share state.

```ts
import { circuitBreaker, operation, retry, timeout } from "caracal"
import { fetchAdapter } from "caracal/fetch"

const breaker = circuitBreaker.local({
  name: "partner-api",
  failureThreshold: 0.5,
  openMs: 10_000,
})

const api = operation({
  name: "partner-api",
  adapter: fetchAdapter(),
  policies: [breaker, timeout({ ms: 5_000 }), retry({ maxAttempts: 3 })],
})

const response = await api.execute(
  { url: "https://api.partner.com/orders/42" },
  { metadata: { region: "eu-west-2" } },
)
```

## Adapter options

```ts
fetchAdapter({
  fetch: customFetch,                                              // defaults to globalThis.fetch
  replay: "safe",                                                  // adapter-wide override of per-method inference
  classifyResponse: (res) => (res.status >= 500 ? "retryable" : "success"),
  classifyError: (err) => "retryable",
})
```

All options are optional.

## Request arguments

Each `execute` call takes a `FetchOperationArgs` object:

| Field | Type | Description |
|---|---|---|
| `url` | `string \| URL \| Request` | The request target |
| `options` | `RequestInit` (optional) | Method, headers, body, signal, etc. |

```ts
// Simple GET
api.execute({ url: "https://api.example.com/orders" })

// POST with body
api.execute({
  url: "https://api.example.com/orders",
  options: {
    method: "POST",
    body: JSON.stringify(order),
    headers: { "Content-Type": "application/json" },
  },
})

// Pre-built Request object
api.execute({ url: new Request("https://api.example.com/orders") })
```

## Cancellation

The fetch adapter declares `abort: "supported"`. When a timeout expires or the caller's signal is aborted, Caracal combines all relevant signals using `AbortSignal.any` and passes the combined signal to `fetch`. The three signal sources are:

1. A signal embedded in a `Request` object passed as `url`
2. `options.signal`
3. Caracal's timeout or caller abort signal

All three are merged, none overrides the others.

## Replay safety

The adapter infers replay safety from the HTTP method unless an adapter-wide `replay` option is provided:

| Method | Default `replay` |
|---|---|
| `GET`, `HEAD` | `"safe"` - retry is permitted |
| `POST`, `PATCH` | `"unsafe"` - retry is blocked |
| `PUT`, `DELETE`, and others | `"unknown"` - retry is blocked |

Set `replay` as a fixed value to override globally, or as a function `(args) => replay` to vary it per request, for example to mark a specific idempotent `POST` as `"safe"`.

## Classification

Network errors and thrown exceptions (including aborts) are classified as `retryable` by default. Successful responses where `fetch` resolves are classified by status code:

| Status | Default classification |
|---|---|
| `5xx` | `retryable` |
| `408` Request Timeout | `retryable` |
| `429` Too Many Requests | `retryable` |
| All other responses | `success` |

`4xx` responses other than 408 and 429 are classified as `success` - they are valid server responses to a bad request, not transient failures. Override `classifyResponse` or `classifyError` to change this.

## Retry-After

`caracal/fetch` exports an opt-in delay function for the HTTP `Retry-After` header. Retry stays a core policy, so the pacing policy is the single `delay` knob:

```ts
import { operation, retry } from "caracal"
import { fetchAdapter, retryAfterDelay } from "caracal/fetch"

const api = operation({
  name: "partner-api",
  adapter: fetchAdapter(),
  policies: [retry({ maxAttempts: 3, delay: retryAfterDelay })],
})
```

`retryAfterDelay` waits for the longer of:

- exponential backoff (`100ms × 2^(attempt - 1)`, capped at 30s), and
- the delay the server asked for in `Retry-After`, parsed as delta-seconds or an HTTP-date.

The result is then extended by up to 10% additive jitter, so replicas do not retry in lockstep. Jitter only ever lengthens the wait and the wait is clamped at 30s - both are deliberate safety bounds, not protocol semantics.

The defaults are configurable when you need them:

```ts
import { createRetryAfterDelay } from "caracal/fetch"

delay: createRetryAfterDelay({
  baseMs: 250,        // default 100
  factor: 3,          // default 2
  maxDelayMs: 60_000, // default 30_000
  jitterRatio: 0.2,   // default 0.1
})
```

Every option is optional, and invalid values throw a `RangeError` when the delay is created. For complete control, pass your own `(attempt, context) => number` instead.

To compose your own policy, use the parser directly:

```ts
import { retryAfterMs } from "caracal/fetch"

delay: (attempt, context) =>
  Math.max(250 * 2 ** (attempt - 1), retryAfterMs(context) ?? 0)
```

`retryAfterMs(context)` returns the parsed header in milliseconds, or `undefined` when the outcome carries no usable `Retry-After`. It never throws on malformed values. It reads the response from `context.result`, or from `context.error` when a custom `fetch` implementation throws a response-bearing error.

`Retry-After` is **not** honoured automatically. The adapter only classifies `429`, `408`, and `5xx` responses as `retryable`; whether a retry waits for the header is decided entirely by the `delay` you configure. A `POST` or `PATCH` is not retried at all unless you mark it replay-safe - see [Replay safety](#replay-safety).

## Streaming responses

`fetch` resolves as soon as response headers arrive, the body has not been consumed yet. Any bulkhead permit is released at that point, not after the body is fully read. If body consumption is your actual capacity boundary, consume the body inside a custom adapter before returning.
