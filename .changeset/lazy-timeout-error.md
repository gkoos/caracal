---
"@gkoos/caracal": minor
---

Changed

- `timeout` builds its `TimeoutError` when the timer fires instead of when the attempt starts. Constructing an Error captures a stack, which measures ~6µs against ~0.6µs with `Error.stackTraceLimit = 0` and grows with pipeline depth, and it was paid on every attempt to benefit only the ones that time out. The bench gate moves from 18.2µs to 3.6µs for `timeout` alone and from 22.7µs to 4.0µs for the full local policy set. The trade-off is the stack: it previously named the calling frame and now shows the timer internals, so `timeoutMs` and the operation name and execution id on `timeout.triggered` are what identify the call

Fixed

- `createRetryAfterDelay` clamps its result to 2147483647 ms. `maxDelayMs` bounds the deterministic wait but jitter is additive, so a high `maxDelayMs` could produce a delay `retry` rejects, and the resulting `RangeError` replaced the caller's real error instead of pacing the retry
- the `createRetryAfterDelay` doc comment no longer claims a server-provided minimum is never retried early. `maxDelayMs` caps `Retry-After` too, which `docs/fetch.md` already stated
