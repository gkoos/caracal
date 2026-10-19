---
"@gkoos/caracal": patch
---

Preserve the caller's abort reason exactly when `retry` reports a cancellation, including an explicit `null`. Retry's admission check and its inter-attempt wait used `signal.reason ?? new DOMException(...)`, so `abort(null)` surfaced as an `AbortError` while every other cancellation path - `operation`, the bulkheads, the breaker and the rate limiter - preserved `null` through `AbortSignal.throwIfAborted()`. The fallback now applies only when `reason` is `undefined`, so the same cancellation reports the same reason with or without `retry` in the pipeline.
