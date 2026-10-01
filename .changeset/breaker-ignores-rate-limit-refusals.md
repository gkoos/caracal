---
"@gkoos/caracal": minor
---

An outer circuit breaker no longer records a rate limiter's admission refusal (`RateLimitExceededError`) as a dependency failure. The refusal means the adapter call never started, so shedding is not evidence about the dependency - previously a rate limiter shedding normal traffic could open a healthy breaker, which then shed the rest of the run under `breaker-open`. The `countBulkheadRejections` option is renamed `countAdmissionRejections` and now covers both bulkhead and rate-limit refusals; `lease-lost` refusals and coordinator failures still count.
