---
"@gkoos/caracal": minor
---

An outer circuit breaker no longer records a bulkhead admission refusal (`capacity`, `wait-timeout`, `admission-expired`) as a dependency failure. The refusal means the adapter call never started, so shedding is not evidence about the dependency - previously a saturated bulkhead could open a healthy breaker, which then shed the rest of the run under `breaker-open`. `countBulkheadRejections: true` records refusals again; `lease-lost` refusals and timeouts still count.
