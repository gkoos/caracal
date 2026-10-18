---
"@gkoos/caracal": minor
---

Add shared runtime configuration for distributed policies. `redisConfigurationManager` reads and updates a per-identity configuration record, and the distributed bulkhead, rate limiter and circuit breaker enforce its settings on the next command.

Constructor settings remain revision 0 until the first explicit update. Records are distributed-only, never expire, and replace the complete settings snapshot: the bulkhead limit, the rate limit rate and burst, and the breaker failureThreshold, minimumThroughput, windowSize, openMs, halfOpenProbes and halfOpenSuccesses. Retention periods and probe leases stay constructor-owned, so changing `openMs` does not move a cleanup TTL.

Updating requires the whole fleet to run the configuration-aware protocol; an older client ignores the record and keeps its constructor settings.
