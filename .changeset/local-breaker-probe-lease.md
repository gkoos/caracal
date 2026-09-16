---
"@gkoos/caracal": minor
---

The local circuit breaker now arms a per-probe lease (`probeLeaseTtlMs`, default `openMs × 2`) when it admits a half-open probe. An adapter promise that never settles releases its slot when the lease expires and its late result is dropped as stale, so a hung attempt can no longer wedge the breaker in half-open for the lifetime of the process. A new `breaker.probe-expired` event reports the reclaim. This mirrors the distributed breaker's existing probe lease.
