---
"@gkoos/caracal": minor
---

Add the opt-in `@gkoos/caracal/serval` entry point: the delegated coordination
strategy from the Serval RFC, shipped as an experimental surface. It exports the
shared session (`createServalSession`), a cached-verdict breaker
(`createServalBreaker`), a delegated quota rate limiter
(`createServalRateLimitCoordinator`) and a delegated slot bulkhead
(`createServalBulkheadCoordinator`) with their two Redis authorities
(`redisRateLimitGrantIssuer`, `redisBulkheadAllocationIssuer`). Each is passed to the
existing `circuitBreaker.distributed`, `rateLimit.distributed` or
`bulkhead.distributed` as its coordinator, so nothing about the strict coordinators or
the root package changes.

`docs/serval/api.md` documents the configuration, lifecycle, approximate guarantees,
explicit deviations, failure behaviour, metrics, local-guardrail recipe and migration
steps; `docs/serval/benchmarks.md` carries the measured trade-offs and the operating
envelopes, including the finding that a Redis outage stops a composed pipeline which
includes the distributed breaker.
