---
"@gkoos/caracal": minor
---

Carry the server's own recovery clock and configuration revision in the strict circuit
breaker's `observe` and `settleProbe` replies. `ObserveResult` and `SettleProbeResult`
gained optional `openedAt` and `revision` fields, so a consumer can anchor a recovery
cooldown to the moment the breaker actually opened rather than to the time the reply was
received, and can tell which configuration the decision was made under. Both fields are
optional and the local breaker is unaffected: a coordinator that reports neither keeps
working, and the delegated strategy falls back to the reply's own receipt time.

Add the Redis scripts and keys the delegated authorities use. `rateGrantV1` issues a
bounded quota grant against the same GCRA state `rateLimitV1` enforces, so the delegated
path is not a second budget, and `bulkheadAllocationV1` maintains a per-replica
allocation hash beside the fleet's counter of granted slots. Both are reached through
`@gkoos/caracal/serval` as `redisRateLimitGrantIssuer` and
`redisBulkheadAllocationIssuer`; the strict coordinators and the commands they send are
unchanged. The new key suffixes (`grants`, `grant-expiry`, `allocation`,
`allocation-expiry`, `allocated`, `allocation-ops`) and every command the scripts require
are documented in `docs/redis.md`.
