---
"@gkoos/caracal": minor
---

Add a rate limiter policy (`rateLimit.local` / `rateLimit.distributed`) using the Generic Cell Rate Algorithm (GCRA). It enforces a sustained rate plus a bounded burst across a scope, rejects with a `RateLimitExceededError` carrying a `retryAfterMs` hint that composes with the retry delay path, and fails closed on coordinator error. Ships a Redis coordinator (`redisRateLimitCoordinator`) with an atomic Lua script, `ratelimit.admitted` / `ratelimit.rejected` / `ratelimit.degraded` events, and documentation.
