---
"@gkoos/caracal": patch
---

`node scripts/bench.mjs` now measures coordinator round trips per execution (bulkhead, closed and half-open breakers, and both together) and per-call EVALSHA latency against `commandTimeout`. The counts in `redis.md`'s round-trip table are asserted by an integration test, so a policy that adds a coordinator call fails CI instead of silently raising the distributed cost.
