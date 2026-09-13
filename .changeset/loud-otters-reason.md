---
"@gkoos/caracal": minor
---

Changed

- `BulkheadRejectedError.reason` and the bulkhead event `reason` are closed unions (`BulkheadRejectedReason`, `BulkheadEventReason`) instead of `string`, and both types are exported. The two sets are different: `coordinator-unavailable` never appears on the error, because that failure rethrows `CoordinatorUnavailableError`; `lease-lost` is only ever delivered as an abort reason; and `cancelled` is event-only, since an aborted waiter receives its own abort reason rather than a `BulkheadRejectedError`

Fixed

- `core-api.md` and `events-and-observability.md` disagreed with each other and with the code about those reason sets. The error reasons are `capacity`, `wait-timeout`, `admission-expired` and `lease-lost`
- five documented reason values - `admission-unknown`, `lease-uncertain`, `already-expired-or-released`, `coordinator-unavailable` and the event-side `cancelled` - had no test asserting them. They do now, and `reason-contract.test.ts` fails when a documented reason stops being reachable or the docs drift from the unions
- `redis.md` claimed both clients disable offline queueing. The cluster client cannot: ioredis 6 keeps `enableOfflineQueue` at its own default for cluster node connections, wherever the flag is set. The documented safeguards are now the ones that do reach those connections - no command replay, no per-request retries, bounded command and connect timeouts - and the cluster integration suite pins the live node configuration
- `redis.md` and `circuit-breaker.md` described in-process scope-state retention as proportional to the scopes currently OPEN or HALF_OPEN. Entries are only removed when that scope is next read or settled as CLOSED, so a scope that opens once and goes quiet is retained for the lifetime of the process
- `scripts.ts` claimed the generation increments on every transition. The distributed `OPEN -> HALF_OPEN` edge deliberately keeps it and clears the probe set instead, which the docs now state alongside the local breaker's differing behaviour
