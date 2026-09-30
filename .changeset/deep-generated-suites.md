---
"@gkoos/caracal": patch
---

Deepen the generated test suites: one seed-and-depth protocol for every generated suite (`CARACAL_TEST_RUNS`), new property coverage for the local GCRA rate limiter (admission arithmetic, burst envelope, exact `retryAfterMs`, `snapshot()`) and local bulkhead permit accounting (occupancy, queue timeout, lease reclaim, FIFO handoff), the `npm run test:generated` and `npm run test:generated:deep` entry points, and a non-gating nightly deep run.
