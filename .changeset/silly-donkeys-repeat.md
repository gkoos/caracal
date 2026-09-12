---
'@gkoos/caracal': minor
---

Fixed

- a `Retry-After` header larger than `2147483647` ms is now clamped by `retryAfterMs` (and by `createRetryAfterDelay`) instead of producing a wait the platform cannot schedule. Previously a delay composed from the parser could exceed the largest `setTimeout` delay and be rejected by `retry`'s own bounds check, replacing the original error
- the distributed bulkhead now emits `bulkhead.rejected` with reason `admission-expired` when a permit's lease deadline passes before the call starts, so the rejection is visible to event sinks instead of only to the caller
- `retry.declined` (`reason: "replay-unsafe" | "not-retryable"`) reports a call that was eligible for retry but not retried - previously indistinguishable from a call with no retry policy. This adds a member to the `OperationEvent` union, so exhaustive switches over `event.type` need a case for it
- `npm run audit:bundle` now also asserts the `fetch` and `postgres` subpath export sets

Changed

- documentation corrections: the breaker `classify` signature is `(error, isSuccess) => BreakerOutcome`; the `CoordinatorUnavailableError` claim applies to the Redis coordinator (and says where the symbol is exported from); `development.md` states the real Node floor; the local-breaker identity asymmetry (one instance merges windows) is documented; the ACL table attributes commands to the shipped `bulkheadLeaseV1`; and the events reference documents the error-only rejection reasons, the stripped `outcome` payloads and the fail-closed `state: "open"` signal
