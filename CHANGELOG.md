# Changelog

## 0.2.0

### Minor Changes

- 93d34ec: Fixed
  
  - a `Retry-After` header larger than `2147483647` ms is now clamped by `retryAfterMs` (and by `createRetryAfterDelay`) instead of producing a wait the platform cannot schedule. Previously a delay composed from the parser could exceed the largest `setTimeout` delay and be rejected by `retry`'s own bounds check, replacing the original error
  - the distributed bulkhead now emits `bulkhead.rejected` with reason `admission-expired` when a permit's lease deadline passes before the call starts, so the rejection is visible to event sinks instead of only to the caller
  - `retry.declined` (`reason: "replay-unsafe" | "not-retryable"`) reports a call that was eligible for retry but not retried - previously indistinguishable from a call with no retry policy. This adds a member to the `OperationEvent` union, so exhaustive switches over `event.type` need a case for it
  - `npm run audit:bundle` now also asserts the `fetch` and `postgres` subpath export sets
  
  Changed
  
  - documentation corrections: the breaker `classify` signature is `(error, isSuccess) => BreakerOutcome`; the `CoordinatorUnavailableError` claim applies to the Redis coordinator (and says where the symbol is exported from); `development.md` states the real Node floor; the local-breaker identity asymmetry (one instance merges windows) is documented; the ACL table attributes commands to the shipped `bulkheadLeaseV1`; and the events reference documents the error-only rejection reasons, the stripped `outcome` payloads and the fail-closed `state: "open"` signal

### Patch Changes

- fc8d667: Fixed
  
  - an `async` event sink can no longer crash the process: a sink whose `emit` returns a rejected promise is now observed and dropped, matching the documented promise that a failing sink cannot affect execution (synchronous throws were already contained)
  - `timeout({ ms })` and retry delays now reject durations above `2147483647` ms, which `setTimeout` clamps to 1 ms and would otherwise turn into an immediate timeout or an immediate retry
  - `createCoordinationClusterClient` accepts an optional third argument for connection options, so a secured cluster (ACL credentials, TLS) can be configured through the documented factory while the coordination safeguards stay pinned
  - the events reference no longer claims the distributed bulkhead omits `bulkhead.admitted` and `bulkhead.released` (it emits both, and `bulkhead.released` can carry an `already-expired-or-released` reason), the custom-policy guidance now distinguishes `phase` from array order, and `abort: "unsupported"` is documented as suppressing policy-generated cancellation rather than all cancellation

## 0.1.1

### Patch Changes

- cf60c44: Fixed
  
  - `failureThreshold` values that the distributed coordinator cannot resolve to thousandths are rejected at construction instead of being silently reinterpreted. Below `0.0005` the numerator rounded to 0, which made the comparison unconditionally true - the breaker opened on a success-only window and re-opened after every recovery; at `0.9995` and above it rounded to 1000, requiring every observation to fail, so the breaker effectively never opened
  - `circuitBreaker.local` enforces the same `0.0005 <= failureThreshold < 0.9995` range, so one policy config behaves the same under either coordination
  - an ignored result recorded nothing but also settled nothing, so a distributed half-open probe kept its slot until the probe lease elapsed: after an operation whose classifier returned `ignored`, the next call was rejected with `CircuitOpenError` for up to `probeLeaseTtlMs` (60s with the defaults), while the local breaker released its slot immediately. An ignored probe result now releases its slot without recording an outcome or advancing recovery, so one policy config behaves the same under either coordination
  - packaging and event fixes: `engines.node` now requires `>=20.3.0` (the runtime, timeout and bulkhead policies, and the fetch adapter use `AbortSignal.any`, which landed in 20.3.0, so 20.0-20.2 installed cleanly and then failed at runtime); the published package now also ships `test/harness`, so the `@gkoos/caracal/testing` entry point has published source like every other entry point; and a local bulkhead's `bulkhead.released` event now reports its occupancy before the queued successor is admitted, instead of appearing to include it

## 0.1.0 - 2026-09-09

Initial public release.
