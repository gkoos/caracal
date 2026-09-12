# Changelog

## 0.1.1

### Patch Changes

- cf60c44: Fixed
  
  - `failureThreshold` values that the distributed coordinator cannot resolve to thousandths are rejected at construction instead of being silently reinterpreted. Below `0.0005` the numerator rounded to 0, which made the comparison unconditionally true - the breaker opened on a success-only window and re-opened after every recovery; at `0.9995` and above it rounded to 1000, requiring every observation to fail, so the breaker effectively never opened
  - `circuitBreaker.local` enforces the same `0.0005 <= failureThreshold < 0.9995` range, so one policy config behaves the same under either coordination
  - an ignored result recorded nothing but also settled nothing, so a distributed half-open probe kept its slot until the probe lease elapsed: after an operation whose classifier returned `ignored`, the next call was rejected with `CircuitOpenError` for up to `probeLeaseTtlMs` (60s with the defaults), while the local breaker released its slot immediately. An ignored probe result now releases its slot without recording an outcome or advancing recovery, so one policy config behaves the same under either coordination
  - packaging and event fixes: `engines.node` now requires `>=20.3.0` (the runtime, timeout and bulkhead policies, and the fetch adapter use `AbortSignal.any`, which landed in 20.3.0, so 20.0-20.2 installed cleanly and then failed at runtime); the published package now also ships `test/harness`, so the `@gkoos/caracal/testing` entry point has published source like every other entry point; and a local bulkhead's `bulkhead.released` event now reports its occupancy before the queued successor is admitted, instead of appearing to include it

## 0.1.0 - 2026-09-09

Initial public release.
