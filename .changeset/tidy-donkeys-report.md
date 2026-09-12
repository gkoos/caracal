---
'@gkoos/caracal': patch
---

Fixed

- `failureThreshold` values that the distributed coordinator cannot resolve to thousandths are rejected at construction instead of being silently reinterpreted. Below `0.0005` the numerator rounded to 0, which made the comparison unconditionally true - the breaker opened on a success-only window and re-opened after every recovery; at `0.9995` and above it rounded to 1000, requiring every observation to fail, so the breaker effectively never opened
- `circuitBreaker.local` enforces the same `0.0005 <= failureThreshold < 0.9995` range, so one policy config behaves the same under either coordination
