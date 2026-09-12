---
'@gkoos/caracal': patch
---

Fixed

- an `async` event sink can no longer crash the process: a sink whose `emit` returns a rejected promise is now observed and dropped, matching the documented promise that a failing sink cannot affect execution (synchronous throws were already contained)
- `timeout({ ms })` and retry delays now reject durations above `2147483647` ms, which `setTimeout` clamps to 1 ms and would otherwise turn into an immediate timeout or an immediate retry
- `createCoordinationClusterClient` accepts an optional third argument for connection options, so a secured cluster (ACL credentials, TLS) can be configured through the documented factory while the coordination safeguards stay pinned
- the events reference no longer claims the distributed bulkhead omits `bulkhead.admitted` and `bulkhead.released` (it emits both, and `bulkhead.released` can carry an `already-expired-or-released` reason), the custom-policy guidance now distinguishes `phase` from array order, and `abort: "unsupported"` is documented as suppressing policy-generated cancellation rather than all cancellation
