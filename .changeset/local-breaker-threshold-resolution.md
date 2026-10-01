---
"@gkoos/caracal": patch
---

`circuitBreaker.local` now compares its `failureThreshold` in the same resolution the distributed coordinator does - the integer numerator of thousandths, rather than the raw float ratio. The threshold is resolved once, and both coordinations compare the numerator, so a threshold that is not a multiple of `0.001` no longer means two different things. `failureThreshold: 0.5004` is compared as `0.5` under either coordination; before, the local breaker compared it exactly, so one failure and one success left it closed while the distributed breaker opened on the same window. Only the local breaker's *comparison* changes: the accepted range and every threshold that is already a multiple of `0.001` are unaffected.
