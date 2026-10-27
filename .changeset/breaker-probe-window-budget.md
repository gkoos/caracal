---
"@gkoos/caracal": minor
---

Bound the recovery probe budget by the window, not by what is in flight.

`halfOpenProbes` was enforced as "probes in flight": every settlement removed its probe token, so a
probe that produced no result freed its slot immediately. An `ignored` result is exactly that - the
classifier ignores admission refusals, cancellations and superseded attempts - and it neither
re-opened the breaker nor started a cooldown. A delegated replica holding a cached HALF_OPEN verdict
forwards every admission to the authority, so a fleet whose probes were being refused locally turned
every arriving request into a probe: 512 probes against a configured `halfOpenProbes: 3`, with no
CLOSED transition, where the strict path stays OPEN and probes three times.

The recovery window is now the budget. `halfOpenProbes` probes are admitted per window, the window
refills every `openMs`, and an inconclusive result releases its in-flight slot without returning its
window budget - so a probe that reported nothing cannot be replaced by the next request that arrives.
A counting result does return it, and a token whose lease lapsed returns both, so a dead worker
cannot consume the window and recovery can still accumulate `halfOpenSuccesses` within one window.
