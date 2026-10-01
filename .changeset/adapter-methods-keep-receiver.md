---
"@gkoos/caracal": patch
---

`Adapter.classify` and `Adapter.dispose` are now called on the adapter, as `capabilities` and `execute` always were. Both were previously taken off the adapter and invoked detached, so an adapter that reads its own state through `this` - a class instance, or an object literal reading a sibling method - threw a `TypeError` from the classification path. That error replaced the attempt's own value or error and escaped the policy that asked for the verdict: `retry` classified the classifier's failure instead of the outcome, and the circuit breaker, which only records classified outcomes, recorded no observation at all - including for the attempts that had succeeded. A `dispose` that used `this` threw into the isolation that exists to swallow a *user's* dispose error, so an abandoned body or handle was never released and nothing reported that it had not been. The receiver is now bound once per `execute()`, so nothing is added to the per-attempt path.
