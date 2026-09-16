---
"@gkoos/caracal": minor
---

The `Adapter` interface gains an optional `dispose(outcome, context)` hook. The runtime invokes it whenever it abandons a settled result rather than returning it to the caller - today, when `retry` schedules another attempt - so an adapter can release a body or handle instead of leaking it. `dispose` is fire-and-forget and isolated like event sinks. The fetch adapter implements it by cancelling the abandoned response's body, and the adapter contract suite now checks that an abandoned result is disposed.
