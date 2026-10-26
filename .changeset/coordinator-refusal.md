---
"@gkoos/caracal": minor
---

Add `CoordinatorRefusalError`, and rename `ServalAuthorityUnavailableError` to
`ServalAuthorityRefusalError` on the `@gkoos/caracal/serval` entry point.

A coordinator that refuses an admission by decision used to throw an error named for
unavailability, and the distributed breaker's only reading of a thrown coordinator error was "the
coordinator is in trouble" — so a `staleClosed: "fail-closed"` refusal reached `onCoordinatorError`,
whose default is fail-open, and the call ran. A refusal is an answer, not a failure. It now throws
a type the policy recognises as a decision, the call is rejected with `CircuitOpenError`, no
`breaker.degraded` is emitted, and the caller's own cancellation still wins if it gave up while the
read was in flight.

The rename is breaking for callers that catch the old name. Nothing else changes: a coordinator
that cannot answer keeps throwing its own unavailability error, and `onCoordinatorError` continues
to govern that case alone.
