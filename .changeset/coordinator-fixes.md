---
"@gkoos/caracal": minor
---

Fixes and additions on the strict coordinators, found while building the delegated strategy.

A coordinator that refuses an admission by decision used to throw an error named for unavailability,
and the distributed breaker's only reading of a thrown coordinator error was that the coordinator was
in trouble. A `staleClosed: "fail-closed"` refusal therefore reached `onCoordinatorError`, whose
default is fail-open, and the call ran. A refusal is an answer, not a failure: it now throws
`CoordinatorRefusalError`, the policy recognises it as a decision, the call is rejected with
`CircuitOpenError`, and no `breaker.degraded` is emitted. The caller's own cancellation still wins if
it gave up while the read was in flight, a coordinator that cannot answer keeps throwing its own
unavailability error, and `onCoordinatorError` continues to govern that case alone.

The strict circuit breaker's `observe` and `settleProbe` replies carry optional `openedAt` and
`revision` fields, so a consumer can anchor a recovery cooldown to the moment the breaker opened
rather than to the time the reply was received, and can tell which configuration the decision was made
under. Both are optional: a coordinator that reports neither keeps working.

Server-side reclamation no longer fails or stalls on a large backlog of lapsed entries. The rate
limiter's cleanup handed every lapsed grant to a single `HDEL` through `unpack`, which refuses more
results than the interpreter's stack holds - past roughly 8000 lapsed grants the whole script failed,
and because it failed before removing anything, every later issuance failed the same way. Both
cleanups now drain in bounded batches, and a lapsed grant whose ledger entry the drain has not reached
is no longer served as a duplicate.
