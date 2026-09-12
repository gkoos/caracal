---
'@gkoos/caracal': minor
---

Changed

- events no longer carry the attempt payload: `outcome` on `attempt.settled`, `execution.settled`, `retry.scheduled`, `retry.exhausted` and `retry.declined` is an `EventOutcome` (`{ status }` only), so neither a result value nor an error object reaches a sink. Previously the error was passed through verbatim while the type claimed otherwise, which let a sink retain a response body or a credential - and mutate the same error object a policy would classify afterwards
- the `timeout` documentation is precise about what it bounds: the wrapped work, not the coordinator calls an outer distributed policy makes. A new composition section shows the outer-timeout arrangement for a total deadline

Fixed

- documented that a replay-safe `POST` still needs a re-sendable body: a `Request` argument is single-use and fails on its second attempt, and a stream body cannot be replayed at all. Body-bearing retries are now covered by the fetch integration suite
- documented that Caracal does not dispose a response body that `retry` discards - a known gap with no cleanup hook yet, with the safe workarounds
- `SECURITY.md`'s supported versions track the released line, every Node-floor mention matches `engines` (both now asserted by the package-contract test), the README's truncated comparison intro is complete and labelled a snapshot, and `redis.md` states which topologies CI exercises
