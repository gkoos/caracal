# Changelog

## 0.3.0

### Minor Changes

- 0e09c1b: Changed
  
  - `BulkheadRejectedError.reason` and the bulkhead event `reason` are closed unions (`BulkheadRejectedReason`, `BulkheadEventReason`) instead of `string`, and both types are exported. The two sets are different: `coordinator-unavailable` never appears on the error, because that failure rethrows `CoordinatorUnavailableError`; `lease-lost` is only ever delivered as an abort reason; and `cancelled` is event-only, since an aborted waiter receives its own abort reason rather than a `BulkheadRejectedError`
  
  Fixed
  
  - `core-api.md` and `events-and-observability.md` disagreed with each other and with the code about those reason sets. The error reasons are `capacity`, `wait-timeout`, `admission-expired` and `lease-lost`
  - five documented reason values - `admission-unknown`, `lease-uncertain`, `already-expired-or-released`, `coordinator-unavailable` and the event-side `cancelled` - had no test asserting them. They do now, and `reason-contract.test.ts` fails when a documented reason stops being reachable or the docs drift from the unions
  - `redis.md` claimed both clients disable offline queueing. The cluster client cannot: ioredis 6 keeps `enableOfflineQueue` at its own default for cluster node connections, wherever the flag is set. The documented safeguards are now the ones that do reach those connections - no command replay, no per-request retries, bounded command and connect timeouts - and the cluster integration suite pins the live node configuration
  - `redis.md` and `circuit-breaker.md` described in-process scope-state retention as proportional to the scopes currently OPEN or HALF_OPEN. Entries are only removed when that scope is next read or settled as CLOSED, so a scope that opens once and goes quiet is retained for the lifetime of the process
  - `scripts.ts` claimed the generation increments on every transition. The distributed `OPEN -> HALF_OPEN` edge deliberately keeps it and clears the probe set instead, which the docs now state alongside the local breaker's differing behaviour
- c5c9758: Changed
  
  - events no longer carry the attempt payload: `outcome` on `attempt.settled`, `execution.settled`, `retry.scheduled`, `retry.exhausted` and `retry.declined` is an `EventOutcome` (`{ status }` only), so neither a result value nor an error object reaches a sink. Previously the error was passed through verbatim while the type claimed otherwise, which let a sink retain a response body or a credential - and mutate the same error object a policy would classify afterwards
  - the `timeout` documentation is precise about what it bounds: the wrapped work, not the coordinator calls an outer distributed policy makes. A new composition section shows the outer-timeout arrangement for a total deadline
  
  Fixed
  
  - documented that a replay-safe `POST` still needs a re-sendable body: a `Request` argument is single-use and fails on its second attempt, and a stream body cannot be replayed at all. Body-bearing retries are now covered by the fetch integration suite
  - documented that Caracal does not dispose a response body that `retry` discards - a known gap with no cleanup hook yet, with the safe workarounds
  - `SECURITY.md`'s supported versions track the released line, every Node-floor mention matches `engines` (both now asserted by the package-contract test), the README's truncated comparison intro is complete and labelled a snapshot, and `redis.md` states which topologies CI exercises
- 4710c90: Changed
  
  - `windowSize` above 10 000 is now rejected at construction in both coordinations. Threshold evaluation scans every retained member inside a blocking Lua script, so only a lower bound was safe to allow; a configuration above the cap must lower it
  
  Fixed
  
  - constructing an operation no longer freezes the caller's `events` array. `normalizeSinks` returned the array by reference and the result was frozen, so a user's own array became non-extensible as a side effect of `operation({ ... })`. Policies were already copied first; sinks now match
  - `breaker.observation` reports the generation the coordinator recorded the observation under, rather than the generation the attempt was admitted with. The two differ when the state hash was lost while its window survived and the script mints a new epoch
  - `classify` is documented as pure and as called more than once per attempt (the operation, `retry` and the breaker each ask). The operation no longer calls it at all when no event sink is configured, because the verdict is only used to fill the `attempt.settled` event
  - the composite admission signal is derived once per context instead of on every read. It was rebuilt five to eight times per attempt and was not stable by identity, which would have leaked any future `removeEventListener` against it
  - coordination keys are memoized per identity in a bounded cache: the breaker computes three keys per coordinator call and uses one or two, and each costs a SHA-256 digest and five byte-length checks
  - operation-level overhead per execution drops: the attempt/outer policy partition is computed once at construction rather than by two `filter` calls per execution, and the event outcome summaries are shared frozen constants
- 6c1ccf6: Added
  
  - `EventOutcome` is exported from the package root. It was already documented as the shape of every `outcome` field on an event, but only the internal core entry exported it, so a sink author could not name the type
  
  Fixed
  
  - `retry.declined` no longer fires for a call that succeeded. It reports a retry policy declining to schedule another attempt for a **non-success** outcome - which is what the events reference and the 0.2.0 changelog describe - so a counter over it no longer tracks successes
  - `fetch.md` states the real ceiling of `retryAfterDelay`: `maxDelayMs` caps the deterministic wait and additive jitter extends it, so the longest wait with the defaults is 33s, not 30s
  - the local breaker's `open -> half-open` trigger is documented as what it is - the first admission attempt after `openMs` - instead of as a timer. `snapshot()` reports the last transition, so a breaker with no traffic still reads `open`
  - `timeout-and-retry.md` lists `retry.declined` among its relevant events
  - `development.md` states the real Node floor beside `node:check`, and `adapter-contracts.md` documents the lifecycle-order check the contract harness performs and the `runAdapterContractSuite` runner it exports
  - `core-api.md` gains a consolidated error reference and states the `coordination` property every returned policy carries; `docs/README.md` indexes the documentation

### Patch Changes

- 2718ed4: Fixed
  
  - an operation with no event sink no longer builds events. Both `emitRuntimeEvent` and the operation's own emitter constructed the event object - including a `Date.now()` call - before checking whether a sink would receive it, so four lifecycle events per execution were allocated for nobody. `test/unit/no-sink-fast-path.test.ts` pins it and `npm run bench:gate` measures it
  - `npm run bench` no longer fails when the Redis script cache is already warm: capturing a script body used to depend on the server choosing `EVAL`, which it does not when it already has the SHA

## 0.2.0

### Minor Changes

- 93d34ec: Fixed
  
  - a `Retry-After` header larger than `2147483647` ms is now clamped by `retryAfterMs` (and by `createRetryAfterDelay`) instead of producing a wait the platform cannot schedule. Previously a delay composed from the parser could exceed the largest `setTimeout` delay and be rejected by `retry`'s own bounds check, replacing the original error
  - the distributed bulkhead now emits `bulkhead.rejected` with reason `admission-expired` when a permit's lease deadline passes before the call starts, so the rejection is visible to event sinks instead of only to the caller
  - `retry.declined` (`reason: "replay-unsafe" | "not-retryable"`) reports a call that was eligible for retry but not retried - previously indistinguishable from a call with no retry policy. This adds a member to the `OperationEvent` union, so exhaustive switches over `event.type` need a case for it
  - `npm run audit:bundle` now also asserts the `fetch` and `postgres` subpath export sets
  
  Changed
  
  - documentation corrections: the breaker `classify` signature is `(error, isSuccess) => BreakerOutcome`; the `CoordinatorUnavailableError` claim applies to the Redis coordinator (and says where the symbol is exported from); `development.md` states the real Node floor; the local-breaker identity asymmetry (one instance merges windows) is documented; the ACL table attributes commands to the shipped `bulkheadLeaseV1`; and the events reference documents the error-only rejection reasons, the stripped `outcome` payloads and the fail-closed `state: "open"` signal

### Patch Changes

- fc8d667: Fixed
  
  - an `async` event sink can no longer crash the process: a sink whose `emit` returns a rejected promise is now observed and dropped, matching the documented promise that a failing sink cannot affect execution (synchronous throws were already contained)
  - `timeout({ ms })` and retry delays now reject durations above `2147483647` ms, which `setTimeout` clamps to 1 ms and would otherwise turn into an immediate timeout or an immediate retry
  - `createCoordinationClusterClient` accepts an optional third argument for connection options, so a secured cluster (ACL credentials, TLS) can be configured through the documented factory while the coordination safeguards stay pinned
  - the events reference no longer claims the distributed bulkhead omits `bulkhead.admitted` and `bulkhead.released` (it emits both, and `bulkhead.released` can carry an `already-expired-or-released` reason), the custom-policy guidance now distinguishes `phase` from array order, and `abort: "unsupported"` is documented as suppressing policy-generated cancellation rather than all cancellation

## 0.1.1

### Patch Changes

- cf60c44: Fixed
  
  - `failureThreshold` values that the distributed coordinator cannot resolve to thousandths are rejected at construction instead of being silently reinterpreted. Below `0.0005` the numerator rounded to 0, which made the comparison unconditionally true - the breaker opened on a success-only window and re-opened after every recovery; at `0.9995` and above it rounded to 1000, requiring every observation to fail, so the breaker effectively never opened
  - `circuitBreaker.local` enforces the same `0.0005 <= failureThreshold < 0.9995` range, so one policy config behaves the same under either coordination
  - an ignored result recorded nothing but also settled nothing, so a distributed half-open probe kept its slot until the probe lease elapsed: after an operation whose classifier returned `ignored`, the next call was rejected with `CircuitOpenError` for up to `probeLeaseTtlMs` (60s with the defaults), while the local breaker released its slot immediately. An ignored probe result now releases its slot without recording an outcome or advancing recovery, so one policy config behaves the same under either coordination
  - packaging and event fixes: `engines.node` now requires `>=20.3.0` (the runtime, timeout and bulkhead policies, and the fetch adapter use `AbortSignal.any`, which landed in 20.3.0, so 20.0-20.2 installed cleanly and then failed at runtime); the published package now also ships `test/harness`, so the `@gkoos/caracal/testing` entry point has published source like every other entry point; and a local bulkhead's `bulkhead.released` event now reports its occupancy before the queued successor is admitted, instead of appearing to include it

## 0.1.0 - 2026-09-09

Initial public release.
