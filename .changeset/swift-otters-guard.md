---
"@gkoos/caracal": minor
---

Added

- `EventOutcome` is exported from the package root. It was already documented as the shape of every `outcome` field on an event, but only the internal core entry exported it, so a sink author could not name the type

Fixed

- `retry.declined` no longer fires for a call that succeeded. It reports a retry policy declining to schedule another attempt for a **non-success** outcome - which is what the events reference and the 0.2.0 changelog describe - so a counter over it no longer tracks successes
- `fetch.md` states the real ceiling of `retryAfterDelay`: `maxDelayMs` caps the deterministic wait and additive jitter extends it, so the longest wait with the defaults is 33s, not 30s
- the local breaker's `open -> half-open` trigger is documented as what it is - the first admission attempt after `openMs` - instead of as a timer. `snapshot()` reports the last transition, so a breaker with no traffic still reads `open`
- `timeout-and-retry.md` lists `retry.declined` among its relevant events
- `development.md` states the real Node floor beside `node:check`, and `adapter-contracts.md` documents the lifecycle-order check the contract harness performs and the `runAdapterContractSuite` runner it exports
- `core-api.md` gains a consolidated error reference and states the `coordination` property every returned policy carries; `docs/README.md` indexes the documentation
