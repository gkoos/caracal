---
"@gkoos/caracal": minor
---

Changed

- `windowSize` above 10 000 is now rejected at construction in both coordinations. Threshold evaluation scans every retained member inside a blocking Lua script, so only a lower bound was safe to allow; a configuration above the cap must lower it

Fixed

- constructing an operation no longer freezes the caller's `events` array. `normalizeSinks` returned the array by reference and the result was frozen, so a user's own array became non-extensible as a side effect of `operation({ ... })`. Policies were already copied first; sinks now match
- `breaker.observation` reports the generation the coordinator recorded the observation under, rather than the generation the attempt was admitted with. The two differ when the state hash was lost while its window survived and the script mints a new epoch
- `classify` is documented as pure and as called more than once per attempt (the operation, `retry` and the breaker each ask). The operation no longer calls it at all when no event sink is configured, because the verdict is only used to fill the `attempt.settled` event
- the composite admission signal is derived once per context instead of on every read. It was rebuilt five to eight times per attempt and was not stable by identity, which would have leaked any future `removeEventListener` against it
- coordination keys are memoized per identity in a bounded cache: the breaker computes three keys per coordinator call and uses one or two, and each costs a SHA-256 digest and five byte-length checks
- operation-level overhead per execution drops: the attempt/outer policy partition is computed once at construction rather than by two `filter` calls per execution, and the event outcome summaries are shared frozen constants
