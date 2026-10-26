---
"@gkoos/caracal": minor
---

The delegated rate limiter clears an expired-grant backlog of any size.

The grant script forgot expired grants with a single `HDEL` fed by `unpack(expired)`. `unpack`
returns at most as many results as the interpreter's stack holds — 8000 in the Lua 5.1 that Redis
and Valkey embed — so a larger backlog failed the whole script with `too many results to unpack`,
and because it failed before removing anything, the backlog stayed put to fail every later
issuance the same way. Measured against a live Valkey: 7999 expired grants drained on the next
issuance and 8000 did not.

The drain now hands the expired identifiers to `HDEL` in bounded slices, so the ledger and the
expiry set are pruned whatever the backlog's size, and a live grant is still left alone. A case in
the Redis integration suite seeds 8192 expired grants beside one live grant and asserts that the
backlog is gone and the live grant survived.
