---
"@gkoos/caracal": minor
---

Server-side reclamation no longer fails or stalls on a large backlog of lapsed entries.

The grant script forgot expired grants with a single `HDEL` fed by `unpack(expired)`. `unpack`
returns at most as many results as the interpreter's stack holds — 8000 in the Lua 5.1 that Redis
and Valkey embed — so a larger backlog failed the whole script with `too many results to unpack`,
and because it failed before removing anything, the backlog stayed put to fail every later issuance
the same way. Measured against a live Valkey: 7999 expired grants drained on the next issuance and
8000 did not.

Both cleanups now drain in bounded batches: a slice small enough for `unpack`, and a fixed number of
slices per call, so no call pays a cost proportional to everything that has lapsed. A backlog past
one call's budget is drained by the calls that follow. For the grant ledger that costs nothing but
time — the keys carry the TTL of the newest grant, and a retry is answered by field — while the
bulkhead's reservation set carries no TTL, because its drain is what returns capacity: the bound is
far above the one member a call adds, so that capacity arrives within a call or two.

Because the drain can now lag, a retry no longer takes the grant ledger's word alone: a lapsed grant
whose entry the drain has not reached is neither deduplicated nor served, decided against the expiry
set. Two cases in the Redis integration suite cover it — a backlog past the per-call budget that
drains over the calls that follow without forgetting a live grant, and a retry of a lapsed grant the
drain has not reached.
