# Serval review findings

A working list, not a specification. It records one external review of this branch, what each
finding looks like in the code today, and what would close it. Nothing here is a claim about
behaviour that a test proves.

- Review: `feat/serval-authority-protocol` at `d507ea8` — six implementation bugs plus
  documentation gaps.
- Validated against: `eb6d994` plus the four commits on top of it.
- Method: reading the cited code. Item 3's halves, items 5 and 6, and the frozen-slot defect noted
  at the end were reproduced by running something; D2 was closed by measuring it.

## Status

| # | Finding | Status |
|---|---|---|
| 1 | `staleClosed: "fail-closed"` admits requests through the policy | Fixed |
| 2 | Bulkhead replacement grants rejected after the previous grant advances its revision | Fixed |
| 3 | Breaker freshness bypasses the send-time deadline | Fixed |
| 4 | `maximumOutstandingPermits` does not enforce a permit cap | Fixed before the reset point |
| 5 | Relinquishment during another modification orphans frozen slots | Fixed |
| 6 | Expired unused quota permanently occupies registry entries | Fixed |
| D1 | Outage behaviour claim | Fixed |
| D2 | Warm-authority fault behaviour | Fixed |
| D3 | Tracked-concurrency measurements | Fixed |
| D4 | Seeded comparisons | Open |
| D5 | Authority and reconciliation contracts | Open |

Provenance: fixes for 1, 2, 3, 5 and 6 existed in commits the branch reset dropped. Item 4's fix is
`ae098cf`, which is below `eb6d994` and survived.

## 1. `staleClosed: "fail-closed"` admits requests through the policy

- `src/coordination/serval/breaker.ts:414` throws `ServalAuthorityUnavailableError` (since renamed
  `ServalAuthorityRefusalError`, extending the core's `CoordinatorRefusalError`, and the policy now
  rejects on it).
- `CoordinatorRefusalError` appears nowhere in `src`, so a refusal by decision is indistinguishable
  from an outage.
- `src/core/circuit-breaker.ts`, `catch (readError)`: emits `breaker.coordinator-error`, computes
  `effectiveBehavior` from `onCoordinatorError` (default fail-open) and admits.
- Effect: an operation configured to fail closed executes its adapter anyway.
- Reproduced by the reviewer against the policy; not reproduced here.
- Closes with: a distinct refusal error thrown only for a decision, a policy branch that rejects
  instead of admitting, and a test through `operation(...)` asserting the adapter never ran.

## 2. Replacement grants rejected after the previous grant advances its revision

- `src/coordination/serval/bulkhead.ts:492` compares `reply.revision < scope.revision`.
- `scope.revision =` occurs only at line 498, the adoption. It is never reset: not in the `expired`
  branch (527-531) and not on retirement (569-572), both of which clear `grantId` but leave the
  revision behind.
- Effect: after a grant expires or is retired, the next grant's revision 1 is rejected as stale, and
  keeps being rejected.
- Reviewer reproduced against real Valkey.
- Fixed by `b077127`. The Lua settles what the fix should be: `allocate` writes
  `grantId|1|slots|expiresAt`, so the revision is a per-grant counter and a *lower* one means a
  newer grant. `adopt` now rejects a reply only when a reply from a later request has already been
  applied, using the `requestStartedAt` every modification already carries; a tie is not stale
  because a retry reuses its original send time. The revision stays as the compare-and-swap the next
  request sends, which the server arbitrates.
- Not taken: resetting the revision where a grant ends. That was the suggestion above, and it
  patches the symptom; the comparison across a grant boundary is the mistake. Grant identity is not
  available in the reply either (the reply is `{status, reason, allocated, rev, expiresAt, serverTime,
  duplicate, revision, changedAt, body?}`), so scoping by grant would need a protocol change.
- The same conflation appeared twice in the history harness and both are scoped to the grant now:
  the revision mark resets with the grant, and the occupancy check allows the physical overshoot the
  contract permits when a replacement is smaller than the work still running.

## 3. Breaker freshness bypasses the send-time deadline

- Half fixed by `6a22bda`: `breaker.ts:309` now uses
  `servalDeadline(context.requestStartedAt, freshnessMs)`, so the receipt-anchored second deadline
  is gone.
- The second half reproduces by running it: with `freshnessMs: 100`, a settlement at 1060 whose
  reply the authority had already superseded still moved the deadline, and a read at 1100 — past the
  1075 the cold-start read granted — was served from the cache instead of applying `staleClosed`.
- Fixed: the window moves only for a reply that verified the verdict this replica holds. The session
  now separates the two halves of the version comparison — `superseded` for a record older than what
  we hold, `unchanged` for one that answers with exactly what we hold — where it previously reported
  both as `stale`, and `cacheVerdict` writes the deadline for an applied record or an unchanged one
  only. A superseded reply leaves the window where it was.
- Tests: `test/unit/serval-breaker.test.ts` "does not move the deadline for a settlement the
  authority has superseded", which fails before the fix with the read resolving from the cache;
  `test/unit/serval-session.test.ts` pins both reasons.
- The unchanged case still renews the window, and that is load-bearing: "follows the configured
  stale-CLOSED behaviour" relies on a read that returns the cached verdict renewing the allowance, so
  treating every ignored reply alike would have removed it.

## 4. `maximumOutstandingPermits` does not enforce a permit cap

- Not reproducible here. `src/coordination/redis/rate-limit.ts:300-306` passes the cap through
  (`-1` for uncapped), and `src/coordination/redis/scripts.ts:466` refuses when
  `liveGrantedPermits() + granted > maximumOutstanding`, summing live permits across grants.
- The grant-count conversion the review found is gone; `ae098cf` is below the reset point.
- Not checked: whether a cap smaller than the requested grant is rejected.

## 5. Relinquishment during another modification orphans frozen slots

- `bulkhead.ts:722-730`: `relinquish` adds to `scope.frozen` and then awaits `startModification`.
- `bulkhead.ts:416-417`: `startModification` returns the in-flight promise when one exists, so the
  reduction is never submitted.
- Effect: the slots stay frozen with no retained operation to settle them.
- `906d17c` bounds `frozen` in `adopt` but does not cover this: the other modification's reply
  reports the same allocation, so the bound never applies.
- Reproduced by running it: with a renewal holding the wire, `relinquish` answered `frozen`, the
  authority was asked twice (allocate, renew) and never for the reduction, `frozenPendingReturn` was
  1 with `freeSpendable` 0, and `granted` still counted the slot. After the grant lapsed the count
  survived into the next allocation, so the loss was permanent.
- Fixed: the freeze is an intent the modification machinery submits. A new `submitPendingReturn`
  follows the operation holding the wire and submits the reduction when it settles, a return the
  authority refused stays pending and is retried by the nudge an admission already performs, and an
  `expired` refusal drops the intent with the grant. A second path with the same cause goes with it:
  a return refused with `revision`/`exists` cleared the operation and left the freeze orphaned.
- The reconciliation route the header and the `relinquish` doc named does not exist —
  `unfrozenAfterReconciliation` and `reconciliations` have no implementation (D5) — so the fix
  settles the freeze inside the modification machinery instead of deferring to it, and those two
  comments now describe what the code does.
- Tests: three cases in `test/unit/serval-bulkhead.test.ts` — a return frozen while a renewal holds
  the wire, a refused return retried, and the intent dropped when the allocation lapses — plus
  `refuseNext` on the fake authority, so the refusal path is reachable at all.
- Not taken: refusing the relinquish while a modification is in flight. It is one line and never
  freezes without a submitter, but it drops the return, and the caller is a scale-down or `close()`
  that may never retry — silently keeping capacity is what `relinquish` exists to prevent.

## 6. Expired unused quota permanently occupies registry entries

- `src/coordination/serval/rate-limit.ts:254-266`: `reclaimIdle` requires `scope.stock === 0`, so a
  scope whose stock has expired but is recorded as non-zero is never reclaimable, and expiry runs
  only when that scope is touched again.
- The bulkhead has the same defect: `reclaimIdle` requires `scope.allocated === 0`, and nothing clears
  an untouched allocation — `isValid` only gates admission, and the `expired` refusal that zeroes it
  needs a modification, which needs a touch.
- Reproduced by running it: with `maximumScopes: 1`, a scope holding unspent stock past its window —
  and one holding a lapsed allocation — made the next scope throw `ServalRegistryPressureError` while
  the process held nothing at all.
- Fixed: reclamation uses the predicate the admission uses. Stock counts only until its deadline; an
  allocation is held only until its validity plus the grace window passes, after which the server's
  reservation is gone; and a retained operation stops pinning a scope once its reply could no longer
  be adopted — `replayableUntil`, the rule `operationFor` already applied, now shared with
  reclamation. Every reclaimable entry is dropped in one pass, so pressure settles the registry in a
  single scan.
- The property suite caught what the unit cases did not: its conservation identity reads
  `diagnostics().stock`, so a permit that is neither spent, held nor discarded must be *counted* where
  it is dropped. Reclamation now adds discarded stock to `permitsExpired`, and the diagnostic keeps
  reporting the recorded count with the deadline beside it.
- Not covered: a bulkhead scope whose retained modification is past its reply window is still pinned
  by it. The rate limiter's operation has an explicit replay window to reuse; the bulkhead's
  `Modification` does not yet, and the retry path is what settles it today.
- Tests: `test/unit/serval-rate-limit.test.ts` (expired stock, and a retained operation past its
  replay window) and `test/unit/serval-bulkhead.test.ts` (a lapsed allocation, plus a live one that
  must still be refused).

## Documentation gaps

| Area | Status | Evidence |
|---|---|---|
| Outage behaviour | Fixed | `docs/serval/benchmarks.md:91-100` now says observation and settlement failures are reported as degradation and do not fail an otherwise successful call. The quoted line is gone. |
| Warm-authority fault behaviour | Fixed | The lab has a `warm-outage` cell: every replica is served by the healthy authority, then its own connection is cut mid-run. `docs/serval/benchmarks.md` carries the numbers — a delegated replica admitted 8 of 30 with 2 after the cut, against the strict path's 6 with 0 after it, and the cold cell from the same run admitted 0 of 24 on both paths. |
| Tracked-concurrency measurements | Fixed | The column now comes from the bulkhead's own `bulkhead.admitted`/`bulkhead.rejected` events for the delegated policy, so it is measured for every profile: the strict row is the shared Redis count — the largest view across replicas, not a sum — instead of a structural 0, and the delegated rows stay per-process sums. `docs/serval/benchmarks.md` says what the column is and that a closed loop caps it at the replica count, so tracked and physical cannot diverge in the lab; that case stays with the composition suite. The "sampled after the operation finishes" half still does not reproduce. |
| Seeded comparisons | Open | `bench/serval/worker.ts` contains no reference to `seed`; the seed names namespaces and labels only. |
| Authority and reconciliation contracts | Open | Breaker defaults to a fixed incarnation (`breaker.ts:615-616`); the Redis rate-limit and bulkhead issuers contain no incarnation (0 matches); `unfrozenAfterReconciliation` (90, 289) and `reconciliations` (93, 292) have no other reference in the file. |

## Also open, found while validating

- A bulkhead history sees `active + frozen > allocated` when a reply reports a smaller allocation.
  `906d17c` bounds the count in `adopt`, and the property suite is green at the four seeds that
  failed and across ten further runs, but no focused unit case pins the mechanism.
- The two claims in the table above that need reproduction rather than reading are items 2 and 6;
  both now have one, item 2 from the reviewer and item 6 from a unit case and the property suite.
- The warm-outage numbers cannot say *which* layer refused an offer once the cut lands: the
  delegated rate limiter holds a four-permit grant it cannot renew, and the breaker's verdict window
  lapses at one second with `onCoordinatorError` defaulting to fail-open. A breaker-only warm-outage
  cell would attribute the failures; the aggregate deliberately does not guess.
