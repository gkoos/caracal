# Serval review findings

A working list, not a specification. It records one external review of this branch, what each
finding looks like in the code today, and what would close it. Nothing here is a claim about
behaviour that a test proves.

- Review: `feat/serval-authority-protocol` at `d507ea8` — six implementation bugs plus
  documentation gaps.
- Validated against: `eb6d994` plus the four commits on top of it.
- Method: reading the cited code. Only item 3's first half, and the frozen-slot defect noted at the
  end, were reproduced by running something.

## Status

| # | Finding | Status |
|---|---|---|
| 1 | `staleClosed: "fail-closed"` admits requests through the policy | Fixed |
| 2 | Bulkhead replacement grants rejected after the previous grant advances its revision | Fixed |
| 3 | Breaker freshness bypasses the send-time deadline | Half fixed |
| 4 | `maximumOutstandingPermits` does not enforce a permit cap | Fixed before the reset point |
| 5 | Relinquishment during another modification orphans frozen slots | Open |
| 6 | Expired unused quota permanently occupies registry entries | Open |
| D1 | Outage behaviour claim | Fixed |
| D2 | Warm-authority fault behaviour | Open |
| D3 | Tracked-concurrency measurements | Partly open |
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
- Still open: `breaker.ts:297` treats `applied` and `ignored` alike, so an ignored, superseded reply
  still writes the local window.
- Closes with: a window written only for a reply that verified the verdict, plus a test for the
  superseded case.

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
- Closes with: serializing or queuing a different-action modification while keeping the freeze.

## 6. Expired unused quota permanently occupies registry entries

- `src/coordination/serval/rate-limit.ts:254-266`: `reclaimIdle` requires `scope.stock === 0`, so a
  scope whose stock has expired but is recorded as non-zero is never reclaimable, and expiry runs
  only when that scope is touched again.
- Closes with: expiry processed during reclamation, so idle expired stock becomes reclaimable.

## Documentation gaps

| Area | Status | Evidence |
|---|---|---|
| Outage behaviour | Fixed | `docs/serval/benchmarks.md:91-100` now says observation and settlement failures are reported as degradation and do not fail an otherwise successful call. The quoted line is gone. |
| Warm-authority fault behaviour | Open | `bench/serval/lab.ts` points isolated workers at `redis://127.0.0.1:1` at startup, so the outage cells are cold starts and cannot show warm-cache outage behaviour. |
| Tracked-concurrency measurements | Partly open | `bench/serval/worker.ts:190` attaches the local guardrail only for `serval-guarded`, and 209-211 samples only `if (delegated)`, so the strict column is 0 by construction. The "sampled after the operation finishes" half does not reproduce here: the sample is inside the adapter's `execute`. |
| Seeded comparisons | Open | `bench/serval/worker.ts` contains no reference to `seed`; the seed names namespaces and labels only. |
| Authority and reconciliation contracts | Open | Breaker defaults to a fixed incarnation (`breaker.ts:615-616`); the Redis rate-limit and bulkhead issuers contain no incarnation (0 matches); `unfrozenAfterReconciliation` (90, 289) and `reconciliations` (93, 292) have no other reference in the file. |

## Also open, found while validating

- A bulkhead history sees `active + frozen > allocated` when a reply reports a smaller allocation.
  `906d17c` bounds the count in `adopt`, and the property suite is green at the four seeds that
  failed and across ten further runs, but no focused unit case pins the mechanism.
- The two claims in the table above that need reproduction rather than reading are items 2 and 6;
  item 2 already has one from the reviewer.
