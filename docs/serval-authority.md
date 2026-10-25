# Serval authority protocol

This page is the normative protocol companion to [Serval: delegated coordination](serval.md). It
defines the identifiers, the conservative validity arithmetic, the state machines and the
exceptional transitions the contracts depend on. Read it as the specification the
reference model implements.

## Identifiers and versions

| Identifier | Scope | Ordering | Reused? |
|---|---|---|---|
| `namespace` | deployment | - | - |
| `replicaId` | one process lifetime | - | no |
| `authorityIncarnation` | one verified authority lifecycle | only via an external fencing source | no |
| `grantId` | one stable allocation across renewals and resizes | - | no |
| `operationId` | one acquire/renew/resize/relinquish operation, including its retries | - | no |
| `leaseRevision` | monotonic within `(grantId, authorityIncarnation)` | yes | reset by a new grant |
| `epoch` | authority state generation | yes, within an incarnation | reset by a new incarnation |
| `version` | state revision | yes, within an incarnation | reset by a new incarnation |
| `configRevision` | configuration revision | yes | - |

Three rules keep these from collapsing into each other:

1. `epoch`/`version` order **state**; `leaseRevision` orders **one allocation's modifications**; the
   two must never be conflated.
2. `grantId` is not an idempotency key. Two different modifications of the same grant have different
   `operationId`s.
3. An `authorityIncarnation` is opaque. A client never derives an order from it by parsing or by
   numeric comparison.

## Send-time-anchored validity

Every acquisition request is anchored before dispatch:

```text
requestStartedAt : monotonic milliseconds, recorded immediately before the request is written
deadline         : requestStartedAt + validForMs - safetyMarginMs
```

`validForMs` is the **full** lifetime from issuance, so `requestStartedAt + validForMs` is a
conservative upper bound on the authority's expiry regardless of network delay. A reply is usable
only if it is received at or before `deadline`:

```text
usable(reply) = receivedAt <= requestStartedAt + validForMs - safetyMarginMs
```

### Worked examples

**A. Late reply is unusable.** `requestStartedAt = 1000`, `validForMs = 100`, `margin = 0`, so
`deadline = 1100`. A reply at `1150` is rejected; the ledger is unchanged and the policy follows its
degraded behaviour.

**B. A retry cannot mint a fresh window.** Operation `O1` is first sent at `1000`. The reply is lost,
so the client retries at `1300` with the **same** `operationId` and the **same** `requestStartedAt`.
The reply arrives at `1350` with `validForMs = 100`. Because the anchor is still `1000`, the deadline
is still `1100` and the reply is rejected. A retry never advances the anchor.

**C. A renewal extends forward, never backwards.** Grant `G` is usable until `1100`. A renewal
operation `R2` is sent at `1050` with `validForMs = 500`, so its deadline is `1550`. The reply is
received at `1200` and the extension takes effect **on receipt**: admissions are permitted again from
`1200`, and the grant's new deadline is `1550`. The interval `[1100, 1200)` is a gap in which no
admission is allowed, because the renewal's new validity is never back-dated. A renewal cannot
resurrect authority that has already been lost.

**D. A duplicated renewal reply does not extend twice.** `R2`'s reply is delivered twice, both with
`leaseRevision = 4`. The second delivery matches an already-recorded operation result and returns it
without advancing the deadline again. Had the reply carried `leaseRevision = 3`, it would be rejected
as stale, because revision `4` has already been applied.

**E. Clock jumps do not lengthen authority.** A wall-clock jump forward or backward does not change
`requestStartedAt`, `deadline` or any local admission decision; only monotonic elapsed time is read.

## Operation ledger and replay safety

The client keeps a per-identity operation ledger:

| Field | Purpose |
|---|---|
| `operationId` | identity of one logical operation, stable across retries |
| `requestStartedAt` | immutable send-time anchor |
| `deadline` | conservative response deadline derived from the anchor |
| `result` | recorded outcome once the operation settles |
| `retainedUntil` | `deadline + replayRetentionMs`, or a monotonic generation that makes replay impossible |

Rules:

- A retry looks up the ledger entry by `operationId` and reuses its anchor. A retry for an unknown
  `operationId` is a **new** operation with a fresh anchor.
- A settled operation returns its recorded result for every duplicate delivery.
- A ledger entry is retired only after `retainedUntil`. Until then it is a **pinned** record: it is
  never evicted to satisfy a cache bound.
- A reply that arrives after `deadline` is rejected even if the ledger entry has been retired, so
  eviction cannot be used to re-open a stale window.

## Common grant lifecycle

Every policy shares one authority lifecycle; only the payload differs.

| State | Meaning | Allowed transitions |
|---|---|---|
| `absent` | no local authority | `acquiring` |
| `acquiring` | an operation is in flight; the anchor is fixed | `valid` (usable reply), `absent` (late, refused or failed reply) |
| `valid` | local authority exists and `now < deadline` | `valid` (higher-revision renewal), `refreshing`, `draining`, `expired` |
| `refreshing` | a renewal or resize is in flight; the previous deadline still governs | `valid` (confirmed, new deadline), `draining` (previous deadline passed first), `uncertain` (ambiguous outcome) |
| `draining` | no new admissions; existing holders or spent accounting still tracked | `expired` |
| `uncertain` | outcome ambiguous or authority unknown | `valid` (reconciliation proves the allocation is intact), `draining` (conservative resolution) |
| `expired` | `now >= deadline` and nothing pending | `acquiring` (a new issuance, a new `grantId`) |
| `retired` | superseded or relinquished under a newer revision | terminal for that `grantId` |

Invariants over the lifecycle:

- `valid` and `refreshing` are the only states in which **new** admissions may occur, and both
  require `now < deadline`.
- `expired` never transitions back to `valid`; a new issuance is a new grant with a new `grantId`.
- A transition to `valid` carries a strictly higher `leaseRevision` than the state it replaces, or
  it is a duplicate of an already-applied revision and changes nothing.
- `uncertain` never admits.

## Breaker verdict state machine

The cached verdict is a *hint with a protective floor*, never an independent breaker.

| Cached verdict | Condition | Admission decision |
|---|---|---|
| `closed` | `now <= closedFreshUntil` | admit locally, no Redis call |
| `closed` | `now > closedFreshUntil` | configured stale-CLOSED behaviour: fail-open or fail-closed |
| `open` | `now < cooldownEligibleAt` | reject locally, no Redis call |
| `open` | `now >= cooldownEligibleAt`, no probe in flight | start one coalesced authoritative `admitProbe()` |
| `open` | `now >= cooldownEligibleAt`, probe in flight | reject (coalesced) |
| `half-open` | authoritative probe reservation held by this process | admit the reserved probe attempt |
| `half-open` | reservation held elsewhere | reject |
| any | Redis unavailable | cached OPEN rejects; cached CLOSED applies its freshness rule |

Transitions:

- `closed -> open`: applied from the authoritative write reply, which carries
  `(state, generation, version, openedAt, configRevision)`. The client sets
  `cooldownEligibleAt = openedAt + cooldownMs` from that reply, not from a local guess.
- `open -> half-open`: **only** an authoritative `admitProbe()` reservation. A read, a notification
  or the passage of the cooldown alone never produces HALF_OPEN locally.
- `half-open -> closed|open`: applied from the authoritative probe settlement.
- A settlement whose generation predates the current OPEN episode is ignored; it cannot close a newer
  episode.
- The cooldown hint is an **eligibility** hint. After eligibility the process keeps rejecting until
  the probe returns, and if Redis is unavailable it keeps rejecting rather than fabricating a probe.
  This is the all-replicas-cached-OPEN recovery path, and it is a mandatory test.

## Rate grant state machine

| State | Meaning | Transitions |
|---|---|---|
| `absent` | no local stock | `requesting` |
| `requesting` | an issuance operation is in flight | `stocked` (usable reply), `absent` (late or refused) |
| `stocked` | stock remains and `now < deadline` | `stocked` (spend, refill), `depleted`, `expired` |
| `depleted` | stock spent, deadline not reached | `requesting` (refill) |
| `expired` | `now >= deadline`, stock discarded | `requesting` |
| `uncertain` | ambiguous issuance outcome | `stocked` (reconciliation), `absent` (conservative) |

Rules:

- Spending decrements local stock. Stock is **never** replenished by anything other than a new,
  successful issuance reply; an expiry discards the remainder instead of carrying it forward.
- A grant is spendable only within its conservative deadline; a permit held past the deadline is not
  spendable and is not credited anywhere.
- Concurrent local admissions are serialised against the stock counter, so a grant is never spent
  twice.
- The server-side issuance operation is idempotent on `operationId` **for as long as the grant
  it produced is live**: a retry returns the recorded grant instead of deducting tokens twice.
  A refusal is not recorded, because it deducted nothing.
- A grant is a consumable, so nothing reconstructs it: the issuance reply is its only authority,
  and no reconciliation can restore what a replica has already spent.

### Envelope derivation

With server rate `R` (permits/second), burst capacity `B` and grant spend horizon `V`, admissions in
any interval `[a, b]` of length `L` are bounded by `R * (L + V) + B`:

- Every admission in `[a, b]` is backed by a permit issued in `[a - V, b]`. A permit issued before
  `a - V` has a deadline before `a` and cannot be spent in the interval.
- Token-bucket issuance over an interval of length `L + V` is at most `R * (L + V) + B`.
- Therefore admissions, which are a subset of issuance, satisfy the same bound.

Two consequences are stated deliberately, because they are the mistakes the envelope exists to
prevent:

- An issuance bound in an aligned window is **not** an admission bound in every rolling window, which
  is why the bound above is stated over an interval of length `L + V`.
- Unused quota must not be recycled into an earlier accounting window; recycling would break the
  "issued in `[a - V, b]`" premise and permit double spending.

## Bulkhead allocation state machine

`grantId` is fixed for the whole life of the allocation; `leaseRevision` advances on every committed
modification; each modification has its own `operationId`.

| State | Meaning | Transitions |
|---|---|---|
| `absent` | no allocation | `allocating` |
| `allocating` | an acquire is in flight | `allocated` (usable reply), `absent` (late or refused) |
| `allocated` | spendable capacity exists and `now < deadline` | `allocated` (renew, resize, spend, release), `draining` (expiry), `uncertain` |
| `draining` | no new admissions; holders still tracked | `allocated` (higher-revision renewal or resize), `expired` |
| `uncertain` | ambiguous modification outcome; frozen slots stay frozen | `allocated` (reconciliation confirms), `draining` (conservative) |
| `expired` | allocation validity ended | `allocating` (a new `grantId`) |
| `retired` | voluntarily relinquished or superseded | terminal |

Local slot accounting is explicit, and every state maintains:

```text
active + freeSpendable + frozenPendingReturn <= allocated     (before server commitment)
active + freeSpendable                       <= allocated     (after commitment)
```

**Holder lifecycle** is separate from allocation validity: a holder is `active` from admission until
settled. On allocation expiry, new admissions stop and the holders stay recorded. Reclamation after
expiry plus grace may redistribute capacity while old holders still run, producing physical overshoot
that must be measured rather than denied.

The server keeps an allocation's slots reserved for its validity **plus a grace window**, so a replica
that lost a reply can reconcile before the capacity is handed to another replica. The reservation set
carries no TTL for exactly that reason: it is the granted counter's source of truth, and a set that
expired before the next call could not decrement the counter, which would cost the fleet that capacity
permanently. An allocation is identified by the operation that created it, so one replica cannot hold
two allocations at once.

**Renew versus resize versus return:**

| Operation | Effect on `allocated` | Notes |
|---|---|---|
| Renew | none | Idempotent; preserves `grantId`; a new deadline anchored to the renewal's send time |
| Resize up | `+ delta` | Reserves only the delta; the server validates the expected `leaseRevision` |
| Resize down | `- returned`, where `returned <= freeSpendable` | Only free slots may be returned; active holders are never returned |
| Relinquish | `- returned`, then `retired` | Freeze-before-return handoff |

### Freeze-before-return handoff

```text
local                                   server
-----                                   ------
lock allocation
freeze N free slots            ->  freeSpendable -= N, frozenPendingReturn += N
send conditional reduce(op, rev)
                                        validate expected leaseRevision
                                        commit once, dedupe by operationId
                                        capacity becomes reallocatable
reply: confirmed          <- 
retire frozen slots under the old revision

reply: refused/unknown    <-
keep slots frozen until reconciliation proves the old allocation
is still valid and not superseded; only then unfreeze
```

Rules that make the handoff safe:

- The freeze happens **before** the request is sent. A server-side reallocation of voluntarily
  returned slots can therefore never precede the client's freeze.
- A timeout, a disconnect or an unknown outcome never unfreezes. Only authoritative reconciliation
  can prove a non-commit, and a stale reply never restores capacity.
- A new allocation or a successful resize-up grants capacity under a **new** revision, independently
  of the frozen slots, with no double counting.

## Configuration revision and deficit accounting

- `configRevision` is distinct from `epoch`/`version`. A configuration change rolls authority forward;
  it does not silently reinterpret an existing grant under new semantics.
- Issued authority keeps its original semantics and conservative deadline. Notifications of a
  configuration change expedite convergence but do **not** establish revocation.
- On a limit reduction to `newLimit`, the server computes `deficit = outstandingReserved - newLimit`
  and pauses or reduces new issuance until `deficit <= 0`.
- Bulkhead holder records survive a grant expiry or a resize-down: if active occupancy exceeds the new
  target, the replica admits nothing new and keeps the holder records until they release.
- A future-effective change is only admissible if its activation delay exceeds the maximum usable
  validity already issued.

## Authority incarnation, fencing and rollback

`authorityIncarnation` identifies a **verified** authority lifecycle. It is never read out of the
rollback-prone data set, and a random identifier is not sufficient: it neither orders incarnations nor
fences a writer.

Two v1 strategies, chosen per deployment:

**Fenced holdoff.** Valid only with an independently established `T_last_old_issue`:

```text
resume issuance at T_last_old_issue + maximumOldGrantValidity + safetyMargin
```

The deployment must state who fences the old authority, how a successful fence is verified, and how
`T_last_old_issue` is conservatively bounded including detection latency and clock uncertainty. This
is an **admission-authority** boundary only: physical work can outlive it.

**Unfenced competing authority.** If the old authority can keep issuing or renewing, no finite wait
suffices. Issuance resumes immediately, overlapping grants are permitted during the bounded validity
window, and the overlap is an explicit deviation rather than a bounded guarantee. Repeated failovers
repeat the overlap and are measured as such.

**Adoption is monotonic and authoritative:**

- A client adopts a new incarnation only through an authoritative reconciliation or fencing decision.
- A late command reply or notification that advertises an unfamiliar incarnation never switches the
  client on its own.
- Once an incarnation is retired, no delayed message from it can switch the client back, even if its
  numeric epoch looks higher.
- Retired-incarnation rejection knowledge is retained for the maximum late-message window.
- Unknown or unordered competing incarnations put the client into the `uncertain` state with the
  configured conservative admission behaviour.

**Rollback handling.** On reconnect with intact history, versions, epochs and live allocations are
reconciled. On suspected rollback, the session enters the explicit uncertain-authority state, emits
telemetry and applies the chosen strategy above.

## Cache classes and eviction

| Class | Records | Eviction rule |
|---|---|---|
| Disposable | expired and unreferenced state | evictable at will |
| Pinned | live grants, active holders, pending requests, consumed-quota accounting, operation-ledger tombstones | never evicted to satisfy a bound |
| Protective | last-known OPEN / HALF_OPEN knowledge | never silently downgraded to permissive cold start |

- A pinned record is released only when its own retention rule expires, never to make room.
- When the registry cannot admit a new scope, the session refuses the registration with an observable
  error instead of dropping accounting.
- A late reply to a retired entry is discarded. Retired `operationId`s stay protected for
  `replayRetentionMs`, or are fenced by a monotonic generation that makes replay impossible after
  eviction.

## Notification invalidation

An invalidation is a compact versioned hint, not a state command:

```text
{ identity, authorityIncarnation, epoch, version, configRevision, kind }
```

- A subscriber applies it only when it advances the local `(incarnation, epoch, version)`; an older or
  equal version is ignored, so reordering and duplication are harmless.
- An invalidation for an identity the process does not hold is dropped, so fan-out can be broad
  without per-scope subscription management.
- The invalidation never carries a grant payload; the client refreshes through an authoritative read
  or uses the versioned verdict returned by its own write.
- Delivery is best-effort. Correctness comes from expiry, reconciliation and reconnect snapshots.

## Decision-to-test map

| Decision | Reference model entry point | Acceptance case |
|---|---|---|
| 1 Send-time anchoring | `GrantLedger` | late reply, retry anchor, clock jump |
| 2 Cached-OPEN liveness | `BreakerLiveness` | cooldown, coalescing, Redis unavailable |
| 3 History loss | `AuthorityHistory` | fenced holdoff, unfenced deviation |
| 4 Configuration deficit | `ConfigurationLedger` | issuance pause, outstanding grants stay usable |
| 5 Pinning and tombstones | `AuthorityRegistry` | registry pressure, late reply after eviction |
| 6 Tracked vs physical occupancy | `OccupancyLedger` | overlapping retired generations |
| 7 Renew/resize/return | `BulkheadAllocation` | renewal, delta resize, resize-down below occupancy |
| 8 Rate envelope | `RateEnvelope` | rolling bound, no carry |
| 9 Freeze-before-return | `BulkheadAllocation` | frozen during flight, lost reply, reconciliation |
| 10 Fencing and adoption | `IncarnationAdoption` | retired incarnation, unknown incarnation |
| 11 Identity and revision ordering | `GrantLedger`, `BulkheadAllocation` | duplicate renewal, older resize reply |



