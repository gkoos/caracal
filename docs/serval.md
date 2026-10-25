# Serval: delegated coordination

Serval is an **opt-in, explicitly approximate** coordination strategy for Caracal's distributed
policies. It keeps Redis as the authority for shared policy state and allocation accounting, but
lets each process enforce locally held, time-bounded *authority* on the request path. The ordinary
admission path then needs no Redis round trip while valid local authority exists.

> **Status: Phase 1.** This page and [Serval authority
> protocol](serval-authority.md) are the RFC and the protocol contracts. Phase 0 delivered the
> contracts and the executable reference model; Phase 1 delivered the shared session, its
> deterministic test infrastructure and its generated histories. **No Serval option or export ships
> yet** - the session is internal until the policy coordinators land - and the strict Redis
> coordinators (`redisCoordinator`, `redisCircuitBreakerCoordinator`, `redisRateLimitCoordinator`)
> remain the supported distributed implementations. See
> [Implementation status](#implementation-status).

Serval is **not** a drop-in replacement for the strict Redis coordinators. Its consistency and
failure guarantees differ on purpose, and those differences are stated as a deviation envelope
rather than hidden behind the same interface. Do not try to hide Serval behind the current
coordinator interfaces either: today's breaker interface presumes a per-admission read, and the rate
limit and bulkhead interfaces presume a per-action call. Serval therefore needs an explicit
**delegated capability** - an alternate policy path - while the strict implementations stay
untouched.

## Goals

1. One coherent shared infrastructure for identity, replica membership, epochs, authority expiry,
   refresh, notifications, reconciliation, observability and lifecycle.
2. Policy-specific algorithms, not a universal `acquire()`: cached breaker verdicts, consumable rate
   quotas, reusable bulkhead slots.
3. No Redis round trip on the ordinary local-admission path while valid local authority exists.
4. Redis remains authoritative for global state transitions and capacity issuance; its scripts are
   atomic and replay-safe where needed.
5. Explicit, configurable degraded behaviour on cold start, disconnection, lease expiry and
   reconnect.
6. Reuse of Caracal's existing scope and operation identity, configuration protections, cancellation
   semantics and events.
7. Measured overshoot, false rejections, Redis cost, tail latency and downstream harm against the
   strict Redis coordinator and local-only baselines.
8. Composition with local policies as independently configured hard guardrails.

## Non-goals for v1

- Exact equivalence to the strict Redis coordinator under all interleavings.
- Zero overshoot under partitions, crashed replicas or delayed invalidations.
- A general-purpose distributed policy execution engine.
- Cross-policy global optimisation or automatic policy composition.
- Replacing Caracal's local or strict Redis policies.
- Guaranteed physical termination of work merely because a coordination lease expired.

## Core invariants

| Invariant | Meaning |
|---|---|
| No knowingly expired authority | A process never initiates new work on an allocation whose conservative deadline, anchored to the acquisition request **send time**, has passed. A late response cannot extend validity. |
| Monotonic state application | Older epochs or state versions never overwrite newer cached state. |
| No double allocation while authority history is intact | Redis reserves outstanding quotas and slots atomically and deduplicates ambiguous retries. A failover or restore that loses acknowledged issuance may permit overlapping grants; that is a documented deviation, measured separately. |
| Scope isolation | Authority for `(policy kind, policy name, operation, scope)` never authorises another identity. |
| Conservative unknown state | A missing or invalid local grant is never silently treated as a valid one; the policy follows its documented degraded behaviour. |
| Independent local guardrails | A Serval admission never bypasses a composed local policy. |
| Distinct accounting | Observability distinguishes tracked occupancy, Redis-accounted allocation and physical downstream work, and distinguishes approximate fleet targets from local tracked-occupancy ceilings. |
| No unsafe eviction | Active allocations, consumed-quota accounting, pending acquisitions, holder records and protective breaker state are never discarded to meet a cache bound. |
| Configuration changes are not instantaneous revocations | Disconnected clients may spend previously issued authority until its conservative expiry; new issuance accounts for outstanding grants or pauses. |

## Architecture

```text
Caracal runtime / policy composition
    |                |                 |
  breaker         rate limit         bulkhead
    |                |                 |
 cached verdict   local quota       local slot pool
    \                |                 /
      Serval coordination session (per process)
      - identity and replica ID; scope registry
      - bounded local authority state
      - epoch/version handling; expiry
      - invalidation, refresh, reconciliation
      - connection lifecycle, metrics, backpressure
                        |
          Redis authoritative scripts/state
          - breaker observations/transitions
          - quota issuance/accounting
          - bulkhead allocation/accounting
                        |
             notification channel
          (wake-up hint, not the sole correctness path)
```

One **session per process** owns the connection lifecycle, the notification subscription, the bounded
caches, refresh scheduling and cleanup. A session may serve many policy identities. The shared
infrastructure never interprets policy semantics; it carries a small tagged protocol internally
(`BreakerVerdict | RateGrant | BulkheadAllocation`) with policy-specific operations and lifecycle
rules.

### Identity and ownership

- **Authority key:** `(namespace, policy kind, policy name, operation, scope)`, retaining the current
  hash-tagging and cluster-slot scheme so one identity's keys stay in one slot.
- **Replica ID:** stable for a process lifetime. A restart receives a new **incarnation**, and is
  never entitled to its predecessor's grants.
- **Record fields:** each authority record carries `authorityIncarnation`, `epoch`, `version` (where
  relevant), `grantId`, `operationId`, `leaseRevision`, `issuedAt`, `expiresAt`, `configRevision`
  and a policy-specific payload.
- **Two distinct revision spaces:** state revisions and configuration revisions. A configuration
  change rolls authority forward or invalidates it; it never silently reinterprets an old grant.

### Grant validity is anchored to the request send time

A client timestamps the start of each acquisition request with a **monotonic** clock. A reply carries
the server-issued validity duration; the client computes

```text
deadline = requestStartedAt + validForMs - safetyMarginMs
```

and never `receivedAt + validForMs`. A reply that arrives after `deadline` is unusable. Retries and
duplicate replies for the same operation preserve that operation's original request-start anchor;
they must not extend it. Only a *distinct, successful renewal* may extend a grant, under a higher
`leaseRevision`. Redis server time governs issuance and accounting, but client wall-clock
synchronisation is **not** assumed.

A Redis-stored epoch cannot survive a rollback of the same data set. Incarnation identifiers are
fencing and diagnostic metadata, not proof of instant revocation. See
[Serval authority protocol](serval-authority.md) for the state machines and the worked examples.

## The eleven mandatory decisions

Phase 0 settles each decision as a concrete v1 contract. Every decision below names the acceptance
case that exercises it; the cases live in `test/unit/serval-authority.test.ts` (deterministic
scenarios) and `test/property/serval-authority.property.test.ts` (generated adversarial histories).

### 1. Send-time anchoring, late and duplicate replies, conservative time

- A client records a monotonic `requestStartedAt` immediately before dispatch.
- `deadline = requestStartedAt + validForMs - safetyMarginMs`; a reply arriving after it is rejected
  and leaves local authority unchanged.
- A retry reuses the operation's `operationId` **and** its original `requestStartedAt`.
- A duplicate or replayed reply for an already-settled operation returns the recorded result and
  changes nothing.
- Client time is monotonic elapsed time only. Server time governs issuance; the contract assumes no
  wall-clock synchronisation and no bounded clock offset between processes.
- `safetyMarginMs` is a configured budget that is only ever subtracted, never added.

Acceptance: *decision 1* - "a 100 ms grant whose reply arrives after 150 ms is unusable", "a retried
reply never resets the deadline", "clock jumps do not lengthen a grant".

### 2. Cached-OPEN cooldown and probe liveness

- A cached OPEN rejects **before** cooldown eligibility. Rejection is protective and local.
- The write reply that opened the breaker carries an authoritative cooldown hint
  (`openedAt`, `cooldownMs`), so `cooldownEligibleAt` is server-derived, not guessed.
- Once `now >= cooldownEligibleAt`, one eligible request per identity attempts authoritative
  `admitProbe()` even while the cached verdict still says OPEN. Concurrent local attempts are
  coalesced into that single in-flight probe.
- The cooldown's **origin** is the instant this process adopted the OPEN verdict, and its length is
  the policy's `openMs`. The strict protocol reports the server's open time but not the server's
  clock, so a client cannot convert that timestamp into a local deadline; anchoring to the adoption
  instant instead is conservative, because a verdict is adopted at or after the moment the breaker
  opened. A probe that still arrives early - a shortened cooldown, or a client clock that leads the
  server's - is refused by the authority, and the refusal re-anchors the local cooldown so one skewed
  replica cannot probe on every request. The open time is retained for diagnostics and for a later
  phase that can measure the clock offset.
- A read alone never performs OPEN to HALF_OPEN. HALF_OPEN exists only as an authoritative
  reservation. If Redis is unavailable, the process rejects rather than fabricating a probe.
- A cached CLOSED admits only within its configured freshness allowance; after that the explicit
  fail-open/fail-closed setting applies.

Acceptance: *decision 2* - "all replicas cached OPEN, cooldown elapsed, no observations", "probes are
coalesced", "Redis unavailable while OPEN rejects instead of probing".

### 3. Redis authority-history loss, incarnation identity, holdoff or overlap

Redis asynchronous replication can lose acknowledged issuance on promotion or restore. v1 chooses
**one** strategy per deployment, and refuses to claim a holdoff it cannot verify:

- **Fenced holdoff (preferred, when a trustworthy fence exists).** If the deployment supplies an
  independently established `T_last_old_issue` (the last possible old issuance or renewal time) and
  a verified fence that stops the old authority from issuing, new issuance pauses for
  `T_last_old_issue + maximumOldGrantValidity + safetyMargin`.
- **Unfenced competing-authority deviation (default when no fence can be verified).** Issuance
  resumes immediately and overlapping grants are permitted during the bounded validity window.
  There is **no** bounded single-event overlap claim; local guardrails carry the protection and the
  physical-work overshoot is measured. Repeated failovers repeat the overlap.

An `authorityIncarnation` is an opaque identifier of a verified authority lifecycle, sourced from
outside the rollback-prone data set (or from the deployment's fencing source). A counter stored in
Redis is **not** sufficient, and a random identifier neither orders incarnations nor fences a writer.
Ordinary reconnect with intact history reconciles versions and live allocations; suspected rollback
enters an explicit uncertain-authority state and emits telemetry.

Acceptance: *decision 3* - "fenced rollback pauses issuance until the boundary", "unfenced rollback
resumes issuance and is reported as a deviation".

### 4. Configuration revision, effective time and deficit accounting

- A configuration update cannot instantly revoke disconnected grants. Previously issued authority
  may be used until its conservative deadline; notifications expedite convergence but do not
  establish revocation.
- For a limit reduction, the server computes `deficit = outstandingReserved - newLimit` and **pauses
  or reduces new issuance until the deficit clears**.
- Bulkhead holder records survive grant expiry and downsizing; a resize-down below active occupancy
  admits no new work until capacity becomes free under a valid allocation.
- If a future-effective change is adopted, its activation delay must exceed the maximum existing
  usable validity.
- Breaker cached-CLOSED freshness, rate-grant spend horizon and bulkhead new-admission allocation
  lifetime are **separate** budgets, not one shared TTL.

Acceptance: *decision 4* - "a limit reduction pauses issuance while the deficit is outstanding",
"outstanding grants stay usable after the revision change".

### 5. Pinned records and replay tombstones

Entries fall into three classes:

| Class | Examples | Eviction |
|---|---|---|
| Disposable | expired, unreferenced state | freely evictable |
| Pinned | active grants, holders, pending requests, consumed-quota accounting, deduplication tombstones | never evicted to satisfy a bound |
| Protective | last-known OPEN / HALF_OPEN knowledge | never silently downgraded to permissive cold start |

An `operationId` remains protected for at least the maximum late-response/retry window
(`replayRetentionMs`), or is guarded by an equivalent monotonic generation that makes replay
impossible after eviction. When registry capacity is exhausted, the session **refuses new scope
registration with an observable error** instead of dropping accounting. A late reply to a retired
entry never resurrects authority.

Acceptance: *decision 5* - "registry pressure refuses a new scope instead of evicting pinned
records", "a late reply after eviction cannot mint authority".

### 6. Tracked versus physical occupancy

Serval's bulkhead target is an **approximate fleet allocation**, not a physical concurrency bound.
The local guardrail bound is:

```text
tracked occupancy <= sum(L_i) over every process incarnation with tracked live holders
```

For `N` concurrently relevant incarnations with identical limit `L`: `tracked occupancy <= N x L`.
Retired incarnations whose remote work is still running must be counted for any physical claim.
Tracked occupancy is **not** physical concurrency: fetch may resolve at response headers,
abort-unsupported work may continue, and remote work can outlive a crashed process. v1 therefore
makes **no unqualified global physical bound**; physical work is measured independently in the
benchmark and fault lab.

Acceptance: *decision 6* - "the tracked-occupancy ceiling holds for overlapping retired
generations", "physical work may exceed the ceiling and is reported as a deviation, not a violation".

### 7. Bulkhead renew, resize and free-only return

- Renewing an existing allocation is **not** an additional grant. A renewal preserves grant identity
  and accounting, or atomically replaces it with a versioned successor without counting both.
- Resize-up reserves only the delta. Resize-down returns **free** slots only.
- If active occupancy exceeds the new target, the replica admits nothing new and retains the active
  holder records until release.
- Reconnect cannot forget existing holders.
- `grantId` is stable across renewals; each modification carries its own `operationId`; a successful
  change advances `leaseRevision`.
- Allocation validity (may I start *new* work?) and holder lifetime (is started work still active?)
  are separate clocks. On allocation expiry, new admissions stop; existing work continues.

Acceptance: *decision 7* - "a renewal does not double the allocation", "resize-up reserves only the
delta", "resize-down below occupancy stops new admissions without forgetting holders".

### 8. Rate issuance-and-spending envelope

The rate limiter is GCRA, so delegated quota needs its own explicit contract rather than an
"exact distributed GCRA" label. v1 chooses **token-bucket issuance with send-time-anchored spend
horizons and no carry**:

- The server holds one token bucket per identity: rate `R` permits/second, burst capacity `B`. A
  grant of `n` permits deducts `n` tokens at issuance, atomically, deduplicated by `operationId`.
- A grant's spend horizon is `validForMs = V`. Permits are spendable only within
  `requestStartedAt + V - margin`.
- Unused stock simply expires; it is **never** credited back into a later accounting window, so a
  permit cannot be spent twice.
- Issuance is refused, with a `retryAfterMs` hint, when the bucket is empty or when granting would
  exceed the configured maximum outstanding stock.

The resulting rolling-window bound, for any interval of length `L`:

```text
admissions(L) <= R * (L + V) + B
```

*Why:* every admission is backed by a permit issued no earlier than `L + V` before the interval's
end (an older permit has expired), and a token bucket admits at most `R * elapsed + B` issuance over
any interval. This bounds **admissions**, not issuance, and holds only under intact authority
history. Adaptive grant sizing is a Phase 3 optimisation, not part of the envelope.

Acceptance: *decision 8* - "a generated history with spend-at-expiry never exceeds
`R * (L + V) + B`", "unused quota is not credited back".

### 9. Freeze-before-return and ambiguous handoff

Capacity return is a two-sided transfer of admission authority, not a server-side decrement:

1. Under the local allocation lock, **freeze the free slots first** and remove them from the
   spendable pool *before sending* any reduction. Active holders are never returned as free slots.
2. Record `operationId`, expected `leaseRevision`, requested amount and frozen count, then send an
   idempotent conditional reduction. The server validates the expected revision, commits once, and
   only then makes the capacity reallocatable.
3. While the reply is delayed, lost or uncertain, frozen slots stay unavailable. A timeout or
   disconnect must not unfreeze them.
4. On confirmed success the frozen slots are retired under the old revision. Unfreezing on a proven
   non-commit requires authoritative reconciliation showing the old allocation is still valid and
   not superseded. A stale reply never restores capacity.
5. A new allocation or a successful resize-up grants spendable capacity through a **new revision**,
   independently of the frozen slots, with no double counting.

For an intact allocation, `active + freeSpendable + frozenPendingReturn <= allocated` before the
server commits; afterwards the frozen slots leave the local allocation entirely. Expiry-based
reclamation is a distinct, explicitly approximate path that may overlap continuing work.

Acceptance: *decision 9* - "frozen slots are unavailable while the reply is in flight", "a lost
reply does not unfreeze", "reconciliation, not a stale reply, restores capacity".

### 10. Fencing, last-old-issuance boundary, repeated failover, monotonic adoption

- A holdoff is meaningful only with an independently established `T_last_old_issue`. Phase 0
  requires the deployment to name who fences the old authority, how successful fencing is verified,
  and how the timestamp is conservatively bounded including detection and clock uncertainty.
- If an old primary can keep issuing or renewing, **no finite wait suffices**; the session must use
  the unfenced competing-authority deviation model and stop claiming a bounded overlap.
- A client adopts a new incarnation only through an authoritative reconciliation or fencing
  decision. A late command reply or notification advertising an unfamiliar identifier never switches
  the client. Once an incarnation is retired, no delayed message from it can switch the client back,
  even if its numeric epoch looks higher.
- Retired-incarnation rejection knowledge is retained for the maximum late-message window. Unknown
  or unordered competing incarnations put the client into the uncertain-authority state with the
  configured conservative admission behaviour.

Acceptance: *decision 10* - "a delayed reply from a retired incarnation is ignored", "an unknown
incarnation yields the uncertain state rather than silent adoption".

### 11. `grantId`, `operationId`, `leaseRevision` and replay-safe renewal ordering

- `grantId` identifies a stable allocation across renewals and resizes; it is **not** an idempotency
  key for every modification.
- `operationId` identifies **one** acquire/renew/resize/relinquish operation including all of its
  retries, with an immutable `requestStartedAt` and response deadline retained until the operation
  can no longer be replayed.
- `leaseRevision` is a monotonic version within a grant and authority incarnation. A successful
  renewal or resize advances it. An older response cannot overwrite capacity, spendability, expiry
  or frozen-state decisions made at a newer revision.
- Modifications to one allocation are serialised in v1, with a Redis-side compare-and-swap on the
  expected lease revision plus an idempotent operation-result lookup. A duplicate after a later
  revision has committed returns the recorded result without re-applying it, and the client must not
  regress.
- A renewal's new deadline is anchored to **that renewal operation's** original send time. The old
  usable deadline is kept until the renewal is confirmed; after an uncertain outcome the client does
  not assume extension.

Acceptance: *decision 11* - "a duplicate renewal reply cannot extend expiry twice", "an older resize
reply cannot restore retired capacity", "`grantId` is stable across a legitimate renewal".

## Per-policy contracts

Each policy has a falsifiable correctness contract and a permitted-deviation envelope. "Invariant"
means a property the reference model asserts as a hard failure; "deviation" means a measured
difference from strict Redis semantics that the implementation must report rather than conceal.

### Circuit breaker: cached verdict plus centralised evidence

| Aspect | v1 contract |
|---|---|
| Local fast path | Cached CLOSED admits within `freshnessMs`; cached OPEN rejects until `cooldownEligibleAt`; at or after eligibility a coalesced authoritative `admitProbe()` is attempted even while OPEN is cached |
| Settlement | Classified outcome plus admission generation sent to Redis, reusing the existing window, UUID deduplication, generation checks, configuration validation and probe settlement semantics |
| Response extension | Atomic observe/settle replies return `(state, generation, version, openedAt, configRevision)` so the reporting replica refreshes without a read |
| Invariant | A cached OPEN never transitions to HALF_OPEN without an authoritative probe reservation; a settlement from an obsolete generation cannot close a newer OPEN episode |
| Deviation | Propagation delay admits extra work at the moment of failure; measured as paired excess against a reference |
| Not changed | Thresholds, windows, probe counts, classification and cancellation semantics |

Outcome settlement is **awaited in `finally`** initially, exactly as the strict coordinator does
today. Removing the admission read does not remove settlement latency, and the two improvements are
measured separately. Bounded async outcome queues or batching are a later, separately benchmarked
experiment with explicit lost-evidence and overload semantics.

### Rate limiter: delegated admission quotas

| Aspect | v1 contract |
|---|---|
| Server authority | One token bucket per identity (rate `R`, burst `B`); issuance deducts tokens atomically and deduplicates by `operationId` |
| Grant | `validForMs = V`, spend deadline anchored to request send time |
| Refill | Start small, replenish when local stock falls below a threshold, size requests from recent demand, expire unused grants |
| Invariant | No double spending; no local admission beyond granted quota; the rolling bound `R * (L + V) + B` holds under intact authority history |
| Deviation | Hot-replica starvation while cold replicas hold unused quota; burst distortion at boundaries; temporary excess from stale or duplicate grants |
| Explicitly not claimed | Exact distributed GCRA semantics |

Retry-after semantics: when local quota is empty but global capacity remains, the client attempts a
bounded synchronous replenish and returns `retryAfterMs` from the server hint, or the local refill
estimate when the server hint is absent. Redis rollback behaviour is covered by decision 3.

### Bulkhead: delegated reusable slots

| Aspect | v1 contract |
|---|---|
| Server authority | A maximum local slot budget per replica, with `grantId`, `leaseRevision` and per-operation `operationId` |
| Local authority | The process acquires and releases within its allocation without a Redis call per operation |
| Renewal | Idempotent; never an additional grant; cannot resurrect an allocation whose authority was lost |
| Resize | Up reserves only the delta; down returns only free slots |
| Return | Freeze-before-return handoff (decision 9) |
| Invariant | No local over-admission relative to the active allocation; expired allocation admits no new work; holders survive reconnect |
| Deviation | Expiry-based reclamation may overlap continuing work, producing measurable physical overshoot |
| Not claimed | A strict global physical concurrency bound |

## Guarantees and deviations

| Area | v1 stance | Evidence required |
|---|---|---|
| Breaker stale CLOSED | Short bounded freshness, explicit fail-open/fail-closed fallback | Excess admissions versus Redis outage availability |
| Breaker OPEN during outage | Retain protective rejection; authoritative probe required | Recovery time and false rejects |
| Outcome writes | Awaited initially; async or batch as a separate experiment | p99 impact, dropped evidence, overload behaviour |
| Rate semantics | Explicit approximate quota envelope, not exact GCRA | Window and burst violations, fairness |
| Rate grant sizing | Small initial grants, adaptive refill | Redis RTT versus stranded capacity under skew |
| Bulkhead reclamation | Expiry plus configurable grace | Peak and duration overshoot under partition and a slow dependency |
| Local guardrails | Recommended, independently configured | Measured bound with autoscaling and abort-unsupported work |
| High-cardinality scopes | Bounded cache and subscriptions, idle cleanup | Memory, connections, Redis key count |
| Configuration changes | Old grants remain usable until conservative expiry; issuance pauses while the deficit is outstanding | Stale-use duration, outstanding deficit, convergence |
| Redis failover or history loss | Explicit authority incarnation; a fenced last-old-issuance boundary is required for any finite holdoff, otherwise the unfenced competing-issuance deviation applies | Ledger rollback, old-primary continued issuance, repeated failover, delayed retired-incarnation replies |
| Bulkhead return handoff | Freeze free slots before the idempotent Redis reduction; keep them frozen through an uncertain outcome | Delayed or lost reply with concurrent reallocation; reconciliation before unfreeze |
| Renewal identity and ordering | Stable grant ID, per-operation ID, monotonic lease revision; serialised modification in v1 | Duplicate renewal, reordered resize, stale deadline replay, lost response |

## Fault-assumption matrix

Which invariants hold, and under what assumption. Nothing outside this table is claimed.

| Assumption | Held | Not held |
|---|---|---|
| No partition, Redis available | All invariants, including no double allocation and the rate rolling-window bound | - |
| Bounded clocks, monotonic elapsed time on the client | Send-time anchoring and conservative deadlines | - |
| Redis unreachable, breaker cached OPEN | Protective rejection; no fabricated probe | Cached-CLOSED freshness is not extended by an outage |
| Redis unreachable, breaker cached CLOSED | Within `freshnessMs` only, then the configured fallback | Unlimited admission |
| Redis unreachable, rate or bulkhead grant | Spending within the remaining conservative validity only | New issuance; no invented permits |
| Redis unreachable, grant exhausted | Configured reject or synchronous fallback | Silent fail-open without configuration |
| Client wall-clock skew | Irrelevant: only monotonic elapsed time is used | No server/client clock agreement is assumed |
| Redis history intact after reconnect | Version, epoch and live-allocation reconciliation | - |
| Redis history lost, fence verified | Bounded issuance holdoff from `T_last_old_issue` | Physical work continues past the boundary |
| Redis history lost, no fence | Explicit competing-issuance deviation, measured | No bounded overlap claim; repeated failover repeats the overlap |
| Delayed or lost notification | Eventual convergence via expiry and reconciliation | Prompt invalidation is not guaranteed |
| Process crash or retirement | Tracked occupancy for that incarnation stops growing; the ceiling is recomputed | Remote work may continue: not a physical bound |

## Lifecycle and degraded behaviour

Session states: `uninitialized`, `reconciling`, `valid`, `refreshing`, `stale`, `disconnected`,
`uncertain`, `closed`. Cold start is distinguished from a warm cache, and a Redis failure while
CLOSED is distinguished from one while OPEN.

- **`disconnected`** means the process cannot reach the authority: the subscription is down, or no
  reconciliation has ever succeeded while a read has failed.
- **`stale`** means the last successful reconciliation is older than its allowance, so cached
  authority may be outdated even though the connection is up.
- **`uncertain`** is entered on a suspected rollback - a snapshot older than what is cached, or a
  retired incarnation reappearing - or on an unfamiliar competing incarnation. It is sticky: only an
  authoritative reconciliation that sees no rollback clears it.

- **Cold start:** no local authority. The policy follows its configured cold-start behaviour; a
  missing grant is never treated as a valid one.
- **Disconnection:** local authority remains spendable until its conservative deadline; new issuance
  is unavailable. Cached protective state is retained.
- **Reconnect:** authority-incarnation, epoch and configuration reconciliation runs **before** newly
  granted authority is used. Incarnation changes do not instantly revoke grants held by disconnected
  replicas; the issuance holdoff/overlap contract applies.
- **Shutdown:** stop new admissions and relinquish best-effort. Safety never depends on a clean
  shutdown.

## Notifications

- A Redis state transition publishes a compact **versioned invalidation**, never an imperative
  unversioned state command.
- Fan-out must reach every interested process. A Redis Streams consumer group shared by replicas
  load-balances messages rather than broadcasting them, so a consumer group is **not** the fan-out
  mechanism; the design uses a per-process cursor or subscription that broadcasts.
- A write response is also a cache-refresh channel: the atomic write returns the authoritative
  versioned verdict so no second read is needed.
- Notifications can be delayed or lost; periodic reconciliation, expiry and reconnect snapshots
  provide eventual convergence.
- v1 starts with the simplest transport that is easy to validate. Streams are not committed to
  before subscriber fan-out and scope-cardinality costs are measured.

## Cache classes, cardinality and errors

- Registry entries are classified as disposable, pinned or protective (decision 5). Live grants,
  active holders, pending requests, consumed-grant replay guards and retained protective knowledge
  are pinned or represented by a safe tombstone.
- Under memory pressure the session **refuses new scope registrations** with an observable error
  rather than evicting live accounting.
- Late replies to retired entries cannot resurrect grants; retired request identifiers stay
  protected for the replay window or are fenced by a monotonic generation.
- Cache and subscription cardinality is bounded and observable, especially for high-cardinality
  scopes, with idle cleanup.

## Composition with local policies

Serval provides approximate fleet-wide coordination; local policies provide deterministic
per-process admission ceilings. If every request traverses a local bulkhead of limit `L_i` in each
active process `i`, and the permit is held until downstream work truly completes, then tracked
occupancy is at most the sum of `L_i` over processes with tracked live holders. A local rate limiter
bounds aggregate admissions by the sum of local rates plus their burst envelopes. A local breaker
reacts to failures visible to that process even when fleet-wide evidence is delayed.

Composition must be tested for: policy ordering; retries and per-attempt admission; admission
refusals not being misclassified as downstream failures; abort-aware versus abort-unsupported
adapters; local queue behaviour; one scope shared by different operations; several policies on one
call; and local versus distributed event classification.

Recommended deployment guidance: expose Serval's approximate target alongside an optional local
guardrail recipe, not as an implicit mandatory coupling. Local limits can strand capacity, and
autoscaling changes the aggregate bound.

## Public API

Illustrative only; the syntax is finalised in Phase 0 review, and no option is exposed until its
semantics are implemented and tested. Existing `circuitBreaker.distributed`,
`rateLimit.distributed` and `bulkhead.distributed` keep working unchanged.

```ts
const serval = createServalSession(redis, {
  namespace: "orders:prod",
  // lifecycle, notification and reconciliation configuration
})

const breaker = circuitBreaker.distributed({
  name: "payments",
  scope: (ctx) => String(ctx.metadata.tenantId),
  coordinator: serval.circuitBreaker({ freshnessMs: 100 }),
})

const rate = rateLimit.distributed({
  name: "payments-rate",
  rate: 100,
  burst: 20,
  scope: (ctx) => String(ctx.metadata.tenantId),
  coordinator: serval.rateLimit({ /* quota policy */ }),
})

const capacity = bulkhead.distributed({
  name: "payments-capacity",
  limit: 80,
  scope: (ctx) => String(ctx.metadata.tenantId),
  coordinator: serval.bulkhead({ /* allocation policy */ }),
})

// Compose local policies independently as hard per-process guardrails.
```

The current policy interfaces call the coordinator per admission or per action, so the delegated
path must be explicit: the session-backed coordinator advertises a delegated capability and the
policy takes the cached path, rather than silently issuing a Redis call per admission.

## Implementation status

Phases 0 to 6 are implemented: the strategy ships as the opt-in, experimental
`@gkoos/caracal/serval` entry point, documented in `docs/serval-api.md`, with its
measured trade-offs in `docs/serval-benchmarks.md` and a scheduled fault-lab workflow.

The session and the three delegated coordinators are reachable **only** through that
entry point. The root package, `caracal/redis` and the three policy factories are
unchanged, so opting in is a coordinator argument rather than a configuration flag.

| Phase | Delivered |
|---|---|
| 0 | These two pages, and the executable reference model with one acceptance group per mandatory decision |
| 1 | The shared session under `src/coordination/serval/`: `authority.ts` (vocabulary, versioning, conservative expiry, incarnation tracking), `cache.ts` (the bounded pinned registry), `metrics.ts`, `scheduler.ts`, `notifications.ts` (versioned invalidations and the Redis pub/sub transport), `reconciliation.ts` and `session.ts`; plus the deterministic test infrastructure and the generated histories |
| 2 | `breaker.ts`: the cached-verdict breaker. It is a `BreakerCoordinator`, so `circuitBreaker.distributed({ coordinator })` opts in with no change to the policy or to the strict coordinator. The strict observe/settle replies now carry the server's open time and the configuration revision, so a replica that saw a transition refreshes without a read |
| 3 | `rate-limit.ts`: delegated admission quotas, plus `rateGrantV1` and `redisRateLimitGrantIssuer` in `caracal/redis`. It is a `RateLimitCoordinator`, so `rateLimit.distributed({ coordinator })` opts in unchanged. Grants are drawn from the **same** GCRA state the strict coordinator uses, so the delegated path is not a second budget |
| 4 | `bulkhead.ts`: delegated slot allocations, plus `bulkheadAllocationV1` and `redisBulkheadAllocationIssuer` in `caracal/redis`. It is a `BulkheadCoordinator`, so `bulkhead.distributed({ coordinator })` opts in unchanged. The allocation hash has a key suffix of its own, so it can never land on the strict coordinator's lease set |

Three things Phase 3 settles, because they are easy to get wrong:

- **The rate limiter has no session, no snapshot and no reconciliation.** A breaker
  verdict is re-derivable state, so a snapshot converges it. A quota grant is a
  *consumable*: no read can reconstruct what a replica holds, so its authority is the
  issuance reply itself and the write response is its only refresh channel.
- **A retry is deduplicated server-side, and it cannot extend the window.** The
  issuance ledger records a granted operation for exactly as long as its grant is
  live, so an ambiguous reply - a timeout that may or may not have committed - is
  recoverable by retrying the same `operationId`, and the recovered grant keeps the
  *original* send-time anchor. A refusal is deliberately not recorded: it deducted
  nothing, and re-evaluating it is what the caller asked for.
- **The configured maximum outstanding stock is enforced as a bound on live
  grants.** The server counts live grants (`floor(maximumOutstandingPermits /
  grantSize)` of them) rather than summing live permits, which is an O(1) check and
  is conservative for any request size. A replica that hoards is bounded by it, and
  the capacity it strands frees up when its grants expire.

Retry-after is explicit: a refusal carries the server's own hint, a local refusal
after losing the race for stock carries the shortest honest wait, and a reply that
arrived too late to be adopted carries the local estimate of one horizon - the
documented fallback when the server hint is absent.

Phase 4 adds `bulkhead.ts`: delegated slot allocations, plus `bulkheadAllocationV1`
and `redisBulkheadAllocationIssuer` in `caracal/redis`. It is a `BulkheadCoordinator`,
so `bulkhead.distributed({ coordinator })` opts in unchanged. Four things it settles:

- **The fleet limit is a sum of allocations.** The server keeps one counter of granted
  slots and updates it atomically with every commit, and it validates every
  modification against the `leaseRevision` the caller expects, so a reply that lost a
  race can never restore capacity, spendability or a deadline.
- **A reservation outlives the validity it was granted for.** The allocation's validity
  decides whether *new* work may start; the reservation keeps its slots reserved for a
  further grace window, which is what lets a replica reconcile a lost reply before the
  capacity is handed to someone else. The reservation set is deliberately not given a
  TTL: it is the counter's source of truth, and an expiring set would leave the fleet's
  counter inflated for good.
- **Allocation validity and holder lifetime are separate clocks.** When the validity
  lapses, new admissions stop and every holder stays recorded, and a holder keeps
  renewing its own permit. A holder that outlives its allocation is physical work the
  logical accounting cannot bound, which is why the integration suite *measures* the
  tracked overshoot instead of claiming a global concurrency bound.
- **Returned capacity is frozen before it is sent, and only reconciliation unfreezes
  it.** A reduction is submitted after the slots leave the local spendable pool; an
  unknown outcome keeps them frozen, and the retry of the same `operationId` is what
  settles it - idempotently, with the original send-time anchor, so a late commit
  cannot extend anything either.

Phase 5 composes the three policies in one operation and runs the fault lab:

| Phase | Delivered |
|---|---|
| 5 | `test/integration/serval-composition.integration.test.ts`: local and delegated layers together, per-attempt admission under retry, refusals that stay out of breaker evidence, cancellation with a dependency that ignores it, a Redis outage that leaves local authority intact, and bounded shutdown. `bench/serval/`: a multi-process fault lab over four profiles, five fault cells and paired seeds, reporting Redis cost, admission outcomes, tracked occupancy and the dependency's own physical concurrency |

Three findings from that phase are worth stating, because they shape the operating
envelope rather than the implementation:

- **A refusal is not evidence, and a settlement is not free.** A rate or bulkhead
  refusal never reaches the breaker's window, and a delegation does not remove the
  awaited observation write: the lab measures roughly 1.7 Redis commands per offered
  request for Serval against 5.0 for the strict coordinators, not zero.
- **Redis unavailable stops a composed pipeline on both paths.** The delegated layers
  keep their local authority - which is what the rate limiter and the bulkhead are for -
  but the distributed breaker's settlement is awaited by design, so a call fails rather
  than pretending its outcome was recorded. A deployment that must survive an outage
  should not put the distributed breaker on that path.
- **Tracked occupancy is asserted; physical concurrency is measured.** The lab accounts
  for the dependency's own work, including attempts that were abandoned and kept
  running, so the two numbers are reported separately rather than conflated.

`docs/serval-benchmarks.md` carries the measured table, the recommended envelopes and
the known limitations.

The session is deliberately **not** reachable from the root entry point: it is exported
from `@gkoos/caracal/serval`, so opting in is explicit and the strict coordinators stay
the default. The public surface - exports, configuration, lifecycle, guarantees,
failure behaviour, metrics, guardrails and migration - is documented in
`docs/serval-api.md`.

## Executable contracts

Phase 0's contracts are executable, not prose alone:

| Artefact | Role |
|---|---|
| `test/support/serval/reference-model.ts` | Deterministic reference model: monotonic time, grant ledger, breaker liveness, authority history and fencing, configuration deficits, cache classes, occupancy accounting, bulkhead allocation with freeze-before-return, and rate envelope accounting |
| `test/support/serval/histories.ts` | Seeded adversarial history generator (delayed, duplicated, reordered and dropped replies; rollback; eviction; cooldown) |
| `test/unit/serval-authority.test.ts` | One acceptance group per mandatory decision, with the deterministic worked examples, plus the shared authority vocabulary |
| `test/property/serval-authority.property.test.ts` | The same invariants over generated adversarial histories, honouring `CARACAL_TEST_SEED` |

Phase 1's session has its own deterministic infrastructure and cases:

| Artefact | Role |
|---|---|
| `test/support/serval/deterministic-scheduler.ts` | A virtual clock and task queue: time moves only when a test advances it, and pending timers are countable |
| `test/support/serval/fake-authority.ts` | A scriptable authority: versioned records on the server's clock, plus read failures, a rewound ledger, a held read and a failover |
| `test/support/serval/notification-transport.ts` | A transport whose delivery, duplication, disconnect and failed open the test controls |
| `test/support/serval/session-history.ts` | The generated session history with its own oracle for deadlines, versions and bounds |
| `test/unit/serval-cache.test.ts` | Record classes, pinning, tombstones and refusal under pressure |
| `test/unit/serval-session.test.ts` | Identity, the scope registry, lifecycle, authority application, reconciliation and diagnostics |
| `test/unit/serval-notifications.test.ts` | The invalidation codec, the Redis transport and the channel |
| `test/unit/serval-reconciliation.test.ts` | The reconciler on its own: coalescing, malformed snapshots, rollback and a clean stop |

Phase 2 adds the breaker's own cases:

| Artefact | Role |
|---|---|
| `test/support/serval/fake-breaker-authority.ts` | A scriptable strict coordinator: a small state machine that counts every call, can fail the next ones, and can hold a probe open so an in-flight attempt is observable |
| `test/support/serval/breaker-history.ts` | The generated breaker history, checking each admission against the authority's own state |
| `test/unit/serval-breaker.test.ts` | The fast path, the cooldown, probe coalescing, outage protection, convergence, the stale-CLOSED behaviours and the end-to-end policy path |
| `test/property/serval-breaker.property.test.ts` | The same invariants over generated histories, honouring `CARACAL_TEST_SEED` |
| `test/integration/serval-breaker.redis.integration.test.ts` | The measured claims against a real Redis: zero admission reads while the cached verdict is valid, and recovery through the strict probe path after the cooldown |

Phase 3 adds the rate limiter's cases:

| Artefact | Role |
|---|---|
| `test/support/serval/fake-grant-authority.ts` | A scriptable issuer **backed by the reference model's token bucket**, so the generated histories drive the implementation against the specified authority rather than a second implementation of it. Adds a lost reply that may or may not have committed, a held reply, and call counting |
| `test/support/serval/rate-limit-harness.ts` | The coordinator wired to a manual clock and that authority: the GCRA bucket refills from elapsed time, so advancing the clock is exactly what moves it |
| `test/support/serval/rate-limit-history.ts` | The generated quota history: concurrent admissions, ambiguous and late replies, outages, several scopes, and a conservation check that no permit is created locally |
| `test/unit/serval-rate-limit.test.ts` | The local stock, the issuance operation, expiry without carry, the retry-after hint, the registry bound and the end-to-end policy path |
| `test/property/serval-rate-limit.property.test.ts` | The same invariants over generated histories, honouring `CARACAL_TEST_SEED` |
| `test/integration/serval-rate-limit.redis.integration.test.ts` | One shared bucket with the strict path, a deduplicated retry that deducts once, measured round trips per admission, the hoarding cap and contention |

Phase 4 adds the bulkhead's cases:

| Artefact | Role |
|---|---|
| `test/support/serval/fake-allocation-authority.ts` | A server model: one counter of granted slots, one allocation per replica, a reservation that outlives validity, and an idempotent operation ledger - plus an oracle that fires when the server grants past its limit, counts a renewal as capacity, or holds two allocations for one replica |
| `test/support/serval/bulkhead-harness.ts` | The coordinator wired to a shared manual clock and that authority, so replicas can be advanced together |
| `test/support/serval/bulkhead-history.ts` | The generated allocation history: concurrent admissions, releases, renewals, voluntary returns, outages and lost replies |
| `test/unit/serval-bulkhead.test.ts` | The local allocation, demand-driven growth, the lapsed allocation with holders retained, renewal as a non-grant, the send-time anchor, freeze-before-return, a lost reply settled by retry, shutdown, the registry bound and the policy path |
| `test/property/serval-bulkhead.property.test.ts` | The same invariants over generated histories, honouring `CARACAL_TEST_SEED` |
| `test/integration/serval-bulkhead.redis.integration.test.ts` | Allocations summed across replicas, renewal that adds no capacity, the freeze-before-return handoff, and the measured tracked overshoot a reclaimed allocation permits |

Phase 5 composes them and runs the fault campaign:

| Artefact | Role |
|---|---|
| `test/integration/serval-composition.integration.test.ts` | All three delegated policies over one operation, local guardrails, retry placement and per-attempt admission, refusals that stay out of breaker evidence, cancellation against an adapter that ignores it, a Redis outage with local authority intact, and bounded shutdown |
| `bench/serval/lab.ts` | The fault lab: four profiles, five fault cells, paired seeds, real worker processes, and raw per-seed results written to `bench/serval/results/` |
| `bench/serval/worker.ts` | One replica: its own process, its own Redis connection, every coordination command counted through a proxy over the client |
| `bench/serval/downstream.ts` | The controlled dependency: programmable mode and latency, with physical concurrency and overshoot area measured at the server, and no listener that stops work when a client disconnects |

The reference model is the specification's proof obligation for Phase 0, and the fake authority plus
the deterministic scheduler play that role for the session. Phase 2 onwards replaces the fakes with
real Redis integration suites.

## Implementation phases

| Phase | Scope | Gate |
|---|---|---|
| 0 (this page) | Authority protocol specification and executable contracts | Every policy has a falsifiable contract and deviation envelope; all eleven decisions have executable model cases |
| 1 | Shared session, replica incarnation, bounded pinned registry, notification transport, reconciliation, deterministic scheduler | Reordering, expiry, duplicate messages, partitions, reconnect and bounded cleanup pass; no policy algorithm inside the session |
| 2 | Circuit breaker | Cached-OPEN recovery via authoritative probe after cooldown; protective through outage; admission reads near zero on warm valid CLOSED |
| 3 | Rate limiter | No double spending in generated histories; correct server accounting under contention |
| 4 | Bulkhead | Resize-down below occupancy stops new admissions without forgetting holders; frozen capacity is never spent |
| 5 | Composition and fault campaign | No cross-policy interference, stuck recovery or unreported degradation |
| 6 | Documentation and release | Reproducible benchmarks, complete conformance suite, explicit known limitations |

Serval ships as an **opt-in experimental strategy** until multi-seed, real-process evidence supports
stability.







