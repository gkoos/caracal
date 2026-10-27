/**
 * Acceptance tests for the Serval authority protocol.
 *
 * One group per contract in `docs/serval/authority.md`. Each group asserts the
 * contract's own worked examples and invariants against the deterministic
 * reference model in `test/support/serval/reference-model.ts`, so the RFC has an
 * executable obligation rather than prose alone. The generated suite in
 * `test/property/serval-authority.property.test.ts` replays the same model over
 * adversarial histories.
 */

import { describe, expect, it } from "vitest"
import {
  SERVAL_DEFAULT_SAFETY_MARGIN_MS,
  ServalIncarnationTracker,
  compareServalVersions,
  isServalReplyUsable,
  isValidServalIdentity,
  isValidServalRecord,
  isValidServalVersion,
  servalDeadline,
  servalInvalidationChannel,
  servalRecordKey,
  servalScopeKey,
} from "../../../src/serval/authority.js"
import type {
  BulkheadOperation,
  GrantReply,
} from "../../support/serval/reference-model.js"
import {
  AuthorityHistory,
  AuthorityRegistry,
  BreakerLiveness,
  BulkheadAllocation,
  ConfigurationLedger,
  GrantLedger,
  IncarnationAdoption,
  MonotonicTime,
  OccupancyLedger,
  RateEnvelope,
} from "../../support/serval/reference-model.js"
import {
  TEST_IDENTITY,
  identityFor,
} from "../../support/serval/session-harness.js"

function grantReply(overrides: Partial<GrantReply> = {}): GrantReply {
  return {
    operationId: "op-1",
    grantId: "g",
    authorityIncarnation: "A",
    leaseRevision: 1,
    validForMs: 100,
    ...overrides,
  }
}

describe("Contract 1: send-time anchoring and conservative validity", () => {
  it("rejects a 100 ms grant whose reply arrives after 150 ms", () => {
    const clock = new MonotonicTime(1_000)
    const ledger = new GrantLedger(clock)
    ledger.beginOperation("op-1")

    clock.advanceTo(1_150)
    const outcome = ledger.acceptReply(grantReply({ validForMs: 100 }))

    expect(outcome).toMatchObject({ accepted: false, reason: "late" })
    expect(ledger.grant).toBeUndefined()
    expect(ledger.mayAdmit(clock.now())).toMatchObject({
      reason: "no-authority",
    })
  })

  it("accepts the same grant when the reply arrives inside the deadline", () => {
    const clock = new MonotonicTime(1_000)
    const ledger = new GrantLedger(clock)
    ledger.beginOperation("op-1")

    clock.advanceTo(1_100)
    expect(ledger.acceptReply(grantReply({ validForMs: 100 }))).toMatchObject({
      accepted: true,
      deadline: 1_100,
    })
    expect(ledger.mayAdmit(1_099)).toMatchObject({ kind: "admit" })
    expect(ledger.mayAdmit(1_100)).toMatchObject({ reason: "expired" })
  })

  it("keeps a retry on the original send-time anchor", () => {
    const clock = new MonotonicTime(1_000)
    const ledger = new GrantLedger(clock)
    const first = ledger.beginOperation("op-1")

    clock.advanceTo(1_300)
    const retry = ledger.beginOperation("op-1")

    expect(first.retried).toBe(false)
    expect(retry).toMatchObject({ retried: true, requestStartedAt: 1_000 })

    clock.advanceTo(1_350)
    expect(ledger.acceptReply(grantReply({ validForMs: 100 }))).toMatchObject({
      accepted: false,
      reason: "late",
    })
  })

  it("treats a clock jump as elapsed time only", () => {
    const clock = new MonotonicTime(0)
    const ledger = new GrantLedger(clock)
    ledger.beginOperation("op-1")
    expect(ledger.acceptReply(grantReply({ validForMs: 100 }))).toMatchObject({
      accepted: true,
      deadline: 100,
    })

    clock.advanceTo(50_000)
    expect(ledger.mayAdmit(clock.now())).toMatchObject({ reason: "expired" })
  })

  it("subtracts the safety margin from the deadline and never adds it", () => {
    const clock = new MonotonicTime(0)
    const ledger = new GrantLedger(clock, { safetyMarginMs: 25 })
    ledger.beginOperation("op-1")

    clock.advanceTo(70)
    expect(ledger.acceptReply(grantReply({ validForMs: 100 }))).toMatchObject({
      accepted: true,
      deadline: 75,
    })

    expect(ledger.mayAdmit(74)).toMatchObject({ kind: "admit" })
    expect(ledger.mayAdmit(75)).toMatchObject({ reason: "expired" })
    expect(ledger.violations).toEqual([])
  })
})

describe("Contract 2: cached-OPEN cooldown and probe liveness", () => {
  function openBreaker(openedAt = 1_000) {
    const breaker = new BreakerLiveness({ cooldownMs: 5_000, freshnessMs: 100 })
    breaker.applyTransition(
      { state: "open", version: 1, generation: 1, openedAt },
      0,
    )
    return breaker
  }

  it("rejects locally before cooldown eligibility, without a Redis call", () => {
    const breaker = openBreaker()

    expect(breaker.admit(5_999)).toMatchObject({ reason: "cached-open" })
    expect(breaker.probeAttempts).toBe(0)
    expect(breaker.cooldownEligibleAt).toBe(6_000)
  })

  it("coalesces concurrent attempts into one authoritative probe after cooldown", () => {
    const breaker = openBreaker()

    expect(breaker.admit(6_000)).toMatchObject({
      kind: "admit",
      source: "probe",
    })
    expect(breaker.admit(6_001)).toMatchObject({ reason: "probe-in-flight" })
    expect(breaker.admit(6_002)).toMatchObject({ reason: "probe-in-flight" })
    expect(breaker.probeAttempts).toBe(1)
    expect(breaker.state).toBe("open")
  })

  it("rejects while Redis is unavailable instead of fabricating a probe", () => {
    const breaker = openBreaker(0)
    breaker.setRedisAvailable(false)

    expect(breaker.admit(10_000)).toMatchObject({ reason: "unavailable" })
    expect(breaker.probeAttempts).toBe(0)
    expect(breaker.state).toBe("open")
    expect(breaker.violations).toEqual([])
  })

  it("does not let an obsolete settlement close a newer OPEN episode", () => {
    const breaker = openBreaker(0)
    breaker.applyTransition(
      { state: "open", version: 2, generation: 7, openedAt: 0 },
      0,
    )

    expect(breaker.admit(10_000)).toMatchObject({ source: "probe" })
    expect(breaker.settleProbe("success", 5, 10_000)).toBe("obsolete")
    expect(breaker.state).toBe("open")
  })

  it("ignores a stale transition and admits from a fresh cached CLOSED", () => {
    const breaker = new BreakerLiveness({ freshnessMs: 100 })
    breaker.applyTransition(
      { state: "closed", version: 5, generation: 1, openedAt: 0 },
      1_000,
    )
    breaker.applyTransition(
      { state: "open", version: 3, generation: 1, openedAt: 0 },
      1_000,
    )

    expect(breaker.state).toBe("closed")
    expect(breaker.admit(1_050)).toMatchObject({ source: "cached-closed" })
    expect(breaker.admit(1_100)).toMatchObject({ source: "cached-closed" })
    expect(breaker.admit(1_101)).toMatchObject({ reason: "stale-closed" })
    expect(breaker.probeAttempts).toBe(0)
  })
})

describe("Contract 3: authority-history loss", () => {
  it("pauses issuance until the fenced last-old-issuance boundary", () => {
    const history = new AuthorityHistory({
      fencing: "verified",
      maximumOldGrantValidityMs: 100,
      safetyMarginMs: 10,
    })

    const loss = history.loseHistory({
      at: 2_000,
      lastOldIssuanceAt: 1_000,
      incarnation: "B",
    })

    expect(loss).toEqual({ mode: "fenced-holdoff", resumesAt: 1_110 })
    expect(history.claimsBoundedHoldoff).toBe(true)
    expect(history.issue(1_109)).toMatchObject({
      issued: false,
      reason: "issuance-paused",
    })
    expect(history.issue(1_110)).toMatchObject({ issued: true })
    expect(history.deviations).toEqual([])
    expect(history.violations).toEqual([])
  })

  it("resumes immediately without a fence and reports the deviation", () => {
    const history = new AuthorityHistory({
      fencing: "none",
      maximumOldGrantValidityMs: 100,
    })

    const loss = history.loseHistory({
      at: 2_000,
      lastOldIssuanceAt: 1_000,
      incarnation: "B",
    })

    expect(loss.mode).toBe("unfenced-overlap")
    expect(history.claimsBoundedHoldoff).toBe(false)
    expect(history.issue(2_000)).toMatchObject({ issued: true })
    expect(history.deviations).toHaveLength(1)
  })
})

describe("Contract 4: configuration revision and deficit accounting", () => {
  it("pauses issuance while a reduction deficit is outstanding", () => {
    const ledger = new ConfigurationLedger(10)

    expect(ledger.reserve(10)).toEqual({ granted: 10, paused: false })
    expect(ledger.applyRevision({ revision: 2, limit: 4 })).toEqual({
      deficit: 6,
      issuancePaused: true,
    })
    expect(ledger.reserve(1)).toEqual({ granted: 0, paused: true })
    expect(ledger.refusedIssuances).toBe(1)

    // Releasing outstanding grants below the new limit clears the deficit and
    // issuance resumes against the reduced limit.
    expect(ledger.release(8)).toEqual({ deficit: 0, issuancePaused: false })
    expect(ledger.outstandingReserved).toBe(2)
    expect(ledger.reserve(2)).toEqual({ granted: 2, paused: false })
    expect(ledger.reserve(1)).toEqual({ granted: 0, paused: false })
    expect(ledger.violations).toEqual([])
  })

  it("keeps outstanding grants usable across the revision change", () => {
    const ledger = new ConfigurationLedger(10)
    ledger.reserve(8)

    ledger.applyRevision({ revision: 2, limit: 2 })

    expect(ledger.outstandingReserved).toBe(8)
    expect(ledger.limit).toBe(2)
    expect(ledger.revision).toBe(2)
  })

  it("flags a revision that does not advance", () => {
    const ledger = new ConfigurationLedger(10)
    ledger.applyRevision({ revision: 3, limit: 5 })
    ledger.applyRevision({ revision: 2, limit: 5 })

    expect(ledger.revision).toBe(3)
    expect(ledger.violations.join(" ")).toMatch(/did not advance/)
  })
})

describe("Contract 5: pinned records and replay tombstones", () => {
  it("refuses a new scope instead of evicting pinned records", () => {
    const registry = new AuthorityRegistry(2)
    registry.register({ key: "a", entryClass: "pinned", retainedUntil: 0 }, 0)
    registry.register({ key: "b", entryClass: "pinned", retainedUntil: 0 }, 0)

    expect(
      registry.register(
        { key: "c", entryClass: "pinned", retainedUntil: 0 },
        1_000,
      ),
    ).toEqual({ registered: false, reason: "capacity" })
    expect(registry.refusals).toBe(1)
    expect(registry.has("a")).toBe(true)
    expect(registry.has("b")).toBe(true)
    expect(registry.evictions).toBe(0)
  })

  it("evicts a disposable record but never a protective one", () => {
    const registry = new AuthorityRegistry(2)
    registry.register(
      { key: "open-scope", entryClass: "protective", retainedUntil: 0 },
      0,
    )
    registry.register(
      { key: "idle-scope", entryClass: "disposable", retainedUntil: 10 },
      0,
    )

    expect(
      registry.register(
        { key: "new-scope", entryClass: "pinned", retainedUntil: 0 },
        1_000,
      ),
    ).toEqual({ registered: true })
    expect(registry.has("open-scope")).toBe(true)
    expect(registry.has("idle-scope")).toBe(false)
    expect(registry.evictions).toBe(1)
  })

  it("cannot mint authority from a reply after the replay window closed", () => {
    const clock = new MonotonicTime(0)
    const ledger = new GrantLedger(clock, {
      replayRetentionMs: 10,
      maximumGrantValidityMs: 100,
    })
    ledger.beginOperation("op-1")
    expect(ledger.acceptReply(grantReply({ validForMs: 50 }))).toMatchObject({
      accepted: true,
      deadline: 50,
    })

    clock.advanceTo(1_000)
    expect(ledger.collectRetiredOperations()).toBe(1)
    const before = ledger.grant

    expect(
      ledger.acceptReply(grantReply({ validForMs: 50, leaseRevision: 2 })),
    ).toMatchObject({ accepted: false, reason: "unknown-operation" })
    expect(ledger.grant).toBe(before)
  })
})

describe("Contract 6: tracked occupancy versus physical work", () => {
  it("holds the tracked-occupancy ceiling across retired generations", () => {
    const occupancy = new OccupancyLedger()
    occupancy.register("A", 2)
    occupancy.register("B", 2)

    expect(occupancy.admit("A")).toBe(true)
    expect(occupancy.admit("A")).toBe(true)
    expect(occupancy.admit("A")).toBe(false)
    occupancy.retire("A")

    occupancy.register("C", 2)
    expect(occupancy.admit("C")).toBe(true)
    expect(occupancy.admit("C")).toBe(true)

    expect(occupancy.trackedTotal).toBe(4)
    expect(occupancy.trackedCeiling).toBe(4)
    expect(occupancy.violations).toEqual([])
  })

  it("counts work that outlives a retired process as a separate deviation", () => {
    const occupancy = new OccupancyLedger()
    occupancy.register("A", 2)
    occupancy.admit("A")
    occupancy.retire("A")
    occupancy.physicalWorkSurvives("A", 3)

    expect(occupancy.trackedTotal).toBe(1)
    expect(occupancy.trackedCeiling).toBe(2)
    expect(occupancy.physicalTotal).toBe(4)
    expect(occupancy.untrackedPhysical).toBe(3)
    expect(occupancy.physicalTotal).toBeGreaterThan(occupancy.trackedCeiling)
    expect(occupancy.violations).toEqual([])
  })

  it("refuses an admission after retirement", () => {
    const occupancy = new OccupancyLedger()
    occupancy.register("A", 2)
    occupancy.retire("A")

    expect(occupancy.admit("A")).toBe(false)
    expect(occupancy.violations.join(" ")).toMatch(/after retirement/)
  })
})

describe("Contract 7: bulkhead renew, resize and free-only return", () => {
  it("does not double the allocation on a renewal", () => {
    const clock = new MonotonicTime(0)
    const allocation = new BulkheadAllocation(clock, { serverCapacity: 20 })
    allocation.allocate(5, 100)

    const renewal = allocation.beginRenew(100)
    expect(allocation.complete(renewal, { kind: "committed" })).toBe("applied")

    expect(allocation.allocated).toBe(5)
    expect(allocation.freeSpendable).toBe(5)
    expect(allocation.serverAllocated).toBe(5)
    expect(allocation.leaseRevision).toBe(1)
    expect(allocation.grantId).toBe("grant-0")
    expect(allocation.violations).toEqual([])
  })

  it("reserves only the delta on a resize-up", () => {
    const clock = new MonotonicTime(0)
    const allocation = new BulkheadAllocation(clock, { serverCapacity: 20 })
    allocation.allocate(5, 100)

    const resize = allocation.beginResizeUp(2, 100)
    expect(allocation.complete(resize, { kind: "committed" })).toBe("applied")

    expect(allocation.allocated).toBe(7)
    expect(allocation.freeSpendable).toBe(7)
    expect(allocation.serverAllocated).toBe(7)
    expect(allocation.violations).toEqual([])
  })

  it("stops new admissions below occupancy without forgetting holders", () => {
    const clock = new MonotonicTime(0)
    const allocation = new BulkheadAllocation(clock, { serverCapacity: 20 })
    allocation.allocate(5, 100)
    for (let holder = 0; holder < 4; holder += 1) {
      expect(allocation.acquireHolder()).toBe(true)
    }

    const shrink = allocation.beginResizeDown(1, 100)
    expect(allocation.complete(shrink, { kind: "committed" })).toBe("applied")

    expect(allocation.allocated).toBe(4)
    expect(allocation.active).toBe(4)
    expect(allocation.freeSpendable).toBe(0)
    expect(allocation.acquireHolder()).toBe(false)
    expect(allocation.active).toBe(4)

    allocation.releaseHolder()
    expect(allocation.active).toBe(3)
    expect(allocation.freeSpendable).toBe(1)
    expect(allocation.violations).toEqual([])
  })

  it("never returns an active holder as a free slot", () => {
    const clock = new MonotonicTime(0)
    const allocation = new BulkheadAllocation(clock, { serverCapacity: 20 })
    allocation.allocate(5, 100)
    for (let holder = 0; holder < 4; holder += 1) {
      allocation.acquireHolder()
    }

    const shrink = allocation.beginResizeDown(4, 100)

    expect(shrink.amount).toBe(1)
    expect(allocation.frozenPendingReturn).toBe(1)
    expect(allocation.freeSpendable).toBe(0)
    expect(allocation.violations.join(" ")).toMatch(/free-only return violated/)
  })

  it("admits nothing new under an expired allocation but keeps its holders", () => {
    const clock = new MonotonicTime(0)
    const allocation = new BulkheadAllocation(clock, { serverCapacity: 20 })
    allocation.allocate(5, 100)
    expect(allocation.acquireHolder()).toBe(true)

    clock.advanceTo(200)
    expect(allocation.acquireHolder()).toBe(false)
    expect(allocation.state).toBe("draining")
    expect(allocation.active).toBe(1)

    allocation.releaseHolder()
    allocation.expire(200)
    expect(allocation.state).toBe("expired")
    expect(allocation.violations).toEqual([])
  })
})

describe("Contract 8: rate issuance-and-spending envelope", () => {
  it("holds the rolling-window bound when permits are spent at expiry", () => {
    const envelope = new RateEnvelope({
      ratePerSecond: 20,
      burst: 5,
      spendHorizonMs: 40,
    })

    const issuance = envelope.issue(0, 0, 5, "op-1")
    expect(issuance).toMatchObject({ granted: 5, duplicate: false })
    expect(issuance.grantId).toBeDefined()

    for (let permit = 0; permit < 5; permit += 1) {
      expect(envelope.spend(issuance.grantId as string, 40)).toBe(true)
    }

    envelope.assertRollingBound(40)
    expect(envelope.maxAdmissionsInWindow(40)).toBe(5)
    expect(envelope.rollingBound(40)).toBeCloseTo(6.6)
    expect(envelope.violations).toEqual([])
  })

  it("discards unused quota instead of crediting it back", () => {
    const envelope = new RateEnvelope({
      ratePerSecond: 20,
      burst: 5,
      spendHorizonMs: 40,
    })

    const issuance = envelope.issue(0, 0, 5, "op-1")
    expect(envelope.expireGrant(issuance.grantId as string)).toBe(5)

    // Only the refill is available: the discarded remainder is gone.
    expect(envelope.availableTokens(100)).toBe(2)
    expect(envelope.discardedQuota).toBe(5)
    expect(envelope.violations).toEqual([])
  })

  it("refuses to spend a permit past its deadline", () => {
    const envelope = new RateEnvelope({
      ratePerSecond: 20,
      burst: 1,
      spendHorizonMs: 10,
    })
    const issuance = envelope.issue(0, 0, 1, "op-1")

    expect(envelope.spend(issuance.grantId as string, 10)).toBe(true)
    expect(envelope.spend(issuance.grantId as string, 11)).toBe(false)
    expect(envelope.violations.join(" ")).toMatch(
      /past its deadline|double spend/,
    )
  })

  it("never spends the same permit twice", () => {
    const envelope = new RateEnvelope({
      ratePerSecond: 20,
      burst: 2,
      spendHorizonMs: 40,
    })
    const issuance = envelope.issue(0, 0, 2, "op-1")
    const grantId = issuance.grantId as string

    expect(envelope.spend(grantId, 0)).toBe(true)
    expect(envelope.spend(grantId, 0)).toBe(true)
    expect(envelope.spend(grantId, 0)).toBe(false)
    expect(envelope.violations.join(" ")).toMatch(/double spend/)
  })

  it("deduplicates a retried issuance without deducting tokens twice", () => {
    const envelope = new RateEnvelope({
      ratePerSecond: 20,
      burst: 5,
      spendHorizonMs: 40,
    })

    const first = envelope.issue(0, 0, 3, "op-1")
    const second = envelope.issue(0, 0, 3, "op-1")

    expect(first).toMatchObject({ granted: 3, duplicate: false })
    expect(second).toMatchObject({ granted: 3, duplicate: true })
    expect(envelope.availableTokens(0)).toBe(2)
    expect(envelope.violations).toEqual([])
  })
})

describe("Contract 9: freeze-before-return and ambiguous handoff", () => {
  function frozenAllocation() {
    const clock = new MonotonicTime(0)
    const allocation = new BulkheadAllocation(clock, { serverCapacity: 20 })
    allocation.allocate(5, 1_000)
    const shrink = allocation.beginResizeDown(2, 1_000)
    return { clock, allocation, shrink }
  }

  it("freezes free slots before the reduction is sent", () => {
    const { allocation, shrink } = frozenAllocation()

    expect(shrink.amount).toBe(2)
    expect(allocation.frozenPendingReturn).toBe(2)
    expect(allocation.freeSpendable).toBe(3)
    expect(allocation.allocated).toBe(5)
  })

  it("keeps frozen slots unspendable while the reply is in flight", () => {
    const { allocation, shrink } = frozenAllocation()

    expect(allocation.complete(shrink, { kind: "lost" })).toBe("uncertain")
    expect(allocation.state).toBe("uncertain")
    expect(allocation.frozenPendingReturn).toBe(2)
    expect(allocation.acquireHolder()).toBe(false)
    expect(allocation.retry(shrink)).toBe(shrink)
    expect(allocation.violations).toEqual([])
  })

  it("unfreezes only on reconciliation, never on a stale reply", () => {
    const { allocation, shrink } = frozenAllocation()
    allocation.complete(shrink, { kind: "lost" })

    expect(
      allocation.reconcile({
        serverAllocated: 5,
        serverRevision: 0,
        frozenNonCommit: true,
      }),
    ).toBe("unfrozen")
    expect(allocation.frozenPendingReturn).toBe(0)
    expect(allocation.freeSpendable).toBe(5)
    expect(allocation.state).toBe("allocated")

    // A late reply for a reconciled non-commit cannot restore the capacity.
    expect(allocation.complete(shrink, { kind: "committed" })).toBe("stale")
    expect(allocation.allocated).toBe(5)
    expect(allocation.leaseRevision).toBe(0)
    expect(allocation.violations).toEqual([])
  })

  it("does not unfreeze when reconciliation cannot prove the non-commit", () => {
    const { allocation, shrink } = frozenAllocation()
    allocation.complete(shrink, { kind: "refused", reason: "lease-revision" })

    expect(
      allocation.reconcile({
        serverAllocated: 3,
        serverRevision: 1,
        frozenNonCommit: false,
      }),
    ).toBe("kept-frozen")
    expect(allocation.frozenPendingReturn).toBe(2)
    expect(allocation.violations).toEqual([])
  })
})

describe("Contract 10: fencing, adoption and monotonic client state", () => {
  it("ignores a delayed message from a retired incarnation", () => {
    const adoption = new IncarnationAdoption()
    expect(adoption.reconcile("A")).toMatchObject({ adopted: true })
    expect(adoption.observe("A")).toBe("current")

    adoption.retire("A")
    expect(adoption.current).toBeUndefined()
    expect(adoption.state).toBe("uncertain")
    expect(adoption.observe("A")).toBe("ignored-retired")

    expect(adoption.reconcile("B")).toMatchObject({ adopted: true })
    expect(adoption.observe("A")).toBe("ignored-retired")
    expect(adoption.current).toBe("B")
    expect(adoption.state).toBe("valid")
    expect(adoption.violations).toEqual([])
  })

  it("treats an unknown incarnation as uncertain, not as adoption", () => {
    const adoption = new IncarnationAdoption()
    adoption.reconcile("A")

    expect(adoption.observe("C")).toBe("uncertain")
    expect(adoption.state).toBe("uncertain")
    expect(adoption.current).toBe("A")
  })

  it("refuses to re-adopt a retired incarnation by reconciliation", () => {
    const adoption = new IncarnationAdoption()
    adoption.reconcile("A")
    adoption.retire("A")

    expect(adoption.reconcile("A")).toMatchObject({
      adopted: false,
      reason: "retired-incarnation",
    })
    expect(adoption.violations.join(" ")).toMatch(/retired incarnation/)
  })

  it("retires a ledger incarnation without revoking held authority", () => {
    const clock = new MonotonicTime(0)
    const ledger = new GrantLedger(clock)
    ledger.beginOperation("op-1")
    expect(ledger.acceptReply(grantReply({ validForMs: 100 }))).toMatchObject({
      accepted: true,
      deadline: 100,
    })

    ledger.retireIncarnation("A")
    expect(ledger.mayAdmit(50)).toMatchObject({ kind: "admit" })

    ledger.beginOperation("op-2")
    clock.advanceTo(10)
    expect(
      ledger.acceptReply(
        grantReply({ operationId: "op-2", validForMs: 100, leaseRevision: 2 }),
      ),
    ).toMatchObject({ accepted: false, reason: "retired-incarnation" })

    ledger.beginOperation("op-3")
    expect(
      ledger.acceptReply(
        grantReply({
          operationId: "op-3",
          authorityIncarnation: "B",
          validForMs: 100,
          leaseRevision: 3,
        }),
      ),
    ).toMatchObject({ accepted: false, reason: "unknown-incarnation" })
    expect(ledger.uncertain).toBe(true)
    expect(ledger.mayAdmit(50)).toMatchObject({ reason: "uncertain" })

    ledger.clearUncertain()
    expect(ledger.mayAdmit(50)).toMatchObject({ kind: "admit" })
  })
})

describe("Contract 11: grantId, operationId and leaseRevision ordering", () => {
  it("does not extend expiry twice from a duplicated renewal reply", () => {
    const clock = new MonotonicTime(0)
    const ledger = new GrantLedger(clock)
    ledger.beginOperation("op-1")
    expect(ledger.acceptReply(grantReply({ validForMs: 100 }))).toMatchObject({
      accepted: true,
      deadline: 100,
    })

    const duplicate = ledger.acceptReply(grantReply({ validForMs: 100 }))
    expect(duplicate).toMatchObject({ duplicate: true, deadline: 100 })
    expect(ledger.grant?.deadline).toBe(100)
    expect(ledger.grant?.leaseRevision).toBe(1)
  })

  it("anchors a renewal to its own send time, not to the receipt time", () => {
    const clock = new MonotonicTime(0)
    const ledger = new GrantLedger(clock)
    ledger.beginOperation("op-1")
    ledger.acceptReply(grantReply({ validForMs: 100 }))

    clock.advanceTo(50)
    ledger.beginOperation("op-2")
    clock.advanceTo(60)

    expect(
      ledger.acceptReply(
        grantReply({ operationId: "op-2", validForMs: 100, leaseRevision: 2 }),
      ),
    ).toMatchObject({ accepted: true, deadline: 150 })
    expect(ledger.grant?.deadline).toBe(150)
    expect(ledger.grant?.grantId).toBe("g")
    expect(ledger.violations).toEqual([])
  })

  it("ignores a reply whose lease revision does not advance", () => {
    const clock = new MonotonicTime(0)
    const ledger = new GrantLedger(clock)
    ledger.beginOperation("op-1")
    ledger.acceptReply(grantReply({ validForMs: 100 }))
    clock.advanceTo(50)
    ledger.beginOperation("op-2")
    ledger.acceptReply(
      grantReply({ operationId: "op-2", validForMs: 100, leaseRevision: 2 }),
    )

    clock.advanceTo(60)
    ledger.beginOperation("op-3")
    expect(
      ledger.acceptReply(
        grantReply({ operationId: "op-3", validForMs: 300, leaseRevision: 1 }),
      ),
    ).toMatchObject({ accepted: false, reason: "stale-revision" })
    expect(ledger.grant?.deadline).toBe(150)
    expect(ledger.grant?.leaseRevision).toBe(2)
  })

  it("cannot restore retired capacity from an older resize reply", () => {
    const clock = new MonotonicTime(0)
    const allocation = new BulkheadAllocation(clock, { serverCapacity: 20 })
    allocation.allocate(5, 1_000)

    const shrink = allocation.beginResizeDown(2, 1_000)
    expect(allocation.complete(shrink, { kind: "committed" })).toBe("applied")
    expect(allocation.allocated).toBe(3)
    expect(allocation.leaseRevision).toBe(1)

    const stale: BulkheadOperation = {
      ...shrink,
      operationId: "op-old",
      expectedLeaseRevision: 0,
    }
    expect(allocation.complete(stale, { kind: "committed" })).toBe("stale")
    expect(allocation.allocated).toBe(3)
    expect(allocation.leaseRevision).toBe(1)
    expect(allocation.frozenPendingReturn).toBe(0)
    expect(allocation.violations).toEqual([])
  })

  it("advances the lease revision on every committed modification", () => {
    const clock = new MonotonicTime(0)
    const allocation = new BulkheadAllocation(clock, { serverCapacity: 20 })
    allocation.allocate(5, 1_000)

    const renewal = allocation.beginRenew(1_000)
    expect(allocation.complete(renewal, { kind: "committed" })).toBe("applied")
    const resize = allocation.beginResizeUp(1, 1_000)
    expect(allocation.complete(resize, { kind: "committed" })).toBe("applied")

    expect(allocation.leaseRevision).toBe(2)
    expect(allocation.grantId).toBe("grant-0")
    expect(allocation.allocated).toBe(6)
    expect(allocation.violations).toEqual([])
  })
})

describe("shared authority vocabulary", () => {
  it("derives a deadline from the send time and only subtracts the margin", () => {
    expect(servalDeadline(1_000, 100)).toBe(
      1_100 - SERVAL_DEFAULT_SAFETY_MARGIN_MS,
    )
    expect(servalDeadline(1_000, 100, 0)).toBe(1_100)
    expect(servalDeadline(1_000, 100, 150)).toBe(950)

    for (const invalid of [-1, Number.NaN, Number.POSITIVE_INFINITY]) {
      expect(() => servalDeadline(invalid, 100)).toThrow(RangeError)
    }
    expect(() => servalDeadline(0, -1)).toThrow(RangeError)
  })

  it("decides reply usability from the send time, never the receipt time", () => {
    expect(
      isServalReplyUsable({
        requestStartedAt: 1_000,
        validForMs: 100,
        safetyMarginMs: 0,
        receivedAt: 1_100,
      }),
    ).toBe(true)

    // The RFC's worked example: a 100 ms grant with a 150 ms delayed response.
    expect(
      isServalReplyUsable({
        requestStartedAt: 1_000,
        validForMs: 100,
        safetyMarginMs: 0,
        receivedAt: 1_150,
      }),
    ).toBe(false)

    // The margin shortens the window; it is never added to it.
    expect(
      isServalReplyUsable({
        requestStartedAt: 1_000,
        validForMs: 100,
        safetyMarginMs: 25,
        receivedAt: 1_076,
      }),
    ).toBe(false)
  })

  it("orders versions within one incarnation and never across one", () => {
    const base = {
      authorityIncarnation: "authority-1",
      epoch: 1,
      version: 2,
      configRevision: 1,
    }

    expect(compareServalVersions(undefined, base)).toBe("newer")
    expect(compareServalVersions(base, { ...base })).toBe("same")
    expect(compareServalVersions(base, { ...base, version: 3 })).toBe("newer")
    expect(compareServalVersions(base, { ...base, version: 1 })).toBe("older")
    // A higher epoch wins even with a lower version: epochs order state.
    expect(compareServalVersions(base, { ...base, epoch: 2, version: 0 })).toBe(
      "newer",
    )
    expect(compareServalVersions(base, { ...base, epoch: 0, version: 9 })).toBe(
      "older",
    )
    // A configuration roll-forward is newer at the same epoch and version.
    expect(compareServalVersions(base, { ...base, configRevision: 4 })).toBe(
      "newer",
    )
    expect(compareServalVersions(base, { ...base, configRevision: 0 })).toBe(
      "older",
    )
    expect(
      compareServalVersions(base, {
        ...base,
        authorityIncarnation: "authority-2",
      }),
    ).toBe("other-incarnation")
  })

  it("validates identities, versions and records", () => {
    expect(isValidServalIdentity(TEST_IDENTITY)).toBe(true)
    expect(isValidServalIdentity({ ...TEST_IDENTITY, scope: " " })).toBe(false)
    expect(
      isValidServalIdentity({ ...TEST_IDENTITY, policyKind: "other" }),
    ).toBe(false)
    expect(
      isValidServalIdentity({ ...TEST_IDENTITY, scope: "x".repeat(1025) }),
    ).toBe(false)

    const version = {
      authorityIncarnation: "authority-1",
      epoch: 0,
      version: 1,
      configRevision: 1,
    }
    expect(isValidServalVersion(version)).toBe(true)
    expect(isValidServalVersion({ ...version, epoch: -1 })).toBe(false)
    expect(isValidServalVersion({ ...version, version: 1.5 })).toBe(false)
    expect(isValidServalVersion({ ...version, authorityIncarnation: "" })).toBe(
      false,
    )

    const record = {
      identity: TEST_IDENTITY,
      version,
      issuedAt: 1_000,
      expiresAt: 2_000,
      payload: undefined,
    }
    expect(isValidServalRecord(record)).toBe(true)
    expect(isValidServalRecord({ ...record, expiresAt: 999 })).toBe(false)
    expect(isValidServalRecord({ ...record, issuedAt: -1 })).toBe(false)
  })

  it("keys one identity into one cluster slot and keeps identities separate", () => {
    const key = servalRecordKey("ns", TEST_IDENTITY)

    expect(key).toMatch(/^caracal:v1:\{[0-9a-f]{64}\}:record$/)
    expect(servalRecordKey("ns", TEST_IDENTITY)).toBe(key)

    const tag = (value: string) =>
      value.slice(value.indexOf("{") + 1, value.indexOf("}"))
    expect(tag(servalRecordKey("ns", identityFor("tenant-b")))).not.toBe(
      tag(key),
    )
    expect(tag(servalInvalidationChannel("ns"))).not.toBe(tag(key))

    // Scope isolation: every part of the identity is part of the key.
    const keys = new Set([
      servalScopeKey(TEST_IDENTITY),
      servalScopeKey({ ...TEST_IDENTITY, scope: "tenant-b" }),
      servalScopeKey({ ...TEST_IDENTITY, operation: "refund" }),
      servalScopeKey({ ...TEST_IDENTITY, policyName: "refunds" }),
      servalScopeKey({ ...TEST_IDENTITY, policyKind: "rate-limit" }),
    ])
    expect(keys.size).toBe(5)
  })
})

describe("incarnation tracking", () => {
  it("adopts only through reconciliation and never re-adopts a retired incarnation", () => {
    const tracker = new ServalIncarnationTracker()
    expect(tracker.state).toBe("uninitialized")

    // A reply or a hint never adopts: it puts the client into the uncertain state.
    expect(tracker.observe("authority-1")).toBe("uncertain")
    expect(tracker.state).toBe("uncertain")

    expect(tracker.reconcile("authority-1")).toBe(true)
    expect(tracker.state).toBe("valid")
    expect(tracker.observe("authority-1")).toBe("current")

    tracker.retire("authority-1")
    expect(tracker.current).toBeUndefined()
    expect(tracker.state).toBe("uncertain")
    expect(tracker.observe("authority-1")).toBe("ignored-retired")
    // A retired incarnation is not re-adopted by a reconciliation either.
    expect(tracker.reconcile("authority-1")).toBe(false)

    expect(tracker.reconcile("authority-2")).toBe(true)
    expect(tracker.state).toBe("valid")
  })

  it("bounds the memory of retired incarnations", () => {
    const tracker = new ServalIncarnationTracker({
      maximumRetiredIncarnations: 2,
    })

    tracker.retire("authority-1")
    tracker.retire("authority-2")
    tracker.retire("authority-3")

    expect(tracker.retired).toEqual(["authority-2", "authority-3"])
    // The oldest retirement is forgotten, so it is treated as unfamiliar rather
    // than as retired - the documented cost of a bounded window.
    expect(tracker.observe("authority-1")).toBe("uncertain")
  })

  it("rejects an unusable incarnation identifier", () => {
    const tracker = new ServalIncarnationTracker()

    expect(() => tracker.reconcile("")).toThrow(TypeError)
    expect(() => tracker.retire("   ")).toThrow(TypeError)
    expect(() => tracker.observe("x".repeat(257))).toThrow(TypeError)
    expect(
      () => new ServalIncarnationTracker({ maximumRetiredIncarnations: 0 }),
    ).toThrow(RangeError)
  })
})
