import { randomUUID } from "node:crypto"
import type { BulkheadCoordinator } from "../../core/bulkhead.js"
import type {
  BulkheadAllocationAction,
  BulkheadAllocationIssuer,
  BulkheadAllocationReply,
} from "../redis/bulkhead.js"
import {
  SERVAL_DEFAULT_SAFETY_MARGIN_MS,
  servalDeadline,
  servalRemainingValidity,
} from "./authority.js"
import { ServalRegistryPressureError } from "./cache.js"

/**
 * The delegated bulkhead.
 *
 * It is a `BulkheadCoordinator`, so `bulkhead.distributed({ coordinator })` opts in
 * without changing the policy. Redis allocates each replica a budget of slots; this
 * layer admits within that budget with no round trip, and reconciles the budget out
 * of band.
 *
 * The contract is `docs/serval/README.md` contracts 6, 7, 9 and 11, and three separations
 * carry the whole design:
 *
 * - **Allocation validity and holder lifetime are different clocks.** When the
 *   allocation's validity ends, new admissions stop and every holder stays recorded.
 *   A holder is never forgotten because the authority lapsed, so the process cannot
 *   lose track of work it started.
 * - **Renewal is not a grant.** It extends the deadline and advances `leaseRevision`
 *   without changing capacity, so renewing can never double count and a replayed
 *   renewal cannot extend anything twice.
 * - **Returned capacity is frozen before it is sent.** A reduction is submitted only
 *   after the slots have left the local spendable pool, so a server-side
 *   reallocation can never precede this process's own freeze. A freeze is an intent
 *   the machinery settles: one made while another modification holds the wire is
 *   submitted as soon as that one settles, an unknown outcome keeps the slots frozen
 *   until the retry of the same operation settles them, and a freeze dies with the
 *   grant that granted it rather than being inherited by the next one.
 *
 * Tracked occupancy is this process's accounting, not physical concurrency: a holder
 * whose downstream work ignores cancellation keeps its slot until it settles, and
 * capacity reclaimed after expiry can be reallocated while old holders still run.
 * That overshoot is measurable rather than denied.
 */

/** How long an allocation stays valid before it must be renewed. */
export const SERVAL_BULKHEAD_DEFAULT_VALIDITY_MS = 10_000

/** How long the server holds an allocation's slots after its validity ends. */
export const SERVAL_BULKHEAD_DEFAULT_GRACE_MS = 10_000

/** Slots a replica asks for when it holds none. Demand grows it from there. */
export const SERVAL_BULKHEAD_DEFAULT_INITIAL_SLOTS = 1

/** Registry capacity when the caller does not choose one. */
export const SERVAL_BULKHEAD_DEFAULT_SCOPES = 1_024

/** The shortest gap between two redistribution attempts for one scope. */
export const SERVAL_BULKHEAD_DEFAULT_REDISTRIBUTION_MS = 100

/**
 * How many times one admission may consult the authority.
 *
 * It bounds a caller that arrives just as its allocation lapses: the first round
 * renews or re-allocates, the second grows the pool, and a caller that still cannot
 * be served is refused rather than spinning.
 */
export const SERVAL_BULKHEAD_MAXIMUM_ATTEMPTS = 3

export type ServalBulkheadState =
  | "absent"
  | "allocating"
  | "allocated"
  | "draining"
  | "uncertain"
  | "expired"
  | "retired"

export interface ServalBulkheadMetrics {
  readonly admissions: number
  /** Refusals answered without asking the authority. */
  readonly localRefusals: number
  readonly allocations: number
  readonly renewals: number
  readonly resizes: number
  readonly returns: number
  readonly refusals: number
  /** Replies discarded because a newer revision was already applied. */
  readonly staleReplies: number
  /** Frozen slots restored after a reconciliation proved a non-commit. */
  readonly unfrozenAfterReconciliation: number
  readonly modificationFailures: number
  readonly duplicates: number
  readonly reconciliations: number
  readonly registryRefusals: number
  readonly scopes: number
  readonly frozenSlots: number
  readonly activeHolders: number
  readonly allocatedSlots: number
}

export interface ServalBulkheadDiagnostics {
  readonly identity: {
    readonly name: string
    readonly operation: string
    readonly scope: string
  }
  readonly state: ServalBulkheadState
  readonly grantId: string
  readonly revision: number
  readonly allocated: number
  readonly active: number
  readonly freeSpendable: number
  readonly frozenPendingReturn: number
  /** Local monotonic instant the allocation stops being valid for new admissions. */
  readonly deadline: number
}

export type ServalRelinquishResult =
  | { readonly type: "nothing" }
  | {
      readonly type: "returned"
      readonly slots: number
      readonly revision: number
    }
  | {
      readonly type: "frozen"
      readonly slots: number
      readonly reason: "unknown" | "unavailable"
    }

export interface ServalBulkheadCoordinatorOptions {
  readonly authority: BulkheadAllocationIssuer
  /** Stable for the process lifetime; it identifies this replica's allocation. */
  readonly replicaId: string
  readonly validityMs?: number
  readonly graceMs?: number
  readonly initialSlots?: number
  /** This replica's own ceiling; demand never grows past it. */
  readonly maximumSlots?: number
  /** Renew this long before the validity deadline. */
  readonly renewLeadMs?: number
  readonly redistributionIntervalMs?: number
  readonly safetyMarginMs?: number
  readonly maximumScopes?: number
  /** The monotonic clock every local deadline is measured on. */
  readonly now?: () => number
  readonly idFactory?: () => string
}

export interface ServalBulkheadCoordinator extends BulkheadCoordinator {
  metrics(): ServalBulkheadMetrics
  /** The local allocation state of one identity, for tests and diagnostics. */
  diagnostics(identity: {
    readonly name: string
    readonly operation: string
    readonly scope: string
  }): ServalBulkheadDiagnostics | undefined
  /**
   * Freezes free slots and returns them to the fleet.
   *
   * A lost reply leaves them frozen until the retry of the same operation settles them, and a
   * freeze made while another modification holds the wire is submitted as soon as that one
   * settles.
   */
  relinquish(
    identity: {
      readonly name: string
      readonly operation: string
      readonly scope: string
    },
    slots?: number,
  ): Promise<ServalRelinquishResult>
  /** Stops admitting and returns every free slot, best effort. */
  close(): Promise<void>
  /** Resolves when no modification is in flight. */
  whenIdle(): Promise<void>
}

interface Identity {
  readonly name: string
  readonly operation: string
  readonly scope: string
}

/**
 * One allocation modification. Immutable, so a retry carries the same operation id,
 * the same expected revision and the same send-time anchor.
 */
interface Modification {
  readonly operationId: string
  readonly action: BulkheadAllocationAction
  readonly slots: number
  readonly expectedRevision: number
  /** Local monotonic send time: the anchor for the reply's deadline. */
  readonly requestStartedAt: number
  readonly validityMs: number
  /** Slots this modification freezes, when it is returning capacity. */
  readonly frozenSlots: number
  /** The allocation's slots before the modification, to prove a non-commit. */
  readonly slotsBefore: number
}

interface AllocationScope {
  readonly identity: Identity
  state: ServalBulkheadState
  grantId: string
  revision: number
  allocated: number
  readonly active: Set<string>
  frozen: number
  /** Local monotonic deadline of the allocation's validity. */
  deadline: number
  /** The ceiling the policy last reported for this identity. */
  limit: number
  /** No redistribution attempt before this instant. */
  redistributionUntil: number
  /**
   * When the request that produced the allocation we hold was sent. Staleness is judged by this,
   * not by the revision: the revision belongs to a single grant, and a replacement grant numbers
   * itself from 1 again, so a lower revision means a newer grant rather than a staler reply.
   */
  adoptedFromRequestStartedAt: number
  /** A modification whose reply has not settled, retained for retry or reconciliation. */
  modification: Modification | undefined
  issuing: Promise<void> | undefined
}

function assertNonNegative(name: string, value: number): number {
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0) {
    throw new RangeError(`${name} must be a non-negative finite number`)
  }
  return value
}

function assertPositiveInteger(name: string, value: number): number {
  if (!Number.isSafeInteger(value) || value < 1) {
    throw new RangeError(`${name} must be a positive integer`)
  }
  return value
}

export function createServalBulkheadCoordinator(
  options: ServalBulkheadCoordinatorOptions,
): ServalBulkheadCoordinator {
  const authority = options.authority
  if (authority === undefined || typeof authority.modify !== "function") {
    throw new TypeError("A delegated bulkhead requires an allocation issuer")
  }
  if (typeof options.replicaId !== "string" || !options.replicaId.trim()) {
    throw new TypeError("A delegated bulkhead requires a replica id")
  }
  const replicaId = options.replicaId
  const validityMs = assertPositiveInteger(
    "validityMs",
    options.validityMs ?? SERVAL_BULKHEAD_DEFAULT_VALIDITY_MS,
  )
  const graceMs = assertNonNegative(
    "graceMs",
    options.graceMs ?? SERVAL_BULKHEAD_DEFAULT_GRACE_MS,
  )
  const initialSlots = assertPositiveInteger(
    "initialSlots",
    options.initialSlots ?? SERVAL_BULKHEAD_DEFAULT_INITIAL_SLOTS,
  )
  const configuredMaximumSlots = options.maximumSlots
  if (configuredMaximumSlots !== undefined) {
    assertPositiveInteger("maximumSlots", configuredMaximumSlots)
  }
  const renewLeadMs = assertNonNegative(
    "renewLeadMs",
    options.renewLeadMs ?? Math.max(1, Math.floor(validityMs / 3)),
  )
  const redistributionIntervalMs = assertNonNegative(
    "redistributionIntervalMs",
    options.redistributionIntervalMs ??
      SERVAL_BULKHEAD_DEFAULT_REDISTRIBUTION_MS,
  )
  const safetyMarginMs = assertNonNegative(
    "safetyMarginMs",
    options.safetyMarginMs ?? SERVAL_DEFAULT_SAFETY_MARGIN_MS,
  )
  const maximumScopes = assertPositiveInteger(
    "maximumScopes",
    options.maximumScopes ?? SERVAL_BULKHEAD_DEFAULT_SCOPES,
  )
  const clock = options.now ?? (() => performance.now())
  const idFactory = options.idFactory ?? randomUUID

  const counters = {
    admissions: 0,
    localRefusals: 0,
    allocations: 0,
    renewals: 0,
    resizes: 0,
    returns: 0,
    refusals: 0,
    staleReplies: 0,
    unfrozenAfterReconciliation: 0,
    modificationFailures: 0,
    duplicates: 0,
    reconciliations: 0,
    registryRefusals: 0,
  }
  const scopes = new Map<string, AllocationScope>()
  const inFlight = new Set<Promise<unknown>>()
  let closed = false

  function track<T>(work: Promise<T>): Promise<T> {
    const tracked = work.finally(() => inFlight.delete(tracked))
    inFlight.add(tracked)
    return tracked
  }

  function scopeKey(identity: Identity): string {
    return JSON.stringify([identity.name, identity.operation, identity.scope])
  }

  /**
   * Whether this scope still holds an allocation the server may honour.
   *
   * The server keeps a reservation for the allocation's validity plus the grace window, so
   * until that passes the slots are this process's to spend or return. Afterwards the scope
   * holds nothing, whether or not it was touched again - which is what keeps a registry
   * entry from being pinned by an allocation nobody asked about.
   */
  function holdsAllocation(scope: AllocationScope, now: number): boolean {
    return scope.allocated > 0 && now < scope.deadline + graceMs
  }

  /**
   * A scope holds nothing once its allocation has lapsed, it has no holders, no frozen slots
   * and no unsettled modification, so it is reclaimable. Anything else is pinned: a registry
   * that cannot hold more refuses the new scope rather than dropping accounting for work that
   * is still running.
   *
   * Every reclaimable scope is dropped in one pass, so pressure settles the registry in a
   * single scan rather than one scope per new key.
   */
  function reclaimIdle(): boolean {
    const now = clock()
    let reclaimed = false
    for (const [key, scope] of scopes) {
      if (
        !holdsAllocation(scope, now) &&
        scope.active.size === 0 &&
        scope.frozen === 0 &&
        scope.modification === undefined &&
        scope.issuing === undefined
      ) {
        scopes.delete(key)
        reclaimed = true
      }
    }
    return reclaimed
  }

  function scopeFor(identity: Identity, limit: number): AllocationScope {
    const key = scopeKey(identity)
    const existing = scopes.get(key)
    if (existing !== undefined) {
      existing.limit = limit
      return existing
    }

    if (scopes.size >= maximumScopes && !reclaimIdle()) {
      counters.registryRefusals += 1
      throw new ServalRegistryPressureError(
        `The Serval bulkhead registry is full (${maximumScopes} scopes); refusing to track ${key}`,
      )
    }

    const scope: AllocationScope = {
      identity,
      state: "absent",
      grantId: "",
      revision: 0,
      allocated: 0,
      active: new Set(),
      frozen: 0,
      deadline: 0,
      limit,
      redistributionUntil: 0,
      adoptedFromRequestStartedAt: -1,
      modification: undefined,
      issuing: undefined,
    }
    scopes.set(key, scope)
    return scope
  }

  /** Slots that may be spent right now: granted, not held, not frozen. */
  function freeSpendable(scope: AllocationScope): number {
    return Math.max(0, scope.allocated - scope.active.size - scope.frozen)
  }

  /**
   * The allocation's validity: whether new work may start under it.
   *
   * An ambiguous *modification* does not invalidate the capacity this process already
   * holds - a frozen return only removes the slots it froze - so `uncertain` stays
   * usable until the deadline passes.
   */
  function isValid(scope: AllocationScope, now: number): boolean {
    return (
      scope.allocated > 0 &&
      now < scope.deadline &&
      (scope.state === "allocated" || scope.state === "uncertain")
    )
  }

  /**
   * Converts a server-clock expiry into a local deadline.
   *
   * The reply carries both the expiry and the server's own reading, so the
   * difference is a duration; anchoring that duration to this operation's send time
   * is what keeps a renewal from extending authority it did not have.
   */
  /** Slots this replica may hold: its own ceiling, never past the fleet's. */
  function maximumSlots(scope: AllocationScope): number {
    return Math.min(
      configuredMaximumSlots ?? scope.limit,
      Math.max(1, scope.limit),
    )
  }

  /**
   * Submits a frozen return that has not reached the authority yet.
   *
   * A freeze is an intent to give capacity back, and the modification carrying it is what settles
   * it. When another modification holds the wire, the intent waits for that one: the slots stay
   * frozen, which is what freeze-before-send requires, and they are submitted as soon as it
   * settles. Without this the freeze was recorded with nothing to submit it, and the slots stayed
   * unavailable - and unreturned - for the life of the grant.
   *
   * `frozen` is either carried by the in-flight modification or wholly unsubmitted, never a mix: a
   * second return is refused while one is in flight, so the two cannot diverge.
   */
  function submitPendingReturn(scope: AllocationScope): Promise<void> {
    const issuing = scope.issuing
    if (issuing !== undefined) {
      // The wire is busy. Follow that operation, then submit what the freeze left pending.
      return issuing.then(
        () => submitPendingReturn(scope),
        () => undefined,
      )
    }

    if (scope.frozen <= 0) return Promise.resolve()
    if (clock() < scope.redistributionUntil) return Promise.resolve()

    if (scope.allocated === 0) {
      // The allocation is gone, so there is nothing left to return: the intent dies with the
      // grant rather than being inherited by the next one.
      scope.frozen = 0
      return Promise.resolve()
    }

    if (scope.modification !== undefined) {
      // An unsettled operation owns the next request on the wire, and `retryRetained` retries it.
      // Submitting now would replace it and lose the reply it is waiting for.
      return Promise.resolve()
    }

    return startModification(scope, "resize", {
      slots: -scope.frozen,
      expectedRevision: scope.revision,
      frozenSlots: scope.frozen,
    })
  }

  /**
   * Starts one modification, or joins the one already in flight.
   *
   * A retained modification is retried with the same operation id, which is what
   * makes an ambiguous reply recoverable: the server returns its recorded result
   * instead of applying the change twice. The retry carries the original send-time
   * anchor, so a late commit cannot extend authority either.
   */
  function startModification(
    scope: AllocationScope,
    action: BulkheadAllocationAction,
    request: {
      readonly slots?: number
      readonly expectedRevision?: number
      readonly frozenSlots?: number
    } = {},
  ): Promise<void> {
    const existing = scope.issuing
    if (existing !== undefined) return existing

    const now = clock()
    const retained = scope.modification
    const modification: Modification =
      retained !== undefined && retained.action === action
        ? retained
        : {
            operationId: idFactory(),
            action,
            slots: request.slots ?? 0,
            expectedRevision: request.expectedRevision ?? scope.revision,
            requestStartedAt: now,
            validityMs,
            frozenSlots: request.frozenSlots ?? 0,
            slotsBefore: scope.allocated,
          }
    scope.modification = modification
    if (action === "allocate") scope.state = "allocating"

    const issuing = track(applyModification(scope, modification)).finally(
      () => {
        if (scope.issuing === issuing) scope.issuing = undefined
        // A return frozen while this operation held the wire takes it now.
        void track(submitPendingReturn(scope)).catch(() => undefined)
      },
    )
    scope.issuing = issuing
    return issuing
  }

  async function applyModification(
    scope: AllocationScope,
    modification: Modification,
  ): Promise<void> {
    let reply: BulkheadAllocationReply
    try {
      reply = await authority.modify({
        identity: scope.identity,
        replicaId,
        operationId: modification.operationId,
        action: modification.action,
        ...(modification.action === "allocate" ||
        modification.action === "resize"
          ? { slots: modification.slots }
          : {}),
        ...(modification.action === "allocate" ||
        modification.action === "renew"
          ? { validityMs: modification.validityMs }
          : {}),
        expectedRevision: modification.expectedRevision,
        limit: Math.max(1, scope.limit),
        graceMs,
      })
    } catch (error) {
      // An ambiguous outcome: the modification may have committed. Nothing local is
      // assumed from it, the operation is retained for a deduplicated retry, and any
      // frozen slots stay frozen.
      scope.state = "uncertain"
      counters.modificationFailures += 1
      scope.redistributionUntil = clock() + redistributionIntervalMs
      throw error
    }
    applyReply(scope, modification, reply)
  }

  /** Adopts an authoritative allocation, unless a reply from a later request is already applied. */
  function adopt(
    scope: AllocationScope,
    reply: {
      readonly slots: number
      readonly revision: number
      readonly expiresAt: number
      readonly serverTime: number
    },
    requestStartedAt: number,
  ): boolean {
    if (requestStartedAt < scope.adoptedFromRequestStartedAt) {
      // A stale reply never restores capacity, spendability or a deadline. Staleness is judged by
      // when the request was sent rather than by the revision it carries: the revision is a
      // per-grant counter, so a replacement allocation legitimately restarts at 1, and a lower
      // number means a newer grant rather than an older reply. A tie is not stale - a retry reuses
      // its original send time, and millisecond resolution cannot separate a same-tick update from
      // a race.
      counters.staleReplies += 1
      return false
    }
    scope.adoptedFromRequestStartedAt = requestStartedAt
    scope.allocated = reply.slots
    scope.revision = reply.revision
    // Frozen slots are slots this process asked the server to take back, and the modification that
    // froze them is what retires them. A reply reporting an allocation that no longer covers them
    // is that answer arriving by another route - the server has them - so they stop being ours to
    // return. Without this they stay frozen for the life of the grant, claiming a slot the
    // allocation does not hold. This only ever reduces the count, and adopt is the one place the
    // allocation changes, so every path is covered here rather than one branch at a time.
    scope.frozen = Math.min(
      scope.frozen,
      Math.max(0, reply.slots - scope.active.size),
    )
    scope.deadline =
      reply.slots === 0
        ? 0
        : servalDeadline(
            requestStartedAt,
            servalRemainingValidity(reply.expiresAt, reply.serverTime),
            safetyMarginMs,
          )
    return true
  }

  function applyReply(
    scope: AllocationScope,
    modification: Modification,
    reply: BulkheadAllocationReply,
  ): void {
    const now = clock()

    if (reply.type === "refused") {
      counters.refusals += 1
      // A refusal is definitive: the operation is complete and cannot be retried.
      scope.modification = undefined

      if (reply.reason === "expired") {
        // The allocation's validity ended. Within the grace window the server still
        // holds its reservation, so asking again before it lapses is futile - and the
        // holders stay recorded, because they are still running.
        scope.redistributionUntil = now + graceMs
        // The allocation is gone, so the frozen slots are not ours to return any more: the intent
        // dies with the grant instead of being inherited by the next one.
        scope.frozen = 0
        scope.allocated = 0
        scope.grantId = ""
        scope.deadline = 0
        scope.state = "expired"
        return
      }

      if (reply.reason === "revision" || reply.reason === "exists") {
        // Our view is behind, or an earlier operation did commit. Either way the
        // refusal carries the authority's allocation, so adopt it if it is ahead.
        adopt(scope, reply, modification.requestStartedAt)
        if (scope.allocated === 0) scope.state = "absent"
        return
      }

      // "limit": the fleet is at its ceiling. Stop asking for a while instead of
      // paying a round trip per admission.
      scope.redistributionUntil = now + redistributionIntervalMs
      if (scope.allocated === 0) scope.state = "absent"
      return
    }

    if (reply.duplicate) counters.duplicates += 1
    if (!adopt(scope, reply, modification.requestStartedAt)) return

    scope.modification = undefined
    if (modification.action === "allocate") {
      counters.allocations += 1
      // The server identifies an allocation by the operation that created it.
      scope.grantId = modification.operationId
    }
    if (modification.action === "renew") counters.renewals += 1
    if (modification.action === "resize") {
      if (modification.slots < 0) counters.returns += 1
      else counters.resizes += 1
    }
    if (modification.frozenSlots > 0) {
      // The return committed: those slots leave the local allocation for good, so
      // they are retired rather than unfrozen.
      scope.frozen = Math.max(0, scope.frozen - modification.frozenSlots)
    }
    if (scope.allocated === 0) {
      scope.grantId = ""
      scope.state = "retired"
      return
    }
    scope.state = "allocated"
  }

  /**
   * Brings the local allocation up to what the moment requires: a new allocation, a
   * renewal, or a demand-driven increase. It never spends authority it does not hold,
   * and it never waits: a scope that cannot be served is refused.
   */
  function maintain(scope: AllocationScope, demand: boolean): Promise<void> {
    if (closed) return Promise.resolve()
    const now = clock()

    if (scope.allocated === 0) {
      if (now < scope.redistributionUntil) return Promise.resolve()
      return startModification(scope, "allocate", { slots: initialSlots })
    }

    if (now >= scope.deadline) {
      // Validity ended: no new admissions under it, and a fresh allocation can only
      // be granted once the reservation's grace window has lapsed.
      scope.state = "draining"
      if (now < scope.redistributionUntil) return Promise.resolve()
      return startModification(scope, "renew", {
        expectedRevision: scope.revision,
      })
    }

    if (now >= scope.deadline - renewLeadMs) {
      // Renewal extends the deadline and advances the revision; it is not a grant.
      return startModification(scope, "renew", {
        expectedRevision: scope.revision,
      })
    }

    if (demand && freeSpendable(scope) === 0) {
      if (now < scope.redistributionUntil) return Promise.resolve()
      const ceiling = maximumSlots(scope)
      if (scope.allocated >= ceiling) return Promise.resolve()
      // Conservative, demand-driven growth: double what we hold, capped by our own
      // ceiling and by the fleet's remaining capacity.
      const growth = Math.max(
        1,
        Math.min(ceiling - scope.allocated, scope.allocated),
      )
      return startModification(scope, "resize", {
        slots: growth,
        expectedRevision: scope.revision,
      })
    }

    return Promise.resolve()
  }

  /**
   * Retries a retained modification in the background, or nudges a frozen return.
   *
   * Both are idempotent, and until one settles the slots it froze cannot be spent or restored - so
   * an admission that does not need the authority still nudges it, without waiting for the reply.
   */
  function retryRetained(scope: AllocationScope): void {
    if (closed) return
    if (clock() < scope.redistributionUntil) return
    const retained = scope.modification
    if (retained !== undefined) {
      void track(startModification(scope, retained.action)).catch(
        () => undefined,
      )
      return
    }
    void track(submitPendingReturn(scope)).catch(() => undefined)
  }

  function admit(
    scope: AllocationScope,
    token: string,
  ): { allowed: boolean; occupancy: number } {
    scope.active.add(token)
    counters.admissions += 1
    return { allowed: true, occupancy: scope.active.size }
  }

  /**
   * One admission. The fast path is synchronous: check the local pool, take a slot.
   * Only a scope that needs authority pays a round trip.
   */
  async function acquire(
    scope: AllocationScope,
    token: string,
  ): Promise<{ allowed: boolean; occupancy: number }> {
    if (closed) {
      counters.localRefusals += 1
      return { allowed: false, occupancy: scope.active.size }
    }

    for (
      let attempt = 0;
      attempt < SERVAL_BULKHEAD_MAXIMUM_ATTEMPTS;
      attempt += 1
    ) {
      if (isValid(scope, clock()) && freeSpendable(scope) > 0) {
        retryRetained(scope)
        return admit(scope, token)
      }
      await maintain(scope, true)
    }

    if (isValid(scope, clock()) && freeSpendable(scope) > 0) {
      retryRetained(scope)
      return admit(scope, token)
    }
    retryRetained(scope)
    counters.localRefusals += 1
    return { allowed: false, occupancy: scope.active.size }
  }

  /**
   * Freezes free slots, then asks the server to take them back.
   *
   * The freeze happens before the request is sent, so a server-side reallocation of
   * voluntarily returned slots can never precede this process's own freeze. A lost
   * reply leaves them frozen until the retry of the same operation settles them, and a
   * freeze made while another modification holds the wire is submitted as soon as that
   * one settles.
   */
  async function relinquish(
    identity: Identity,
    slots?: number,
  ): Promise<ServalRelinquishResult> {
    const key = scopeKey(identity)
    const scope = scopes.get(key)
    if (scope === undefined || scope.allocated === 0) {
      return { type: "nothing" }
    }

    if ((scope.modification?.frozenSlots ?? 0) > 0) {
      // A return is already in flight or awaiting reconciliation: freezing again
      // would return the same slots twice.
      return {
        type: "frozen",
        slots: scope.modification?.frozenSlots ?? 0,
        reason: "unknown",
      }
    }

    const free = freeSpendable(scope)
    const frozen = Math.min(slots ?? free, free)
    if (frozen <= 0) return { type: "nothing" }

    scope.frozen += frozen
    try {
      await submitPendingReturn(scope)
    } catch {
      // The outcome is unknown, so the slots stay frozen. Retrying the same operation is what
      // settles them.
      return { type: "frozen", slots: frozen, reason: "unavailable" }
    }

    if (scope.frozen >= frozen) {
      return { type: "frozen", slots: frozen, reason: "unknown" }
    }
    return { type: "returned", slots: frozen, revision: scope.revision }
  }

  const coordinator: ServalBulkheadCoordinator = {
    async command(identity, action, token, leaseMs, limit) {
      if (
        !["acquire", "renew", "release"].includes(action) ||
        typeof token !== "string" ||
        !token ||
        token.length > 256
      )
        throw new TypeError("Invalid bulkhead lease action or token")
      if (
        !Number.isSafeInteger(leaseMs) ||
        leaseMs < 1 ||
        !Number.isSafeInteger(limit) ||
        limit < 1
      )
        throw new RangeError("Invalid bulkhead lease duration or limit")

      const scope = scopeFor(identity, limit)
      if (action === "renew") {
        // A holder is tracked for its whole life, so renewal is local: an allocation
        // that lapsed never aborts work that is still running.
        return {
          allowed: scope.active.has(token),
          occupancy: scope.active.size,
        }
      }
      if (action === "release") {
        const released = scope.active.delete(token)
        return { allowed: released, occupancy: scope.active.size }
      }
      if (scope.active.has(token)) {
        return { allowed: true, occupancy: scope.active.size }
      }
      return acquire(scope, token)
    },

    metrics() {
      let frozenSlots = 0
      let activeHolders = 0
      let allocatedSlots = 0
      for (const scope of scopes.values()) {
        frozenSlots += scope.frozen
        activeHolders += scope.active.size
        allocatedSlots += scope.allocated
      }
      return {
        ...counters,
        scopes: scopes.size,
        frozenSlots,
        activeHolders,
        allocatedSlots,
      }
    },

    diagnostics(identity) {
      const scope = scopes.get(scopeKey(identity))
      if (scope === undefined) return undefined
      return {
        identity: scope.identity,
        state: scope.state,
        grantId: scope.grantId,
        revision: scope.revision,
        allocated: scope.allocated,
        active: scope.active.size,
        freeSpendable: freeSpendable(scope),
        frozenPendingReturn: scope.frozen,
        deadline: scope.deadline,
      }
    },

    relinquish,

    async close() {
      if (closed) return
      closed = true
      for (const scope of [...scopes.values()]) {
        try {
          await relinquish(scope.identity)
        } catch {
          // Best effort: shutdown never depends on a clean handoff, and a slot that
          // could not be returned is reclaimed by the server after its grace window.
        }
      }
      await coordinator.whenIdle()
    },

    async whenIdle() {
      while (inFlight.size > 0) {
        await Promise.all(
          [...inFlight].map((work) => work.catch(() => undefined)),
        )
      }
    },
  }

  return Object.freeze(coordinator)
}
