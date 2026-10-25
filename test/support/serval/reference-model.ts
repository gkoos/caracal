/**
 * The Serval authority reference model.
 *
 * `docs/serval.md` and `docs/serval-authority.md` are the contract; this module is
 * their proof obligation. It is deliberately small, deterministic and Redis-free:
 * every decision in the RFC has a model entry point here, and every acceptance
 * case in `test/unit/serval-authority.test.ts` and
 * `test/property/serval-authority.property.test.ts` drives one of these models.
 *
 * The model is not the implementation. It records a *violation* whenever a
 * contract invariant is broken, so a test fails with the invariant's name rather
 * than with a diff of two large structures. The session has its own oracle; this
 * module stays the specification's proof obligation.
 */

/** A monotonic millisecond clock. Only elapsed time is read, never wall time. */
export class MonotonicTime {
  #now: number

  constructor(initialNow = 0) {
    this.#now = initialNow
  }

  now(): number {
    return this.#now
  }

  advanceTo(target: number): void {
    if (!Number.isFinite(target) || target < this.#now) {
      throw new RangeError(
        `MonotonicTime cannot move from ${this.#now} to ${target}`,
      )
    }
    this.#now = target
  }

  advance(deltaMs: number): void {
    if (!Number.isFinite(deltaMs) || deltaMs < 0) {
      throw new RangeError(`MonotonicTime cannot advance by ${deltaMs}`)
    }
    this.advanceTo(this.#now + deltaMs)
  }
}

/**
 * Shared violation recording. A model that cannot record a violation cannot
 * fail a test, so every contract check funnels through `violation()`.
 */
export abstract class ContractModel {
  readonly #violations: string[] = []

  protected violation(message: string): void {
    this.#violations.push(message)
  }

  get violations(): readonly string[] {
    return this.#violations
  }
}

// ---------------------------------------------------------------------------
// Decisions 1 and 11 - send-time-anchored validity, identity and revision order
// ---------------------------------------------------------------------------

export interface GrantReply {
  readonly operationId: string
  readonly grantId: string
  readonly authorityIncarnation: string
  readonly leaseRevision: number
  /** The full lifetime the server grants, measured from issuance. */
  readonly validForMs: number
  readonly payload?: number
}

export interface GrantLedgerOptions {
  readonly safetyMarginMs?: number
  /** How long a settled operation is protected against a replayed reply. */
  readonly replayRetentionMs?: number
  /** Upper bound used to size the replay-retention window. */
  readonly maximumGrantValidityMs?: number
}

export interface Grant {
  readonly grantId: string
  readonly authorityIncarnation: string
  readonly leaseRevision: number
  readonly deadline: number
  readonly payload: number | undefined
}

export type GrantReplyOutcome =
  | {
      readonly accepted: true
      readonly duplicate: boolean
      readonly deadline: number
    }
  | {
      readonly accepted: false
      readonly duplicate: boolean
      readonly reason:
        | "late"
        | "unknown-operation"
        | "stale-revision"
        | "retired-incarnation"
        | "unknown-incarnation"
        | "replacement-while-valid"
    }

export type AdmissionDecision =
  | { readonly kind: "admit"; readonly grantId: string }
  | {
      readonly kind: "reject"
      readonly reason: "no-authority" | "expired" | "uncertain"
    }

interface OperationRecord {
  readonly requestStartedAt: number
  readonly retainedUntil: number
  settled: boolean
  result?: GrantReplyOutcome
}

/**
 * The client-side grant ledger: send-time anchoring, monotonic revision order,
 * incarnation adoption and replay tombstones (decisions 1, 5 and 11).
 */
export class GrantLedger extends ContractModel {
  readonly #clock: MonotonicTime
  readonly #safetyMarginMs: number
  readonly #replayRetentionMs: number
  readonly #maximumGrantValidityMs: number
  readonly #operations = new Map<string, OperationRecord>()
  readonly #retiredIncarnations = new Set<string>()
  #grant: Grant | undefined
  #uncertain = false
  #admissions = 0

  constructor(clock: MonotonicTime, options: GrantLedgerOptions = {}) {
    super()
    this.#clock = clock
    this.#safetyMarginMs = options.safetyMarginMs ?? 0
    this.#replayRetentionMs = options.replayRetentionMs ?? 30_000
    this.#maximumGrantValidityMs = options.maximumGrantValidityMs ?? 60_000
  }

  get grant(): Grant | undefined {
    return this.#grant
  }

  get uncertain(): boolean {
    return this.#uncertain
  }

  get admissions(): number {
    return this.#admissions
  }

  /**
   * Records the send-time anchor for an operation. A retry for a known
   * `operationId` returns the original anchor: a retry never mints a new window.
   */
  beginOperation(operationId: string): {
    readonly requestStartedAt: number
    readonly retried: boolean
  } {
    const existing = this.#operations.get(operationId)
    if (existing !== undefined) {
      return { requestStartedAt: existing.requestStartedAt, retried: true }
    }

    const requestStartedAt = this.#clock.now()
    this.#operations.set(operationId, {
      requestStartedAt,
      retainedUntil:
        requestStartedAt +
        this.#maximumGrantValidityMs +
        this.#replayRetentionMs,
      settled: false,
    })
    return { requestStartedAt, retried: false }
  }

  /** True while an operation is still protected against a replayed reply. */
  isRetained(operationId: string): boolean {
    return this.#operations.has(operationId)
  }

  /** Retires operations whose replay window has closed. Never retires live work. */
  collectRetiredOperations(now = this.#clock.now()): number {
    let retired = 0
    for (const [operationId, record] of this.#operations) {
      if (record.settled && now > record.retainedUntil) {
        this.#operations.delete(operationId)
        retired += 1
      }
    }
    return retired
  }

  /** Marks an incarnation as retired; its delayed replies can no longer be adopted. */
  retireIncarnation(incarnation: string): void {
    this.#retiredIncarnations.add(incarnation)
  }

  clearUncertain(): void {
    this.#uncertain = false
  }

  /**
   * Applies an authoritative reply. The deadline is computed from the operation's
   * original send-time anchor, never from the receipt time, and a reply that
   * arrives after that deadline is unusable (decision 1).
   */
  acceptReply(reply: GrantReply): GrantReplyOutcome {
    const record = this.#operations.get(reply.operationId)
    if (record === undefined) {
      // The replay window has closed: a late reply cannot mint authority.
      return { accepted: false, duplicate: false, reason: "unknown-operation" }
    }

    if (record.settled && record.result !== undefined) {
      return { ...record.result, duplicate: true }
    }

    const settle = (outcome: GrantReplyOutcome): GrantReplyOutcome => {
      record.settled = true
      record.result = outcome
      return outcome
    }

    const deadline =
      record.requestStartedAt + reply.validForMs - this.#safetyMarginMs
    const receivedAt = this.#clock.now()

    if (receivedAt > deadline) {
      return settle({ accepted: false, duplicate: false, reason: "late" })
    }

    if (this.#retiredIncarnations.has(reply.authorityIncarnation)) {
      return settle({
        accepted: false,
        duplicate: false,
        reason: "retired-incarnation",
      })
    }

    const current = this.#grant
    if (
      current !== undefined &&
      current.authorityIncarnation !== reply.authorityIncarnation
    ) {
      // Adoption requires an authoritative reconciliation, never a stray reply.
      this.#uncertain = true
      return settle({
        accepted: false,
        duplicate: false,
        reason: "unknown-incarnation",
      })
    }

    if (current !== undefined && current.grantId === reply.grantId) {
      if (reply.leaseRevision <= current.leaseRevision) {
        return settle({
          accepted: false,
          duplicate: false,
          reason: "stale-revision",
        })
      }
    } else if (current !== undefined && receivedAt < current.deadline) {
      return settle({
        accepted: false,
        duplicate: false,
        reason: "replacement-while-valid",
      })
    }

    this.#grant = {
      grantId: reply.grantId,
      authorityIncarnation: reply.authorityIncarnation,
      leaseRevision: reply.leaseRevision,
      deadline,
      payload: reply.payload,
    }
    this.#uncertain = false
    return settle({ accepted: true, duplicate: false, deadline })
  }

  /**
   * The local admission decision. Authority is evaluated at the moment of
   * admission, so a renewal can never back-date coverage over a gap: the interval
   * between the old deadline and the renewal's receipt admits nothing.
   */
  mayAdmit(now = this.#clock.now()): AdmissionDecision {
    if (this.#uncertain) {
      return { kind: "reject", reason: "uncertain" }
    }

    const grant = this.#grant
    if (grant === undefined) {
      return { kind: "reject", reason: "no-authority" }
    }
    if (now >= grant.deadline) {
      return { kind: "reject", reason: "expired" }
    }

    this.#admissions += 1
    return { kind: "admit", grantId: grant.grantId }
  }
}

// ---------------------------------------------------------------------------
// Decision 2 - cached-OPEN cooldown, coalesced probing, protective liveness
// ---------------------------------------------------------------------------

export type BreakerState = "closed" | "open" | "half-open"

export interface BreakerTransition {
  readonly state: BreakerState
  readonly version: number
  readonly generation: number
  /** Server time at which the breaker opened; the cooldown is anchored to it. */
  readonly openedAt: number
}

export interface BreakerLivenessOptions {
  readonly cooldownMs?: number
  readonly freshnessMs?: number
  readonly staleClosedBehavior?: "fail-open" | "fail-closed"
}

export type BreakerAdmission =
  | {
      readonly kind: "admit"
      readonly source: "cached-closed" | "stale-closed" | "probe"
    }
  | {
      readonly kind: "reject"
      readonly reason:
        | "cached-open"
        | "probe-in-flight"
        | "stale-closed"
        | "unavailable"
        | "reserved-elsewhere"
    }

/**
 * The cached breaker verdict (decision 2). A cached OPEN is a protective floor:
 * it rejects until an authoritative cooldown hint makes a probe eligible, and it
 * never becomes HALF_OPEN without a reservation from the authority.
 */
export class BreakerLiveness extends ContractModel {
  readonly #cooldownMs: number
  readonly #freshnessMs: number
  readonly #staleClosedBehavior: "fail-open" | "fail-closed"
  #state: BreakerState = "closed"
  #version = 0
  #generation = 0
  #generationAtOpen = 0
  #openedAt = 0
  #closedFreshUntil = 0
  #probeInFlight = false
  #redisAvailable = true
  #probeAttempts = 0
  #localAdmissions = 0

  constructor(options: BreakerLivenessOptions = {}) {
    super()
    this.#cooldownMs = options.cooldownMs ?? 5_000
    this.#freshnessMs = options.freshnessMs ?? 100
    this.#staleClosedBehavior = options.staleClosedBehavior ?? "fail-closed"
  }

  get state(): BreakerState {
    return this.#state
  }

  get cooldownEligibleAt(): number {
    return this.#openedAt + this.#cooldownMs
  }

  get probeInFlight(): boolean {
    return this.#probeInFlight
  }

  /** Authoritative admission attempts, i.e. Redis round trips on this path. */
  get probeAttempts(): number {
    return this.#probeAttempts
  }

  get localAdmissions(): number {
    return this.#localAdmissions
  }

  setRedisAvailable(available: boolean): void {
    this.#redisAvailable = available
  }

  admit(now: number): BreakerAdmission {
    if (this.#state === "closed") {
      if (now <= this.#closedFreshUntil) {
        this.#localAdmissions += 1
        return { kind: "admit", source: "cached-closed" }
      }
      if (this.#staleClosedBehavior === "fail-open") {
        this.#localAdmissions += 1
        return { kind: "admit", source: "stale-closed" }
      }
      return { kind: "reject", reason: "stale-closed" }
    }

    if (this.#state === "half-open") {
      if (this.#probeInFlight) {
        return { kind: "admit", source: "probe" }
      }
      return { kind: "reject", reason: "reserved-elsewhere" }
    }

    if (now < this.cooldownEligibleAt) {
      return { kind: "reject", reason: "cached-open" }
    }
    if (this.#probeInFlight) {
      return { kind: "reject", reason: "probe-in-flight" }
    }
    if (!this.#redisAvailable) {
      // Never fabricate a probe while the authority is unreachable.
      return { kind: "reject", reason: "unavailable" }
    }

    this.#probeInFlight = true
    this.#probeAttempts += 1
    return { kind: "admit", source: "probe" }
  }

  /** Applies an authoritative transition. A stale version cannot overwrite state. */
  applyTransition(transition: BreakerTransition, now: number): void {
    if (transition.version <= this.#version) {
      return
    }

    this.#version = transition.version
    this.#generation = transition.generation
    this.#state = transition.state

    if (transition.state === "open") {
      this.#generationAtOpen = transition.generation
      this.#openedAt = transition.openedAt
    }
    if (transition.state === "closed") {
      this.#closedFreshUntil = now + this.#freshnessMs
    }
    if (transition.state !== "half-open") {
      this.#probeInFlight = false
    }
  }

  /**
   * Settles an authoritative probe. A settlement from a generation that predates
   * the current OPEN episode cannot close it.
   */
  settleProbe(
    outcome: "success" | "failure",
    settlementGeneration: number,
    now: number,
  ): "applied" | "obsolete" | "no-reservation" {
    if (!this.#probeInFlight) {
      this.violation("probe settled without an authoritative reservation")
      return "no-reservation"
    }
    if (settlementGeneration < this.#generationAtOpen) {
      return "obsolete"
    }

    this.#probeInFlight = false
    this.#version += 1

    if (outcome === "success") {
      this.#state = "closed"
      this.#closedFreshUntil = now + this.#freshnessMs
      return "applied"
    }

    this.#state = "open"
    this.#generation += 1
    this.#generationAtOpen = this.#generation
    this.#openedAt = now
    return "applied"
  }
}

// ---------------------------------------------------------------------------
// Decision 3 - authority-history loss: fenced holdoff or unfenced deviation
// ---------------------------------------------------------------------------

export type FencingSource = "verified" | "none"
export type HistoryLossMode = "fenced-holdoff" | "unfenced-overlap"

export interface AuthorityHistoryOptions {
  readonly fencing: FencingSource
  readonly maximumOldGrantValidityMs: number
  readonly safetyMarginMs?: number
}

export interface HistoryLossEvent {
  readonly at: number
  /** The last possible old issuance or renewal time, independently established. */
  readonly lastOldIssuanceAt: number
  readonly incarnation: string
}

/**
 * The server-side authority lifecycle across a failover or restore that loses
 * acknowledged issuance. A holdoff is only ever claimed when a verified fence
 * makes `T_last_old_issue` meaningful (decisions 3 and 10).
 */
export class AuthorityHistory extends ContractModel {
  readonly #fencing: FencingSource
  readonly #maximumOldGrantValidityMs: number
  readonly #safetyMarginMs: number
  readonly #deviations: string[] = []
  #incarnation = "authority-0"
  #mode: HistoryLossMode = "fenced-holdoff"
  #pausedUntil = 0
  #issuances = 0

  constructor(options: AuthorityHistoryOptions) {
    super()
    this.#fencing = options.fencing
    this.#maximumOldGrantValidityMs = options.maximumOldGrantValidityMs
    this.#safetyMarginMs = options.safetyMarginMs ?? 0
  }

  get incarnation(): string {
    return this.#incarnation
  }

  get mode(): HistoryLossMode {
    return this.#mode
  }

  get pausedUntil(): number {
    return this.#pausedUntil
  }

  get deviations(): readonly string[] {
    return this.#deviations
  }

  get issuances(): number {
    return this.#issuances
  }

  /** True only while the model is allowed to claim a bounded overlap window. */
  get claimsBoundedHoldoff(): boolean {
    return this.#mode === "fenced-holdoff"
  }

  loseHistory(event: HistoryLossEvent): {
    readonly mode: HistoryLossMode
    readonly resumesAt: number
  } {
    this.#incarnation = event.incarnation

    if (this.#fencing === "none") {
      // No verified fence: no finite wait is meaningful, so no holdoff is claimed.
      this.#mode = "unfenced-overlap"
      this.#pausedUntil = event.at
      this.#deviations.push(`competing authority permitted from ${event.at}`)
      return { mode: this.#mode, resumesAt: this.#pausedUntil }
    }

    this.#mode = "fenced-holdoff"
    this.#pausedUntil =
      event.lastOldIssuanceAt +
      this.#maximumOldGrantValidityMs +
      this.#safetyMarginMs
    return { mode: this.#mode, resumesAt: this.#pausedUntil }
  }

  issue(now: number): { readonly issued: boolean; readonly reason?: string } {
    if (now < this.#pausedUntil) {
      return { issued: false, reason: "issuance-paused" }
    }
    this.#issuances += 1
    return { issued: true }
  }
}

// ---------------------------------------------------------------------------
// Decision 10 - monotonic, authoritative incarnation adoption
// ---------------------------------------------------------------------------

export type AdoptionState = "uninitialized" | "valid" | "uncertain"
export type AdoptionObservation = "current" | "ignored-retired" | "uncertain"

/**
 * Client-side incarnation adoption. Only an authoritative reconciliation or
 * fencing decision switches the client; a delayed reply or notification never
 * does, and a retired incarnation is never re-adopted.
 */
export class IncarnationAdoption extends ContractModel {
  readonly #retired = new Set<string>()
  #current: string | undefined
  #state: AdoptionState = "uninitialized"
  #adoptions = 0

  get current(): string | undefined {
    return this.#current
  }

  get state(): AdoptionState {
    return this.#state
  }

  get adoptions(): number {
    return this.#adoptions
  }

  get retiredIncarnations(): readonly string[] {
    return [...this.#retired]
  }

  reconcile(
    incarnation: string,
    options: { readonly supersedesRetired?: boolean } = {},
  ): { readonly adopted: boolean; readonly reason?: string } {
    if (this.#retired.has(incarnation) && options.supersedesRetired !== true) {
      this.violation(
        `reconciliation adopted a retired incarnation: ${incarnation}`,
      )
      return { adopted: false, reason: "retired-incarnation" }
    }

    this.#current = incarnation
    this.#state = "valid"
    this.#adoptions += 1
    return { adopted: true }
  }

  retire(incarnation: string): void {
    this.#retired.add(incarnation)
    if (this.#current === incarnation) {
      this.#current = undefined
      this.#state = "uncertain"
    }
  }

  /** A late command reply or notification. It never switches the client. */
  observe(incarnation: string): AdoptionObservation {
    if (incarnation === this.#current) {
      return "current"
    }
    if (this.#retired.has(incarnation)) {
      return "ignored-retired"
    }
    this.#state = "uncertain"
    return "uncertain"
  }
}

// ---------------------------------------------------------------------------
// Decision 4 - configuration revision, effective time and deficit accounting
// ---------------------------------------------------------------------------

export interface ConfigurationRevision {
  readonly revision: number
  readonly limit: number
}

export interface DeficitState {
  readonly deficit: number
  readonly issuancePaused: boolean
}

/**
 * A limit reduction does not revoke outstanding authority. It pauses or reduces
 * new issuance until `outstandingReserved - limit` clears (decision 4).
 */
export class ConfigurationLedger extends ContractModel {
  #revision = 0
  #limit: number
  #outstandingReserved = 0
  #issuancePaused = false
  #refusedIssuances = 0

  constructor(limit: number) {
    super()
    if (!Number.isFinite(limit) || limit < 0) {
      throw new RangeError(
        `ConfigurationLedger limit must be >= 0, got ${limit}`,
      )
    }
    this.#limit = limit
  }

  get revision(): number {
    return this.#revision
  }

  get limit(): number {
    return this.#limit
  }

  get outstandingReserved(): number {
    return this.#outstandingReserved
  }

  get issuancePaused(): boolean {
    return this.#issuancePaused
  }

  get refusedIssuances(): number {
    return this.#refusedIssuances
  }

  applyRevision(next: ConfigurationRevision): DeficitState {
    if (next.revision <= this.#revision) {
      this.violation(
        `configuration revision did not advance: ${next.revision} <= ${this.#revision}`,
      )
      return this.#deficitState()
    }

    this.#revision = next.revision
    this.#limit = next.limit
    return this.#deficitState()
  }

  reserve(amount: number): {
    readonly granted: number
    readonly paused: boolean
  } {
    if (this.#issuancePaused) {
      this.#refusedIssuances += 1
      return { granted: 0, paused: true }
    }

    const granted = Math.max(
      0,
      Math.min(amount, this.#limit - this.#outstandingReserved),
    )
    this.#outstandingReserved += granted
    return { granted, paused: this.#issuancePaused }
  }

  release(amount: number): DeficitState {
    this.#outstandingReserved = Math.max(0, this.#outstandingReserved - amount)
    return this.#deficitState()
  }

  #deficitState(): DeficitState {
    const deficit = Math.max(0, this.#outstandingReserved - this.#limit)
    this.#issuancePaused = deficit > 0
    return { deficit, issuancePaused: this.#issuancePaused }
  }
}

// ---------------------------------------------------------------------------
// Decision 5 - pinned and protective records, replay tombstones
// ---------------------------------------------------------------------------

export type RegistryEntryClass = "disposable" | "pinned" | "protective"

export interface RegistryEntry {
  readonly key: string
  readonly entryClass: RegistryEntryClass
  readonly retainedUntil: number
}

/**
 * The bounded authority registry. Pinned accounting and protective knowledge are
 * never evicted to satisfy the bound: a new scope is refused instead (decision 5).
 */
export class AuthorityRegistry extends ContractModel {
  readonly #capacity: number
  readonly #entries = new Map<string, RegistryEntry>()
  #refusals = 0
  #evictions = 0

  constructor(capacity: number) {
    super()
    if (!Number.isInteger(capacity) || capacity < 1) {
      throw new RangeError(
        `AuthorityRegistry capacity must be a positive integer, got ${capacity}`,
      )
    }
    this.#capacity = capacity
  }

  get size(): number {
    return this.#entries.size
  }

  get refusals(): number {
    return this.#refusals
  }

  get evictions(): number {
    return this.#evictions
  }

  has(key: string): boolean {
    return this.#entries.has(key)
  }

  get(key: string): RegistryEntry | undefined {
    return this.#entries.get(key)
  }

  register(
    entry: RegistryEntry,
    now: number,
  ): { readonly registered: boolean; readonly reason?: "capacity" } {
    if (this.#entries.has(entry.key)) {
      this.#entries.set(entry.key, entry)
      return { registered: true }
    }

    if (this.#entries.size >= this.#capacity) {
      this.evictDisposable(now)
    }

    if (this.#entries.size >= this.#capacity) {
      this.#refusals += 1
      return { registered: false, reason: "capacity" }
    }

    this.#entries.set(entry.key, entry)
    return { registered: true }
  }

  /** Evicts only disposable entries whose retention has passed. */
  evictDisposable(now: number): number {
    let evicted = 0
    for (const [key, entry] of this.#entries) {
      if (entry.entryClass === "disposable" && now > entry.retainedUntil) {
        this.#entries.delete(key)
        evicted += 1
      }
    }
    this.#evictions += evicted
    return evicted
  }
}

// ---------------------------------------------------------------------------
// Decision 6 - tracked occupancy versus physical downstream work
// ---------------------------------------------------------------------------

export interface IncarnationOccupancy {
  readonly incarnationId: string
  readonly localLimit: number
  tracked: number
  physical: number
  retired: boolean
}

/**
 * Tracked occupancy is bounded by the sum of the local guardrail limits over
 * incarnations with live holders. Physical work is counted separately, because it
 * can outlive a retired process (decision 6).
 */
export class OccupancyLedger extends ContractModel {
  readonly #incarnations = new Map<string, IncarnationOccupancy>()
  #untrackedPhysical = 0

  get trackedTotal(): number {
    let total = 0
    for (const incarnation of this.#incarnations.values()) {
      total += incarnation.tracked
    }
    return total
  }

  get physicalTotal(): number {
    let total = 0
    for (const incarnation of this.#incarnations.values()) {
      total += incarnation.physical
    }
    return total
  }

  /** Work that survived its process: physical only, never tracked. */
  get untrackedPhysical(): number {
    return this.#untrackedPhysical
  }

  /** The local-guardrail ceiling: local limits of incarnations with live holders. */
  get trackedCeiling(): number {
    let ceiling = 0
    for (const incarnation of this.#incarnations.values()) {
      if (incarnation.tracked > 0) {
        ceiling += incarnation.localLimit
      }
    }
    return ceiling
  }

  incarnation(incarnationId: string): IncarnationOccupancy | undefined {
    return this.#incarnations.get(incarnationId)
  }

  register(incarnationId: string, localLimit: number): void {
    if (!Number.isInteger(localLimit) || localLimit < 1) {
      throw new RangeError(
        `local limit must be a positive integer, got ${localLimit}`,
      )
    }
    this.#incarnations.set(incarnationId, {
      incarnationId,
      localLimit,
      tracked: 0,
      physical: 0,
      retired: false,
    })
  }

  admit(incarnationId: string): boolean {
    const incarnation = this.#incarnations.get(incarnationId)
    if (incarnation === undefined) {
      this.violation(
        `admission for an unregistered incarnation: ${incarnationId}`,
      )
      return false
    }
    if (incarnation.retired) {
      this.violation(`admission after retirement: ${incarnationId}`)
      return false
    }
    if (incarnation.tracked >= incarnation.localLimit) {
      return false
    }

    incarnation.tracked += 1
    incarnation.physical += 1
    this.assertTrackedWithinCeiling()
    return true
  }

  settle(incarnationId: string): void {
    const incarnation = this.#incarnations.get(incarnationId)
    if (incarnation === undefined || incarnation.tracked === 0) {
      this.violation(`settlement without a tracked holder: ${incarnationId}`)
      return
    }
    incarnation.tracked -= 1
    incarnation.physical -= 1
  }

  /** Stops new admissions. Tracked holders stay counted until they settle. */
  retire(incarnationId: string): void {
    const incarnation = this.#incarnations.get(incarnationId)
    if (incarnation === undefined) {
      this.violation(
        `retirement of an unregistered incarnation: ${incarnationId}`,
      )
      return
    }
    incarnation.retired = true
  }

  /**
   * Models downstream work that survives cancellation, a retired process or a
   * resolved response header. It is physical only, and it is a recorded deviation
   * rather than an invariant violation.
   */
  physicalWorkSurvives(incarnationId: string, count: number): void {
    const incarnation = this.#incarnations.get(incarnationId)
    if (incarnation === undefined) {
      this.violation(
        `surviving work for an unregistered incarnation: ${incarnationId}`,
      )
      return
    }
    incarnation.physical += count
    this.#untrackedPhysical += count
  }

  assertTrackedWithinCeiling(): void {
    const tracked = this.trackedTotal
    const ceiling = this.trackedCeiling
    if (tracked > ceiling) {
      this.violation(
        `tracked occupancy ${tracked} exceeds the local-guardrail ceiling ${ceiling}`,
      )
    }
  }
}

// ---------------------------------------------------------------------------
// Decisions 7, 9 and 11 - slot allocation, free-only return, freeze handoff
// ---------------------------------------------------------------------------

export type BulkheadOperationKind =
  | "renew"
  | "resize-up"
  | "resize-down"
  | "relinquish"

export interface BulkheadOperation {
  readonly operationId: string
  readonly kind: BulkheadOperationKind
  readonly amount: number
  readonly expectedLeaseRevision: number
  readonly requestStartedAt: number
  /** The conservative reply deadline, anchored to the send time. */
  readonly deadline: number
  readonly validForMs: number
}

export type BulkheadReply =
  | { readonly kind: "committed" }
  | { readonly kind: "refused"; readonly reason: string }
  | { readonly kind: "lost" }

export type BulkheadCompletion =
  | "applied"
  | "duplicate"
  | "stale"
  | "uncertain"
  | "no-operation"

export type BulkheadState =
  | "absent"
  | "allocated"
  | "draining"
  | "uncertain"
  | "expired"
  | "retired"

export interface BulkheadAllocationOptions {
  readonly safetyMarginMs?: number
  /** The reply lifetime a modification operation is granted. */
  readonly replyTimeoutMs?: number
  /** Fleet-wide capacity the authority never exceeds. */
  readonly serverCapacity: number
}

/**
 * The local slot allocation (decisions 7, 9 and 11). Modifications are
 * serialised, the lease revision only advances, and returned capacity is frozen
 * before the reduction is sent.
 */
export class BulkheadAllocation extends ContractModel {
  readonly #clock: MonotonicTime
  readonly #safetyMarginMs: number
  readonly #replyTimeoutMs: number
  readonly #serverCapacity: number
  readonly #operationResults = new Map<string, BulkheadReply>()
  #grantId = "grant-0"
  #leaseRevision = 0
  #allocated = 0
  #active = 0
  #freeSpendable = 0
  #frozenPendingReturn = 0
  #deadline = 0
  #state: BulkheadState = "absent"
  #serverAllocated = 0
  #inFlight: BulkheadOperation | undefined
  #nextOperation = 0

  constructor(clock: MonotonicTime, options: BulkheadAllocationOptions) {
    super()
    this.#clock = clock
    this.#safetyMarginMs = options.safetyMarginMs ?? 0
    this.#replyTimeoutMs = options.replyTimeoutMs ?? 1_000
    this.#serverCapacity = options.serverCapacity
  }

  get state(): BulkheadState {
    return this.#state
  }

  get grantId(): string {
    return this.#grantId
  }

  get leaseRevision(): number {
    return this.#leaseRevision
  }

  get allocated(): number {
    return this.#allocated
  }

  get active(): number {
    return this.#active
  }

  get freeSpendable(): number {
    return this.#freeSpendable
  }

  get frozenPendingReturn(): number {
    return this.#frozenPendingReturn
  }

  get deadline(): number {
    return this.#deadline
  }

  get serverAllocated(): number {
    return this.#serverAllocated
  }

  get inFlight(): BulkheadOperation | undefined {
    return this.#inFlight
  }

  /** Grants the initial allocation. */
  allocate(amount: number, validForMs: number): void {
    if (this.#state !== "absent") {
      this.violation(`allocation started from state ${this.#state}`)
      return
    }
    if (amount > this.#serverCapacity) {
      this.violation(
        `allocation ${amount} exceeds the fleet capacity ${this.#serverCapacity}`,
      )
      return
    }

    this.#allocated = amount
    this.#freeSpendable = amount
    this.#serverAllocated = amount
    this.#deadline = this.#clock.now() + validForMs - this.#safetyMarginMs
    this.#state = "allocated"
    this.#assertAccounting()
  }

  /** Admits a holder inside the allocation. Never admits past the deadline. */
  acquireHolder(now = this.#clock.now()): boolean {
    if (this.#state !== "allocated") {
      return false
    }
    if (now >= this.#deadline) {
      this.#state = "draining"
      return false
    }
    if (this.#freeSpendable === 0) {
      return false
    }

    this.#active += 1
    this.#freeSpendable -= 1
    this.#assertAccounting()
    return true
  }

  releaseHolder(): void {
    if (this.#active === 0) {
      this.violation("a holder was released without an active holder")
      return
    }
    this.#active -= 1
    this.#freeSpendable += 1
    this.#assertAccounting()
  }

  /** Allocation validity is separate from holder lifetime. */
  expire(now = this.#clock.now()): void {
    if (this.#state === "allocated" && now >= this.#deadline) {
      this.#state = "draining"
    }
    if (this.#state === "draining" && this.#active === 0) {
      this.#state = "expired"
    }
  }

  /** A renewal is idempotent: it never adds capacity and never changes grantId. */
  beginRenew(validForMs = this.#replyTimeoutMs): BulkheadOperation {
    return this.#begin("renew", 0, validForMs)
  }

  /** Resize-up reserves only the delta. */
  beginResizeUp(
    delta: number,
    validForMs = this.#replyTimeoutMs,
  ): BulkheadOperation {
    if (this.#serverAllocated + delta > this.#serverCapacity) {
      this.violation(
        `resize-up of ${delta} would exceed the fleet capacity ${this.#serverCapacity}`,
      )
    }
    return this.#begin("resize-up", delta, validForMs)
  }

  /**
   * Resize-down freezes free slots before the request is sent, so a server-side
   * reallocation can never precede the client's freeze. Active holders are never
   * returned.
   */
  beginResizeDown(
    amount: number,
    validForMs = this.#replyTimeoutMs,
  ): BulkheadOperation {
    if (amount > this.#freeSpendable) {
      this.violation(
        `free-only return violated: requested ${amount} of ${this.#freeSpendable} free slots`,
      )
    }

    const frozen = Math.min(amount, this.#freeSpendable)
    this.#freeze(frozen)
    return this.#begin("resize-down", frozen, validForMs)
  }

  /** Relinquishes every free slot. */
  beginRelinquish(validForMs = this.#replyTimeoutMs): BulkheadOperation {
    const frozen = this.#freeSpendable
    this.#freeze(frozen)
    return this.#begin("relinquish", frozen, validForMs)
  }

  #freeze(amount: number): void {
    this.#freeSpendable -= amount
    this.#frozenPendingReturn += amount
    this.#assertAccounting()
  }

  #begin(
    kind: BulkheadOperationKind,
    amount: number,
    validForMs: number,
  ): BulkheadOperation {
    const inFlight = this.#inFlight
    if (inFlight !== undefined) {
      // v1 serialises modifications per allocation.
      this.violation(
        `a ${kind} operation started while ${inFlight.operationId} was in flight`,
      )
      return inFlight
    }
    if (this.#state !== "allocated") {
      this.violation(`a ${kind} operation started from state ${this.#state}`)
    }

    this.#nextOperation += 1
    const operation: BulkheadOperation = {
      operationId: `op-${this.#nextOperation}`,
      kind,
      amount,
      expectedLeaseRevision: this.#leaseRevision,
      requestStartedAt: this.#clock.now(),
      validForMs,
      deadline: this.#clock.now() + validForMs - this.#safetyMarginMs,
    }
    this.#inFlight = operation
    return operation
  }

  #assertAccounting(): void {
    if (
      this.#active + this.#freeSpendable + this.#frozenPendingReturn >
      this.#allocated
    ) {
      this.violation(
        `slot accounting broke: active ${this.#active} + free ${this.#freeSpendable} + frozen ${this.#frozenPendingReturn} > allocated ${this.#allocated}`,
      )
    }
    if (this.#allocated > this.#serverAllocated) {
      this.violation(
        `local allocation ${this.#allocated} exceeds the server allocation ${this.#serverAllocated}`,
      )
    }
    if (this.#serverAllocated > this.#serverCapacity) {
      this.violation(
        `server allocation ${this.#serverAllocated} exceeds the fleet capacity ${this.#serverCapacity}`,
      )
    }
  }

  /**
   * Applies an operation reply. A duplicate returns the recorded result without
   * re-applying it; a reply that does not match the in-flight operation is stale
   * and changes nothing; an uncertain outcome leaves frozen slots frozen.
   */
  complete(
    operation: BulkheadOperation,
    reply: BulkheadReply,
  ): BulkheadCompletion {
    if (this.#operationResults.has(operation.operationId)) {
      return "duplicate"
    }

    const inFlight = this.#inFlight
    if (
      inFlight === undefined ||
      inFlight.operationId !== operation.operationId
    ) {
      return "stale"
    }

    if (reply.kind === "lost") {
      // Nothing is recorded: the operation is still in flight and may be retried,
      // and the frozen slots stay frozen meanwhile.
      this.#state = "uncertain"
      return "uncertain"
    }

    if (reply.kind === "refused") {
      // A definitive refusal is recorded, but only reconciliation may unfreeze.
      this.#operationResults.set(operation.operationId, reply)
      this.#state = "uncertain"
      return "uncertain"
    }

    if (operation.expectedLeaseRevision !== this.#leaseRevision) {
      return "stale"
    }

    this.#operationResults.set(operation.operationId, reply)
    this.#inFlight = undefined
    this.#state = "allocated"

    switch (operation.kind) {
      case "renew":
        // A renewal never changes capacity; only the deadline moves forward.
        break
      case "resize-up":
        this.#allocated += operation.amount
        this.#serverAllocated += operation.amount
        this.#freeSpendable += operation.amount
        break
      case "resize-down":
      case "relinquish":
        this.#allocated -= operation.amount
        this.#serverAllocated -= operation.amount
        this.#frozenPendingReturn -= operation.amount
        if (operation.kind === "relinquish") {
          this.#state = "retired"
        }
        break
    }

    this.#leaseRevision = operation.expectedLeaseRevision + 1
    if (operation.kind !== "relinquish") {
      this.#deadline =
        operation.requestStartedAt + operation.validForMs - this.#safetyMarginMs
    }
    this.#assertAccounting()
    return "applied"
  }

  /** Re-sends an in-flight operation. A retry never freezes slots a second time. */
  retry(operation: BulkheadOperation): BulkheadOperation {
    const inFlight = this.#inFlight
    if (
      inFlight === undefined ||
      inFlight.operationId !== operation.operationId
    ) {
      this.violation(
        `retry of an operation that is not in flight: ${operation.operationId}`,
      )
      return operation
    }
    return inFlight
  }

  /**
   * Authoritative reconciliation: the only path that may unfreeze slots, and only
   * when the non-commit is proven and the allocation is not superseded.
   */
  reconcile(options: {
    readonly serverAllocated: number
    readonly serverRevision: number
    readonly frozenNonCommit: boolean
  }): "unfrozen" | "kept-frozen" | "rejected" {
    if (options.serverRevision < this.#leaseRevision) {
      return "rejected"
    }

    if (options.frozenNonCommit && this.#frozenPendingReturn > 0) {
      this.#freeSpendable += this.#frozenPendingReturn
      this.#frozenPendingReturn = 0
      this.#inFlight = undefined
      if (this.#state === "uncertain") {
        this.#state = "allocated"
      }
      this.#assertAccounting()
      return "unfrozen"
    }

    if (options.serverAllocated > this.#serverAllocated) {
      this.#serverAllocated = options.serverAllocated
    }
    // An authoritative reconciliation resolves the local ambiguity: the operation
    // is no longer pending, and the frozen slots stay frozen until a non-commit is
    // proven.
    this.#inFlight = undefined
    if (this.#state === "uncertain") {
      this.#state = "allocated"
    }
    this.#assertAccounting()
    return "kept-frozen"
  }
}

// ---------------------------------------------------------------------------
// Decision 8 - rate issuance-and-spending envelope
// ---------------------------------------------------------------------------

export interface RateEnvelopeOptions {
  readonly ratePerSecond: number
  readonly burst: number
  /** The grant spend horizon, `V` in the documented bound. */
  readonly spendHorizonMs: number
  readonly safetyMarginMs?: number
  readonly maximumOutstandingStock?: number
}

export interface RateGrant {
  readonly grantId: string
  readonly operationId: string
  readonly issuedAt: number
  readonly deadline: number
  readonly amount: number
  spent: number
}

export interface RateIssuance {
  readonly granted: number
  readonly duplicate: boolean
  readonly retryAfterMs: number
  /** The grant the permits belong to, or `undefined` when nothing was granted. */
  readonly grantId: string | undefined
}

/**
 * Server token-bucket issuance with send-time-anchored spend horizons and no
 * carry. The rolling-window bound is `R * (L + V) + B` for an interval of length
 * `L` (decision 8).
 */
export class RateEnvelope extends ContractModel {
  readonly #ratePerSecond: number
  readonly #burst: number
  readonly #spendHorizonMs: number
  readonly #safetyMarginMs: number
  readonly #maximumOutstandingStock: number
  readonly #grants = new Map<string, RateGrant>()
  readonly #issuanceByOperation = new Map<
    string,
    { readonly grantId: string | undefined; readonly amount: number }
  >()
  readonly #admissions: number[] = []
  #tokens: number
  #lastRefillAt = 0
  #nextGrant = 0
  #discardedQuota = 0

  constructor(options: RateEnvelopeOptions) {
    super()
    this.#ratePerSecond = options.ratePerSecond
    this.#burst = options.burst
    this.#spendHorizonMs = options.spendHorizonMs
    this.#safetyMarginMs = options.safetyMarginMs ?? 0
    this.#maximumOutstandingStock =
      options.maximumOutstandingStock ?? Number.POSITIVE_INFINITY
    this.#tokens = options.burst
  }

  get admissions(): number {
    return this.#admissions.length
  }

  /** Quota discarded at expiry. It is never credited back into the bucket. */
  get discardedQuota(): number {
    return this.#discardedQuota
  }

  /** The documented rolling-window bound for an interval of length `intervalMs`. */
  rollingBound(intervalMs: number): number {
    return (
      (this.#ratePerSecond * (intervalMs + this.#spendHorizonMs)) / 1000 +
      this.#burst
    )
  }

  availableTokens(now: number): number {
    return this.#refill(now)
  }

  issue(
    now: number,
    requestStartedAt: number,
    amount: number,
    operationId: string,
  ): RateIssuance {
    const existing = this.#issuanceByOperation.get(operationId)
    if (existing !== undefined) {
      // A retry returns the recorded grant instead of deducting tokens twice.
      return {
        granted: existing.amount,
        duplicate: true,
        retryAfterMs: 0,
        grantId: existing.grantId,
      }
    }

    this.#refill(now)
    let granted = Math.min(amount, Math.floor(this.#tokens))
    const outstanding = this.#outstandingStock(now)
    if (outstanding + granted > this.#maximumOutstandingStock) {
      granted = Math.max(0, this.#maximumOutstandingStock - outstanding)
    }

    if (granted <= 0) {
      const retryAfterMs =
        this.#ratePerSecond > 0
          ? Math.ceil(1000 / this.#ratePerSecond)
          : this.#spendHorizonMs
      // The result is recorded even when nothing is granted, so a retry after a
      // refused issuance is still deduplicated.
      this.#issuanceByOperation.set(operationId, {
        grantId: undefined,
        amount: 0,
      })
      return { granted: 0, duplicate: false, retryAfterMs, grantId: undefined }
    }

    this.#tokens -= granted
    this.#nextGrant += 1
    const grant: RateGrant = {
      grantId: `rate-${this.#nextGrant}`,
      operationId,
      issuedAt: now,
      deadline: requestStartedAt + this.#spendHorizonMs - this.#safetyMarginMs,
      amount: granted,
      spent: 0,
    }
    this.#grants.set(grant.grantId, grant)
    this.#issuanceByOperation.set(operationId, {
      grantId: grant.grantId,
      amount: granted,
    })
    return {
      granted,
      duplicate: false,
      retryAfterMs: 0,
      grantId: grant.grantId,
    }
  }

  /** Spends one permit. A permit is spendable only inside its deadline. */
  spend(grantId: string, now: number): boolean {
    const grant = this.#grants.get(grantId)
    if (grant === undefined) {
      this.violation(`spend against an unknown grant: ${grantId}`)
      return false
    }
    if (now > grant.deadline) {
      this.violation(`spent a permit past its deadline: ${grantId}`)
      return false
    }
    if (grant.spent >= grant.amount) {
      this.violation(`double spend on grant ${grantId}`)
      return false
    }

    grant.spent += 1
    this.#admissions.push(now)
    return true
  }

  /** Expires a grant's remainder. The remainder is discarded, never credited back. */
  expireGrant(grantId: string): number {
    const grant = this.#grants.get(grantId)
    if (grant === undefined) {
      return 0
    }
    const discarded = grant.amount - grant.spent
    this.#discardedQuota += discarded
    this.#grants.delete(grantId)
    return discarded
  }

  /** The largest number of admissions in any window of length `intervalMs`. */
  maxAdmissionsInWindow(intervalMs: number): number {
    let max = 0
    for (const end of this.#admissions) {
      for (const start of [end - intervalMs, end]) {
        let count = 0
        for (const at of this.#admissions) {
          if (at >= start && at <= start + intervalMs) {
            count += 1
          }
        }
        max = Math.max(max, count)
      }
    }
    return max
  }

  assertRollingBound(intervalMs: number): void {
    const observed = this.maxAdmissionsInWindow(intervalMs)
    const bound = this.rollingBound(intervalMs)
    if (observed > bound) {
      this.violation(
        `rolling-window admissions ${observed} exceed the bound ${bound} for L=${intervalMs}`,
      )
    }
  }

  #refill(now: number): number {
    const elapsed = now - this.#lastRefillAt
    if (elapsed > 0) {
      this.#tokens = Math.min(
        this.#burst,
        this.#tokens + (this.#ratePerSecond * elapsed) / 1000,
      )
      this.#lastRefillAt = now
    }
    return this.#tokens
  }

  #outstandingStock(now: number): number {
    let outstanding = 0
    for (const grant of this.#grants.values()) {
      if (now <= grant.deadline) {
        outstanding += grant.amount - grant.spent
      }
    }
    return outstanding
  }
}
