import { coordinationKey } from "../redis/keys.js"

/**
 * The shared authority vocabulary for Serval: scope identity, versioning,
 * conservative expiry and incarnation tracking.
 *
 * Nothing here interprets policy semantics. `docs/serval.md` and
 * `docs/serval-authority.md` are the contract; this module carries the parts all
 * three policies share, and every policy-specific algorithm lives in its own
 * module (Phases 2 to 4).
 */

export type ServalPolicyKind = "circuit-breaker" | "rate-limit" | "bulkhead"

const POLICY_KINDS: readonly string[] = [
  "circuit-breaker",
  "rate-limit",
  "bulkhead",
]

export function isServalPolicyKind(value: unknown): value is ServalPolicyKind {
  return typeof value === "string" && POLICY_KINDS.includes(value)
}

/** One authority identity: the tuple the RFC's key and scope isolation use. */
export interface ServalScopeIdentity {
  readonly policyKind: ServalPolicyKind
  readonly policyName: string
  readonly operation: string
  readonly scope: string
}

/**
 * The versioning carried by every authority record.
 *
 * `epoch` and `version` order *state*; `configRevision` orders *configuration*.
 * `authorityIncarnation` is opaque: it is never ordered by parsing, and a client
 * adopts it only through an authoritative reconciliation (see the tracker).
 */
export interface ServalAuthorityVersion {
  readonly authorityIncarnation: string
  readonly epoch: number
  readonly version: number
  readonly configRevision: number
}

export interface ServalAuthorityRecord {
  readonly identity: ServalScopeIdentity
  readonly version: ServalAuthorityVersion
  readonly issuedAt: number
  readonly expiresAt: number
  readonly payload: unknown
}

/** Default subtraction from every computed deadline, in milliseconds. */
export const SERVAL_DEFAULT_SAFETY_MARGIN_MS = 25

/** Upper bound on an identity part, matching the coordination key contract. */
export const SERVAL_MAXIMUM_IDENTITY_BYTES = 1024

/** Upper bound on an incarnation identifier. */
export const SERVAL_MAXIMUM_INCARNATION_BYTES = 256

function isNonEmptyString(value: unknown, maximumBytes: number): boolean {
  return (
    typeof value === "string" &&
    value.trim().length > 0 &&
    Buffer.byteLength(value) <= maximumBytes
  )
}

function isCounter(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0
}

export function isValidServalIdentity(
  value: unknown,
): value is ServalScopeIdentity {
  if (typeof value !== "object" || value === null) return false
  const identity = value as Partial<ServalScopeIdentity>
  return (
    isServalPolicyKind(identity.policyKind) &&
    isNonEmptyString(identity.policyName, SERVAL_MAXIMUM_IDENTITY_BYTES) &&
    isNonEmptyString(identity.operation, SERVAL_MAXIMUM_IDENTITY_BYTES) &&
    isNonEmptyString(identity.scope, SERVAL_MAXIMUM_IDENTITY_BYTES)
  )
}

export function isValidServalVersion(
  value: unknown,
): value is ServalAuthorityVersion {
  if (typeof value !== "object" || value === null) return false
  const version = value as Partial<ServalAuthorityVersion>
  return (
    isNonEmptyString(
      version.authorityIncarnation,
      SERVAL_MAXIMUM_INCARNATION_BYTES,
    ) &&
    isCounter(version.epoch) &&
    isCounter(version.version) &&
    isCounter(version.configRevision)
  )
}

export function isValidServalRecord(
  value: unknown,
): value is ServalAuthorityRecord {
  if (typeof value !== "object" || value === null) return false
  const record = value as Partial<ServalAuthorityRecord>
  if (!isValidServalIdentity(record.identity)) return false
  if (!isValidServalVersion(record.version)) return false
  if (!isCounter(record.issuedAt) || !isCounter(record.expiresAt)) return false
  return record.expiresAt >= record.issuedAt
}

/**
 * The namespaced, hash-tagged key for one scope's authoritative record.
 *
 * Reuses the coordination key scheme so every key of one identity lands on one
 * cluster slot, exactly as the strict coordinators do.
 */
export function servalRecordKey(
  namespace: string,
  identity: ServalScopeIdentity,
): string {
  return coordinationKey(
    namespace,
    `serval:${identity.policyKind}:${identity.policyName}`,
    identity.operation,
    identity.scope,
    "record",
  )
}

/**
 * The one invalidation channel for a namespace.
 *
 * A single channel keeps subscriber fan-out independent of scope cardinality:
 * the payload names the identity, and a process drops what it does not hold.
 */
export function servalInvalidationChannel(namespace: string): string {
  return coordinationKey(
    namespace,
    "serval",
    "invalidate",
    "namespace",
    "channel",
  )
}

/** The in-process key for one scope, unambiguous for any name. */
export function servalScopeKey(identity: ServalScopeIdentity): string {
  return JSON.stringify([
    identity.policyKind,
    identity.policyName,
    identity.operation,
    identity.scope,
  ])
}

/**
 * The conservative deadline for authority acquired by a request that started at
 * `requestStartedAt`.
 *
 * Anchored to the **send** time, never the receipt time, so network delay cannot
 * lengthen authority. The safety margin is only ever subtracted.
 */
export function servalDeadline(
  requestStartedAt: number,
  validForMs: number,
  safetyMarginMs: number = SERVAL_DEFAULT_SAFETY_MARGIN_MS,
): number {
  const inputs = [
    ["requestStartedAt", requestStartedAt],
    ["validForMs", validForMs],
    ["safetyMarginMs", safetyMarginMs],
  ] as const
  for (const [name, value] of inputs) {
    if (typeof value !== "number" || !Number.isFinite(value) || value < 0) {
      throw new RangeError(`${name} must be a non-negative finite number`)
    }
  }
  return requestStartedAt + validForMs - safetyMarginMs
}

/** True when a reply received at `receivedAt` may still be adopted. */
export function isServalReplyUsable(options: {
  readonly requestStartedAt: number
  readonly validForMs: number
  readonly safetyMarginMs?: number
  readonly receivedAt: number
}): boolean {
  return (
    options.receivedAt <=
    servalDeadline(
      options.requestStartedAt,
      options.validForMs,
      options.safetyMarginMs ?? SERVAL_DEFAULT_SAFETY_MARGIN_MS,
    )
  )
}

/**
 * True while a record has not passed the expiry the **server** stamped on it.
 *
 * This compares server timestamps, so it is meaningful only against the server's
 * own clock: the issuance ledger, a snapshot reconciliation, or a model. A client
 * admission decision uses `isServalAuthorityUsable` instead, because client
 * wall-clock synchronisation is not assumed.
 */
export function isServalServerRecordFresh(
  record: ServalAuthorityRecord,
  serverNow: number,
): boolean {
  return serverNow < record.expiresAt
}

/**
 * A cached authority record together with the deadline the client computed for
 * it, anchored to the acquisition request's send time.
 */
export interface ServalCachedAuthority {
  readonly record: ServalAuthorityRecord
  /** Monotonic client deadline: `send + validForMs - safetyMarginMs`. */
  readonly usableUntil: number
}

/**
 * True while locally held authority may still be used for a **new** admission.
 *
 * The deadline is evaluated at the moment of admission, so a renewal can never
 * back-date coverage over a gap.
 */
export function isServalAuthorityUsable(
  cached: ServalCachedAuthority,
  now: number,
): boolean {
  return now < cached.usableUntil
}

export type ServalVersionComparison =
  | "newer"
  | "same"
  | "older"
  | "other-incarnation"

/**
 * Orders two versions **within one authority incarnation**.
 *
 * A different incarnation is reported as `other-incarnation` rather than
 * ordered: adoption is a reconciliation decision, not a numeric comparison.
 */
export function compareServalVersions(
  current: ServalAuthorityVersion | undefined,
  incoming: ServalAuthorityVersion,
): ServalVersionComparison {
  if (current === undefined) return "newer"
  if (current.authorityIncarnation !== incoming.authorityIncarnation) {
    return "other-incarnation"
  }
  if (incoming.epoch !== current.epoch) {
    return incoming.epoch > current.epoch ? "newer" : "older"
  }
  if (incoming.version !== current.version) {
    return incoming.version > current.version ? "newer" : "older"
  }
  if (incoming.configRevision !== current.configRevision) {
    return incoming.configRevision > current.configRevision ? "newer" : "older"
  }
  return "same"
}

export type ServalIncarnationState = "uninitialized" | "valid" | "uncertain"

export type ServalIncarnationObservation =
  | "current"
  | "ignored-retired"
  | "uncertain"

/** How many retired incarnations are remembered for late-message rejection. */
export const SERVAL_MAXIMUM_RETIRED_INCARNATIONS = 8

export interface ServalIncarnationTrackerOptions {
  readonly maximumRetiredIncarnations?: number
}

/**
 * Tracks which authority incarnation this process has adopted.
 *
 * Adoption is monotonic and authoritative: a reconciliation decision adopts, and
 * a delayed reply or notification never does. Once an incarnation is retired, no
 * later message from it can switch the client back, even if its numeric epoch
 * looks higher.
 *
 * The retired set is bounded, because a process can outlive many failovers and
 * the retention window is what matters, not the history.
 */
export class ServalIncarnationTracker {
  readonly #maximumRetired: number
  readonly #retired: string[] = []
  #current: string | undefined
  #uncertain = false

  constructor(options: ServalIncarnationTrackerOptions = {}) {
    const maximum =
      options.maximumRetiredIncarnations ?? SERVAL_MAXIMUM_RETIRED_INCARNATIONS
    if (!Number.isSafeInteger(maximum) || maximum < 1) {
      throw new RangeError(
        "maximumRetiredIncarnations must be a positive integer",
      )
    }
    this.#maximumRetired = maximum
  }

  get current(): string | undefined {
    return this.#current
  }

  get state(): ServalIncarnationState {
    if (this.#current === undefined) {
      return this.#uncertain ? "uncertain" : "uninitialized"
    }
    return this.#uncertain ? "uncertain" : "valid"
  }

  /** Retired incarnations, oldest first. */
  get retired(): readonly string[] {
    return [...this.#retired]
  }

  /**
   * Adopts an incarnation. Returns `false` when it is retired and the caller has
   * not claimed the authority that supersedes it.
   */
  reconcile(
    incarnation: string,
    options: { readonly supersedesRetired?: boolean } = {},
  ): boolean {
    assertIncarnation(incarnation)

    const retiredIndex = this.#retired.indexOf(incarnation)
    if (retiredIndex >= 0 && options.supersedesRetired !== true) {
      return false
    }
    if (retiredIndex >= 0) {
      this.#retired.splice(retiredIndex, 1)
    }

    this.#current = incarnation
    this.#uncertain = false
    return true
  }

  /** Retires an incarnation; its delayed messages can no longer be adopted. */
  retire(incarnation: string): void {
    assertIncarnation(incarnation)
    if (this.#retired.includes(incarnation)) {
      return
    }

    this.#retired.push(incarnation)
    while (this.#retired.length > this.#maximumRetired) {
      this.#retired.shift()
    }

    if (this.#current === incarnation) {
      this.#current = undefined
      this.#uncertain = true
    }
  }

  /** A delayed reply or notification. It never adopts an incarnation. */
  observe(incarnation: string): ServalIncarnationObservation {
    assertIncarnation(incarnation)
    if (incarnation === this.#current) {
      return "current"
    }
    if (this.#retired.includes(incarnation)) {
      return "ignored-retired"
    }
    this.#uncertain = true
    return "uncertain"
  }

  clearUncertain(): void {
    this.#uncertain = false
  }
}

function assertIncarnation(incarnation: unknown): void {
  if (
    typeof incarnation !== "string" ||
    incarnation.trim().length === 0 ||
    Buffer.byteLength(incarnation) > SERVAL_MAXIMUM_INCARNATION_BYTES
  ) {
    throw new TypeError(
      `An authority incarnation must be a nonempty string of at most ${SERVAL_MAXIMUM_INCARNATION_BYTES} UTF-8 bytes`,
    )
  }
}
