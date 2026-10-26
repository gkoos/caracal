import { randomUUID } from "node:crypto"
import {
  SERVAL_DEFAULT_SAFETY_MARGIN_MS,
  SERVAL_MAXIMUM_RETIRED_INCARNATIONS,
  ServalIncarnationTracker,
  compareServalVersions,
  isValidServalIdentity,
  isValidServalRecord,
  servalDeadline,
  servalRemainingValidity,
  servalScopeKey,
  type ServalAuthorityRecord,
  type ServalCachedAuthority,
  type ServalScopeIdentity,
} from "./authority.js"
import {
  ServalRegistryPressureError,
  createServalAuthorityCache,
  type ServalAuthorityCache,
} from "./cache.js"
import {
  createServalMetrics,
  type ServalMetrics,
  type ServalMetricsSnapshot,
} from "./metrics.js"
import {
  createServalNotificationChannel,
  encodeServalInvalidation,
  type ServalInvalidation,
  type ServalInvalidationPublisher,
  type ServalNotificationChannel,
  type ServalNotificationTransport,
} from "./notifications.js"
import {
  createServalReconciler,
  type ServalAuthorityReader,
  type ServalReconciler,
  type ServalRefreshReason,
} from "./reconciliation.js"
import { createServalScheduler, type ServalScheduler } from "./scheduler.js"

/**
 * The per-process Serval session: identity, the shared scope registry, the
 * bounded authority registry, notification handling, reconciliation and
 * lifecycle.
 *
 * It is deliberately policy-agnostic. It knows the authority vocabulary
 * (identities, versions, incarnations, conservative deadlines) and nothing about
 * breaker verdicts, quota envelopes or slot allocations: those are policy-specific
 * and reuse this session unchanged.
 *
 * `docs/serval/README.md` and `docs/serval/authority.md` are the contract.
 */

export type ServalSessionState =
  | "uninitialized"
  | "reconciling"
  | "valid"
  | "refreshing"
  | "stale"
  | "disconnected"
  | "uncertain"
  | "closed"

export class ServalSessionClosedError extends Error {
  readonly coordination = "distributed"

  constructor(message = "The Serval session is closed") {
    super(message)
    this.name = "ServalSessionClosedError"
  }
}

/** Registry capacity when the caller does not choose one. */
export const SERVAL_DEFAULT_REGISTRY_CAPACITY = 1_024

/** Refresh interval when the caller does not choose one. */
export const SERVAL_DEFAULT_RECONCILIATION_INTERVAL_MS = 10_000

export interface ServalSessionOptions {
  readonly namespace: string
  readonly reader: ServalAuthorityReader
  /** Omitted for a reconciliation-only session with no invalidation hints. */
  readonly transport?: ServalNotificationTransport
  /** Omitted for a session that only consumes hints, never publishes one. */
  readonly publisher?: ServalInvalidationPublisher
  readonly scheduler?: ServalScheduler
  /** Stable for the process lifetime; generated when omitted. */
  readonly replicaId?: string
  /** New per session: a restart is never entitled to its predecessor's grants. */
  readonly replicaIncarnation?: string
  readonly idFactory?: () => string
  readonly safetyMarginMs?: number
  readonly registryCapacity?: number
  readonly reconciliationIntervalMs?: number
  /** Age of the last successful reconciliation at which authority is stale. */
  readonly staleAfterMs?: number
  readonly maximumRetiredIncarnations?: number
}

export interface ServalScopeRegistration {
  readonly key: string
  readonly identity: ServalScopeIdentity
  /** Releases one registration; the scope leaves the registry at zero. */
  unregister(): boolean
}

export type ServalAuthorityApplication =
  | { readonly type: "applied"; readonly configurationChanged: boolean }
  | {
      readonly type: "ignored"
      readonly reason: "stale" | "unregistered" | "malformed"
    }
  | {
      readonly type: "rejected"
      readonly reason:
        | "late"
        | "retired-incarnation"
        | "unknown-incarnation"
        | "closed"
    }

export interface ServalSession {
  readonly namespace: string
  readonly replicaId: string
  readonly replicaIncarnation: string
  readonly state: ServalSessionState
  /** The adopted authority incarnation, or `undefined` before reconciliation. */
  readonly authorityIncarnation: string | undefined
  readonly configurationRevision: number
  readonly scopeCount: number
  /** The registered scopes, in registration order. */
  scopes(): readonly ServalScopeIdentity[]
  /** The monotonic clock every local deadline is measured on. */
  now(): number
  metrics(): ServalMetricsSnapshot
  registerScope(identity: ServalScopeIdentity): ServalScopeRegistration
  unregisterScope(key: string): boolean
  readAuthority(
    identity: ServalScopeIdentity,
  ): ServalCachedAuthority | undefined
  /**
   * Applies one authoritative record from a reply. The deadline is computed from
   * `requestStartedAt`, never from the receipt time.
   */
  applyAuthority(input: {
    readonly record: ServalAuthorityRecord
    readonly requestStartedAt: number
    readonly validForMs: number
    readonly receivedAt: number
  }): ServalAuthorityApplication
  /** Retires an incarnation; its delayed messages can no longer be adopted. */
  retireIncarnation(incarnation: string): void
  /**
   * Publishes one versioned invalidation, best-effort. Resolves to whether it was
   * published; a failure is counted and never thrown at the caller.
   */
  publish(invalidation: ServalInvalidation): Promise<boolean>
  /** Forces one reconciliation and resolves when it settles. */
  refresh(reason?: ServalRefreshReason): Promise<void>
  start(): Promise<void>
  close(): Promise<void>
  /** Resolves when no reconciliation is in flight. */
  whenIdle(): Promise<void>
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

export function createServalAuthoritySession(
  options: ServalSessionOptions,
): ServalSession {
  const { namespace } = options
  if (typeof namespace !== "string" || namespace.trim().length === 0) {
    throw new TypeError("A Serval session requires a namespace")
  }

  const safetyMarginMs = assertNonNegative(
    "safetyMarginMs",
    options.safetyMarginMs ?? SERVAL_DEFAULT_SAFETY_MARGIN_MS,
  )
  const registryCapacity = assertPositiveInteger(
    "registryCapacity",
    options.registryCapacity ?? SERVAL_DEFAULT_REGISTRY_CAPACITY,
  )
  const reconciliationIntervalMs = assertPositiveInteger(
    "reconciliationIntervalMs",
    options.reconciliationIntervalMs ??
      SERVAL_DEFAULT_RECONCILIATION_INTERVAL_MS,
  )
  const staleAfterMs = assertNonNegative(
    "staleAfterMs",
    options.staleAfterMs ?? reconciliationIntervalMs * 3,
  )

  const scheduler = options.scheduler ?? createServalScheduler()
  const publisher = options.publisher
  const idFactory = options.idFactory ?? randomUUID
  const replicaId = options.replicaId ?? idFactory()
  const replicaIncarnation = options.replicaIncarnation ?? idFactory()

  const tracker = new ServalIncarnationTracker({
    maximumRetiredIncarnations:
      options.maximumRetiredIncarnations ?? SERVAL_MAXIMUM_RETIRED_INCARNATIONS,
  })
  const metrics: ServalMetrics = createServalMetrics()
  const authority: ServalAuthorityCache<ServalCachedAuthority> =
    createServalAuthorityCache({
      capacity: registryCapacity,
      clock: () => scheduler.now(),
    })
  const registrations = new Map<
    string,
    { identity: ServalScopeIdentity; references: number }
  >()

  let configurationRevision = 0
  let started = false
  let closed = false
  let rollbackSuspected = false
  let refreshing = false
  let readFailed = false
  let pending: Promise<unknown> = Promise.resolve()

  /** Serialises session work so `whenIdle` and `close` can await all of it. */
  function track<T>(work: Promise<T>): Promise<T> {
    const chained = pending.then(() => work)
    pending = chained.then(
      () => undefined,
      () => undefined,
    )
    return chained
  }

  function deriveState(): ServalSessionState {
    if (closed) return "closed"
    // Uncertainty is sticky: only an authoritative reconciliation clears it.
    if (rollbackSuspected || tracker.state === "uncertain") return "uncertain"
    if (channel !== undefined && channel.status === "disconnected") {
      return "disconnected"
    }

    const last = reconciler.lastReconciledAt
    if (last === undefined) {
      if (!started) return "uninitialized"
      // Never contacted: a cold start still retrying is reconciling, one that has
      // already failed a read is disconnected.
      return readFailed ? "disconnected" : "reconciling"
    }
    if (refreshing) return "refreshing"
    if (scheduler.now() - last > staleAfterMs) return "stale"
    return "valid"
  }

  /**
   * A well-formed invalidation is a hint, never a state change: it only triggers
   * a coalesced refresh, and only when it advances what the process already
   * holds. Reordering and duplication are therefore harmless.
   */
  function onInvalidation(invalidation: ServalInvalidation): void {
    metrics.count("invalidationsReceived")

    const key = servalScopeKey(invalidation.identity)
    if (!registrations.has(key)) {
      // Fan-out is namespace-wide; an identity this process does not hold is
      // dropped rather than subscribed to individually.
      metrics.count("invalidationsIgnored")
      return
    }

    const observation = tracker.observe(
      invalidation.version.authorityIncarnation,
    )
    if (observation !== "current") {
      // A retired or unfamiliar incarnation never switches this client.
      metrics.count("invalidationsIgnored")
      return
    }

    const cached = authority.read(key)?.value
    const comparison = compareServalVersions(
      cached?.record.version,
      invalidation.version,
    )
    if (comparison !== "newer") {
      metrics.count("invalidationsIgnored")
      return
    }

    metrics.count("invalidationsApplied")
    reconciler.schedule("invalidation")
  }

  interface RecordOutcome {
    readonly outcome: "applied" | "ignored"
    readonly configurationChanged: boolean
  }

  /**
   * The one place a record enters the cache: monotonic versions, a configuration
   * revision that only moves forward, and a registry that refuses rather than
   * dropping accounting.
   */
  function cacheRecord(
    record: ServalAuthorityRecord,
    usableUntil: number,
    appliedAt: number,
  ): RecordOutcome {
    const key = servalScopeKey(record.identity)
    const cached = authority.read(key)?.value
    const comparison = compareServalVersions(
      cached?.record.version,
      record.version,
    )

    if (comparison === "older" || comparison === "same") {
      metrics.count("authorityRejected")
      metrics.observeScope(key, "rejected", appliedAt)
      return { outcome: "ignored", configurationChanged: false }
    }

    const configurationChanged =
      record.version.configRevision !== configurationRevision
    if (configurationChanged) {
      configurationRevision = Math.max(
        configurationRevision,
        record.version.configRevision,
      )
    }

    try {
      authority.register({
        key,
        recordClass: "pinned",
        value: { record, usableUntil },
        // Retention is a client-clock deadline, never a server timestamp.
        retainedUntil: usableUntil,
      })
    } catch (error) {
      if (error instanceof ServalRegistryPressureError) {
        metrics.count("registryRefusals")
        metrics.observeScope(key, "rejected", appliedAt)
        return { outcome: "ignored", configurationChanged: false }
      }
      throw error
    }

    metrics.count("authorityApplied")
    metrics.observeScope(key, "applied", appliedAt)
    return { outcome: "applied", configurationChanged }
  }

  /**
   * Applies one record from an authoritative snapshot.
   *
   * The record's remaining validity is a **duration** measured on the server's
   * clock; it becomes this client's monotonic deadline from the read's send time.
   * An absolute server timestamp never becomes a client deadline.
   */
  function applySnapshotRecord(
    record: ServalAuthorityRecord,
    context: { readonly requestStartedAt: number; readonly serverTime: number },
  ): "applied" | "ignored" {
    if (closed) return "ignored"

    const key = servalScopeKey(record.identity)
    if (!registrations.has(key)) {
      // Only identities this process holds are cached, which is what keeps the
      // registry bounded by the scope registry.
      return "ignored"
    }

    const remainingMs = servalRemainingValidity(
      record.expiresAt,
      context.serverTime,
    )
    if (remainingMs <= safetyMarginMs) {
      return "ignored"
    }

    const usableUntil = servalDeadline(
      context.requestStartedAt,
      remainingMs,
      safetyMarginMs,
    )
    return cacheRecord(record, usableUntil, context.requestStartedAt).outcome
  }

  function applyAuthority(input: {
    readonly record: ServalAuthorityRecord
    readonly requestStartedAt: number
    readonly validForMs: number
    readonly receivedAt: number
  }): ServalAuthorityApplication {
    if (closed) return { type: "rejected", reason: "closed" }

    if (!isValidServalRecord(input.record)) {
      metrics.count("authorityRejected")
      return { type: "ignored", reason: "malformed" }
    }

    const key = servalScopeKey(input.record.identity)
    if (!registrations.has(key)) {
      metrics.count("authorityRejected")
      return { type: "ignored", reason: "unregistered" }
    }

    const deadline = servalDeadline(
      input.requestStartedAt,
      input.validForMs,
      safetyMarginMs,
    )
    if (input.receivedAt > deadline) {
      metrics.count("authorityRejected")
      metrics.observeScope(key, "rejected", input.receivedAt)
      return { type: "rejected", reason: "late" }
    }

    const observation = tracker.observe(
      input.record.version.authorityIncarnation,
    )
    if (observation === "ignored-retired") {
      metrics.count("authorityRejected")
      metrics.observeScope(key, "rejected", input.receivedAt)
      return { type: "rejected", reason: "retired-incarnation" }
    }
    if (observation === "uncertain") {
      // Adoption needs a reconciliation decision, never a stray reply.
      metrics.count("authorityRejected")
      metrics.observeScope(key, "rejected", input.receivedAt)
      return { type: "rejected", reason: "unknown-incarnation" }
    }

    const applied = cacheRecord(input.record, deadline, input.receivedAt)
    if (applied.outcome === "ignored") {
      return { type: "ignored", reason: "stale" }
    }
    return {
      type: "applied",
      configurationChanged: applied.configurationChanged,
    }
  }

  function registerScope(
    identity: ServalScopeIdentity,
  ): ServalScopeRegistration {
    if (closed) throw new ServalSessionClosedError()

    if (!isValidServalIdentity(identity)) {
      throw new TypeError(
        "A Serval scope identity requires a policy kind, a policy name, an operation and a scope",
      )
    }

    const key = servalScopeKey(identity)
    const existing = registrations.get(key)
    if (existing !== undefined) {
      existing.references += 1
    } else {
      if (registrations.size >= registryCapacity) {
        metrics.count("registryRefusals")
        throw new ServalRegistryPressureError(
          `The Serval scope registry is full (${registryCapacity} scopes); refusing to register ${key}`,
        )
      }

      registrations.set(key, { identity, references: 1 })
      metrics.trackScope(key)
      metrics.count("scopesRegistered")
      metrics.setRegistrySize(registrations.size)
      // A scope registered after the last reconciliation converges on the next
      // read, and registering many scopes coalesces into one.
      reconciler.schedule("invalidation")
    }

    let released = false
    return {
      key,
      identity,
      unregister: () => {
        if (released) return false
        released = true
        return unregisterScope(key)
      },
    }
  }

  function unregisterScope(key: string): boolean {
    const registration = registrations.get(key)
    if (registration === undefined) return false

    registration.references -= 1
    if (registration.references > 0) return false

    registrations.delete(key)
    // An unregistered scope's record is released by its owner, never kept as
    // unaccounted growth.
    authority.release(key)
    metrics.forgetScope(key)
    metrics.count("scopesUnregistered")
    metrics.setRegistrySize(registrations.size)
    return true
  }

  function readAuthority(
    identity: ServalScopeIdentity,
  ): ServalCachedAuthority | undefined {
    return authority.read(servalScopeKey(identity))?.value
  }

  function retireIncarnation(incarnation: string): void {
    const wasCurrent = tracker.current === incarnation
    tracker.retire(incarnation)
    metrics.count("incarnationRetirements")
    if (wasCurrent) {
      // Records belong to the incarnation that issued them and are no longer
      // adoptable once it is retired.
      authority.clear()
    }
  }

  function scopes(): readonly ServalScopeIdentity[] {
    return [...registrations.values()].map(
      (registration) => registration.identity,
    )
  }

  function now(): number {
    return scheduler.now()
  }

  /**
   * Publication is best-effort: the transition is already committed, and a hint
   * that is lost is a hint that arrives by the next reconciliation. A failure is
   * counted rather than thrown at the caller that produced the transition.
   */
  function publish(invalidation: ServalInvalidation): Promise<boolean> {
    if (closed || publisher === undefined) {
      return Promise.resolve(false)
    }
    return track(
      publisher.publish(encodeServalInvalidation(invalidation)).then(
        () => {
          metrics.count("invalidationsPublished")
          return true
        },
        () => {
          metrics.count("publishFailures")
          return false
        },
      ),
    )
  }

  const reconciler: ServalReconciler = createServalReconciler({
    reader: options.reader,
    scheduler,
    tracker,
    replicaId,
    replicaIncarnation,
    intervalMs: reconciliationIntervalMs,
    applyRecord: applySnapshotRecord,
    readCachedVersion: (identity) =>
      authority.read(servalScopeKey(identity))?.value?.record.version,
    onIncarnationChange: (previous) => {
      // Records belong to the incarnation that issued them, so a new incarnation
      // starts from a clean cache; the scope registrations are kept.
      authority.clear()
      if (previous !== undefined) {
        tracker.retire(previous)
        metrics.count("incarnationRetirements")
      }
    },
    onRollbackSuspected: () => {
      rollbackSuspected = true
      metrics.count("rollbacksSuspected")
    },
    onStart: () => {
      refreshing = true
    },
    onResult: (result) => {
      refreshing = false
      if (result.type === "failed") {
        readFailed = true
        metrics.count("reconciliationFailures")
      } else {
        readFailed = false
        metrics.count("reconciliations")
        // Only an authoritative reconciliation that did not see a rollback may
        // clear the uncertain state.
        if (!result.rollbackSuspected) {
          rollbackSuspected = false
        }
      }
      metrics.setRegistrySize(registrations.size)

      // Hints are optional but not abandoned: a subscription that never opened is
      // retried on the next reconciliation instead of leaving the session
      // silently disconnected.
      if (channel !== undefined && !channel.subscribed && !closed) {
        void track(channel.open().catch(() => undefined))
      }
    },
  })

  const channel: ServalNotificationChannel | undefined =
    options.transport === undefined
      ? undefined
      : createServalNotificationChannel({
          transport: options.transport,
          onInvalidation,
          onMalformed: () => {
            metrics.count("invalidationsReceived")
            metrics.count("invalidationsMalformed")
          },
          onReconnect: () => {
            metrics.count("reconnects")
            void track(reconciler.refresh("reconnect").then(() => undefined))
          },
        })

  function whenIdle(): Promise<void> {
    return Promise.all([pending, reconciler.whenIdle()]).then(() => undefined)
  }

  function refresh(
    reason: ServalRefreshReason = "invalidation",
  ): Promise<void> {
    if (closed) return Promise.resolve()
    return track(reconciler.refresh(reason).then(() => undefined))
  }

  async function start(): Promise<void> {
    if (closed) throw new ServalSessionClosedError()
    if (started) return
    started = true

    // Subscribe before the cold-start read, so an invalidation that arrives
    // during the read is a hint against a known version instead of a lost one.
    if (channel !== undefined) {
      try {
        await channel.open()
      } catch {
        // The session stays usable without hints: reconciliation is the
        // correctness path, and the channel reports its own status.
      }
    }

    await track(reconciler.coldStart().then(() => undefined))
    reconciler.start()
  }

  async function close(): Promise<void> {
    if (closed) return
    closed = true

    reconciler.stop()
    try {
      await channel?.close()
    } catch {
      // Best effort: shutdown never depends on a clean unsubscribe.
    }
    await whenIdle()

    authority.clear()
    registrations.clear()
    metrics.setRegistrySize(0)
  }

  return {
    namespace,
    replicaId,
    replicaIncarnation,

    get state() {
      return deriveState()
    },

    get authorityIncarnation() {
      return tracker.current
    },

    get configurationRevision() {
      return configurationRevision
    },

    get scopeCount() {
      return registrations.size
    },

    scopes,
    now,
    publish,

    metrics() {
      return metrics.snapshot()
    },

    registerScope,
    unregisterScope,
    readAuthority,
    applyAuthority,
    retireIncarnation,
    refresh,
    start,
    close,
    whenIdle,
  }
}
