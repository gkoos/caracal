import {
  compareServalVersions,
  isValidServalRecord,
  isValidServalVersion,
  type ServalAuthorityRecord,
  type ServalAuthorityVersion,
  type ServalIncarnationTracker,
  type ServalScopeIdentity,
} from "./authority.js"
import type { ServalScheduler } from "./scheduler.js"

/**
 * Reconciliation: cold start, gaps, periodic refresh and incarnation change.
 *
 * Notifications are hints and can be delayed, duplicated or lost, so convergence
 * never depends on them. A snapshot read is the authoritative decision that
 * adopts an incarnation, and it is the only place an incarnation is adopted.
 *
 * Clock domains are kept apart. The snapshot carries the **server's** clock
 * reading and server-stamped expiry, so a record's remaining validity is a
 * duration; the client turns that duration into its own monotonic deadline from
 * the read's send time. Absolute server timestamps never become client deadlines.
 */

export interface ServalAuthoritySnapshot {
  readonly authorityIncarnation: string
  /** The server's clock reading when the snapshot was produced. */
  readonly serverTime: number
  readonly configRevision: number
  readonly records: readonly ServalAuthorityRecord[]
}

export interface ServalAuthorityReader {
  readSnapshot(options: {
    readonly replicaId: string
    readonly replicaIncarnation: string
  }): Promise<ServalAuthoritySnapshot>
}

export type ServalRefreshReason = "periodic" | "invalidation" | "reconnect"

export type ServalReconciliationResult =
  | {
      readonly type: "reconciled"
      readonly applied: number
      readonly skipped: number
      readonly incarnationChanged: boolean
      /** The authority history regressed, or a retired incarnation reappeared. */
      readonly rollbackSuspected: boolean
    }
  | { readonly type: "failed"; readonly error: unknown }

export interface ServalReconcilerOptions {
  readonly reader: ServalAuthorityReader
  readonly scheduler: ServalScheduler
  readonly tracker: ServalIncarnationTracker
  readonly replicaId: string
  readonly replicaIncarnation: string
  /** Interval between periodic refreshes. */
  readonly intervalMs: number
  /**
   * Applies one snapshot record. `requestStartedAt` is the read's send time and
   * `serverTime` is the server's clock reading, so the callee can derive a
   * conservative client deadline from the record's remaining validity.
   */
  readonly applyRecord: (
    record: ServalAuthorityRecord,
    context: { readonly requestStartedAt: number; readonly serverTime: number },
  ) => "applied" | "ignored"
  /** The version currently cached for an identity, for rollback detection. */
  readonly readCachedVersion: (
    identity: ServalScopeIdentity,
  ) => ServalAuthorityVersion | undefined
  readonly onIncarnationChange?: (
    previous: string | undefined,
    next: string,
  ) => void
  readonly onRollbackSuspected?: () => void
  /** Called when a reconciliation begins, so the caller can report its state. */
  readonly onStart?: (reason: ServalRefreshReason) => void
  readonly onResult?: (
    result: ServalReconciliationResult,
    reason: ServalRefreshReason,
  ) => void
}

export interface ServalReconciler {
  /** Monotonic time of the last reconciliation that adopted the authority. */
  readonly lastReconciledAt: number | undefined
  /** Reads a snapshot and adopts the incarnation. Never throws. */
  coldStart(): Promise<ServalReconciliationResult>
  refresh(reason: ServalRefreshReason): Promise<ServalReconciliationResult>
  /** Schedules one coalesced refresh. False when one is already pending. */
  schedule(reason: ServalRefreshReason): boolean
  /** Starts the periodic refresh loop. Idempotent. */
  start(): void
  stop(): void
  /** Resolves when no refresh is in flight. */
  whenIdle(): Promise<void>
}

export function createServalReconciler(
  options: ServalReconcilerOptions,
): ServalReconciler {
  if (!Number.isSafeInteger(options.intervalMs) || options.intervalMs < 1) {
    throw new RangeError("A Serval refresh interval must be a positive integer")
  }

  let lastReconciledAt: number | undefined
  let inFlight: Promise<ServalReconciliationResult> | undefined
  let scheduledCancel: (() => void) | undefined
  let periodicCancel: (() => void) | undefined
  let stopped = false

  async function execute(
    reason: ServalRefreshReason,
  ): Promise<ServalReconciliationResult> {
    // The send-time anchor of the read, so the callee can convert the record's
    // remaining server-side validity into a conservative client deadline.
    const requestStartedAt = options.scheduler.now()
    options.onStart?.(reason)

    let snapshot: ServalAuthoritySnapshot
    try {
      snapshot = await options.reader.readSnapshot({
        replicaId: options.replicaId,
        replicaIncarnation: options.replicaIncarnation,
      })
    } catch (error) {
      const failed: ServalReconciliationResult = { type: "failed", error }
      options.onResult?.(failed, reason)
      return failed
    }

    if (
      !isValidServalVersion({
        authorityIncarnation: snapshot.authorityIncarnation,
        epoch: 0,
        version: 0,
        configRevision: snapshot.configRevision,
      }) ||
      !Number.isFinite(snapshot.serverTime)
    ) {
      const failed: ServalReconciliationResult = {
        type: "failed",
        error: new TypeError(
          "The authority snapshot carries an invalid incarnation, configuration revision or server time",
        ),
      }
      options.onResult?.(failed, reason)
      return failed
    }

    const previous = options.tracker.current
    const incarnationChanged = previous !== snapshot.authorityIncarnation
    // A snapshot is the authoritative adoption decision; a retired incarnation
    // reappearing is a rollback, not an adoption.
    const adopted = options.tracker.reconcile(snapshot.authorityIncarnation)
    let rollbackSuspected = !adopted
    let applied = 0
    let skipped = 0

    if (adopted) {
      if (incarnationChanged) {
        options.onIncarnationChange?.(previous, snapshot.authorityIncarnation)
      }

      for (const record of snapshot.records) {
        if (
          !isValidServalRecord(record) ||
          record.version.authorityIncarnation !== snapshot.authorityIncarnation
        ) {
          skipped += 1
          continue
        }

        const cached = options.readCachedVersion(record.identity)
        if (cached !== undefined) {
          const comparison = compareServalVersions(cached, record.version)
          if (comparison === "older") {
            // Monotonic state application: an older record never overwrites a
            // newer one, and its arrival is the rollback signal.
            rollbackSuspected = true
            skipped += 1
            continue
          }
        }

        const outcome = options.applyRecord(record, {
          requestStartedAt,
          serverTime: snapshot.serverTime,
        })
        if (outcome === "applied") {
          applied += 1
        } else {
          skipped += 1
        }
      }

      lastReconciledAt = options.scheduler.now()
    }

    if (rollbackSuspected) {
      options.onRollbackSuspected?.()
    }

    const result: ServalReconciliationResult = {
      type: "reconciled",
      applied,
      skipped,
      incarnationChanged,
      rollbackSuspected,
    }
    options.onResult?.(result, reason)
    return result
  }

  function run(
    reason: ServalRefreshReason,
  ): Promise<ServalReconciliationResult> {
    if (inFlight !== undefined) return inFlight

    const started = execute(reason)
    const tracked = started.finally(() => {
      if (inFlight === tracked) inFlight = undefined
    })
    inFlight = tracked
    return tracked
  }

  function startPeriodic(): void {
    if (stopped || periodicCancel !== undefined) return
    periodicCancel = options.scheduler.schedule(options.intervalMs, () => {
      periodicCancel = undefined
      void run("periodic").finally(() => {
        startPeriodic()
      })
    })
  }

  return {
    get lastReconciledAt() {
      return lastReconciledAt
    },

    coldStart() {
      return run("periodic")
    },

    refresh(reason) {
      return run(reason)
    },

    schedule(reason) {
      if (stopped || inFlight !== undefined || scheduledCancel !== undefined) {
        return false
      }
      scheduledCancel = options.scheduler.schedule(0, () => {
        scheduledCancel = undefined
        void run(reason)
      })
      return true
    },

    start() {
      startPeriodic()
    },

    stop() {
      stopped = true
      scheduledCancel?.()
      scheduledCancel = undefined
      periodicCancel?.()
      periodicCancel = undefined
    },

    whenIdle() {
      return (inFlight ?? Promise.resolve()).then(() => undefined)
    },
  }
}
