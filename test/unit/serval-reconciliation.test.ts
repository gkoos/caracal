/**
 * Unit tests for the Serval reconciler in isolation.
 *
 * The session tests cover reconciliation end to end; these pin the parts that are
 * the reconciler's own contract: one refresh at a time, a coalesced request, the
 * reader's identity arguments, a malformed snapshot, and a clean stop.
 */

import { describe, expect, it } from "vitest"
import {
  ServalIncarnationTracker,
  type ServalAuthorityRecord,
} from "../../src/coordination/serval/authority.js"
import {
  createServalReconciler,
  type ServalReconcilerOptions,
  type ServalAuthoritySnapshot,
} from "../../src/coordination/serval/reconciliation.js"
import { createDeterministicScheduler } from "../support/serval/deterministic-scheduler.js"
import { TEST_IDENTITY } from "../support/serval/session-harness.js"

function record(
  version: number,
  overrides: Partial<ServalAuthorityRecord> = {},
): ServalAuthorityRecord {
  return {
    identity: TEST_IDENTITY,
    version: {
      authorityIncarnation: "authority-1",
      epoch: 0,
      version,
      configRevision: 1,
    },
    issuedAt: 1_000,
    expiresAt: 61_000,
    payload: undefined,
    ...overrides,
  }
}

function snapshot(
  records: readonly ServalAuthorityRecord[],
  overrides: Partial<ServalAuthoritySnapshot> = {},
): ServalAuthoritySnapshot {
  return {
    authorityIncarnation: "authority-1",
    serverTime: 1_000,
    configRevision: 1,
    records,
    ...overrides,
  }
}

function reconcilerHarness(
  snapshots: readonly (ServalAuthoritySnapshot | Error)[],
) {
  const scheduler = createDeterministicScheduler(1_000)
  const tracker = new ServalIncarnationTracker()
  const applied: ServalAuthorityRecord[] = []
  const cached = new Map<string, number>()
  const requests: Array<{ replicaId: string; replicaIncarnation: string }> = []
  const results: string[] = []
  let incarnationChanges = 0
  let rollbacks = 0
  let reads = 0

  const reconciler = createServalReconciler({
    scheduler,
    tracker,
    replicaId: "replica-1",
    replicaIncarnation: "replica-1#1",
    intervalMs: 100,
    reader: {
      async readSnapshot(options) {
        requests.push(options)
        const next = snapshots[Math.min(reads, snapshots.length - 1)]
        reads += 1
        if (next instanceof Error) throw next
        return next as ServalAuthoritySnapshot
      },
    },
    applyRecord: (value) => {
      const key = `${value.identity.scope}:${value.version.version}`
      if (cached.has(key)) return "ignored"
      cached.set(key, value.version.version)
      applied.push(value)
      return "applied"
    },
    readCachedVersion: (identity) => {
      const versions = [...cached.entries()]
        .filter(([key]) => key.startsWith(`${identity.scope}:`))
        .map(([, version]) => version)
      if (versions.length === 0) return undefined
      return {
        authorityIncarnation: "authority-1",
        epoch: 0,
        version: Math.max(...versions),
        configRevision: 1,
      }
    },
    onIncarnationChange: () => {
      incarnationChanges += 1
    },
    onRollbackSuspected: () => {
      rollbacks += 1
    },
    onResult: (result) => {
      results.push(result.type)
    },
  })

  return {
    scheduler,
    reconciler,
    tracker,
    applied,
    requests,
    results,
    incarnationChanges: () => incarnationChanges,
    rollbacks: () => rollbacks,
    reads: () => reads,
  }
}

/** Minimal valid options, used only to prove the interval is validated. */
function reconcilerOptionsForValidation(): ServalReconcilerOptions {
  return {
    scheduler: createDeterministicScheduler(0),
    tracker: new ServalIncarnationTracker(),
    replicaId: "replica-1",
    replicaIncarnation: "replica-1#1",
    intervalMs: 100,
    reader: {
      async readSnapshot() {
        return snapshot([])
      },
    },
    applyRecord: () => "ignored",
    readCachedVersion: () => undefined,
  }
}

describe("serval reconciler", () => {
  it("reads a snapshot, adopts the incarnation and applies its records", async () => {
    const harness = reconcilerHarness([snapshot([record(1), record(2)])])

    const result = await harness.reconciler.coldStart()

    expect(result).toMatchObject({
      type: "reconciled",
      applied: 2,
      skipped: 0,
      incarnationChanged: true,
      rollbackSuspected: false,
    })
    expect(harness.tracker.current).toBe("authority-1")
    expect(harness.requests).toEqual([
      { replicaId: "replica-1", replicaIncarnation: "replica-1#1" },
    ])
    expect(harness.reconciler.lastReconciledAt).toBe(1_000)
    expect(harness.incarnationChanges()).toBe(1)
  })

  it("runs one refresh at a time and coalesces a scheduled request", async () => {
    const harness = reconcilerHarness([snapshot([record(1)])])

    const first = harness.reconciler.refresh("periodic")
    const second = harness.reconciler.refresh("invalidation")
    expect(second).toBe(first)
    expect(harness.reconciler.schedule("invalidation")).toBe(false)

    await first
    expect(harness.reads()).toBe(1)
  })

  it("schedules one refresh for many requests and runs it on the scheduler", async () => {
    const harness = reconcilerHarness([snapshot([record(1)])])

    expect(harness.reconciler.schedule("invalidation")).toBe(true)
    expect(harness.reconciler.schedule("invalidation")).toBe(false)
    expect(harness.scheduler.pending()).toBe(1)

    await harness.scheduler.advanceBy(0)
    await harness.reconciler.whenIdle()

    expect(harness.reads()).toBe(1)
    expect(harness.scheduler.pending()).toBe(0)
  })

  it("reports a failed read and never adopts from it", async () => {
    const harness = reconcilerHarness([new Error("unavailable")])

    const result = await harness.reconciler.refresh("periodic")

    expect(result).toMatchObject({ type: "failed" })
    expect(harness.tracker.current).toBeUndefined()
    expect(harness.reconciler.lastReconciledAt).toBeUndefined()
    expect(harness.results).toEqual(["failed"])
  })

  it("rejects a snapshot with an invalid incarnation, revision or server time", async () => {
    const invalid = [
      snapshot([], { authorityIncarnation: "" }),
      snapshot([], { serverTime: Number.NaN }),
      snapshot([], { configRevision: -1 }),
    ]

    for (const candidate of invalid) {
      const harness = reconcilerHarness([candidate])
      expect(await harness.reconciler.refresh("periodic")).toMatchObject({
        type: "failed",
      })
      expect(harness.tracker.current).toBeUndefined()
    }
  })

  it("skips a malformed record and one stamped with another incarnation", async () => {
    const harness = reconcilerHarness([
      snapshot([
        record(1),
        record(2, { expiresAt: 0 }),
        record(3, {
          version: {
            authorityIncarnation: "authority-other",
            epoch: 0,
            version: 3,
            configRevision: 1,
          },
        }),
      ]),
    ])

    expect(await harness.reconciler.refresh("periodic")).toMatchObject({
      type: "reconciled",
      applied: 1,
      skipped: 2,
    })
  })

  it("does not apply an older record and reports the rollback", async () => {
    const harness = reconcilerHarness([
      snapshot([record(2)]),
      snapshot([record(1)]),
    ])

    await harness.reconciler.refresh("periodic")
    const result = await harness.reconciler.refresh("periodic")

    expect(result).toMatchObject({
      type: "reconciled",
      applied: 0,
      skipped: 1,
      incarnationChanged: false,
      rollbackSuspected: true,
    })
    expect(harness.rollbacks()).toBe(1)
    expect(harness.applied).toHaveLength(1)
  })

  it("refuses to adopt a retired incarnation and reports the rollback", async () => {
    const harness = reconcilerHarness([
      snapshot([record(1)]),
      snapshot([record(1)], { authorityIncarnation: "authority-0" }),
    ])

    await harness.reconciler.refresh("periodic")
    harness.tracker.retire("authority-0")

    const result = await harness.reconciler.refresh("periodic")

    expect(result).toMatchObject({
      type: "reconciled",
      incarnationChanged: true,
      rollbackSuspected: true,
    })
    expect(harness.tracker.current).toBe("authority-1")
    expect(harness.rollbacks()).toBe(1)
  })

  it("stops the periodic loop and cancels a pending refresh", async () => {
    const harness = reconcilerHarness([snapshot([record(1)])])
    harness.reconciler.start()
    expect(harness.scheduler.pending()).toBe(1)

    harness.reconciler.stop()
    expect(harness.scheduler.pending()).toBe(0)
    expect(harness.reconciler.schedule("invalidation")).toBe(false)

    await harness.scheduler.advanceBy(1_000)
    expect(harness.reads()).toBe(0)
  })

  it("validates its refresh interval", () => {
    expect(() =>
      createServalReconciler({
        ...reconcilerOptionsForValidation(),
        intervalMs: 0,
      }),
    ).toThrow(RangeError)
  })
})
