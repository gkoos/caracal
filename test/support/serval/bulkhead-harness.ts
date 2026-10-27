import {
  createServalBulkheadCoordinator,
  type ServalBulkheadCoordinator,
} from "../../../src/serval/bulkhead.js"
import {
  createFakeAllocationAuthority,
  type FakeAllocationAuthority,
} from "./fake-allocation-authority.js"

/**
 * The delegated bulkhead wired to a manual clock and the server model.
 *
 * The clock is a plain counter: allocations expire and reservations lapse on it, so
 * advancing it is exactly what moves the authority. Nothing here schedules a timer,
 * which keeps every case deterministic.
 */

/** The bulkhead identity most tests use. */
export const TEST_BULKHEAD_IDENTITY = Object.freeze({
  name: "payments-capacity",
  operation: "charge",
  scope: "tenant-a",
})

/** A clock a test moves by hand, shareable across replicas and the authority. */
export interface ManualClock {
  now(): number
  advanceBy(ms: number): void
}

export function createManualClock(start = 1_000): ManualClock {
  let clock = start
  return {
    now: () => clock,
    advanceBy(ms) {
      clock += ms
    },
  }
}

export interface BulkheadHarnessOptions {
  readonly limit?: number
  readonly validityMs?: number
  readonly graceMs?: number
  readonly initialSlots?: number
  readonly maximumSlots?: number
  readonly renewLeadMs?: number
  readonly redistributionIntervalMs?: number
  readonly replyTimeoutMs?: number
  readonly maximumScopes?: number
  readonly initialNow?: number
  readonly replicaId?: string
  readonly leaseMs?: number
  /** Shared by replicas that must see the same time. */
  readonly clock?: ManualClock
  /** Shared by replicas that must coordinate with each other. */
  readonly authority?: FakeAllocationAuthority
}

export interface BulkheadHarness {
  readonly authority: FakeAllocationAuthority
  readonly coordinator: ServalBulkheadCoordinator
  readonly identity: {
    readonly name: string
    readonly operation: string
    readonly scope: string
  }
  readonly limit: number
  readonly leaseMs: number
  now(): number
  advanceBy(ms: number): void
  /** A distinct token per index, as the policy's per-call permit would be. */
  token(index: number): string
  acquire(token?: string): Promise<{ allowed: boolean; occupancy: number }>
  renew(token: string): Promise<{ allowed: boolean; occupancy: number }>
  release(token: string): Promise<{ allowed: boolean; occupancy: number }>
  whenIdle(): Promise<void>
  close(): Promise<void>
}

export function createBulkheadHarness(
  options: BulkheadHarnessOptions = {},
): BulkheadHarness {
  const limit = options.limit ?? 8
  const leaseMs = options.leaseMs ?? 60_000
  const clock = options.clock ?? createManualClock(options.initialNow ?? 1_000)
  const authority =
    options.authority ??
    createFakeAllocationAuthority({
      now: () => clock.now(),
      limit,
      graceMs: options.graceMs ?? 0,
    })

  const coordinator = createServalBulkheadCoordinator({
    authority: authority.issuer,
    replicaId: options.replicaId ?? "replica-1",
    now: () => clock.now(),
    validityMs: options.validityMs,
    graceMs: options.graceMs,
    initialSlots: options.initialSlots,
    maximumSlots: options.maximumSlots,
    renewLeadMs: options.renewLeadMs,
    redistributionIntervalMs: options.redistributionIntervalMs,
    maximumScopes: options.maximumScopes,
  })

  const identity = TEST_BULKHEAD_IDENTITY
  let sequence = 0

  return {
    authority,
    coordinator,
    identity,
    limit,
    leaseMs,
    now: () => clock.now(),
    advanceBy(ms) {
      clock.advanceBy(ms)
    },
    token(index) {
      return `token-${index}`
    },
    acquire(token) {
      if (token !== undefined) {
        return coordinator.command(identity, "acquire", token, leaseMs, limit)
      }
      sequence += 1
      return coordinator.command(
        identity,
        "acquire",
        `token-${sequence}`,
        leaseMs,
        limit,
      )
    },
    renew(token) {
      return coordinator.command(identity, "renew", token, leaseMs, limit)
    },
    release(token) {
      return coordinator.command(identity, "release", token, leaseMs, limit)
    },
    whenIdle: () => coordinator.whenIdle(),
    close: () => coordinator.close(),
  }
}
