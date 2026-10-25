import type {
  BulkheadAllocationIssuer,
  BulkheadAllocationReply,
  BulkheadAllocationRequest,
} from "../../../src/coordination/redis/bulkhead.js"

/**
 * A scriptable stand-in for the Redis allocation authority.
 *
 * It is a server model, not a copy of the script: it keeps one counter of granted
 * slots per identity, one allocation per replica, a reservation that outlives the
 * allocation's validity by a grace period, and an operation ledger that makes every
 * modification idempotent. The oracle asserts what the server must never do - grant
 * more than the fleet's limit, count a renewal as capacity, hold two allocations for
 * one replica - so a client bug that provokes it is a violation rather than a
 * plausible-looking reply.
 */

export interface FakeAllocation {
  readonly replicaId: string
  readonly grantId: string
  readonly revision: number
  readonly slots: number
  /** Server-clock instant the allocation stops being valid. */
  readonly expiresAt: number
}

export interface FakeAllocationAuthorityOptions {
  /** The server's clock. It is the same virtual clock the test advances. */
  readonly now: () => number
  /** The fleet-wide ceiling the server never exceeds. */
  readonly limit: number
  readonly graceMs?: number
}

export interface FakeAllocationAuthority {
  readonly issuer: BulkheadAllocationIssuer
  readonly calls: { modify: number; duplicates: number }
  /** The server's own counter of granted slots. */
  granted(): number
  /** The highest the counter has ever been. */
  peakGranted(): number
  allocations(): readonly FakeAllocation[]
  allocation(replicaId: string): FakeAllocation | undefined
  /** Fails the next calls without committing anything. */
  failNext(count: number, error?: unknown): void
  /** Commits the next modification, then loses the reply. */
  commitThenFailNext(): void
  /** Holds the next modification until `release`. */
  holdNext(): void
  release(): void
  /** Server-side invariant breaches observed so far. */
  violations(): readonly string[]
  reset(): void
}

export function createFakeAllocationAuthority(
  options: FakeAllocationAuthorityOptions,
): FakeAllocationAuthority {
  const { now, limit } = options
  const graceMs = options.graceMs ?? 0
  const calls = { modify: 0, duplicates: 0 }
  const allocations = new Map<string, FakeAllocation>()
  const reservations = new Map<
    string,
    { readonly endsAt: number; readonly slots: number }
  >()
  const ledger = new Map<string, BulkheadAllocationReply>()
  const breaches: string[] = []
  let failures = 0
  let failureError: unknown
  let commitBeforeFailing = false
  let held: Promise<void> | undefined
  let releaseHeld: (() => void) | undefined
  let granted = 0
  let peak = 0

  function check(): void {
    if (granted > limit) {
      breaches.push(`granted ${granted} exceeds the fleet limit ${limit}`)
    }
    if (granted < 0) breaches.push(`granted slots went negative: ${granted}`)
    peak = Math.max(peak, granted)
  }

  /** Reservations outlive validity by the grace period, so a lost reply is reconcilable. */
  function prune(): void {
    for (const [replicaId, reservation] of [...reservations]) {
      if (now() < reservation.endsAt) continue
      reservations.delete(replicaId)
      granted -= reservation.slots
      const heldAllocation = allocations.get(replicaId)
      if (heldAllocation !== undefined && heldAllocation.expiresAt <= now()) {
        allocations.delete(replicaId)
      }
    }
    check()
  }

  function reserve(replicaId: string, slots: number, expiresAt: number): void {
    reservations.set(replicaId, { endsAt: expiresAt + graceMs, slots })
  }

  function committed(
    request: BulkheadAllocationRequest,
    slots: number,
    revision: number,
    expiresAt: number,
  ): BulkheadAllocationReply {
    return {
      type: "committed",
      slots,
      revision,
      expiresAt,
      serverTime: now(),
      duplicate: request.operationId !== "",
    }
  }

  function apply(request: BulkheadAllocationRequest): BulkheadAllocationReply {
    prune()
    const current = allocations.get(request.replicaId)
    const refused = (
      reason: "limit" | "revision" | "expired" | "exists",
    ): BulkheadAllocationReply => ({
      type: "refused",
      reason,
      slots: current?.slots ?? 0,
      revision: current?.revision ?? 0,
      expiresAt: current?.expiresAt ?? 0,
      serverTime: now(),
    })

    if (request.action === "read") {
      return {
        type: "committed",
        slots: current?.slots ?? 0,
        revision: current?.revision ?? 0,
        expiresAt: current?.expiresAt ?? 0,
        serverTime: now(),
        duplicate: false,
      }
    }

    if (request.action === "allocate") {
      if (current !== undefined) return refused("exists")
      const slots = Math.min(request.slots ?? 0, limit - granted)
      if (slots <= 0) return refused("limit")
      const expiresAt = now() + (request.validityMs ?? 0)
      allocations.set(request.replicaId, {
        replicaId: request.replicaId,
        grantId: request.operationId,
        revision: 1,
        slots,
        expiresAt,
      })
      reserve(request.replicaId, slots, expiresAt)
      granted += slots
      check()
      return committed(request, slots, 1, expiresAt)
    }

    if (current === undefined || now() > current.expiresAt) {
      return refused("expired")
    }
    const expected = request.expectedRevision
    if (
      expected !== undefined &&
      expected >= 0 &&
      expected !== current.revision
    ) {
      return refused("revision")
    }

    if (request.action === "renew") {
      const before = granted
      const expiresAt = now() + (request.validityMs ?? 0)
      allocations.set(request.replicaId, {
        ...current,
        revision: current.revision + 1,
        expiresAt,
      })
      reserve(request.replicaId, current.slots, expiresAt)
      if (granted !== before) {
        breaches.push("a renewal changed the granted slot count")
      }
      check()
      return committed(request, current.slots, current.revision + 1, expiresAt)
    }

    if (request.action === "resize") {
      const delta = request.slots ?? 0
      const revision = current.revision + 1
      let slots = current.slots
      if (delta > 0) {
        const added = Math.min(delta, limit - granted)
        if (added <= 0) return refused("limit")
        slots += added
        granted += added
      } else {
        const returned = Math.min(-delta, current.slots)
        slots -= returned
        granted -= returned
      }
      if (slots === 0) {
        allocations.delete(request.replicaId)
        reservations.delete(request.replicaId)
      } else {
        allocations.set(request.replicaId, { ...current, revision, slots })
        reserve(request.replicaId, slots, current.expiresAt)
      }
      check()
      return committed(request, slots, revision, current.expiresAt)
    }

    throw new Error(`unsupported allocation action ${request.action}`)
  }

  const issuer: BulkheadAllocationIssuer = {
    async modify(request) {
      calls.modify += 1
      const recorded = ledger.get(request.operationId)
      if (recorded !== undefined) {
        // Idempotent: a retry returns the recorded result and changes nothing.
        calls.duplicates += 1
        return recorded
      }
      if (held !== undefined) await held
      if (failures > 0) {
        failures -= 1
        if (commitBeforeFailing) {
          commitBeforeFailing = false
          ledger.set(request.operationId, apply(request))
        }
        throw (
          failureError ??
          new Error("the fake allocation authority is unavailable")
        )
      }
      const reply = apply(request)
      ledger.set(request.operationId, reply)
      return reply
    },
  }

  return {
    issuer,
    calls,

    granted() {
      return granted
    },

    peakGranted() {
      return peak
    },

    allocations() {
      return [...allocations.values()]
    },

    allocation(replicaId) {
      return allocations.get(replicaId)
    },

    failNext(count, error) {
      failures = count
      failureError = error
      commitBeforeFailing = false
    },

    commitThenFailNext() {
      failures = 1
      failureError = undefined
      commitBeforeFailing = true
    },

    holdNext() {
      if (held !== undefined) return
      held = new Promise<void>((resolve) => {
        releaseHeld = resolve
      })
    },

    release() {
      releaseHeld?.()
      releaseHeld = undefined
      held = undefined
    },

    violations() {
      return [...breaches]
    },

    reset() {
      calls.modify = 0
      calls.duplicates = 0
    },
  }
}
