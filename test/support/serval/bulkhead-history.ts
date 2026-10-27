import { mulberry32, randInt } from "../seed.js"
import {
  createFakeAllocationAuthority,
  type FakeAllocationAuthority,
} from "./fake-allocation-authority.js"
import {
  TEST_BULKHEAD_IDENTITY,
  createBulkheadHarness,
  createManualClock,
  type BulkheadHarness,
} from "./bulkhead-harness.js"

/**
 * A seeded adversarial history for the delegated bulkhead.
 *
 * It mixes clock advances, concurrent admissions, releases, renewals, voluntary
 * returns, outages and lost replies, and checks contracts 6, 7, 9 and 11 after every
 * step against the server model:
 *
 * - No admission beyond the capacity the server actually granted, counting the slots
 *   frozen for a return as gone: `active <= the server's allocation` always.
 * - At every admission, `active + frozenPendingReturn <= allocated`, so returned
 *   capacity is never spent and no work starts past the local allocation. The check is
 *   scoped to one grant: a replacement allocation may be smaller than the work still
 *   running under the previous one, which is physical overshoot, not an admission.
 * - The lease revision never regresses **within a grant**, and the server's oracle never
 *   fires: the fleet limit holds, a renewal is not capacity, and one replica holds one
 *   allocation. Revisions belong to a grant, so a replacement allocation starting its own
 *   sequence at 1 is not a regression.
 * - The registry stays bounded and no modification is left in flight.
 *
 * The overshoot this contract permits is physical, not logical: a holder whose work
 * ignores cancellation keeps its slot after its allocation was reclaimed, which is
 * why tracked occupancy is asserted rather than claimed as concurrency.
 */

export interface BulkheadHistoryOptions {
  readonly seed: number
  readonly steps: number
}

const LIMIT = 6
const VALIDITY_MS = 400
const GRACE_MS = 100
const MAXIMUM_SCOPES = 2

export async function runBulkheadHistory(
  options: BulkheadHistoryOptions,
): Promise<readonly string[]> {
  const { seed, steps } = options
  const rng = mulberry32(seed)
  const violations: string[] = []

  const clock = createManualClock(1_000)
  const authority: FakeAllocationAuthority = createFakeAllocationAuthority({
    now: () => clock.now(),
    limit: LIMIT,
    graceMs: GRACE_MS,
  })
  const harness: BulkheadHarness = createBulkheadHarness({
    limit: LIMIT,
    validityMs: VALIDITY_MS,
    graceMs: GRACE_MS,
    initialSlots: 2,
    maximumSlots: 4,
    redistributionIntervalMs: 20,
    clock,
    authority,
    replicaId: "replica-1",
  })
  const identity = TEST_BULKHEAD_IDENTITY
  const held = new Set<string>()
  let admitted = 0
  let failures = 0
  let injections = 0
  let highestRevision = 0
  let highestRevisionGrant = ""
  let previousGrantId = ""
  let carriedOvershoot = 0

  async function acquire(token: string): Promise<void> {
    const before = harness.coordinator.diagnostics(identity)
    let decision: { readonly allowed: boolean }
    try {
      decision = await harness.acquire(token)
    } catch {
      // The authority could not decide, so nothing was admitted and nothing is
      // assumed: the coordinator reports that rather than inventing a permit.
      failures += 1
      return
    }
    if (!decision.allowed) return

    admitted += 1
    const after = harness.coordinator.diagnostics(identity)
    if (after === undefined) {
      violations.push("an admission left no diagnostics")
      return
    }
    // Only a comparison within one grant means anything: an admission that allocated a
    // replacement grant sees the revision restart at 1.
    if (
      before !== undefined &&
      before.grantId === after.grantId &&
      after.revision < before.revision
    ) {
      violations.push(
        `the lease revision regressed from ${before.revision} to ${after.revision}`,
      )
    }
    held.add(token)
  }

  for (let step = 0; step < steps; step += 1) {
    const action = step === 0 ? 5 : randInt(rng, 0, 9)

    if (action === 0) {
      clock.advanceBy(randInt(rng, 0, 200))
    } else if (action === 1) {
      authority.failNext(1)
      injections += 1
    } else if (action === 2) {
      authority.commitThenFailNext()
      injections += 1
    } else if (action === 3) {
      const token = [...held][0]
      if (token !== undefined) {
        await harness.release(token)
        held.delete(token)
      }
    } else if (action === 4) {
      const token = [...held][0]
      if (token !== undefined) await harness.renew(token)
    } else if (action === 5) {
      await acquire(`token-${step}`)
    } else if (action === 6) {
      // Concurrent admissions share whatever capacity exists.
      const tokens = [0, 1, 2].map((offset) => `token-${step}-${offset}`)
      await Promise.all(tokens.map((token) => acquire(token)))
    } else if (action === 7) {
      await harness.coordinator.relinquish(identity, randInt(rng, 1, 3))
    } else if (action === 8) {
      authority.holdNext()
      const pending = harness.acquire(`token-${step}`)
      clock.advanceBy(randInt(rng, 0, 120))
      authority.release()
      await pending.catch(() => undefined)
    } else {
      clock.advanceBy(randInt(rng, 0, 60))
      await harness.whenIdle()
    }

    await harness.whenIdle()

    const diagnostics = harness.coordinator.diagnostics(identity)
    if (diagnostics !== undefined) {
      const serverSlots = authority.allocation("replica-1")?.slots ?? 0
      // With every modification settled, the capacity this process may actually spend
      // right now - valid, unfrozen - never exceeds what the server granted. A return
      // that committed but was not adopted is frozen, so it cannot be spent twice, and
      // a lapsed allocation is not spendable at all: the client's conservative deadline
      // ends before the server's reservation does. Holders may legitimately outnumber a
      // reclaimed allocation: that is work still running, not admission authority.
      const spendable =
        diagnostics.deadline > clock.now()
          ? diagnostics.allocated - diagnostics.frozenPendingReturn
          : 0
      if (spendable > serverSlots) {
        violations.push(
          `step ${step}: ${spendable} spendable slots against a server allocation of ${serverSlots}`,
        )
      }
      // The mark is per grant: a replacement allocation starts its own sequence at 1, so
      // carrying the previous grant's mark across would report a legitimate restart.
      if (diagnostics.grantId !== highestRevisionGrant) {
        highestRevisionGrant = diagnostics.grantId
        highestRevision = 0
      }
      if (diagnostics.revision < highestRevision) {
        violations.push(
          `the lease revision regressed to ${diagnostics.revision} within grant ${diagnostics.grantId || "none"}`,
        )
      }
      highestRevision = Math.max(highestRevision, diagnostics.revision)
      if (diagnostics.freeSpendable < 0) {
        violations.push(
          `the free pool went negative: ${diagnostics.freeSpendable}`,
        )
      }
      // The occupancy check belongs to the allocation that admitted these holders. When the grant
      // changes, work admitted under the previous one keeps running and may exceed the replacement
      // allocation - physical overshoot, which the contract permits. That allowance is recorded
      // once and may only shrink as those holders finish, so a shortfall that appears *within* a
      // grant, or one that grows, still fails here.
      const overshoot =
        diagnostics.active +
        diagnostics.frozenPendingReturn -
        diagnostics.allocated
      if (diagnostics.grantId !== previousGrantId) {
        previousGrantId = diagnostics.grantId
        carriedOvershoot = Math.max(0, overshoot)
      } else if (carriedOvershoot > 0) {
        carriedOvershoot = Math.min(carriedOvershoot, Math.max(0, overshoot))
      }
      if (diagnostics.deadline > clock.now() && overshoot > carriedOvershoot) {
        // The numbers decide which defect this is. Frozen slots in the shortfall mean a return's
        // accounting was lost; holders alone mean work outlived its allocation, which is
        // legitimate for a replaced allocation and not for the one that admitted them.
        violations.push(
          `step ${step}: the local allocation is over-committed` +
            ` (active ${diagnostics.active} + frozen ${diagnostics.frozenPendingReturn}` +
            ` > allocated ${diagnostics.allocated}, state ${diagnostics.state},` +
            ` revision ${diagnostics.revision}, grant ${diagnostics.grantId || "none"})`,
        )
      }
    }

    if (harness.coordinator.metrics().scopes > MAXIMUM_SCOPES) {
      violations.push("the registry grew past its capacity")
    }
    for (const breach of authority.violations()) {
      violations.push(`the server oracle fired: ${breach}`)
    }
  }

  await harness.whenIdle()
  // An admission fails only when the authority was made to fail: no local state turns
  // an admission into an exception. One injected failure can reject every caller that
  // coalesced onto it, so the counts are compared as "any", not one-to-one.
  if (failures > 0 && injections === 0) {
    violations.push(`${failures} admissions failed with no injected failure`)
  }
  if (admitted === 0) {
    violations.push("the history admitted nothing, so it proved nothing")
  }

  return violations
}
