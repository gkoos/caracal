/**
 * Seeded adversarial histories for the Serval authority reference model.
 *
 * Each runner drives one model from `reference-model.ts` through a randomised
 * sequence of delayed, duplicated, reordered, dropped and replayed messages, and
 * compares the model's decisions against an oracle computed independently from
 * the contract text. A disagreement is reported as a violation, so the generated
 * suites fail with the contract that broke rather than with a diff.
 *
 * Every runner is a pure function of its seed: `CARACAL_TEST_SEED` replays a
 * failure exactly.
 */

import { mulberry32, randInt } from "../seed.js"
import {
  BulkheadAllocation,
  type BulkheadOperation,
  type BulkheadReply,
  GrantLedger,
  type GrantReplyOutcome,
  MonotonicTime,
  RateEnvelope,
} from "./reference-model.js"

export interface HistoryOutcome {
  readonly seed: number
  readonly steps: number
  readonly violations: readonly string[]
}

interface OperationScript {
  readonly requestStartedAt: number
  readonly validForMs: number
  readonly revision: number
}

const GRANT_ID = "g"
const INCARNATION = "A"

/**
 * Decision 1 and 11: send-time anchoring, late and duplicate replies, revision
 * order and replay tombstones. The oracle derives each reply's usable deadline
 * from the operation's original send-time anchor, independently of the model.
 */
export function runGrantHistory(seed: number, steps: number): HistoryOutcome {
  const rng = mulberry32(seed)
  const clock = new MonotonicTime(0)
  const ledger = new GrantLedger(clock, {
    safetyMarginMs: 0,
    replayRetentionMs: 40,
    maximumGrantValidityMs: 120,
  })

  const violations: string[] = []
  const scripts = new Map<string, OperationScript>()
  // Operations the ledger has settled and still retains: their next reply is a
  // duplicate rather than a fresh decision.
  const settled = new Map<string, GrantReplyOutcome>()
  let grantDeadline: number | undefined
  let grantRevision: number | undefined
  let lastOperationId: string | undefined

  for (let step = 0; step < steps; step += 1) {
    clock.advance(randInt(rng, 0, 20))

    const retry =
      lastOperationId !== undefined &&
      scripts.has(lastOperationId) &&
      rng() < 0.3
    const operationId = retry ? (lastOperationId as string) : `op-${step}`
    lastOperationId = operationId

    const known = scripts.get(operationId)
    // A retry is only a retry while the ledger still retains the operation: once
    // the replay window closes, the same id is a new operation with a new anchor.
    const retainedBefore = ledger.isRetained(operationId)
    const { requestStartedAt, retried } = ledger.beginOperation(operationId)
    if (retainedBefore) {
      if (!retried) {
        violations.push(`a retry for ${operationId} was not recognised`)
      }
      if (known !== undefined && requestStartedAt !== known.requestStartedAt) {
        violations.push(`a retry moved the send-time anchor of ${operationId}`)
      }
    }

    const script: OperationScript =
      retainedBefore && known !== undefined
        ? known
        : {
            requestStartedAt,
            validForMs: randInt(rng, 5, 100),
            revision: step + 1,
          }
    scripts.set(operationId, script)

    clock.advance(randInt(rng, 0, 140))
    const receivedAt = clock.now()

    // The oracle: a reply is usable only within the send-time-anchored deadline.
    const deadline = script.requestStartedAt + script.validForMs
    const reply = {
      operationId,
      grantId: GRANT_ID,
      authorityIncarnation: INCARNATION,
      leaseRevision: script.revision,
      validForMs: script.validForMs,
    }
    const prior = retainedBefore ? settled.get(operationId) : undefined
    const outcome = ledger.acceptReply(reply)

    if (prior !== undefined) {
      // A duplicate delivery returns the recorded result and changes nothing.
      if (!outcome.duplicate) {
        violations.push(
          `a duplicate reply for ${operationId} was not recognised`,
        )
      }
      if (outcome.accepted !== prior.accepted) {
        violations.push(
          `a duplicate reply for ${operationId} changed the recorded decision`,
        )
      } else if (
        outcome.accepted &&
        prior.accepted &&
        outcome.deadline !== prior.deadline
      ) {
        violations.push(
          `a duplicate reply for ${operationId} changed the recorded deadline`,
        )
      }
    } else {
      const expectedLate = receivedAt > deadline
      const expectedStale =
        !expectedLate &&
        grantRevision !== undefined &&
        script.revision <= grantRevision
      const expectedAccepted = !expectedLate && !expectedStale

      if (outcome.accepted !== expectedAccepted) {
        violations.push(
          `reply ${operationId} decided accepted=${outcome.accepted}, oracle expected ${expectedAccepted} (late=${expectedLate}, stale=${expectedStale})`,
        )
      }
      if (outcome.accepted) {
        if (outcome.deadline !== deadline) {
          violations.push(
            `accepted reply ${operationId} carried deadline ${outcome.deadline}, oracle expected ${deadline}`,
          )
        }
        grantDeadline = deadline
        grantRevision = script.revision
      }
      settled.set(operationId, outcome)
    }

    // Admission probes: the oracle checks every admission against the deadline it
    // computed itself.
    for (let probe = 0; probe < 3; probe += 1) {
      clock.advance(randInt(rng, 0, 30))
      const decision = ledger.mayAdmit(clock.now())
      if (decision.kind !== "admit") {
        continue
      }
      if (grantDeadline === undefined || clock.now() >= grantDeadline) {
        violations.push(
          `admitted at ${clock.now()} without valid authority (deadline ${grantDeadline})`,
        )
      }
    }

    // Retention sweep, then a replay: an evicted operation cannot mint authority.
    if (rng() < 0.25) {
      const retired = ledger.collectRetiredOperations(clock.now() + 1_000)
      if (retired > 0) {
        const before = ledger.grant
        const replayed = ledger.acceptReply({
          operationId: "op-0",
          grantId: GRANT_ID,
          authorityIncarnation: INCARNATION,
          leaseRevision: steps + 1,
          validForMs: 100,
        })
        if (replayed.accepted) {
          violations.push("a replayed reply minted authority after eviction")
        }
        if (ledger.grant !== before) {
          violations.push("a replayed reply changed the grant after eviction")
        }
        // The ledger has forgotten these operations, so from here on the same ids
        // are new operations with fresh anchors.
        settled.clear()
        scripts.clear()
        lastOperationId = undefined
      }
    }
  }

  return { seed, steps, violations: [...ledger.violations, ...violations] }
}

const RATE_PER_SECOND = 20
const RATE_BURST = 5
const RATE_SPEND_HORIZON_MS = 40

/**
 * Decision 8: the rolling-window admission envelope. Permits are issued from a
 * token bucket and spent as late as their deadline allows, which is exactly the
 * case the envelope has to bound.
 */
export function runRateHistory(seed: number, steps: number): HistoryOutcome {
  const rng = mulberry32(seed)
  const envelope = new RateEnvelope({
    ratePerSecond: RATE_PER_SECOND,
    burst: RATE_BURST,
    spendHorizonMs: RATE_SPEND_HORIZON_MS,
  })

  const violations: string[] = []
  const scripts = new Map<
    string,
    { readonly requestStartedAt: number; readonly amount: number }
  >()
  // Only an id that has actually been issued can be the target of a retry.
  const issued: string[] = []
  let clock = 0

  for (let step = 0; step < steps; step += 1) {
    clock += randInt(rng, 0, 25)

    const retry = issued.length > 0 && rng() < 0.25
    const operationId = retry
      ? (issued[randInt(rng, 0, issued.length - 1)] as string)
      : `op-${step}`
    const script = scripts.get(operationId) ?? {
      requestStartedAt: clock,
      amount: randInt(rng, 1, 6),
    }
    scripts.set(operationId, script)
    if (!issued.includes(operationId)) {
      issued.push(operationId)
    }

    const issuance = envelope.issue(
      clock,
      script.requestStartedAt,
      script.amount,
      operationId,
    )
    if (issuance.duplicate !== retry) {
      violations.push(`issuance deduplication disagreed for ${operationId}`)
    }
    if (
      issuance.granted <= 0 ||
      issuance.grantId === undefined ||
      issuance.duplicate
    ) {
      continue
    }

    // Spend as late as the horizon allows: the worst case for the envelope.
    const deadline = script.requestStartedAt + RATE_SPEND_HORIZON_MS
    for (let permit = 0; permit < issuance.granted; permit += 1) {
      const at = Math.min(deadline, clock + randInt(rng, 0, 45))
      if (at < clock) {
        break
      }
      if (!envelope.spend(issuance.grantId, at)) {
        violations.push(
          `a granted permit of ${issuance.grantId} was not spendable at ${at}`,
        )
        break
      }
      clock = at
    }

    if (rng() < 0.3) {
      envelope.expireGrant(issuance.grantId)
    }
  }

  for (const intervalMs of [25, 50, 100]) {
    envelope.assertRollingBound(intervalMs)
  }

  return { seed, steps, violations: [...envelope.violations, ...violations] }
}

/**
 * Decisions 7, 9 and 11: allocation accounting, delta resize, free-only return
 * and the freeze-before-return handoff under delayed, lost and refused replies.
 */
export function runBulkheadHistory(
  seed: number,
  steps: number,
): HistoryOutcome {
  const rng = mulberry32(seed)
  const clock = new MonotonicTime(0)
  const allocation = new BulkheadAllocation(clock, {
    serverCapacity: 40,
    // Long enough that allocation validity never ends inside this history: the
    // expiry path is a deterministic acceptance case, not a generated one.
    replyTimeoutMs: 1_000_000,
  })

  const violations: string[] = []
  allocation.allocate(randInt(rng, 1, 6), 1_000_000)

  let inFlight: BulkheadOperation | undefined

  for (let step = 0; step < steps; step += 1) {
    clock.advance(randInt(rng, 0, 5))

    if (inFlight !== undefined) {
      const frozenBefore = allocation.frozenPendingReturn
      const freeBefore = allocation.freeSpendable
      const roll = rng()
      const reply: BulkheadReply =
        roll < 0.55
          ? { kind: "committed" }
          : roll < 0.8
            ? { kind: "lost" }
            : { kind: "refused", reason: "lease-revision" }

      const completion = allocation.complete(inFlight, reply)
      if (reply.kind === "committed") {
        if (completion !== "applied") {
          violations.push(`a committed reply was reported as ${completion}`)
        }
        if (allocation.leaseRevision !== inFlight.expectedLeaseRevision + 1) {
          violations.push(
            "a committed modification did not advance the lease revision",
          )
        }
        if (rng() < 0.3) {
          const allocatedBefore = allocation.allocated
          if (allocation.complete(inFlight, reply) !== "duplicate") {
            violations.push("a duplicated commit was not recognised")
          }
          if (allocation.allocated !== allocatedBefore) {
            violations.push("a duplicated commit changed the allocation")
          }
        }
        inFlight = undefined
      } else {
        if (completion !== "uncertain") {
          violations.push(`an uncertain reply was reported as ${completion}`)
        }
        if (allocation.frozenPendingReturn !== frozenBefore) {
          violations.push("an uncertain reply changed the frozen slot count")
        }
        if (allocation.freeSpendable !== freeBefore) {
          violations.push("an uncertain reply made frozen slots spendable")
        }
        if (allocation.acquireHolder()) {
          violations.push(
            "a holder was admitted while the outcome was uncertain",
          )
        }

        if (reply.kind === "lost") {
          // The operation stays in flight and is retried. A retry never freezes
          // again, so the frozen count is unchanged.
          if (allocation.retry(inFlight) !== inFlight) {
            violations.push("a pending operation could not be retried")
          }
          if (allocation.frozenPendingReturn !== frozenBefore) {
            violations.push("retrying changed the frozen slot count")
          }
        } else {
          // Only authoritative reconciliation resolves a refusal.
          const reconciled = allocation.reconcile({
            serverAllocated: allocation.serverAllocated,
            serverRevision: allocation.leaseRevision,
            frozenNonCommit: frozenBefore > 0,
          })
          const expected = frozenBefore > 0 ? "unfrozen" : "kept-frozen"
          if (reconciled !== expected) {
            violations.push(`reconciliation returned ${reconciled}`)
          }
          if (allocation.frozenPendingReturn !== 0) {
            violations.push("reconciliation left slots frozen")
          }
          inFlight = undefined
        }
      }

      continue
    }

    const action = randInt(rng, 0, 4)
    if (action === 0) {
      allocation.acquireHolder()
    } else if (action === 1) {
      if (allocation.active > 0) {
        allocation.releaseHolder()
      }
    } else if (action === 2) {
      inFlight = allocation.beginRenew()
    } else if (action === 3) {
      if (allocation.allocated < 20) {
        inFlight = allocation.beginResizeUp(randInt(rng, 0, 2))
      }
    } else {
      inFlight = allocation.beginResizeDown(
        randInt(rng, 0, allocation.freeSpendable),
      )
    }
  }

  if (inFlight !== undefined) {
    allocation.reconcile({
      serverAllocated: allocation.serverAllocated,
      serverRevision: allocation.leaseRevision,
      frozenNonCommit: allocation.frozenPendingReturn > 0,
    })
  }

  return { seed, steps, violations: [...allocation.violations, ...violations] }
}

/** Runs every history family from one seed, so one property case is one call. */
export function runAdversarialHistories(
  seed: number,
  steps: number,
): readonly HistoryOutcome[] {
  return [
    runGrantHistory(seed, steps),
    runRateHistory(seed ^ 0x9e3779b9, steps),
    runBulkheadHistory(seed ^ 0x51ed270b, steps),
  ]
}
