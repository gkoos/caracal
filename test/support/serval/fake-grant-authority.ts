import type {
  RateGrantReply,
  RateGrantRequest,
  RateLimitGrantIssuer,
} from "../../../src/coordination/redis/rate-limit.js"
import { RateEnvelope } from "./reference-model.js"

/**
 * A scriptable stand-in for the Redis grant issuer, backed by the reference model's
 * token bucket.
 *
 * The oracle is the point: `RateEnvelope` is the executable form of Contract 8, so
 * the generated histories drive the real coordinator against the specified server
 * rather than against a second implementation of it. The fake adds what a test
 * needs to steer - call counting, a lost reply that may or may not have committed,
 * and a held reply so a late one is observable.
 */

export interface FakeGrantAuthorityOptions {
  /** The server's clock. It is the same virtual clock the test advances. */
  readonly now: () => number
  readonly ratePerSecond: number
  readonly burst: number
  /** Must match the coordinator's `validForMs` to model one contract. */
  readonly spendHorizonMs: number
  readonly maximumOutstandingStock?: number
}

export interface FakeGrantAuthority {
  readonly issuer: RateLimitGrantIssuer
  readonly envelope: RateEnvelope
  readonly calls: { issue: number; duplicates: number }
  /** Every grant the authority created, in issuance order, deduplicated. */
  readonly grants: readonly {
    readonly operationId: string
    readonly granted: number
    readonly issuedAt: number
  }[]
  /** Permits the authority issued and has not seen expire. */
  permitsIssued(): number
  /** Permits discarded at expiry or lost, never credited back. */
  permitsDiscarded(): number
  /** Fails the next calls without committing anything. */
  failNext(count: number, error?: unknown): void
  /** Commits the next issuance, then loses the reply. */
  commitThenFailNext(): void
  /** Holds the next issuance until `releaseIssue`. */
  holdNextIssue(): void
  releaseIssue(): void
  reset(): void
}

export function createFakeGrantAuthority(
  options: FakeGrantAuthorityOptions,
): FakeGrantAuthority {
  const { now, ratePerSecond, burst, spendHorizonMs } = options
  const envelope = new RateEnvelope({
    ratePerSecond,
    burst,
    spendHorizonMs,
    ...(options.maximumOutstandingStock === undefined
      ? {}
      : { maximumOutstandingStock: options.maximumOutstandingStock }),
  })
  const calls = { issue: 0, duplicates: 0 }
  const grants: { operationId: string; granted: number; issuedAt: number }[] =
    []
  const issued = new Set<string>()

  let failures = 0
  let failureError: unknown
  let commitBeforeFailing = false
  let gate: Promise<void> | undefined
  let releaseGate: (() => void) | undefined

  function commit(request: RateGrantRequest): RateGrantReply {
    const issuance = envelope.issue(
      now(),
      // The server does not receive the client's send time, so the model's own
      // deadline is only used by the model; the client computes its own.
      now(),
      request.permits,
      request.operationId,
    )
    if (issuance.duplicate) calls.duplicates += 1
    if (issuance.granted > 0 && !issued.has(request.operationId)) {
      issued.add(request.operationId)
      grants.push({
        operationId: request.operationId,
        granted: issuance.granted,
        issuedAt: now(),
      })
    }
    return {
      granted: issuance.granted,
      duplicate: issuance.duplicate,
      validForMs: request.validForMs,
      retryAfterMs: issuance.retryAfterMs,
      revision: 0,
    }
  }

  const issuer: RateLimitGrantIssuer = {
    async issue(request) {
      calls.issue += 1
      if (failures > 0) {
        failures -= 1
        if (commitBeforeFailing) {
          // The server executed the operation and the reply was lost: exactly the
          // ambiguous outcome a retry has to be deduplicated against.
          commitBeforeFailing = false
          commit(request)
        }
        throw (
          failureError ?? new Error("the fake grant authority is unavailable")
        )
      }
      if (gate !== undefined) {
        const held = gate
        await held
      }
      return commit(request)
    },
  }

  return {
    issuer,
    envelope,
    calls,
    grants,

    permitsIssued() {
      return grants.reduce((total, grant) => total + grant.granted, 0)
    },

    permitsDiscarded() {
      return envelope.discardedQuota
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

    holdNextIssue() {
      if (gate !== undefined) return
      gate = new Promise<void>((resolve) => {
        releaseGate = resolve
      })
    },

    releaseIssue() {
      releaseGate?.()
      releaseGate = undefined
      gate = undefined
    },

    reset() {
      calls.issue = 0
      calls.duplicates = 0
    },
  }
}
