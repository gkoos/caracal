import { randomUUID } from "node:crypto"
import type { RateLimitCoordinator } from "../core/rate-limit.js"
import type { RateLimitGrantIssuer } from "../coordination/redis/rate-limit.js"
import { SERVAL_DEFAULT_SAFETY_MARGIN_MS, servalDeadline } from "./authority.js"
import { ServalRegistryPressureError } from "./cache.js"

/**
 * The delegated rate limiter.
 *
 * It is a `RateLimitCoordinator`, so `rateLimit.distributed({ coordinator })` opts
 * in without changing the policy or the strict coordinator. Redis issues bounded
 * quota grants; this layer spends them locally, so the ordinary admission path
 * costs no round trip at all.
 *
 * The contract is `docs/serval/README.md` Contract 8: token-bucket issuance with
 * send-time-anchored spend horizons and no carry. Three consequences are the whole
 * design:
 *
 * - **A grant is spent locally and is never replenished by anything but a new
 *   issuance.** Unused stock expires and is discarded; it is never credited into a
 *   later accounting window, because that would let one permit be spent twice.
 * - **A grant's deadline is anchored to the instant its issuance request was sent**,
 *   not to the reply's arrival, and a reply that arrives after that deadline is
 *   unusable. Permits deducted server-side for such a reply are lost, and they are
 *   counted as lost rather than quietly dropped.
 * - **An issuance operation is retried with the same `operationId` and the same
 *   original anchor.** An ambiguous reply may mean the server deducted permits this
 *   process never saw; the retry returns that recorded grant instead of deducting
 *   again, and it cannot extend the window it was anchored to.
 *
 * There is deliberately no session, no snapshot and no reconciliation here. A
 * breaker verdict is re-derivable state, so it is reconciled; a quota grant is a
 * *consumable*, and no read can reconstruct what this replica holds. Its authority
 * is the issuance reply itself, which is why this coordinator takes an issuer
 * rather than a `ServalSession`.
 */

/** The grant spend horizon when the caller does not choose one. */
export const SERVAL_RATE_LIMIT_DEFAULT_VALID_FOR_MS = 1_000

/** Registry capacity when the caller does not choose one. */
export const SERVAL_RATE_LIMIT_DEFAULT_SCOPES = 1_024

/**
 * How many issuance attempts one admission may make.
 *
 * It bounds two things at once: a retry after an ambiguous reply (one more attempt,
 * deduplicated by `operationId`), and a caller that keeps losing the local race for
 * stock to its peers. Without the bound, a caller could spin while its peers take
 * every permit.
 */
export const SERVAL_RATE_LIMIT_MAXIMUM_ATTEMPTS = 4

export type ServalRateGrantState =
  | "absent"
  | "requesting"
  | "stocked"
  | "depleted"
  | "expired"
  | "uncertain"

export interface ServalRateLimitMetrics {
  readonly admissions: number
  /** Issuance round trips, including retries and background refills. */
  readonly issuances: number
  /** Replies the server served from its ledger instead of deducting again. */
  readonly duplicates: number
  readonly permitsGranted: number
  readonly permitsSpent: number
  /** Stock discarded at expiry. It is never credited back. */
  readonly permitsExpired: number
  /** Permits deducted server-side for a reply this process could not adopt. */
  readonly permitsLost: number
  readonly refused: number
  /** Refusals answered from the local retry hint, with no round trip. */
  readonly shortCircuits: number
  /** Calls that lost the local race for stock and were refused locally. */
  readonly contended: number
  readonly failures: number
  readonly registryRefusals: number
  readonly scopes: number
}

export interface ServalRateLimitDiagnostics {
  readonly identity: {
    readonly name: string
    readonly operation: string
    readonly scope: string
  }
  readonly state: ServalRateGrantState
  readonly stock: number
  /** Local monotonic instant the current stock stops being spendable. */
  readonly deadline: number
  readonly refusedUntil: number
  readonly retryAfterMs: number
  readonly grants: number
}

export interface ServalRateLimitCoordinatorOptions {
  readonly authority: RateLimitGrantIssuer
  /**
   * Permits per issuance request. Defaults to the identity's burst capacity, which
   * is the most a single grant can usefully hold; the trade-off is direct - a larger
   * grant means fewer round trips and more capacity stranded in this process.
   */
  readonly grantSize?: number
  /** The spend horizon `V` of every grant this coordinator requests. */
  readonly validForMs?: number
  /** Forwarded to the issuer: a fleet-wide bound on outstanding permits. */
  readonly maximumOutstandingPermits?: number
  readonly maximumScopes?: number
  readonly safetyMarginMs?: number
  /** The monotonic clock every local deadline is measured on. */
  readonly now?: () => number
  readonly idFactory?: () => string
}

export interface ServalRateLimitCoordinator extends RateLimitCoordinator {
  metrics(): ServalRateLimitMetrics
  /** The local quota state of one identity, for tests and diagnostics. */
  diagnostics(identity: {
    readonly name: string
    readonly operation: string
    readonly scope: string
  }): ServalRateLimitDiagnostics | undefined
  /** Resolves when no issuance is in flight, including background refills. */
  whenIdle(): Promise<void>
}

interface Identity {
  readonly name: string
  readonly operation: string
  readonly scope: string
}

/** One issuance operation. Immutable, so a retry cannot re-anchor its validity. */
interface GrantOperation {
  readonly operationId: string
  readonly requestStartedAt: number
  readonly permits: number
  readonly validForMs: number
}

interface QuotaScope {
  readonly identity: Identity
  state: ServalRateGrantState
  stock: number
  deadline: number
  refusedUntil: number
  retryAfterMs: number
  grants: number
  operation: GrantOperation | undefined
  issuing: Promise<void> | undefined
}

interface GrantRequest {
  readonly identity: Identity
  readonly emissionIntervalMs: number
  readonly burstDelayMs: number
  readonly permits: number
  readonly validForMs: number
}

interface Decision {
  readonly allowed: boolean
  readonly retryAfterMs: number
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

export function createServalRateLimitCoordinator(
  options: ServalRateLimitCoordinatorOptions,
): ServalRateLimitCoordinator {
  const authority = options.authority
  if (authority === undefined || typeof authority.issue !== "function") {
    throw new TypeError("A delegated rate limiter requires a grant issuer")
  }

  const configuredGrantSize = options.grantSize
  if (
    configuredGrantSize !== undefined &&
    (!Number.isSafeInteger(configuredGrantSize) || configuredGrantSize < 1)
  ) {
    throw new RangeError("grantSize must be a positive integer")
  }
  const validForMs = assertPositiveInteger(
    "validForMs",
    options.validForMs ?? SERVAL_RATE_LIMIT_DEFAULT_VALID_FOR_MS,
  )
  const maximumOutstandingPermits = options.maximumOutstandingPermits
  if (
    maximumOutstandingPermits !== undefined &&
    (!Number.isSafeInteger(maximumOutstandingPermits) ||
      maximumOutstandingPermits < 1)
  ) {
    throw new RangeError("maximumOutstandingPermits must be a positive integer")
  }
  const maximumScopes = assertPositiveInteger(
    "maximumScopes",
    options.maximumScopes ?? SERVAL_RATE_LIMIT_DEFAULT_SCOPES,
  )
  const safetyMarginMs = assertNonNegative(
    "safetyMarginMs",
    options.safetyMarginMs ?? SERVAL_DEFAULT_SAFETY_MARGIN_MS,
  )
  const clock = options.now ?? (() => performance.now())
  const idFactory = options.idFactory ?? randomUUID

  const counters = {
    admissions: 0,
    issuances: 0,
    duplicates: 0,
    permitsGranted: 0,
    permitsSpent: 0,
    permitsExpired: 0,
    permitsLost: 0,
    refused: 0,
    shortCircuits: 0,
    contended: 0,
    failures: 0,
    registryRefusals: 0,
  }
  const scopes = new Map<string, QuotaScope>()
  const inFlight = new Set<Promise<unknown>>()

  function track<T>(work: Promise<T>): Promise<T> {
    const tracked = work.finally(() => inFlight.delete(tracked))
    inFlight.add(tracked)
    return tracked
  }

  function scopeKey(identity: Identity): string {
    return JSON.stringify([identity.name, identity.operation, identity.scope])
  }

  /**
   * Stock that may still be spent: permits whose validity has not passed.
   *
   * This is the same predicate the spend uses. A scope must never be pinned by stock the
   * gating decision would discard on sight, which is what made an idle expired scope
   * permanent: nothing expires stock that is never touched again.
   */
  function liveStock(scope: QuotaScope, now: number): number {
    return now < scope.deadline ? scope.stock : 0
  }

  /**
   * The instant a retained operation stops being replayable.
   *
   * Past it a reply cannot be adopted, so a retry is a fresh operation and the retained one
   * holds nothing. `authority.md` retains an operation id "until the operation can no
   * longer be replayed", which is this instant.
   */
  function replayableUntil(operation: GrantOperation): number {
    return servalDeadline(
      operation.requestStartedAt,
      operation.validForMs,
      safetyMarginMs,
    )
  }

  /**
   * A scope holds no accounting once its stock has expired, no issuance is in flight, no
   * retry hint is live and any retained operation can no longer be replayed, so it is
   * reclaimable. Anything else is pinned: refusing a new scope is the documented behaviour
   * when a registry cannot hold more, and it is never a reason to drop live quota.
   *
   * Every reclaimable scope is dropped in one pass, so pressure settles the registry in a
   * single scan rather than one scope per new key.
   */
  function reclaimIdle(now: number): boolean {
    let reclaimed = false
    for (const [key, scope] of scopes) {
      const operation = scope.operation
      if (
        liveStock(scope, now) === 0 &&
        (operation === undefined || now > replayableUntil(operation)) &&
        scope.issuing === undefined &&
        now >= scope.refusedUntil
      ) {
        // The reclaim test just proved this stock is past its window, so it is discarded
        // here rather than forgotten: conservation counts every granted permit as spent,
        // held or discarded, and dropping a scope is a discard.
        if (scope.stock > 0) counters.permitsExpired += scope.stock
        scopes.delete(key)
        reclaimed = true
      }
    }
    return reclaimed
  }

  function scopeFor(identity: Identity): QuotaScope {
    const key = scopeKey(identity)
    const existing = scopes.get(key)
    if (existing !== undefined) return existing

    if (scopes.size >= maximumScopes && !reclaimIdle(clock())) {
      counters.registryRefusals += 1
      throw new ServalRegistryPressureError(
        `The Serval quota registry is full (${maximumScopes} scopes); refusing to track ${key}`,
      )
    }

    const scope: QuotaScope = {
      identity,
      state: "absent",
      stock: 0,
      deadline: 0,
      refusedUntil: 0,
      retryAfterMs: 0,
      grants: 0,
      operation: undefined,
      issuing: undefined,
    }
    scopes.set(key, scope)
    return scope
  }

  /**
   * The local spend. It is synchronous on purpose: a check, an await and then a
   * decrement is how a grant gets spent twice.
   */
  function spend(scope: QuotaScope): Decision | undefined {
    const now = clock()

    if (scope.stock > 0) {
      if (now < scope.deadline) {
        scope.stock -= 1
        counters.admissions += 1
        counters.permitsSpent += 1
        if (scope.stock === 0) scope.state = "depleted"
        return { allowed: true, retryAfterMs: 0 }
      }
      // The window closed with stock left. It is discarded, never carried forward.
      counters.permitsExpired += scope.stock
      scope.stock = 0
      scope.deadline = 0
      scope.state = "expired"
    }

    if (now < scope.refusedUntil) {
      // A refusal the server already explained: answer from the hint rather than
      // asking again, which is what keeps a hot replica from polling Redis.
      counters.shortCircuits += 1
      return {
        allowed: false,
        retryAfterMs: Math.max(1, scope.refusedUntil - now),
      }
    }

    return undefined
  }

  /**
   * The operation an issuance belongs to.
   *
   * A retained operation is reused only while its reply could still be adopted;
   * after that a retry is pointless and a fresh operation is the only useful
   * attempt. `requestStartedAt` is the send time, so reusing it is what stops a
   * retry from extending the window.
   */
  function operationFor(
    scope: QuotaScope,
    request: GrantRequest,
    now: number,
  ): GrantOperation {
    const existing = scope.operation
    if (existing !== undefined) {
      if (now <= replayableUntil(existing)) return existing
    }

    const operation: GrantOperation = {
      operationId: idFactory(),
      requestStartedAt: now,
      permits: request.permits,
      validForMs: request.validForMs,
    }
    scope.operation = operation
    return operation
  }

  /**
   * Issues one grant and adopts what it yields. Errors propagate: an unreachable
   * authority never invents a permit, it reports that it could not decide.
   */
  async function issue(
    scope: QuotaScope,
    request: GrantRequest,
  ): Promise<void> {
    const operation = operationFor(scope, request, clock())
    counters.issuances += 1

    let reply: Awaited<ReturnType<RateLimitGrantIssuer["issue"]>>
    try {
      reply = await authority.issue({
        identity: request.identity,
        operationId: operation.operationId,
        emissionIntervalMs: request.emissionIntervalMs,
        burstDelayMs: request.burstDelayMs,
        permits: operation.permits,
        validForMs: operation.validForMs,
        ...(maximumOutstandingPermits === undefined
          ? {}
          : { maximumOutstandingPermits }),
      })
    } catch (error) {
      // An ambiguous outcome: the server may have deducted permits this process
      // never saw. The operation is kept so the retry is deduplicated, and no local
      // authority is reported.
      scope.state = "uncertain"
      counters.failures += 1
      throw error
    }

    const receivedAt = clock()
    if (reply.duplicate) counters.duplicates += 1

    if (reply.granted <= 0) {
      // A definite refusal, so the operation is complete and the hint is the
      // server's own answer for when to ask again.
      const retryAfterMs = Math.max(1, reply.retryAfterMs)
      scope.operation = undefined
      scope.state = "absent"
      scope.refusedUntil = receivedAt + retryAfterMs
      scope.retryAfterMs = retryAfterMs
      counters.refused += 1
      return
    }

    const deadline = servalDeadline(
      operation.requestStartedAt,
      reply.validForMs,
      safetyMarginMs,
    )
    scope.operation = undefined

    if (receivedAt > deadline) {
      // Anchored to the send time, so a reply that outlived its window is unusable
      // however healthy the connection is. The permits are gone; they are counted
      // as lost rather than credited to a window they never had. The server gave no
      // hint, so the local estimate is one horizon: the next attempt starts a fresh
      // operation rather than waiting on a window that already closed.
      const estimate = Math.max(1, reply.validForMs)
      scope.state = "absent"
      scope.refusedUntil = receivedAt + estimate
      scope.retryAfterMs = estimate
      counters.permitsLost += reply.granted
      return
    }

    scope.stock += reply.granted
    scope.deadline = deadline
    scope.state = "stocked"
    scope.grants += 1
    counters.permitsGranted += reply.granted
  }

  /**
   * One issuance per scope at a time; everyone else waits for that reply.
   *
   * The clear is part of the promise the session awaits: if it were attached after
   * `track`, an observer could resume while a settled promise was still published,
   * and the next admission would re-throw the previous attempt's error without
   * asking the authority at all.
   */
  function ensureStock(
    scope: QuotaScope,
    request: GrantRequest,
  ): Promise<void> {
    const existing = scope.issuing
    if (existing !== undefined) return existing

    scope.state = "requesting"
    const issuing = track(
      issue(scope, request).finally(() => {
        if (scope.issuing === issuing) scope.issuing = undefined
      }),
    )
    scope.issuing = issuing
    return issuing
  }

  const coordinator: ServalRateLimitCoordinator = {
    async command(identity, params) {
      if (
        !Number.isSafeInteger(params.emissionIntervalMs) ||
        params.emissionIntervalMs < 1 ||
        !Number.isSafeInteger(params.burstDelayMs) ||
        params.burstDelayMs < 0
      ) {
        throw new RangeError(
          "Invalid rate limit emission interval or burst delay",
        )
      }

      const scope = scopeFor(identity)
      const request: GrantRequest = {
        identity,
        emissionIntervalMs: params.emissionIntervalMs,
        burstDelayMs: params.burstDelayMs,
        // A burst-sized grant is the most a single grant can usefully hold; asking
        // for more would only be refused.
        permits:
          configuredGrantSize ??
          Math.floor(params.burstDelayMs / params.emissionIntervalMs) + 1,
        validForMs,
      }

      for (
        let attempt = 0;
        attempt < SERVAL_RATE_LIMIT_MAXIMUM_ATTEMPTS;
        attempt += 1
      ) {
        const decision = spend(scope)
        if (decision !== undefined) {
          if (decision.allowed && scope.stock === 0) {
            // The last permit just went, so refill in the background: the next
            // admission should not pay the round trip. A failure here is reported
            // through the state and the counters, never as an unhandled rejection.
            void ensureStock(scope, request).catch(() => undefined)
          }
          return decision
        }
        await ensureStock(scope, request)
      }

      // Every attempt lost the local race for stock, or the authority answered too
      // late to be usable. Either way nothing here may be admitted: refuse with the
      // shortest honest wait instead of spinning.
      counters.contended += 1
      return { allowed: false, retryAfterMs: 1 }
    },

    metrics() {
      return { ...counters, scopes: scopes.size }
    },

    diagnostics(identity) {
      const scope = scopes.get(scopeKey(identity))
      if (scope === undefined) return undefined
      return {
        identity: scope.identity,
        state: scope.state,
        // The recorded stock. The deadline beside it is what decides whether any of it may
        // still be spent, and a scope is never pinned by stock that has passed it.
        stock: scope.stock,
        deadline: scope.deadline,
        refusedUntil: scope.refusedUntil,
        retryAfterMs: scope.retryAfterMs,
        grants: scope.grants,
      }
    },

    async whenIdle() {
      while (inFlight.size > 0) {
        await Promise.all(
          [...inFlight].map((work) => work.catch(() => undefined)),
        )
      }
    },
  }

  return Object.freeze(coordinator)
}
