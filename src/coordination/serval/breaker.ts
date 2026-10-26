import type {
  AdmitProbeResult,
  BreakerCoordinator,
  BreakerIdentity,
  BreakerState,
  SettleProbeResult,
} from "../../core/circuit-breaker.js"
import {
  servalDeadline,
  servalScopeKey,
  type ServalAuthorityRecord,
  type ServalCachedAuthority,
  type ServalScopeIdentity,
} from "./authority.js"
import { ServalRegistryPressureError } from "./cache.js"
import type {
  ServalAuthorityReader,
  ServalAuthoritySnapshot,
} from "./reconciliation.js"
import type {
  ServalInvalidationPublisher,
  ServalNotificationTransport,
} from "./notifications.js"
import type { ServalScheduler } from "./scheduler.js"
import { createServalAuthoritySession, type ServalSession } from "./session.js"
import type { ServalAuthorityApplication } from "./session.js"

/**
 * The cached-verdict circuit breaker.
 *
 * It is a `BreakerCoordinator`, so `circuitBreaker.distributed({ coordinator })`
 * opts in without changing the policy or the strict coordinator: the strict
 * implementation stays authoritative for every transition and every probe, and
 * this layer serves the *admission* decision from locally held authority.
 *
 * What it removes is the per-admission read. A warm, valid cached CLOSED verdict
 * admits with no Redis call at all; a cached OPEN rejects locally until its
 * cooldown elapses, and only then does one coalesced probe reach the authority.
 * That is `docs/serval/README.md` Contract 2, and it is deliberately conservative:
 *
 * - A cached verdict is only ever *adopted* through the session, which requires an
 *   authoritative reconciliation (Contract 10), so a stray reply never switches
 *   this process to a different authority incarnation.
 * - The cooldown's origin is the server's `openedAt` from the transition reply.
 *   Converting that to a local deadline uses the reply's receipt time as the
 *   server's clock, so a skewed replica probes late rather than early; when it
 *   does probe early the authority refuses and the local cooldown is re-anchored.
 * - An unreachable authority never turns a cached OPEN into an admission, and a
 *   cached CLOSED past its allowance follows the configured behaviour instead of
 *   being assumed valid.
 */

/** How long a cached CLOSED verdict admits without an authority read. */
export const SERVAL_BREAKER_DEFAULT_FRESHNESS_MS = 1_000

/**
 * The strict breaker protocol has no authority incarnation. The reader supplies a
 * stable one so the session can adopt it; rollback detection rests on the
 * generation comparison, which is what the strict protocol actually orders.
 */
export const SERVAL_BREAKER_AUTHORITY_INCARNATION = "breaker-authority-1"

/** What a cached CLOSED verdict does once its freshness allowance has lapsed. */
export type ServalStaleClosedBehavior = "refresh" | "fail-open" | "fail-closed"

export interface ServalBreakerPayload {
  readonly state: BreakerState
  readonly generation: number
  /** Server-clock time the breaker opened; 0 when it is not open. */
  readonly openedAt: number
}

export interface ServalBreakerCoordinatorOptions {
  readonly session: ServalSession
  /** The strict coordinator that remains authoritative. */
  readonly authority: BreakerCoordinator
  /** How long a cached CLOSED verdict admits without an authority read. */
  readonly freshnessMs?: number
  /**
   * What to do with a cached CLOSED verdict past its allowance. `"refresh"` reads
   * the authority and propagates a failure, so the policy applies its own
   * `onCoordinatorError` behaviour; `"fail-open"` admits from the cached verdict
   * without a call; `"fail-closed"` refuses without a call.
   */
  readonly staleClosed?: ServalStaleClosedBehavior
}

export interface ServalBreakerCoordinator extends BreakerCoordinator {
  /** The cached verdict for an identity, for tests and diagnostics. */
  cached(identity: BreakerIdentity): ServalBreakerPayload | undefined
}

import { CoordinatorRefusalError } from "../../core/circuit-breaker.js"

/**
 * Thrown when the delegated breaker refuses locally, because the authority it holds is no longer
 * usable.
 *
 * It extends the core's `CoordinatorRefusalError` so the policy recognises it as a decision and
 * rejects the call, instead of applying `onCoordinatorError` and admitting it. The name says
 * refusal because that is the thing the policy has to branch on: an earlier name said
 * "unavailable", and that is exactly how a refusal came to be read as an outage.
 */
export class ServalAuthorityRefusalError extends CoordinatorRefusalError {
  readonly coordination = "distributed"

  constructor(message: string, options?: { readonly cause?: unknown }) {
    super(message, options)
    this.name = "ServalAuthorityRefusalError"
  }
}

/** The session identity of one breaker scope. */
export function breakerScopeIdentity(
  identity: BreakerIdentity,
): ServalScopeIdentity {
  return {
    policyKind: "circuit-breaker",
    policyName: identity.name,
    operation: identity.operation,
    scope: identity.scope,
  }
}

/** The breaker identity of one registered session scope. */
export function breakerIdentityOf(scope: ServalScopeIdentity): BreakerIdentity {
  return {
    name: scope.policyName,
    operation: scope.operation,
    scope: scope.scope,
  }
}

function payloadOf(
  cached: ServalCachedAuthority | undefined,
): ServalBreakerPayload | undefined {
  const payload = cached?.record.payload
  if (typeof payload !== "object" || payload === null) return undefined
  const candidate = payload as Partial<ServalBreakerPayload>
  const state = candidate.state
  if (state !== "closed" && state !== "open" && state !== "half-open") {
    return undefined
  }
  if (
    !Number.isSafeInteger(candidate.generation) ||
    !Number.isSafeInteger(candidate.openedAt)
  ) {
    return undefined
  }
  return {
    state,
    generation: candidate.generation as number,
    openedAt: candidate.openedAt as number,
  }
}

/**
 * The session version of a breaker verdict.
 *
 * The strict protocol keeps the generation on OPEN to HALF_OPEN, so the raw
 * generation cannot express that refinement. Folding the state into the low bit
 * keeps the order strict: a generation bump always dominates (2g+1 < 2(g+1)), and a
 * refinement within one generation is still newer than the verdict it refines.
 * A generation regression therefore remains visible as an older version, which is
 * what the session reports as a suspected rollback.
 */
function verdictVersion(verdict: ServalBreakerPayload): number {
  const base = verdict.generation * 2
  return verdict.state === "half-open" ? base + 1 : base
}

type BreakerReadState = Awaited<ReturnType<BreakerCoordinator["readState"]>>

export function createServalBreakerCoordinator(
  options: ServalBreakerCoordinatorOptions,
): ServalBreakerCoordinator {
  const { session, authority } = options
  const freshnessMs = options.freshnessMs ?? SERVAL_BREAKER_DEFAULT_FRESHNESS_MS
  const staleClosed = options.staleClosed ?? "refresh"

  if (!Number.isFinite(freshnessMs) || freshnessMs <= 0) {
    throw new RangeError("freshnessMs must be a positive finite number")
  }

  /** Local windows: the freshness of a CLOSED verdict and a cooldown override. */
  const local = new Map<
    string,
    { cachedAt: number; freshUntil: number; cooldownUntil: number }
  >()
  /** Probe leases, so a probe whose settlement never arrives cannot stall. */
  const probes = new Map<string, number>()
  /** Scopes this coordinator has registered with the session. */
  const registered = new Set<string>()

  function ensureRegistered(scope: ServalScopeIdentity): void {
    const key = servalScopeKey(scope)
    if (registered.has(key)) return
    // The session's registry is the bound on local authority: registering a scope
    // it cannot hold throws, rather than caching accounting nobody tracks.
    session.registerScope(scope)
    registered.add(key)
  }

  function prune(): void {
    if (local.size <= session.scopeCount && probes.size <= session.scopeCount) {
      return
    }
    const live = new Set(session.scopes().map(servalScopeKey))
    for (const key of [...local.keys()]) {
      if (!live.has(key)) local.delete(key)
    }
    for (const key of [...probes.keys()]) {
      if (!live.has(key)) probes.delete(key)
    }
  }

  function probeInFlight(key: string, now: number): boolean {
    const until = probes.get(key)
    if (until === undefined) return false
    if (now >= until) {
      probes.delete(key)
      return false
    }
    return true
  }

  /**
   * The local instant a verdict was adopted.
   *
   * A verdict that arrived from a reply carries its own receipt time. One that a
   * reconciliation wrote does not, but its deadline is that adoption time plus the
   * freshness allowance, so the anchor is recoverable from it - conservative to
   * within the session's safety margin, and the authority refuses an early probe.
   */
  function adoptedAt(
    key: string,
    entry: ServalCachedAuthority | undefined,
    now: number,
  ): number {
    const recorded = local.get(key)?.cachedAt
    if (recorded !== undefined) return recorded
    if (entry === undefined) return now
    return Math.min(entry.usableUntil - freshnessMs, now)
  }

  /**
   * Writes a verdict into the session's cache.
   *
   * Adoption goes through the session, so version ordering, the incarnation check,
   * the send-time-anchored deadline and the registry bound all apply unchanged. A
   * verdict the session refuses is not cached and gets no local window, so the next
   * admission reads the authority again instead of admitting from it.
   */
  function cacheVerdict(
    scope: ServalScopeIdentity,
    verdict: ServalBreakerPayload,
    context: {
      readonly requestStartedAt: number
      readonly receivedAt: number
      readonly revision: number
    },
  ): ServalAuthorityApplication | undefined {
    const key = servalScopeKey(scope)

    try {
      ensureRegistered(scope)
    } catch (error) {
      if (error instanceof ServalRegistryPressureError) {
        // The registry is full. The verdict is not cached, so the next admission reads the
        // authority again rather than acting on accounting nobody holds - but the reply is still
        // the authority's current verdict, so a caller may serve it.
        local.delete(key)
        return undefined
      }
      throw error
    }

    const incarnation = session.authorityIncarnation
    if (incarnation === undefined) {
      // No authority adopted yet: nothing may be cached, so admissions keep
      // reading the authority until the first reconciliation adopts one.
      return undefined
    }

    const record: ServalAuthorityRecord = {
      identity: scope,
      version: {
        authorityIncarnation: incarnation,
        epoch: 0,
        version: verdictVersion(verdict),
        configRevision: context.revision,
      },
      // Record timestamps are integer milliseconds: the client's monotonic clock is
      // fractional, and the session validates the wire shape of a record.
      issuedAt: Math.round(context.requestStartedAt),
      // Anchored to the send time, like every other grant: a reply that arrives late must not
      // extend the window it was granted for.
      expiresAt: Math.round(context.requestStartedAt) + freshnessMs,
      payload: verdict,
    }

    const outcome = session.applyAuthority({
      record,
      requestStartedAt: context.requestStartedAt,
      validForMs: freshnessMs,
      receivedAt: context.receivedAt,
    })

    if (outcome.type === "rejected") {
      local.delete(key)
      return outcome
    }

    const previous = local.get(key)
    local.set(key, {
      cachedAt: context.receivedAt,
      // Measured from the send time, so a delayed reply cannot leave this replica trusting the
      // verdict past the window it was granted. Receipt-anchored, the delay itself became extra
      // authority, which is the one direction that matters here.
      //
      // Only a reply that verified the verdict this replica holds may move the window: one the
      // session applied, or one it reported as unchanged, which means the authority answered with
      // exactly what we hold. A superseded reply described a past observation and verified nothing
      // about now, so it leaves the window where it was - moving it there pushed the deadline past
      // the one the authority granted.
      freshUntil:
        outcome.type === "applied" || outcome.reason === "unchanged"
          ? servalDeadline(context.requestStartedAt, freshnessMs)
          : (previous?.freshUntil ?? 0),
      // A re-anchored cooldown survives a same-version refresh of the verdict.
      cooldownUntil: previous?.cooldownUntil ?? 0,
    })

    return outcome
  }

  /**
   * Whether a reply is authority this process may act on.
   *
   * A record the session refused is not: it arrived after its window, a newer verdict supersedes
   * it, or it was not a record this scope could use. A reply that merely failed to be *cached* -
   * registry pressure, or no incarnation adopted yet - still carries the authority's current
   * verdict, so it is served.
   */
  function adoptable(outcome: ServalAuthorityApplication): boolean {
    if (outcome.type === "applied") return true
    if (outcome.type === "rejected") return false
    return (
      outcome.reason === "unchanged" || outcome.reason === "registry-refused"
    )
  }

  /**
   * The admission answer when the authority's reply could not be adopted.
   *
   * The cached verdict still stands, so it answers: a protective OPEN or HALF_OPEN is returned as
   * it is, and a stale CLOSED follows the configured behaviour rather than borrowing freshness
   * from a reply that was refused. With `staleClosed: "refresh"` the read has already happened and
   * did not produce usable authority, so refusing is the honest answer.
   */
  function refusedReplyVerdict(scope: ServalScopeIdentity): BreakerReadState {
    const cached = payloadOf(session.readAuthority(scope))
    if (cached !== undefined && cached.state !== "closed") {
      return {
        state: cached.state,
        generation: cached.generation,
        openedAt: cached.openedAt,
      }
    }
    if (staleClosed === "fail-open") {
      return { state: "closed", generation: cached?.generation ?? 0 }
    }
    throw new ServalAuthorityRefusalError(
      "The authority's reply could not be adopted: it arrived after its freshness allowance, or a newer verdict supersedes it",
    )
  }

  /** Reads the authority and caches what it says. Errors propagate unchanged. */
  async function refresh(
    identity: BreakerIdentity,
    scope: ServalScopeIdentity,
  ): Promise<BreakerReadState> {
    ensureRegistered(scope)

    const requestStartedAt = session.now()
    const state = await authority.readState(identity)
    const receivedAt = session.now()

    const verdict: ServalBreakerPayload =
      state === null
        ? { state: "closed", generation: 0, openedAt: 0 }
        : {
            state: state.state,
            generation: state.generation,
            openedAt: state.openedAt ?? 0,
          }

    const outcome = cacheVerdict(scope, verdict, {
      requestStartedAt,
      receivedAt,
      revision: state?.revision ?? 0,
    })

    // A reply the session refused to adopt is not authority this process may admit from: it
    // arrived after its window, or a newer verdict we already hold supersedes it. Serving it
    // anyway is what let a superseded or late reply run the adapter, so the cached verdict
    // answers instead.
    if (outcome !== undefined && !adoptable(outcome)) {
      return refusedReplyVerdict(scope)
    }

    return state === null
      ? { state: "closed", generation: 0 }
      : {
          state: state.state,
          generation: state.generation,
          openedAt: state.openedAt,
          revision: state.revision,
          changedAt: state.changedAt,
        }
  }

  /**
   * Announces a transition so the other replicas converge without waiting for the
   * next reconciliation. The hint carries no state: a subscriber refreshes, and
   * version ordering makes a duplicate or a reordered hint harmless.
   */
  function publishTransition(
    scope: ServalScopeIdentity,
    verdict: ServalBreakerPayload,
  ): void {
    const incarnation = session.authorityIncarnation
    if (incarnation === undefined) return
    void session.publish({
      identity: scope,
      version: {
        authorityIncarnation: incarnation,
        epoch: 0,
        version: verdictVersion(verdict),
        configRevision: session.configurationRevision,
      },
      kind: "authority",
    })
  }

  const coordinator: ServalBreakerCoordinator = {
    async readState(identity) {
      const scope = breakerScopeIdentity(identity)
      const key = servalScopeKey(scope)
      prune()

      const entry = session.readAuthority(scope)
      const cached = payloadOf(entry)
      if (cached === undefined) {
        // Cold, or nothing cached yet: the authority answers.
        return refresh(identity, scope)
      }

      if (cached.state !== "closed") {
        // OPEN and HALF_OPEN are protective and carry no expiry: they are returned
        // as they are, and the policy attempts an authoritative probe.
        return {
          state: cached.state,
          generation: cached.generation,
          openedAt: cached.openedAt,
        }
      }

      const now = session.now()
      // The local window is refreshed by every read this coordinator performs,
      // including one that re-reads the same generation; the session's own deadline
      // is the conservative bound for a verdict a reconciliation wrote.
      const freshUntil = Math.max(
        local.get(key)?.freshUntil ?? 0,
        entry?.usableUntil ?? 0,
      )
      if (now < freshUntil) {
        return { state: "closed", generation: cached.generation }
      }

      if (staleClosed === "fail-open") {
        return { state: "closed", generation: cached.generation }
      }
      if (staleClosed === "fail-closed") {
        throw new ServalAuthorityRefusalError(
          "The cached CLOSED verdict is past its freshness allowance and staleClosed is fail-closed",
        )
      }
      return refresh(identity, scope)
    },

    async admitProbe(identity, params) {
      const scope = breakerScopeIdentity(identity)
      const key = servalScopeKey(scope)
      const entry = session.readAuthority(scope)
      const cached = payloadOf(entry)
      const now = session.now()

      if (cached?.state === "open") {
        const window = local.get(key)
        const cachedAt = adoptedAt(key, entry, now)
        // The strict protocol reports the server's open time but not the server's
        // clock, so the client cannot convert that timestamp into a local deadline.
        // It anchors the cooldown to the instant it adopted the verdict instead,
        // which is at or after the server opened: the client therefore waits at
        // least as long as the server requires. An early probe - a shortened
        // cooldown, or a client clock that leads the server's - is refused by the
        // authority, and the re-anchor below keeps that from becoming a storm.
        if (
          now < Math.max(cachedAt + params.openMs, window?.cooldownUntil ?? 0)
        ) {
          // Still cooling down: refuse locally, with no Redis call.
          return {
            type: "rejected",
            reason: "open",
            generation: cached.generation,
          }
        }
        if (probeInFlight(key, now)) {
          // One authoritative probe at a time per identity.
          return {
            type: "rejected",
            reason: "probe-limit",
            generation: cached.generation,
          }
        }
      }

      probes.set(key, now + params.probeLeaseTtlMs)
      const requestStartedAt = session.now()
      let reply: AdmitProbeResult
      try {
        reply = await authority.admitProbe(identity, params)
      } catch (error) {
        probes.delete(key)
        throw error
      }
      const receivedAt = session.now()

      if (reply.type === "admitted") {
        cacheVerdict(
          scope,
          {
            state: "half-open",
            generation: reply.generation,
            openedAt: cached?.openedAt ?? 0,
          },
          { requestStartedAt, receivedAt, revision: 0 },
        )
        return reply
      }

      probes.delete(key)
      if (reply.reason === "closed") {
        // The breaker closed between the cached verdict and this probe.
        cacheVerdict(
          scope,
          { state: "closed", generation: reply.generation, openedAt: 0 },
          { requestStartedAt, receivedAt, revision: 0 },
        )
      } else if (reply.reason === "open") {
        // The authority is still cooling down: re-anchor the local cooldown so a
        // replica whose clock leads the server does not probe on every request.
        const window = local.get(key)
        local.set(key, {
          cachedAt: window?.cachedAt ?? receivedAt,
          freshUntil: window?.freshUntil ?? 0,
          cooldownUntil: receivedAt + params.openMs,
        })
      }
      return reply
    },

    async observe(identity, params) {
      const scope = breakerScopeIdentity(identity)
      const requestStartedAt = session.now()
      const reply = await authority.observe(identity, params)
      const receivedAt = session.now()

      if (reply.type === "observed") {
        // The settlement proves the breaker is CLOSED as of now.
        cacheVerdict(
          scope,
          { state: "closed", generation: reply.generation, openedAt: 0 },
          { requestStartedAt, receivedAt, revision: 0 },
        )
      } else if (reply.type === "opened") {
        const verdict: ServalBreakerPayload = {
          state: "open",
          generation: reply.newGeneration,
          openedAt: reply.openedAt ?? receivedAt,
        }
        cacheVerdict(scope, verdict, {
          requestStartedAt,
          receivedAt,
          revision: reply.revision ?? 0,
        })
        publishTransition(scope, verdict)
      } else {
        // Stale: this replica's view is behind, so drop the local freshness and let
        // the next admission read the authority again.
        const key = servalScopeKey(scope)
        const window = local.get(key)
        if (window !== undefined) {
          local.set(key, { ...window, freshUntil: 0 })
        }
      }
      return reply
    },

    async settleProbe(identity, params) {
      const scope = breakerScopeIdentity(identity)
      const key = servalScopeKey(scope)
      const requestStartedAt = session.now()
      let reply: SettleProbeResult
      try {
        reply = await authority.settleProbe(identity, params)
      } finally {
        // The probe is over either way, so the next attempt may claim a slot.
        probes.delete(key)
      }
      const receivedAt = session.now()

      if (reply.type === "transitioned") {
        const verdict: ServalBreakerPayload =
          reply.newState === "open"
            ? {
                state: "open",
                generation: reply.newGeneration,
                openedAt: reply.openedAt ?? receivedAt,
              }
            : {
                state: reply.newState,
                generation: reply.newGeneration,
                openedAt: 0,
              }
        cacheVerdict(scope, verdict, {
          requestStartedAt,
          receivedAt,
          revision: reply.revision ?? 0,
        })
        publishTransition(scope, verdict)
      } else if (reply.type === "settled") {
        cacheVerdict(
          scope,
          { state: reply.state, generation: reply.generation, openedAt: 0 },
          { requestStartedAt, receivedAt, revision: 0 },
        )
      }
      return reply
    },

    cached(identity) {
      return payloadOf(session.readAuthority(breakerScopeIdentity(identity)))
    },
  }

  return Object.freeze(coordinator)
}

export interface ServalBreakerReaderOptions {
  /** The scopes this process holds. */
  readonly scopes: () => readonly ServalScopeIdentity[]
  /** The monotonic clock the records are stamped from. */
  readonly now: () => number
  readonly authority: BreakerCoordinator
  /** The freshness allowance the snapshot's records carry. */
  readonly freshnessMs?: number
  /** The stable authority incarnation this reader reports. */
  readonly authorityIncarnation?: string
}

/**
 * The authoritative snapshot for breaker scopes.
 *
 * The strict breaker protocol reads one identity at a time, so a snapshot is a
 * fan-out over the scopes this process holds. That is what makes the cached
 * verdicts converge after a reconnect or a cold start, and it is the cost the
 * benchmark has to weigh: one read per held scope per reconciliation.
 */
export function createServalBreakerReader(
  options: ServalBreakerReaderOptions,
): ServalAuthorityReader {
  const { authority, scopes, now } = options
  const freshnessMs = options.freshnessMs ?? SERVAL_BREAKER_DEFAULT_FRESHNESS_MS
  const authorityIncarnation =
    options.authorityIncarnation ?? SERVAL_BREAKER_AUTHORITY_INCARNATION

  return {
    async readSnapshot(): Promise<ServalAuthoritySnapshot> {
      const records: ServalAuthorityRecord[] = []
      let revision = 0

      for (const scope of scopes()) {
        if (scope.policyKind !== "circuit-breaker") continue

        const state = await authority.readState(breakerIdentityOf(scope))
        revision = Math.max(revision, state?.revision ?? 0)

        const issuedAt = Math.round(now())
        const payload: ServalBreakerPayload = {
          state: state?.state ?? "closed",
          generation: state?.generation ?? 0,
          openedAt: state?.openedAt ?? 0,
        }
        records.push({
          identity: scope,
          version: {
            authorityIncarnation,
            epoch: 0,
            version: verdictVersion(payload),
            configRevision: state?.revision ?? 0,
          },
          issuedAt,
          expiresAt: issuedAt + freshnessMs,
          payload,
        })
      }

      return {
        authorityIncarnation,
        // The strict protocol reports no server clock, and this allowance is a
        // client-side duration rather than a server-issued remaining validity, so
        // the reader is its own clock: only the difference reaches the session.
        serverTime: Math.round(now()),
        configRevision: revision,
        records,
      }
    },
  }
}

export interface ServalBreakerOptions {
  readonly namespace: string
  /** The strict coordinator that remains authoritative. */
  readonly authority: BreakerCoordinator
  /** Invalidation hints, both directions. */
  readonly transport?: ServalNotificationTransport
  readonly publisher?: ServalInvalidationPublisher
  readonly scheduler?: ServalScheduler
  readonly freshnessMs?: number
  readonly staleClosed?: ServalStaleClosedBehavior
  readonly authorityIncarnation?: string
  readonly replicaId?: string
  readonly replicaIncarnation?: string
  readonly registryCapacity?: number
  readonly reconciliationIntervalMs?: number
  readonly staleAfterMs?: number
  readonly safetyMarginMs?: number
}

export interface ServalBreaker {
  readonly session: ServalSession
  /** The coordinator to hand to `circuitBreaker.distributed()`. */
  readonly coordinator: ServalBreakerCoordinator
  readonly reader: ServalAuthorityReader
}

/**
 * Wires one cached-verdict breaker: the session, its authoritative reader and the
 * coordinator the policy uses.
 *
 * This is the opt-in. Nothing about the policy or the strict coordinator
 * changes: `circuitBreaker.distributed({ coordinator: breaker.coordinator })`
 * simply stops paying for a read on every admission while a cached verdict is
 * valid, and keeps the strict coordinator authoritative for everything else.
 */
export function createServalBreaker(
  options: ServalBreakerOptions,
): ServalBreaker {
  const freshnessMs = options.freshnessMs ?? SERVAL_BREAKER_DEFAULT_FRESHNESS_MS

  const reader = createServalBreakerReader({
    scopes: () => session.scopes(),
    now: () => session.now(),
    authority: options.authority,
    freshnessMs,
    authorityIncarnation: options.authorityIncarnation,
  })

  const session = createServalAuthoritySession({
    namespace: options.namespace,
    reader,
    transport: options.transport,
    publisher: options.publisher,
    scheduler: options.scheduler,
    replicaId: options.replicaId,
    replicaIncarnation: options.replicaIncarnation,
    registryCapacity: options.registryCapacity,
    reconciliationIntervalMs: options.reconciliationIntervalMs,
    staleAfterMs: options.staleAfterMs,
    safetyMarginMs: options.safetyMarginMs,
  })

  const coordinator = createServalBreakerCoordinator({
    session,
    authority: options.authority,
    freshnessMs,
    staleClosed: options.staleClosed,
  })

  return { session, coordinator, reader }
}
