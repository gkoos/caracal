import { randomUUID } from "node:crypto"
import {
  bulkhead,
  BulkheadRejectedError,
  CircuitOpenError,
  circuitBreaker,
  operation,
  rateLimit,
  RateLimitExceededError,
  timeout,
} from "../../src/index.js"
import { redisBulkheadAllocationIssuer } from "../../src/coordination/redis/bulkhead.js"
import { redisRateLimitGrantIssuer } from "../../src/coordination/redis/rate-limit.js"
import { createServalBreaker } from "../../src/serval/breaker.js"
import { createServalBulkheadCoordinator } from "../../src/serval/bulkhead.js"
import { createServalRateLimitCoordinator } from "../../src/serval/rate-limit.js"
import {
  createCoordinationClient,
  redisCircuitBreakerCoordinator,
  redisCoordinator,
  redisRateLimitCoordinator,
} from "../../src/redis.js"
import { mulberry32 } from "./random.js"

/**
 * One replica of the Serval fault lab.
 *
 * It is a real process with its own event loop, its own Redis connection and its own
 * coordinator state, because that is the only way to observe the deviations that are
 * per-process: overlapping generations, redistributed capacity, and a partitioned
 * replica that keeps admitting from local authority.
 *
 * Every Redis command the coordinators issue is counted through a proxy over the
 * client, so "cost" is what the run actually sent - including the settlement writes
 * that caching the admission read does not remove.
 */

export type Profile = "unprotected" | "strict" | "serval" | "serval-guarded"

export interface RunCell {
  readonly namespace: string
  readonly profile: Profile
  readonly offers: number
  readonly intervalMs: number
  readonly target: number
  readonly guardrailLimit: number
  readonly abort: "supported" | "unsupported"
  readonly timeoutMs: number
  readonly redisUrl: string
  readonly downstreamUrl: string
  readonly identity: { readonly name: string; readonly operation: string }
  /**
   * The run's seed. It decides the offered schedule - the same seed is the same load in every
   * profile, and a different seed is a different arrival pattern.
   */
  readonly seed: number
  /**
   * Breaker thresholds for a fault cell, so the opening condition is the cell's rather than the
   * distributed default - which needs twenty observations before it may open at all, and the
   * short cells do not produce that many.
   */
  readonly breaker?: {
    readonly minimumThroughput: number
    readonly failureThreshold: number
  }
  /**
   * Offers served against the healthy authority before its connection is cut, or absent for a
   * cell that never had one. This is what makes an outage cell *warm*: the replica holds a
   * verdict, a grant and an allocation before it loses the authority that issued them.
   */
  readonly cutAfterOffers?: number
}

export interface Metrics {
  readonly offered: number
  readonly ok: number
  readonly rejectedRate: number
  readonly rejectedBulkhead: number
  readonly failed: number
  readonly other: number
  readonly redisOps: number
  readonly peakTracked: number
  /** A digest of the waits this replica used, so paired load is visible in the report. */
  readonly scheduleDigest: number
  /** Admissions before and after the authority was cut, so a warm-outage cell is legible. */
  readonly okBeforeCut: number
  readonly okAfterCut: number
  /** Calls the breaker refused, and the state machine that refused them. */
  readonly rejectedBreaker: number
  readonly breakerOpens: number
  readonly breakerHalfOpens: number
  readonly breakerProbes: number
  readonly admissionLatency: readonly number[]
  readonly endToEnd: readonly number[]
}

export function percentile(
  samples: readonly number[],
  fraction: number,
): number {
  if (samples.length === 0) return 0
  const sorted = [...samples].sort((left, right) => left - right)
  const index = Math.min(
    sorted.length - 1,
    Math.max(0, Math.ceil(fraction * sorted.length) - 1),
  )
  return Math.round(sorted[index] as number)
}

export async function runCell(cell: RunCell): Promise<Metrics> {
  const raw = createCoordinationClient(cell.redisUrl, 250)
  // A partitioned replica cannot connect at all; waiting for the client's own retry
  // loop would hang the run. The point of the cell is what the coordinators do once
  // their commands fail, not how long a client takes to give up.
  await Promise.race([
    raw.connect().catch(() => undefined),
    new Promise((resolve) => setTimeout(resolve, 300)),
  ])

  let redisOps = 0
  const client = new Proxy(raw as never, {
    get(target, property, receiver) {
      const value = Reflect.get(target as object, property, receiver)
      if (typeof value !== "function") return value
      if (
        property === "eval" ||
        property === "evalsha" ||
        property === "hmget"
      ) {
        return (...args: unknown[]) => {
          redisOps += 1
          return (value as (...inner: unknown[]) => unknown).apply(target, args)
        }
      }
      return (value as (...inner: unknown[]) => unknown).bind(target)
    },
  }) as ReturnType<typeof createCoordinationClient>

  const replicaId = `replica-${process.pid}-${randomUUID().slice(0, 8)}`
  const identity = { ...cell.identity, scope: "lab" }
  const breaker = createServalBreaker({
    namespace: cell.namespace,
    authority: redisCircuitBreakerCoordinator(client, {
      namespace: cell.namespace,
    }),
    replicaId,
    freshnessMs: 1_000,
    reconciliationIntervalMs: 60_000,
  })
  const rate = createServalRateLimitCoordinator({
    authority: redisRateLimitGrantIssuer(client, { namespace: cell.namespace }),
    grantSize: 4,
    validForMs: 1_000,
  })
  const capacity = createServalBulkheadCoordinator({
    authority: redisBulkheadAllocationIssuer(client, {
      namespace: cell.namespace,
    }),
    replicaId,
    validityMs: 5_000,
    graceMs: 1_000,
    initialSlots: 1,
    maximumSlots: Math.max(1, Math.ceil(cell.target / 2)),
    redistributionIntervalMs: 25,
  })

  const localGuardrail = bulkhead.local({
    name: "lab-guardrail",
    limit: cell.guardrailLimit,
  })
  const policies: unknown[] = []
  if (cell.profile === "strict") {
    policies.push(
      circuitBreaker.distributed({
        name: identity.name,
        coordinator: redisCircuitBreakerCoordinator(client, {
          namespace: cell.namespace,
        }),
        scope: () => identity.scope,
        openMs: 500,
        ...(cell.breaker ?? {}),
      }),
      rateLimit.distributed({
        name: identity.name,
        rate: 50,
        burst: 8,
        coordinator: redisRateLimitCoordinator(client, {
          namespace: cell.namespace,
        }),
        scope: () => identity.scope,
      }),
      bulkhead.distributed({
        name: identity.name,
        limit: cell.target,
        coordinator: redisCoordinator(client, { namespace: cell.namespace }),
        scope: () => identity.scope,
      }),
    )
  }
  if (cell.profile === "serval" || cell.profile === "serval-guarded") {
    policies.push(
      circuitBreaker.distributed({
        name: identity.name,
        coordinator: breaker.coordinator,
        scope: () => identity.scope,
        openMs: 500,
        ...(cell.breaker ?? {}),
      }),
      rateLimit.distributed({
        name: identity.name,
        rate: 50,
        burst: 8,
        coordinator: rate,
        scope: () => identity.scope,
      }),
      bulkhead.distributed({
        name: identity.name,
        limit: cell.target,
        coordinator: capacity,
        scope: () => identity.scope,
      }),
    )
  }
  if (cell.profile === "serval-guarded") policies.push(localGuardrail)
  if (cell.timeoutMs > 0) policies.push(timeout({ ms: cell.timeoutMs }))

  let offerStartedAt = 0
  let admissionLatency = 0
  const admissionLatencies: number[] = []
  const endToEnd: number[] = []
  let peakTracked = 0
  let rejectedBreaker = 0
  let breakerOpens = 0
  let breakerHalfOpens = 0
  let breakerProbes = 0
  const op = operation({
    name: identity.operation,
    adapter: {
      capabilities: () => ({ abort: cell.abort, replay: "safe" }) as const,
      execute: async () => {
        admissionLatency = performance.now() - offerStartedAt
        const response = await fetch(cell.downstreamUrl)
        if (!response.ok) throw new Error(`downstream ${response.status}`)
        await response.text()
        return "ok"
      },
    },
    policies: policies as never,
    // Tracked occupancy comes from the bulkhead's own report, for every profile: the local
    // count for a delegated scope, and the number the strict coordinator returned from Redis.
    // Reading it here costs nothing extra - it rides along with the admission the policy
    // already made - so the column is comparable across profiles instead of reading zero
    // wherever the leases live in Redis. Only the delegated policy is sampled; the guarded
    // profile's local guardrail has its own name and its own count.
    events: {
      emit(event) {
        if (
          event.type === "bulkhead.admitted" ||
          event.type === "bulkhead.rejected" ||
          event.type === "bulkhead.waited"
        ) {
          if (event.policyName !== identity.name) return
          if ("occupancy" in event && typeof event.occupancy === "number") {
            peakTracked = Math.max(peakTracked, event.occupancy)
          }
          return
        }
        // The breaker's own state machine, so a fault cell can show that it opened, recovered and
        // probed rather than only how many calls it refused.
        if (event.type === "breaker.state-changed") {
          if (event.policyName !== identity.name) return
          if (event.state === "open") breakerOpens += 1
          else if (event.state === "half-open") breakerHalfOpens += 1
          return
        }
        if (event.type === "breaker.probe-started") {
          if (event.policyName === identity.name) breakerProbes += 1
        }
      },
    },
  })

  await breaker.session.start()
  await breaker.session.whenIdle()

  let ok = 0
  let rejectedRate = 0
  let rejectedBulkhead = 0
  let failed = 0
  let other = 0
  let okBeforeCut = 0
  let okAfterCut = 0

  // The offered schedule is a function of the seed and the cell, never of the profile, so every
  // profile at a seed sees the same load. The jitter is what makes a seed worth having: a
  // metronome measures one arrival pattern, and the delegated rate limiter's behaviour depends on
  // the pattern rather than only on the rate.
  const random = mulberry32(cell.seed)
  const waits: number[] = []
  for (let offer = 0; offer + 1 < cell.offers; offer += 1) {
    waits.push(
      cell.intervalMs <= 0
        ? 0
        : Math.max(1, Math.round(cell.intervalMs * (0.5 + random()))),
    )
  }
  // FNV-1a over those waits: a positive 32-bit digest of the schedule this replica ran.
  let scheduleDigest = 2166136261
  for (const wait of waits) {
    scheduleDigest = Math.imul(scheduleDigest ^ wait, 16777619) >>> 0
  }

  const cutAfter = cell.cutAfterOffers ?? 0
  for (let offer = 0; offer < cell.offers; offer += 1) {
    if (cutAfter > 0 && offer === cutAfter) {
      // A warm-outage cell: the authority has served this replica until now, so cutting the
      // connection here is its path to Redis going away while the authority it already holds
      // is still valid. `disconnect` is permanent, and the client is configured with no
      // offline queue and no retries, so the next command fails instead of hanging the run.
      raw.disconnect()
    }
    offerStartedAt = performance.now()
    try {
      await op.execute(undefined)
      ok += 1
      if (cutAfter > 0 && offer >= cutAfter) okAfterCut += 1
      else okBeforeCut += 1
    } catch (error) {
      if (error instanceof RateLimitExceededError) rejectedRate += 1
      else if (error instanceof BulkheadRejectedError) rejectedBulkhead += 1
      else if (error instanceof CircuitOpenError) rejectedBreaker += 1
      else if (error instanceof Error && error.name === "TimeoutError")
        other += 1
      else failed += 1
    }
    endToEnd.push(performance.now() - offerStartedAt)
    if (admissionLatency > 0) admissionLatencies.push(admissionLatency)
    admissionLatency = 0

    const wait = waits[offer] ?? 0
    if (wait > 0) {
      await new Promise((resolve) => setTimeout(resolve, wait))
    }
  }

  await breaker.session.close()
  await capacity.close()
  await rate.whenIdle()
  raw.disconnect()

  return {
    offered: cell.offers,
    ok,
    rejectedRate,
    rejectedBulkhead,
    failed,
    other,
    redisOps,
    peakTracked,
    scheduleDigest,
    okBeforeCut,
    okAfterCut,
    rejectedBreaker,
    breakerOpens,
    breakerHalfOpens,
    breakerProbes,
    admissionLatency: admissionLatencies,
    endToEnd,
  }
}

process.on("message", (message: { action: string; cell?: RunCell }) => {
  if (message.action === "stop") {
    process.exit(0)
  }
  if (message.action !== "run" || message.cell === undefined) return
  runCell(message.cell)
    .then((metrics) => {
      process.send?.({ metrics })
    })
    .catch((error: unknown) => {
      process.send?.({
        error: error instanceof Error ? error.message : String(error),
      })
    })
})

process.send?.({ ready: true })
