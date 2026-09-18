import { admissionSignal, emitRuntimeEvent } from "./runtime.js"
import type {
  ExecutionContext,
  Next,
  Policy,
  RateLimitEventReason,
} from "./types.js"

/**
 * A rate limit bounds the arrival rate of adapter calls using the Generic Cell
 * Rate Algorithm (GCRA): one scalar of state (the theoretical arrival time)
 * enforces both a sustained rate and a bounded burst, with no background refill
 * process. Rate is the axis next to the bulkhead's concurrency axis.
 */

export class RateLimitExceededError extends Error {
  constructor(
    readonly coordination: "local" | "distributed",
    readonly policyName: string,
    readonly scope: string,
    readonly retryAfterMs: number,
  ) {
    super(`Rate limit ${policyName} exceeded; retry after ${retryAfterMs}ms`)
    this.name = "RateLimitExceededError"
  }
}

export interface LocalRateLimitOptions {
  readonly name: string
  /**
   * Sustained rate in requests per second. Resolved to a whole-millisecond
   * emission interval, so it cannot express more than 1000 requests per second.
   */
  readonly rate: number
  /**
   * Maximum burst: how many requests may arrive clustered together before the
   * strict rate applies. Default: 1 (no burst).
   */
  readonly burst?: number
}

/** Policy-specific capability supplied by caracal/redis. */
export interface RateLimitCoordinator {
  command(
    identity: { name: string; operation: string; scope: string },
    params: {
      readonly emissionIntervalMs: number
      readonly burstDelayMs: number
    },
  ): Promise<{ allowed: boolean; retryAfterMs: number }>
}

export interface DistributedRateLimitOptions {
  readonly name: string
  readonly rate: number
  readonly burst?: number
  readonly coordinator: RateLimitCoordinator
  readonly scope: (context: ExecutionContext) => string
}

interface Knobs {
  readonly emissionIntervalMs: number
  readonly burstDelayMs: number
}

function knobs(rate: number, burst: number): Knobs {
  if (!Number.isFinite(rate) || rate <= 0 || rate > 1000)
    throw new RangeError(
      "rate must be a finite number in (0, 1000] requests per second",
    )
  const emissionIntervalMs = Math.round(1000 / rate)
  if (!Number.isSafeInteger(emissionIntervalMs) || emissionIntervalMs < 1)
    throw new RangeError(
      "rate must resolve to a whole-millisecond emission interval of at least 1ms",
    )
  if (!Number.isSafeInteger(burst) || burst < 1)
    throw new RangeError("burst must be a positive integer")
  const burstDelayMs = (burst - 1) * emissionIntervalMs
  if (!Number.isSafeInteger(burstDelayMs))
    throw new RangeError("burst is too large for the configured rate")
  return { emissionIntervalMs, burstDelayMs }
}

function event(
  context: ExecutionContext,
  coordination: "local" | "distributed",
  policyName: string,
  scope: string,
  type: "admitted" | "rejected" | "degraded",
  retryAfterMs?: number,
  reason?: RateLimitEventReason,
): void {
  emitRuntimeEvent(context, {
    type: `ratelimit.${type}`,
    coordination,
    policyName,
    scope,
    ...(retryAfterMs === undefined ? {} : { retryAfterMs }),
    ...(reason === undefined ? {} : { reason }),
  })
}

type LocalRateLimitPolicy = Policy & {
  readonly coordination: "local"
  snapshot(): { coordination: "local"; nextAllowedAt: number }
}

function local(options: LocalRateLimitOptions): LocalRateLimitPolicy {
  const { name, rate } = options
  const burst = options.burst ?? 1
  if (typeof name !== "string" || !name.trim())
    throw new RangeError("Rate limit needs a name")
  const { emissionIntervalMs, burstDelayMs } = knobs(rate, burst)
  let tat = 0

  return Object.freeze({
    name,
    phase: "attempt" as const,
    coordination: "local" as const,
    snapshot: () => ({
      coordination: "local" as const,
      nextAllowedAt: Math.max(tat, Date.now()),
    }),
    async execute<Result>(
      context: ExecutionContext,
      next: Next<Result>,
    ): Promise<Result> {
      admissionSignal(context)?.throwIfAborted()
      const now = Date.now()
      const anchored = Math.max(tat, now)
      if (anchored - now > burstDelayMs) {
        const retryAfterMs = anchored - burstDelayMs - now
        event(
          context,
          "local",
          name,
          "process",
          "rejected",
          retryAfterMs,
          "rate-exceeded",
        )
        throw new RateLimitExceededError("local", name, "process", retryAfterMs)
      }
      tat = anchored + emissionIntervalMs
      event(context, "local", name, "process", "admitted")
      return await next(context)
    },
  })
}

type DistributedRateLimitPolicy = Policy & {
  readonly coordination: "distributed"
}

function distributed(
  options: DistributedRateLimitOptions,
): DistributedRateLimitPolicy {
  const { name, rate, coordinator, scope: resolveScope } = options
  const burst = options.burst ?? 1
  if (typeof name !== "string" || !name.trim())
    throw new RangeError("Rate limit needs a name")
  if (!coordinator || typeof coordinator !== "object")
    throw new TypeError("coordinator is required")
  if (typeof resolveScope !== "function")
    throw new TypeError("scope must be a function")
  const { emissionIntervalMs, burstDelayMs } = knobs(rate, burst)

  return Object.freeze({
    name,
    phase: "attempt" as const,
    coordination: "distributed" as const,
    async execute<Result>(
      context: ExecutionContext,
      next: Next<Result>,
    ): Promise<Result> {
      admissionSignal(context)?.throwIfAborted()
      const scope = resolveScope(context)
      if (typeof scope !== "string" || !scope.trim())
        throw new TypeError("Invalid rate limit scope")
      const identity = { name, operation: context.operationName, scope }
      let admitted: { allowed: boolean; retryAfterMs: number }
      try {
        admitted = await coordinator.command(identity, {
          emissionIntervalMs,
          burstDelayMs,
        })
      } catch (error) {
        event(
          context,
          "distributed",
          name,
          scope,
          "degraded",
          undefined,
          "admission-unknown",
        )
        event(
          context,
          "distributed",
          name,
          scope,
          "rejected",
          undefined,
          "coordinator-unavailable",
        )
        throw error
      }
      if (!admitted.allowed) {
        event(
          context,
          "distributed",
          name,
          scope,
          "rejected",
          admitted.retryAfterMs,
          "rate-exceeded",
        )
        throw new RateLimitExceededError(
          "distributed",
          name,
          scope,
          admitted.retryAfterMs,
        )
      }
      event(context, "distributed", name, scope, "admitted")
      return await next(context)
    },
  })
}

export const rateLimit = Object.freeze({ local, distributed })
