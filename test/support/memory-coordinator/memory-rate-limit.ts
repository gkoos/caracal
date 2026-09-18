/**
 * Deterministic in-memory RateLimitCoordinator for unit tests.
 *
 * Implements the same GCRA transition as the Redis Lua script so unit tests
 * exercise the distributed policy without a real coordinator. The clock is
 * controlled explicitly (`setNow`) so tests advance time deterministically.
 * This module must never be imported from production runtime code.
 */
import type { RateLimitCoordinator } from "../../../src/core/rate-limit.js"

type Identity = { name: string; operation: string; scope: string }

export function memoryRateLimitCoordinator(): RateLimitCoordinator & {
  /** The stored theoretical arrival time for a scope, or undefined. */
  inspect(identity: Identity): number | undefined
  /** Remove all stored state (useful between sub-tests). */
  reset(): void
  /** The current clock value. */
  now(): number
  /** Advance the clock to an explicit value. */
  setNow(now: number): void
} {
  const states = new Map<string, number>()
  let clock = 0

  const scopeKey = (identity: Identity) =>
    JSON.stringify([identity.name, identity.operation, identity.scope])

  return {
    inspect(identity) {
      return states.get(scopeKey(identity))
    },
    reset() {
      states.clear()
    },
    now: () => clock,
    setNow(now) {
      if (!Number.isFinite(now) || now < clock)
        throw new RangeError("memory rate limit clock cannot move backwards")
      clock = now
    },
    async command(identity, params) {
      const key = scopeKey(identity)
      const tat = states.get(key) ?? 0
      const anchored = Math.max(tat, clock)
      if (anchored - clock > params.burstDelayMs) {
        return {
          allowed: false,
          retryAfterMs: anchored - params.burstDelayMs - clock,
        }
      }
      states.set(key, anchored + params.emissionIntervalMs)
      return { allowed: true, retryAfterMs: 0 }
    },
  }
}
