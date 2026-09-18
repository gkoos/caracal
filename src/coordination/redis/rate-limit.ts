import type { RateLimitCoordinator } from "../../core/rate-limit.js"
import { CoordinatorUnavailableError } from "./client.js"
import { evalScript } from "./eval-script.js"
import { coordinationKey } from "./keys.js"
import type { RedisScriptClient } from "./leases.js"
import { rateLimitV1 } from "./scripts.js"

/**
 * Policy-specific Redis coordinator for `rateLimit.distributed()`.
 *
 * The caller owns `client` and must connect/disconnect it independently.
 * `namespace` is prepended to every key; use a per-service, per-environment
 * value to avoid cross-deployment state collisions.
 */
export function redisRateLimitCoordinator(
  client: RedisScriptClient,
  options: { namespace: string },
): RateLimitCoordinator {
  const namespace = options.namespace
  // Validate namespace eagerly (coordinationKey throws on bad input).
  coordinationKey(
    namespace,
    "ratelimit:validate",
    "validate",
    "validate",
    "rate",
  )

  const coordinator: RateLimitCoordinator = {
    async command(identity, params) {
      if (
        !Number.isSafeInteger(params.emissionIntervalMs) ||
        params.emissionIntervalMs < 1 ||
        !Number.isSafeInteger(params.burstDelayMs) ||
        params.burstDelayMs < 0
      )
        throw new RangeError(
          "Invalid rate limit emission interval or burst delay",
        )
      const key = coordinationKey(
        namespace,
        `ratelimit:${identity.name}`,
        identity.operation,
        identity.scope,
        "rate",
      )
      try {
        const result = await evalScript(
          client,
          rateLimitV1,
          1,
          key,
          params.emissionIntervalMs,
          params.burstDelayMs,
        )
        if (
          !Array.isArray(result) ||
          result.length !== 2 ||
          ![0, 1].includes(result[0]) ||
          !Number.isSafeInteger(result[1]) ||
          result[1] < 0
        )
          throw new Error("Invalid rate limit reply")
        return {
          allowed: result[0] === 1,
          retryAfterMs: result[1] as number,
        }
      } catch (error) {
        throw new CoordinatorUnavailableError(error)
      }
    },
  }
  return Object.freeze(coordinator)
}
