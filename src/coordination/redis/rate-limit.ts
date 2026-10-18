import type { RateLimitCoordinator } from "../../core/rate-limit.js"
import {
  ConfigurationError,
  decodeConfigurationRecord,
} from "../../core/configuration.js"
import { CoordinatorUnavailableError } from "./client.js"
import { evalScript } from "./eval-script.js"
import { coordinationKey } from "./keys.js"
import type { RedisScriptClient } from "./leases.js"
import { createRevisionCache } from "./revisions.js"
import { rateLimitV1 } from "./scripts.js"

const ENVELOPE_VERSION = 1
const UNSUPPORTED_RECORD = "unsupported configuration record"

/** A record the server cannot use is a configuration error, not an outage. */
function configurationError(error: unknown): ConfigurationError | undefined {
  if (error instanceof ConfigurationError) return error
  const message = error instanceof Error ? error.message : String(error)
  if (message.includes(UNSUPPORTED_RECORD))
    return new ConfigurationError("unsupported-schema", message)
  return undefined
}

interface RateReply {
  readonly allowed: boolean
  readonly retryAfterMs: number
  readonly revision: number
  readonly body: string | undefined
}

function decodeReply(result: unknown): RateReply {
  if (!Array.isArray(result) || (result.length !== 5 && result.length !== 6))
    throw new Error("Invalid rate limit reply")
  const [envelope, allowed, retryAfterMs, revision, changedAt, body] = result
  if (
    envelope !== ENVELOPE_VERSION ||
    (allowed !== 0 && allowed !== 1) ||
    !Number.isSafeInteger(retryAfterMs) ||
    (retryAfterMs as number) < 0 ||
    !Number.isSafeInteger(revision) ||
    (revision as number) < 0 ||
    !Number.isSafeInteger(changedAt) ||
    (body !== undefined && typeof body !== "string")
  )
    throw new Error("Invalid rate limit reply")
  return {
    allowed: allowed === 1,
    retryAfterMs: retryAfterMs as number,
    revision: revision as number,
    body: body === "" ? undefined : (body as string | undefined),
  }
}

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
  coordinationKey(
    namespace,
    "ratelimit:validate",
    "validate",
    "validate",
    "rate",
  )
  const revisions = createRevisionCache()
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
      const policy = `ratelimit:${identity.name}`
      const key = coordinationKey(
        namespace,
        policy,
        identity.operation,
        identity.scope,
        "rate",
      )
      const configurationKey = coordinationKey(
        namespace,
        policy,
        identity.operation,
        identity.scope,
        "configuration",
      )
      try {
        const result = await evalScript(
          client,
          rateLimitV1,
          2,
          key,
          configurationKey,
          params.emissionIntervalMs,
          params.burstDelayMs,
          revisions.get(configurationKey),
        )
        const reply = decodeReply(result)
        if (reply.revision > 0) revisions.set(configurationKey, reply.revision)
        if (reply.body !== undefined) {
          const snapshot = decodeConfigurationRecord(reply.body)
          if (snapshot.settings.policy !== "rateLimit")
            throw new ConfigurationError(
              "corrupt-record",
              "configuration record policy does not match the identity",
            )
        }
        return {
          allowed: reply.allowed,
          retryAfterMs: reply.retryAfterMs,
        }
      } catch (error) {
        const configuration = configurationError(error)
        if (configuration !== undefined) throw configuration
        throw new CoordinatorUnavailableError(error)
      }
    },
  }
  return Object.freeze(coordinator)
}
