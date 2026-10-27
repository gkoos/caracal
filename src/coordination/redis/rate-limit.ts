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
import { rateGrantV1, rateLimitV1 } from "./scripts.js"

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

interface GrantReply {
  readonly granted: number
  readonly retryAfterMs: number
  readonly validForMs: number
  readonly duplicate: boolean
  readonly revision: number
  readonly body: string | undefined
}

function decodeGrantReply(result: unknown): GrantReply {
  if (!Array.isArray(result) || (result.length !== 7 && result.length !== 8))
    throw new Error("Invalid rate grant reply")
  const [
    envelope,
    granted,
    retryAfterMs,
    validForMs,
    duplicate,
    revision,
    changedAt,
    body,
  ] = result
  if (
    envelope !== ENVELOPE_VERSION ||
    !Number.isSafeInteger(granted) ||
    (granted as number) < 0 ||
    !Number.isSafeInteger(retryAfterMs) ||
    (retryAfterMs as number) < 0 ||
    !Number.isSafeInteger(validForMs) ||
    (validForMs as number) < 1 ||
    (duplicate !== 0 && duplicate !== 1) ||
    !Number.isSafeInteger(revision) ||
    (revision as number) < 0 ||
    !Number.isSafeInteger(changedAt) ||
    (body !== undefined && typeof body !== "string")
  )
    throw new Error("Invalid rate grant reply")
  return {
    granted: granted as number,
    retryAfterMs: retryAfterMs as number,
    validForMs: validForMs as number,
    duplicate: duplicate === 1,
    revision: revision as number,
    body: body === "" ? undefined : (body as string | undefined),
  }
}

/**
 * One quota issuance operation: the request is immutable, so a retry of the same
 * `operationId` carries the same parameters and is deduplicated server-side.
 */
export interface RateGrantRequest {
  readonly identity: {
    readonly name: string
    readonly operation: string
    readonly scope: string
  }
  /** Identifies the operation, including all of its retries. */
  readonly operationId: string
  readonly emissionIntervalMs: number
  readonly burstDelayMs: number
  /** How many permits this grant asks for. A grant may be smaller, or refused. */
  readonly permits: number
  /** The grant's spend horizon, `V`. */
  readonly validForMs: number
  /**
   * Upper bound on permits issued and not yet expired across the fleet. The
   * server enforces it as a bound on live grants, which is conservative for any
   * request size; omit it to let the bucket alone bound issuance.
   */
  readonly maximumOutstandingPermits?: number
}

export interface RateGrantReply {
  /** Permits granted. Zero means refused, and `retryAfterMs` is then positive. */
  readonly granted: number
  /** The server returned the recorded grant from its ledger; nothing was deducted. */
  readonly duplicate: boolean
  /** The server-issued spend horizon for this grant. */
  readonly validForMs: number
  readonly retryAfterMs: number
  readonly revision: number
}

/**
 * The grant-issuance capability of a delegated quota strategy.
 *
 * A grant is issued once and never renewed, so `operationId` identifies it
 * throughout: the caller keeps one operation per in-flight issuance and reuses its
 * id for retries, which is what makes an ambiguous reply safe.
 */
export interface RateLimitGrantIssuer {
  issue(request: RateGrantRequest): Promise<RateGrantReply>
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

/**
 * Redis-backed quota issuance for a delegated rate limiter.
 *
 * It writes to the same rate state key the strict `redisRateLimitCoordinator`
 * uses, so both paths draw on one budget: a grant of `n` permits is `n` arrivals
 * taken atomically, and a strict call made afterwards waits for the same bucket.
 * Mixing the two paths for one identity is therefore safe but not additive - the
 * delegated path is not a second budget.
 */
export function redisRateLimitGrantIssuer(
  client: RedisScriptClient,
  options: { namespace: string },
): RateLimitGrantIssuer {
  const namespace = options.namespace
  const revisions = createRevisionCache()

  const issuer: RateLimitGrantIssuer = {
    async issue(request) {
      if (
        !Number.isSafeInteger(request.emissionIntervalMs) ||
        request.emissionIntervalMs < 1 ||
        !Number.isSafeInteger(request.burstDelayMs) ||
        request.burstDelayMs < 0
      )
        throw new RangeError(
          "Invalid rate limit emission interval or burst delay",
        )
      if (
        typeof request.operationId !== "string" ||
        !request.operationId.trim() ||
        Buffer.byteLength(request.operationId) > 1024
      )
        throw new TypeError("Invalid rate grant operation id")
      if (!Number.isSafeInteger(request.permits) || request.permits < 1)
        throw new RangeError("Invalid rate grant size")
      if (!Number.isSafeInteger(request.validForMs) || request.validForMs < 1)
        throw new RangeError("Invalid rate grant validity")

      const { identity } = request
      const policy = `ratelimit:${identity.name}`
      const key = coordinationKey(
        namespace,
        policy,
        identity.operation,
        identity.scope,
        "rate",
      )
      const ledgerKey = coordinationKey(
        namespace,
        policy,
        identity.operation,
        identity.scope,
        "grants",
      )
      const expiryKey = coordinationKey(
        namespace,
        policy,
        identity.operation,
        identity.scope,
        "grant-expiry",
      )
      const configurationKey = coordinationKey(
        namespace,
        policy,
        identity.operation,
        identity.scope,
        "configuration",
      )

      // The cap is a bound on live permits, and the server sums what the live grants
      // hold, so it holds for every grant size rather than for the one this caller
      // happens to request.
      const maximumOutstanding =
        request.maximumOutstandingPermits === undefined
          ? -1
          : request.maximumOutstandingPermits

      try {
        const result = await evalScript(
          client,
          rateGrantV1,
          4,
          key,
          ledgerKey,
          expiryKey,
          configurationKey,
          request.emissionIntervalMs,
          request.burstDelayMs,
          revisions.get(configurationKey),
          request.operationId,
          request.permits,
          request.validForMs,
          maximumOutstanding,
        )
        const reply = decodeGrantReply(result)
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
          granted: reply.granted,
          duplicate: reply.duplicate,
          validForMs: reply.validForMs,
          retryAfterMs: reply.retryAfterMs,
          revision: reply.revision,
        }
      } catch (error) {
        const configuration = configurationError(error)
        if (configuration !== undefined) throw configuration
        throw new CoordinatorUnavailableError(error)
      }
    },
  }
  return Object.freeze(issuer)
}
