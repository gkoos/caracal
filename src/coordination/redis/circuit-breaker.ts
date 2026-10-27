import type {
  AdmitProbeResult,
  BreakerCoordinator,
  BreakerIdentity,
  BreakerState,
  ObserveResult,
  SettleProbeResult,
} from "../../core/circuit-breaker.js"
import {
  ConfigurationError,
  decodeConfigurationRecord,
} from "../../core/configuration.js"
import { CoordinatorUnavailableError } from "./client.js"
import { evalScript } from "./eval-script.js"
import { coordinationKey } from "./keys.js"
import type { RedisScriptClient } from "./leases.js"
import { createRevisionCache } from "./revisions.js"
import {
  breakerAdmitProbeV1,
  breakerObserveV1,
  breakerReadStateV1,
  breakerSettleProbeV1,
} from "./scripts.js"

// ---------------------------------------------------------------------------
// Key helpers
// ---------------------------------------------------------------------------

function breakerKeys(
  namespace: string,
  identity: BreakerIdentity,
): [hashKey: string, obsKey: string, probeKey: string] {
  const policy = `breaker:${identity.name}`
  const hash = coordinationKey(
    namespace,
    policy,
    identity.operation,
    identity.scope,
    "breaker",
  )
  const obs = coordinationKey(
    namespace,
    policy,
    identity.operation,
    identity.scope,
    "observations",
  )
  const prob = coordinationKey(
    namespace,
    policy,
    identity.operation,
    identity.scope,
    "probes",
  )
  return [hash, obs, prob]
}

// ---------------------------------------------------------------------------
// Reply validation helpers
// ---------------------------------------------------------------------------

function breakerConfigurationKey(
  namespace: string,
  identity: BreakerIdentity,
): string {
  return coordinationKey(
    namespace,
    `breaker:${identity.name}`,
    identity.operation,
    identity.scope,
    "configuration",
  )
}

interface Envelope {
  readonly revision: number
  readonly changedAt: number
  readonly values: number[]
  readonly body: string | undefined
}

function decodeEnvelope(
  reply: unknown,
  payloadLength: number,
  label: string,
): Envelope {
  const expected = 3 + payloadLength
  if (
    !Array.isArray(reply) ||
    (reply.length !== expected && reply.length !== expected + 1)
  )
    throw new Error(`Invalid ${label} reply from Redis`)
  const values = reply as unknown[]
  if (
    values[0] !== 1 ||
    !Number.isSafeInteger(values[1]) ||
    !Number.isSafeInteger(values[2]) ||
    values
      .slice(3, expected)
      .some((v) => typeof v !== "number" || !Number.isSafeInteger(v))
  )
    throw new Error(`Invalid ${label} reply from Redis`)
  const body = values[expected]
  if (body !== undefined && typeof body !== "string")
    throw new Error(`Invalid ${label} reply from Redis`)
  return {
    revision: values[1] as number,
    changedAt: values[2] as number,
    values: values.slice(3, expected) as number[],
    body: body === "" ? undefined : (body as string | undefined),
  }
}

function assertBody(body: string | undefined): void {
  if (body === undefined) return
  const snapshot = decodeConfigurationRecord(body)
  if (snapshot.settings.policy !== "circuitBreaker")
    throw new ConfigurationError(
      "corrupt-record",
      "configuration record policy does not match the identity",
    )
}

function toCoordinatorError(error: unknown): Error {
  if (error instanceof ConfigurationError) return error
  const message = error instanceof Error ? error.message : String(error)
  if (message.includes("unsupported configuration record"))
    return new ConfigurationError("unsupported-schema", message)
  return new CoordinatorUnavailableError(error)
}

const STATE_CODES: BreakerState[] = ["closed", "open", "half-open"]

function decodeState(code: number): BreakerState {
  const s = STATE_CODES[code]
  if (!s) throw new Error(`Unknown breaker state code: ${code}`)
  return s
}

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

/**
 * Policy-specific Redis coordinator for `circuitBreaker.distributed()`.
 *
 * The caller owns `client` and must connect/disconnect it independently.
 * `namespace` is prepended to every key; use a per-service, per-environment
 * value to avoid cross-deployment state collisions.
 */
export function redisCircuitBreakerCoordinator(
  client: RedisScriptClient,
  options: { readonly namespace: string },
): BreakerCoordinator {
  const { namespace } = options
  // Validate namespace eagerly (coordinationKey throws on bad input).
  coordinationKey(
    namespace,
    "breaker:validate",
    "validate",
    "validate",
    "breaker",
  )
  const revisions = createRevisionCache()

  const coordinator: BreakerCoordinator = {
    async readState(identity) {
      const [hashKey] = breakerKeys(namespace, identity)
      const configurationKey = breakerConfigurationKey(namespace, identity)
      try {
        const reply = await evalScript(
          client,
          breakerReadStateV1,
          2,
          hashKey,
          configurationKey,
          revisions.get(configurationKey),
        )
        const envelope = decodeEnvelope(reply, 4, "breakerReadStateV1")
        if (envelope.revision > 0)
          revisions.set(configurationKey, envelope.revision)
        assertBody(envelope.body)
        const [present, stateCode, generation, openedAt] = envelope.values
        if (present === 0) return null
        return {
          state: decodeState(stateCode),
          generation,
          openedAt,
          revision: envelope.revision,
          changedAt: envelope.changedAt,
        }
      } catch (error) {
        throw toCoordinatorError(error)
      }
    },

    async observe(identity, params) {
      const [hashKey, obsKey] = breakerKeys(namespace, identity)
      const configurationKey = breakerConfigurationKey(namespace, identity)
      const {
        generation,
        outcome,
        uuid,
        windowTtlMs,
        minimumThroughput,
        failureThresholdNumerator,
        windowSize,
        openMs,
        closedCleanupMs,
      } = params
      try {
        const reply = await evalScript(
          client,
          breakerObserveV1,
          3,
          hashKey,
          obsKey,
          configurationKey,
          outcome,
          generation,
          windowTtlMs,
          minimumThroughput,
          failureThresholdNumerator,
          windowSize,
          closedCleanupMs ?? Math.max(openMs * 2, windowTtlMs),
          uuid,
          revisions.get(configurationKey),
        )
        const envelope = decodeEnvelope(reply, 6, "breakerObserveV1")
        if (envelope.revision > 0)
          revisions.set(configurationKey, envelope.revision)
        assertBody(envelope.body)
        const [status, , newGen, windowTotal, windowFailures, openedAt] =
          envelope.values
        if (status === 0) {
          return {
            type: "stale",
            currentGeneration: newGen,
          } satisfies ObserveResult
        }
        if (status === 2) {
          return {
            type: "opened",
            newGeneration: newGen,
            windowTotal,
            windowFailures,
            openedAt,
            revision: envelope.revision,
          } satisfies ObserveResult
        }
        return {
          type: "observed",
          generation: newGen,
          windowTotal,
          windowFailures,
        } satisfies ObserveResult
      } catch (error) {
        throw toCoordinatorError(error)
      }
    },

    async admitProbe(identity, params) {
      const [hashKey, , probeKey] = breakerKeys(namespace, identity)
      const configurationKey = breakerConfigurationKey(namespace, identity)
      const { probeToken, openMs, halfOpenProbes, probeLeaseTtlMs } = params
      try {
        const reply = await evalScript(
          client,
          breakerAdmitProbeV1,
          3,
          hashKey,
          probeKey,
          configurationKey,
          probeToken,
          openMs,
          halfOpenProbes,
          probeLeaseTtlMs,
          revisions.get(configurationKey),
        )
        const envelope = decodeEnvelope(reply, 5, "breakerAdmitProbeV1")
        if (envelope.revision > 0)
          revisions.set(configurationKey, envelope.revision)
        assertBody(envelope.body)
        const [status, stateCode, gen, probeCount, transitioned] =
          envelope.values
        if (status === 0) {
          const reason =
            stateCode === 0
              ? "closed"
              : stateCode === 1
                ? "open"
                : "probe-limit"
          return {
            type: "rejected",
            reason,
            generation: gen,
          } satisfies AdmitProbeResult
        }
        return {
          type: "admitted",
          generation: gen,
          probeCount,
          stateChanged: transitioned === 1,
        } satisfies AdmitProbeResult
      } catch (error) {
        throw toCoordinatorError(error)
      }
    },

    async settleProbe(identity, params) {
      const [hashKey, , probeKey] = breakerKeys(namespace, identity)
      const configurationKey = breakerConfigurationKey(namespace, identity)
      const {
        probeToken,
        outcome,
        generation,
        halfOpenSuccesses,
        openMs,
        closedCleanupMs,
        windowTtlMs,
      } = params
      const cleanup =
        closedCleanupMs ?? Math.max(openMs * 2, windowTtlMs ?? openMs * 2)
      try {
        const reply = await evalScript(
          client,
          breakerSettleProbeV1,
          3,
          hashKey,
          probeKey,
          configurationKey,
          probeToken,
          outcome,
          generation,
          halfOpenSuccesses,
          cleanup,
          windowTtlMs ?? cleanup,
          revisions.get(configurationKey),
        )
        const envelope = decodeEnvelope(reply, 4, "breakerSettleProbeV1")
        if (envelope.revision > 0)
          revisions.set(configurationKey, envelope.revision)
        assertBody(envelope.body)
        const [status, stateCode, newGen, openedAt] = envelope.values
        if (status === 0) {
          return {
            type: "stale",
            generation: newGen,
          } satisfies SettleProbeResult
        }
        const state = decodeState(stateCode)
        if (status === 2) {
          return {
            type: "transitioned",
            newState: state,
            newGeneration: newGen,
            previousState: "half-open",
            // A transition to CLOSED has no cooldown, so it carries no open time.
            ...(state === "open" ? { openedAt } : {}),
            revision: envelope.revision,
          } satisfies SettleProbeResult
        }
        return {
          type: "settled",
          state,
          generation: newGen,
        } satisfies SettleProbeResult
      } catch (error) {
        throw toCoordinatorError(error)
      }
    },
  }

  return Object.freeze(coordinator)
}
