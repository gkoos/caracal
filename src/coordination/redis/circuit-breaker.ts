import type {
  AdmitProbeResult,
  BreakerCoordinator,
  BreakerIdentity,
  BreakerState,
  ObserveResult,
  SettleProbeResult,
} from "../../core/circuit-breaker.js"
import { CoordinatorUnavailableError } from "./client.js"
import { evalScript } from "./eval-script.js"
import { coordinationKey } from "./keys.js"
import type { RedisScriptClient } from "./leases.js"
import {
  breakerAdmitProbeV1,
  breakerObserveV1,
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

function assertArray(reply: unknown, minLen: number, label: string): unknown[] {
  if (
    !Array.isArray(reply) ||
    reply.length < minLen ||
    reply.some((v) => typeof v !== "number" || !Number.isSafeInteger(v))
  ) {
    throw new Error(
      `Invalid ${label} reply from Redis: ${JSON.stringify(reply)}`,
    )
  }
  return reply as unknown[]
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

  const coordinator: BreakerCoordinator = {
    async readState(identity) {
      const [hashKey] = breakerKeys(namespace, identity)
      try {
        const fields = await client.hmget(hashKey, "state", "generation")
        const rawState = fields[0]
        if (!rawState) return null
        const state = rawState as BreakerState
        if (!STATE_CODES.includes(state))
          throw new Error(`Unknown breaker state: ${rawState}`)
        const generation = parseInt(fields[1] ?? "0", 10)
        return { state, generation }
      } catch (error) {
        throw new CoordinatorUnavailableError(error)
      }
    },

    async observe(identity, params) {
      const [hashKey, obsKey] = breakerKeys(namespace, identity)
      const {
        generation,
        outcome,
        uuid,
        windowTtlMs,
        minimumThroughput,
        failureThresholdNumerator,
        windowSize,
        openMs,
      } = params
      try {
        const reply = await evalScript(
          client,
          breakerObserveV1,
          2,
          hashKey,
          obsKey,
          outcome,
          generation,
          windowTtlMs,
          minimumThroughput,
          failureThresholdNumerator,
          windowSize,
          openMs,
          uuid,
        )
        const r = assertArray(reply, 5, "breakerObserveV1") as number[]
        const [status, , newGen, windowTotal, windowFailures] = r
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
          } satisfies ObserveResult
        }
        return {
          type: "observed",
          generation: newGen,
          windowTotal,
          windowFailures,
        } satisfies ObserveResult
      } catch (error) {
        throw new CoordinatorUnavailableError(error)
      }
    },

    async admitProbe(identity, params) {
      const [hashKey, , probeKey] = breakerKeys(namespace, identity)
      const { probeToken, openMs, halfOpenProbes, probeLeaseTtlMs } = params
      try {
        const reply = await evalScript(
          client,
          breakerAdmitProbeV1,
          2,
          hashKey,
          probeKey,
          probeToken,
          openMs,
          halfOpenProbes,
          probeLeaseTtlMs,
        )
        const r = assertArray(reply, 5, "breakerAdmitProbeV1") as number[]
        const [status, stateCode, gen, probeCount, transitioned] = r
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
        throw new CoordinatorUnavailableError(error)
      }
    },

    async settleProbe(identity, params) {
      const [hashKey, , probeKey] = breakerKeys(namespace, identity)
      const {
        probeToken,
        outcome,
        generation,
        halfOpenSuccesses,
        openMs,
        windowTtlMs,
      } = params
      try {
        const reply = await evalScript(
          client,
          breakerSettleProbeV1,
          2,
          hashKey,
          probeKey,
          probeToken,
          outcome,
          generation,
          halfOpenSuccesses,
          openMs,
          // Floor for the CLOSED cleanup TTL; older callers that do not pass it
          // keep the historical openMs x 2 behaviour.
          windowTtlMs ?? openMs * 2,
        )
        const r = assertArray(reply, 3, "breakerSettleProbeV1") as number[]
        const [status, stateCode, newGen] = r
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
          } satisfies SettleProbeResult
        }
        return {
          type: "settled",
          state,
          generation: newGen,
        } satisfies SettleProbeResult
      } catch (error) {
        throw new CoordinatorUnavailableError(error)
      }
    },
  }

  return Object.freeze(coordinator)
}
