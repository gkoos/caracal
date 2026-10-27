/**
 * In-memory BreakerCoordinator for unit tests.
 *
 * Implements the same semantics as the Redis Lua scripts so that unit tests
 * exercise the distributed policy without a real coordinator.  This module
 * must never be imported from production runtime code.
 */
import type {
  AdmitProbeResult,
  BreakerCoordinator,
  BreakerIdentity,
  BreakerState,
  ObserveResult,
  SettleProbeResult,
} from "../../../src/core/circuit-breaker.js"

type Observation = {
  generation: number
  outcome: "success" | "failure"
  timestamp: number
}

type ScopeRecord = {
  state: BreakerState
  generation: number
  openedAt: number
  probeSuccesses: number
  observations: Observation[]
  /** token → expiresAt (ms) */
  probeTokens: Map<string, number>
  /** Probes admitted in the current recovery window. */
  windowProbes: number
  /** When the current recovery window's budget started. */
  windowStartedAt: number
}

function scopeKey(identity: BreakerIdentity): string {
  return JSON.stringify([identity.name, identity.operation, identity.scope])
}

/**
 * Creates a fresh in-memory BreakerCoordinator.  Each call returns an
 * independent instance with no shared state.  Suitable for unit tests only.
 */
export function memoryBreakerCoordinator(): BreakerCoordinator & {
  /** Direct access to per-scope records for test assertions. */
  inspect(identity: BreakerIdentity): ScopeRecord | undefined
  /** Remove all stored state (useful between sub-tests). */
  reset(): void
} {
  const scopes = new Map<string, ScopeRecord>()

  function getOrCreate(identity: BreakerIdentity): ScopeRecord {
    const key = scopeKey(identity)
    let rec = scopes.get(key)
    if (!rec) {
      rec = {
        state: "closed",
        generation: 0,
        openedAt: 0,
        probeSuccesses: 0,
        observations: [],
        probeTokens: new Map(),
        windowProbes: 0,
        windowStartedAt: 0,
      }
      scopes.set(key, rec)
    }
    return rec
  }

  function pruneProbes(rec: ScopeRecord, now: number): void {
    for (const [token, expiresAt] of rec.probeTokens) {
      if (expiresAt <= now) rec.probeTokens.delete(token)
    }
  }

  return {
    inspect(identity) {
      return scopes.get(scopeKey(identity))
    },

    reset() {
      scopes.clear()
    },

    async readState(identity) {
      const rec = scopes.get(scopeKey(identity))
      if (!rec) return null
      return { state: rec.state, generation: rec.generation }
    },

    async observe(identity, params) {
      const {
        generation: expectGen,
        outcome,
        windowTtlMs,
        minimumThroughput,
        failureThresholdNumerator,
        windowSize,
      } = params
      const now = Date.now()
      const rec = getOrCreate(identity)

      if (rec.state !== "closed" || rec.generation !== expectGen) {
        return {
          type: "stale",
          currentGeneration: rec.generation,
        } satisfies ObserveResult
      }

      // Prune observations older than windowTtlMs
      rec.observations = rec.observations.filter(
        (o) => now - o.timestamp < windowTtlMs,
      )

      // Add new observation
      rec.observations.push({ generation: expectGen, outcome, timestamp: now })

      // Evict oldest if over windowSize
      if (rec.observations.length > windowSize) {
        rec.observations.splice(0, rec.observations.length - windowSize)
      }

      // Count current-generation observations
      const genObs = rec.observations.filter((o) => o.generation === expectGen)
      const wTotal = genObs.length
      const wFail = genObs.filter((o) => o.outcome === "failure").length

      // Evaluate threshold
      if (
        wTotal >= minimumThroughput &&
        wFail * 1000 >= failureThresholdNumerator * wTotal
      ) {
        rec.generation++
        rec.state = "open"
        rec.openedAt = now
        rec.probeSuccesses = 0
        rec.probeTokens.clear()
        return {
          type: "opened",
          newGeneration: rec.generation,
          windowTotal: wTotal,
          windowFailures: wFail,
          openedAt: now,
        } satisfies ObserveResult
      }

      return {
        type: "observed",
        generation: expectGen,
        windowTotal: wTotal,
        windowFailures: wFail,
      } satisfies ObserveResult
    },

    async admitProbe(identity, params) {
      const { probeToken, openMs, halfOpenProbes, probeLeaseTtlMs } = params
      const now = Date.now()
      const rec = getOrCreate(identity)

      if (rec.state === "closed") {
        return {
          type: "rejected",
          reason: "closed",
          generation: rec.generation,
        } satisfies AdmitProbeResult
      }

      let stateChanged = false

      if (rec.state === "open") {
        if (now - rec.openedAt < openMs) {
          return {
            type: "rejected",
            reason: "open",
            generation: rec.generation,
          } satisfies AdmitProbeResult
        }
        // Transition OPEN → HALF_OPEN, opening a fresh recovery window.
        rec.state = "half-open"
        rec.probeSuccesses = 0
        rec.probeTokens.clear()
        rec.windowProbes = 0
        rec.windowStartedAt = now
        stateChanged = true
      }

      // Prune expired probe tokens first. A token whose lease elapsed belongs to a
      // probe that will never report, so its slot and its window budget both come
      // back before the window is judged - a crashed worker cannot consume the
      // window for good.
      const before = rec.probeTokens.size
      pruneProbes(rec, now)
      rec.windowProbes = Math.max(
        0,
        rec.windowProbes - (before - rec.probeTokens.size),
      )

      // The recovery window is a rolling budget: halfOpenProbes probes per openMs.
      // It refills on time rather than on an outcome, because an inconclusive
      // result releases its in-flight slot but does not return its window budget.
      if (now - rec.windowStartedAt >= openMs) {
        rec.windowProbes = 0
        rec.windowStartedAt = now
      }

      // The window is the budget, not the in-flight count: halfOpenProbes probes
      // per recovery window, so an inconclusive result cannot be replaced by the
      // next request that arrives.
      if (
        rec.probeTokens.size >= halfOpenProbes ||
        rec.windowProbes >= halfOpenProbes
      ) {
        return {
          type: "rejected",
          reason: "probe-limit",
          generation: rec.generation,
        } satisfies AdmitProbeResult
      }

      rec.probeTokens.set(probeToken, now + probeLeaseTtlMs)
      rec.windowProbes += 1
      return {
        type: "admitted",
        generation: rec.generation,
        probeCount: rec.probeTokens.size,
        stateChanged,
      } satisfies AdmitProbeResult
    },

    async settleProbe(identity, params) {
      const {
        probeToken,
        outcome,
        generation: expectGen,
        halfOpenSuccesses,
      } = params
      const now = Date.now()
      const rec = getOrCreate(identity)

      const expiresAt = rec.probeTokens.get(probeToken)
      if (expiresAt === undefined) {
        return {
          type: "stale",
          generation: rec.generation,
        } satisfies SettleProbeResult
      }
      // A settle always consumes the token (mirrors ZREM in the Lua script),
      // even when the lease has already elapsed.
      rec.probeTokens.delete(probeToken)
      if (expiresAt <= now) {
        // The lease elapsed while the probe was still running: its slot is
        // recoverable and its window budget goes back with it.
        rec.windowProbes = Math.max(0, rec.windowProbes - 1)
        return {
          type: "stale",
          generation: rec.generation,
        } satisfies SettleProbeResult
      }

      if (rec.state !== "half-open" || rec.generation !== expectGen) {
        return {
          type: "stale",
          generation: rec.generation,
        } satisfies SettleProbeResult
      }

      if (outcome === "ignored") {
        // Release only: the result is not recorded, no success counter advances and
        // the state does not transition. The window's probe budget is deliberately
        // not returned - an inconclusive result is not evidence the dependency is
        // reachable (matches the Lua script).
        return {
          type: "settled",
          state: "half-open",
          generation: rec.generation,
        } satisfies SettleProbeResult
      }

      if (outcome === "failure") {
        rec.generation++
        rec.state = "open"
        rec.openedAt = now
        rec.probeSuccesses = 0
        rec.probeTokens.clear()
        rec.windowProbes = 0
        rec.windowStartedAt = 0
        return {
          type: "transitioned",
          newState: "open",
          newGeneration: rec.generation,
          previousState: "half-open",
          openedAt: now,
        } satisfies SettleProbeResult
      }

      rec.probeSuccesses++
      if (rec.probeSuccesses >= halfOpenSuccesses) {
        rec.generation++
        rec.state = "closed"
        rec.probeSuccesses = 0
        rec.probeTokens.clear()
        rec.windowProbes = 0
        rec.windowStartedAt = 0
        return {
          type: "transitioned",
          newState: "closed",
          newGeneration: rec.generation,
          previousState: "half-open",
        } satisfies SettleProbeResult
      }

      // A counting outcome returns its slot to the window: a success is evidence
      // the dependency is reachable, and recovery has to be able to accumulate
      // halfOpenSuccesses within the window.
      rec.windowProbes = Math.max(0, rec.windowProbes - 1)
      return {
        type: "settled",
        state: "half-open",
        generation: rec.generation,
      } satisfies SettleProbeResult
    },
  }
}
