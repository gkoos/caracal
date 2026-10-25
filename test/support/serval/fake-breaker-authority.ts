import type {
  AdmitProbeResult,
  BreakerCoordinator,
  BreakerIdentity,
  BreakerState,
  ObserveResult,
  SettleProbeResult,
} from "../../../src/core/circuit-breaker.js"

/**
 * A scriptable stand-in for the strict Redis breaker coordinator.
 *
 * It implements the same observable protocol over a small state machine, counts
 * every call, and can be told to fail the next calls. That is what lets the Serval
 * tests measure how many round trips the cached verdict actually removes, and what
 * happens when the authority is unreachable.
 */

export interface FakeBreakerAuthorityOptions {
  /** The clock the authority reports its open times on. */
  readonly now?: () => number
  readonly state?: BreakerState
  readonly generation?: number
  readonly revision?: number
}

export interface FakeBreakerAuthority {
  readonly coordinator: BreakerCoordinator
  /** Calls by method, so a test can assert the round trips a path costs. */
  readonly calls: {
    readState: number
    observe: number
    admitProbe: number
    settleProbe: number
  }
  state(): BreakerState
  generation(): number
  /** Forces the verdict the authority reports, as an out-of-band transition does. */
  setState(
    state: BreakerState,
    options?: { openedAt?: number; revision?: number },
  ): void
  /** Fails the next `count` calls of every kind. */
  failNext(count: number, error?: unknown): void
  /** The outcome the next settled probe records. */
  setProbeOutcome(outcome: "success" | "failure" | "ignored"): void
  /** The authority's clock reading when the breaker last opened. */
  openedAt(): number
  /** Holds the next probe admission until `releaseProbe`, so an in-flight probe is observable. */
  holdNextProbe(): void
  releaseProbe(): void
  reset(): void
}

export function createFakeBreakerAuthority(
  options: FakeBreakerAuthorityOptions = {},
): FakeBreakerAuthority {
  const now = options.now ?? (() => Date.now())
  const calls = { readState: 0, observe: 0, admitProbe: 0, settleProbe: 0 }

  let state: BreakerState = options.state ?? "closed"
  let generation = options.generation ?? 0
  let revision = options.revision ?? 0
  let openedAt = state === "open" ? now() : 0
  let probeOutcome: "success" | "failure" | "ignored" = "success"
  let failures = 0
  let failureError: unknown
  let probeGate: Promise<void> | undefined
  let releaseProbeGate: (() => void) | undefined

  function before(call: keyof typeof calls): void {
    calls[call] += 1
    if (failures > 0) {
      failures -= 1
      throw failureError ?? new Error("the fake authority is unavailable")
    }
  }

  const coordinator: BreakerCoordinator = {
    async readState() {
      before("readState")
      return { state, generation, openedAt, revision }
    },

    async observe(_identity: BreakerIdentity, params): Promise<ObserveResult> {
      before("observe")
      if (params.outcome === "failure" && state === "closed") {
        generation += 1
        state = "open"
        openedAt = now()
        return {
          type: "opened",
          newGeneration: generation,
          windowTotal: params.minimumThroughput,
          windowFailures: params.minimumThroughput,
          openedAt,
          revision,
        }
      }
      return {
        type: "observed",
        generation,
        windowTotal: 1,
        windowFailures: 0,
      }
    },

    async admitProbe(
      _identity: BreakerIdentity,
      params,
    ): Promise<AdmitProbeResult> {
      before("admitProbe")
      if (probeGate !== undefined) {
        const gate = probeGate
        await gate
      }
      if (state === "closed") {
        return { type: "rejected", reason: "closed", generation }
      }
      if (state === "open" && now() - openedAt < params.openMs) {
        return { type: "rejected", reason: "open", generation }
      }
      state = "half-open"
      return {
        type: "admitted",
        generation,
        probeCount: 1,
        stateChanged: true,
      }
    },

    async settleProbe(): Promise<SettleProbeResult> {
      before("settleProbe")
      if (state !== "half-open") {
        return { type: "stale", generation }
      }
      if (probeOutcome === "ignored") {
        return { type: "settled", state, generation }
      }
      generation += 1
      if (probeOutcome === "failure") {
        state = "open"
        openedAt = now()
        return {
          type: "transitioned",
          newState: "open",
          newGeneration: generation,
          previousState: "half-open",
          openedAt,
          revision,
        }
      }
      state = "closed"
      return {
        type: "transitioned",
        newState: "closed",
        newGeneration: generation,
        previousState: "half-open",
        revision,
      }
    },
  }

  return {
    coordinator,
    calls,

    state() {
      return state
    },

    generation() {
      return generation
    },

    setState(next, override = {}) {
      state = next
      generation += 1
      openedAt = next === "open" ? (override.openedAt ?? now()) : 0
      if (override.revision !== undefined) revision = override.revision
    },

    failNext(count, error) {
      failures = count
      failureError = error
    },

    setProbeOutcome(outcome) {
      probeOutcome = outcome
    },

    openedAt() {
      return openedAt
    },

    holdNextProbe() {
      if (probeGate !== undefined) return
      probeGate = new Promise<void>((resolve) => {
        releaseProbeGate = resolve
      })
    },

    releaseProbe() {
      releaseProbeGate?.()
      releaseProbeGate = undefined
      probeGate = undefined
    },

    reset() {
      calls.readState = 0
      calls.observe = 0
      calls.admitProbe = 0
      calls.settleProbe = 0
    },
  }
}
