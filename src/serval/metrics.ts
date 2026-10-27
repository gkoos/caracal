/**
 * Per-process Serval diagnostics.
 *
 * Counters are fixed so a snapshot is cheap and comparable across replicas, and
 * the per-scope view is bounded by the scope registry rather than by traffic:
 * only registered scopes are tracked, and unregistering forgets them. A scope
 * observation for an untracked key is dropped and counted, never stored - an
 * unbounded diagnostics map would reintroduce exactly the growth the registry
 * bound exists to prevent.
 */

export const SERVAL_COUNTERS = [
  "scopesRegistered",
  "scopesUnregistered",
  "registryRefusals",
  "invalidationsReceived",
  "invalidationsApplied",
  "invalidationsIgnored",
  "invalidationsMalformed",
  "refreshes",
  "reconciliations",
  "reconciliationFailures",
  "reconnects",
  "authorityApplied",
  "authorityRejected",
  "invalidationsPublished",
  "publishFailures",
  "incarnationRetirements",
  "rollbacksSuspected",
  "diagnosticsDropped",
] as const

export type ServalCounterName = (typeof SERVAL_COUNTERS)[number]

export type ServalScopeOutcome = "applied" | "ignored" | "rejected"

export interface ServalScopeDiagnostics {
  readonly key: string
  readonly applied: number
  readonly ignored: number
  readonly rejected: number
  readonly lastAppliedAt: number | undefined
}

export interface ServalMetricsSnapshot {
  readonly counters: Readonly<Record<ServalCounterName, number>>
  readonly registrySize: number
  readonly scopes: readonly ServalScopeDiagnostics[]
}

export interface ServalMetrics {
  count(name: ServalCounterName, by?: number): void
  /** Starts tracking a scope. Called when a scope is registered. */
  trackScope(key: string): void
  /** Stops tracking a scope and drops its entry. */
  forgetScope(key: string): void
  observeScope(key: string, outcome: ServalScopeOutcome, at?: number): void
  setRegistrySize(size: number): void
  snapshot(): ServalMetricsSnapshot
}

export function createServalMetrics(): ServalMetrics {
  const counters = Object.fromEntries(
    SERVAL_COUNTERS.map((name) => [name, 0]),
  ) as Record<ServalCounterName, number>

  const scopes = new Map<
    string,
    {
      key: string
      applied: number
      ignored: number
      rejected: number
      lastAppliedAt: number | undefined
    }
  >()
  let registrySize = 0

  return {
    count(name, by = 1) {
      counters[name] += by
    },

    trackScope(key) {
      if (!scopes.has(key)) {
        scopes.set(key, {
          key,
          applied: 0,
          ignored: 0,
          rejected: 0,
          lastAppliedAt: undefined,
        })
      }
    },

    forgetScope(key) {
      scopes.delete(key)
    },

    observeScope(key, outcome, at) {
      const scope = scopes.get(key)
      if (scope === undefined) {
        counters.diagnosticsDropped += 1
        return
      }
      scope[outcome] += 1
      if (outcome === "applied") {
        scope.lastAppliedAt = at
      }
    },

    setRegistrySize(size) {
      registrySize = size
    },

    snapshot() {
      return {
        counters: { ...counters },
        registrySize,
        scopes: [...scopes.values()].map((scope) => ({ ...scope })),
      }
    },
  }
}
