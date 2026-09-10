/**
 * In-process record of the scopes whose last known distributed state was
 * non-closed.
 *
 * It exists for one decision only: when `readState()` fails, a scope last seen
 * OPEN or HALF_OPEN must fail closed, because admitting work to a known-open
 * breaker removes the protection it exists to provide.
 *
 * CLOSED states are deliberately not retained. A missing entry and a CLOSED
 * entry are treated identically by that decision, so storing CLOSED would grow
 * this map with every scope ever observed while changing nothing.
 *
 * Entries are keyed by `(operation, scope)` to match the coordinator identity
 * `(name, operation, scope)`: one policy instance shared by several operations
 * must not read another operation's last known state.
 */

/** Non-closed breaker state retained in process memory. */
export type RetainedScopeState = "open" | "half-open"

export interface ScopeStateCache {
  remember(operation: string, scope: string, state: RetainedScopeState): void
  forget(operation: string, scope: string): void
  read(operation: string, scope: string): RetainedScopeState | undefined
  /** Number of retained scopes. Internal; used by tests. */
  size(): number
}

export function createScopeStateCache(): ScopeStateCache {
  const retained = new Map<string, RetainedScopeState>()
  // JSON encoding keeps the parts unambiguous even if a name contains the
  // separator or other special characters.
  const keyFor = (operation: string, scope: string) =>
    JSON.stringify([operation, scope])

  return {
    remember(operation, scope, state) {
      retained.set(keyFor(operation, scope), state)
    },
    forget(operation, scope) {
      retained.delete(keyFor(operation, scope))
    },
    read(operation, scope) {
      return retained.get(keyFor(operation, scope))
    },
    size() {
      return retained.size
    },
  }
}
