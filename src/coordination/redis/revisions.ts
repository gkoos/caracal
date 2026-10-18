/**
 * Last configuration revision each identity has reported, per coordinator.
 *
 * Every coordination reply carries the revision it was decided under. Sending
 * the known revision back lets the server omit the record body while the caller
 * is current, so the steady state costs one integer instead of the snapshot.
 *
 * Bounded, because a process can touch many scopes: the oldest entry is
 * evicted first and the next command for that identity simply carries a body
 * again. This is not `createScopeStateCache`, whose retention rules exist for a
 * different decision and whose size is traffic-dependent by design.
 */

export const REVISION_CACHE_LIMIT = 1_024

export interface RevisionCache {
  /** The last revision seen for a key, or 0 when none has been seen. */
  get(key: string): number
  /** Records a revision. Non-positive revisions are ignored. */
  set(key: string, revision: number): void
  /** Retained key count. Internal; used by tests. */
  size(): number
}

export function createRevisionCache(
  limit = REVISION_CACHE_LIMIT,
): RevisionCache {
  const known = new Map<string, number>()
  return {
    get(key) {
      return known.get(key) ?? 0
    },
    set(key, revision) {
      if (!Number.isSafeInteger(revision) || revision <= 0) return
      // Re-insert so the most recently used entry is last, and evict the first.
      known.delete(key)
      known.set(key, revision)
      if (known.size > limit) {
        const oldest = known.keys().next().value
        if (oldest !== undefined) known.delete(oldest)
      }
    },
    size() {
      return known.size
    },
  }
}
