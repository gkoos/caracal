import { createHash } from "node:crypto"

/**
 * Build a namespaced Redis key for a policy-scoped coordination slot.
 *
 * The SHA-256 hash of `[namespace, policy, operation, scope]` forms the
 * stable identity; `suffix` distinguishes multiple keys that share the
 * same identity (e.g. `:leases`, `:breaker`, `:observations`, `:probes`).
 * Default suffix is `"leases"` for backward compatibility with the bulkhead.
 */
/**
 * Keys are memoized per identity because deriving one is not free: a
 * `JSON.stringify`, a SHA-256 digest and five byte-length checks. The breaker
 * recomputes three keys per coordinator call and uses one or two of them, so the
 * same identity is hashed repeatedly within a single command.
 *
 * The cache is bounded, for the same reason the docs bound scope cardinality:
 * an unbounded map keyed by every scope ever seen would reintroduce the growth
 * the hashing exists to survive.
 */
export const COORDINATION_KEY_CACHE_LIMIT = 1_024
const keyCache = new Map<string, string>()

/** Retained key count. Internal; used by tests. */
export function coordinationKeyCacheSize(): number {
  return keyCache.size
}

export function coordinationKey(
  namespace: string,
  policy: string,
  operation: string,
  scope: string,
  suffix = "leases",
): string {
  const cacheKey = JSON.stringify([namespace, policy, operation, scope, suffix])
  const cached = keyCache.get(cacheKey)
  if (cached !== undefined) return cached

  for (const value of [namespace, policy, operation, scope, suffix]) {
    if (
      typeof value !== "string" ||
      !value.trim() ||
      Buffer.byteLength(value) > 1024
    )
      throw new TypeError(
        "Coordination identities must be nonempty strings of at most 1024 UTF-8 bytes",
      )
  }
  const identity = createHash("sha256")
    .update(JSON.stringify([namespace, policy, operation, scope]))
    .digest("hex")
  const key = `caracal:v1:{${identity}}:${suffix}`

  if (keyCache.size >= COORDINATION_KEY_CACHE_LIMIT) {
    // Map iterates in insertion order, so the first key is the oldest.
    const oldest = keyCache.keys().next().value
    if (oldest !== undefined) keyCache.delete(oldest)
  }
  keyCache.set(cacheKey, key)
  return key
}
