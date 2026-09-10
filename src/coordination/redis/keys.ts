import { createHash } from "node:crypto"

/**
 * Build a namespaced Redis key for a policy-scoped coordination slot.
 *
 * The SHA-256 hash of `[namespace, policy, operation, scope]` forms the
 * stable identity; `suffix` distinguishes multiple keys that share the
 * same identity (e.g. `:leases`, `:breaker`, `:observations`, `:probes`).
 * Default suffix is `"leases"` for backward compatibility with the bulkhead.
 */
export function coordinationKey(
  namespace: string,
  policy: string,
  operation: string,
  scope: string,
  suffix = "leases",
): string {
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
  return `caracal:v1:{${identity}}:${suffix}`
}
