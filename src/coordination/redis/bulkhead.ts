import type { BulkheadCoordinator } from "../../core/bulkhead.js"
import { CoordinatorUnavailableError } from "./client.js"
import { evalScript } from "./eval-script.js"
import { coordinationKey } from "./keys.js"
import type { RedisScriptClient } from "./leases.js"
import { bulkheadLeaseV1 } from "./scripts.js"
export function redisCoordinator(
  client: RedisScriptClient,
  options: { namespace: string },
): BulkheadCoordinator {
  const namespace = options.namespace
  coordinationKey(namespace, "bulkhead", "validate", "validate")
  const coordinator: BulkheadCoordinator = {
    async command(identity, action, token, leaseMs, limit) {
      if (
        !["acquire", "renew", "release"].includes(action) ||
        typeof token !== "string" ||
        !token ||
        token.length > 256
      )
        throw new TypeError("Invalid bulkhead lease action or token")
      if (
        ![leaseMs, limit].every(
          (value) => Number.isSafeInteger(value) && value > 0,
        ) ||
        leaseMs > 86400000
      )
        throw new RangeError("Invalid bulkhead lease duration or limit")
      const key = coordinationKey(
        namespace,
        `bulkhead:${identity.name}`,
        identity.operation,
        identity.scope,
      )
      try {
        const result = await evalScript(
          client,
          bulkheadLeaseV1,
          1,
          key,
          action,
          token,
          leaseMs,
          limit,
        )
        if (
          !Array.isArray(result) ||
          result.length !== 2 ||
          ![0, 1].includes(result[0]) ||
          !Number.isSafeInteger(result[1]) ||
          result[1] < 0
        )
          throw new Error("Invalid bulkhead reply")
        return { allowed: result[0] === 1, occupancy: result[1] as number }
      } catch (error) {
        throw new CoordinatorUnavailableError(error)
      }
    },
  }
  return Object.freeze(coordinator)
}
