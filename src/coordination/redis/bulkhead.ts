import type { BulkheadCoordinator } from "../../core/bulkhead.js"
import {
  ConfigurationError,
  decodeConfigurationRecord,
} from "../../core/configuration.js"
import { CoordinatorUnavailableError } from "./client.js"
import { evalScript } from "./eval-script.js"
import { coordinationKey } from "./keys.js"
import type { RedisScriptClient } from "./leases.js"
import { createRevisionCache } from "./revisions.js"
import { bulkheadLeaseV1 } from "./scripts.js"

const ENVELOPE_VERSION = 1
const UNSUPPORTED_RECORD = "unsupported configuration record"

/** A record the server cannot use is a configuration error, not an outage. */
function configurationError(error: unknown): ConfigurationError | undefined {
  if (error instanceof ConfigurationError) return error
  const message = error instanceof Error ? error.message : String(error)
  if (message.includes(UNSUPPORTED_RECORD))
    return new ConfigurationError("unsupported-schema", message)
  return undefined
}

interface LeaseReply {
  readonly allowed: boolean
  readonly occupancy: number
  readonly revision: number
  readonly body: string | undefined
}

function decodeReply(result: unknown): LeaseReply {
  if (!Array.isArray(result) || (result.length !== 5 && result.length !== 6))
    throw new Error("Invalid bulkhead reply")
  const [envelope, allowed, occupancy, revision, changedAt, body] = result
  if (
    envelope !== ENVELOPE_VERSION ||
    (allowed !== 0 && allowed !== 1) ||
    !Number.isSafeInteger(occupancy) ||
    (occupancy as number) < 0 ||
    !Number.isSafeInteger(revision) ||
    (revision as number) < 0 ||
    !Number.isSafeInteger(changedAt) ||
    (body !== undefined && typeof body !== "string")
  )
    throw new Error("Invalid bulkhead reply")
  return {
    allowed: allowed === 1,
    occupancy: occupancy as number,
    revision: revision as number,
    // An empty body is the no-record case; older replies may still send it.
    body: body === "" ? undefined : (body as string | undefined),
  }
}

export function redisCoordinator(
  client: RedisScriptClient,
  options: { namespace: string },
): BulkheadCoordinator {
  const namespace = options.namespace
  coordinationKey(namespace, "bulkhead", "validate", "validate")
  const revisions = createRevisionCache()
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
      const policy = `bulkhead:${identity.name}`
      const key = coordinationKey(
        namespace,
        policy,
        identity.operation,
        identity.scope,
      )
      const configurationKey = coordinationKey(
        namespace,
        policy,
        identity.operation,
        identity.scope,
        "configuration",
      )
      try {
        const result = await evalScript(
          client,
          bulkheadLeaseV1,
          2,
          key,
          configurationKey,
          action,
          token,
          leaseMs,
          limit,
          revisions.get(configurationKey),
        )
        const reply = decodeReply(result)
        if (reply.revision > 0) revisions.set(configurationKey, reply.revision)
        if (reply.body !== undefined) {
          const snapshot = decodeConfigurationRecord(reply.body)
          if (snapshot.settings.policy !== "bulkhead")
            throw new ConfigurationError(
              "corrupt-record",
              "configuration record policy does not match the identity",
            )
        }
        return { allowed: reply.allowed, occupancy: reply.occupancy }
      } catch (error) {
        const configuration = configurationError(error)
        if (configuration !== undefined) throw configuration
        throw new CoordinatorUnavailableError(error)
      }
    },
  }
  return Object.freeze(coordinator)
}
