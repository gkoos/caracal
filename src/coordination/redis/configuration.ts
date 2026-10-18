import {
  CONFIGURATION_SCHEMA_VERSION,
  ConfigurationError,
  configurationFingerprint,
  decodeConfigurationRecord,
  encodeConfigurationRecord,
  resolveConfiguration,
} from "../../core/configuration.js"
import type {
  ConfigurationIdentity,
  ConfigurationSettings,
  ConfigurationSnapshot,
  ConfigurationUpdateResult,
} from "../../core/configuration.js"
import { CoordinatorUnavailableError } from "./client.js"
import { evalScript, type ScriptClient } from "./eval-script.js"
import { coordinationKey } from "./keys.js"
import { configurationReadV1, configurationUpdateV1 } from "./scripts.js"

const POLICY_PREFIX: Record<ConfigurationIdentity["policy"], string> = {
  bulkhead: "bulkhead",
  rateLimit: "ratelimit",
  circuitBreaker: "breaker",
}

/**
 * Manages the shared configuration record for one namespace.
 *
 * The record lives beside the runtime keys of the same identity and shares
 * their hash tag, so a record and the state it governs are always in the same
 * cluster slot. The caller owns the client.
 */
export interface ConfigurationManager {
  read(identity: ConfigurationIdentity): Promise<ConfigurationSnapshot | null>
  update(
    identity: ConfigurationIdentity,
    expectedRevision: number,
    settings: ConfigurationSettings,
  ): Promise<ConfigurationUpdateResult>
}

function recordKey(namespace: string, identity: ConfigurationIdentity): string {
  const prefix = POLICY_PREFIX[identity.policy]
  if (prefix === undefined)
    throw new TypeError(
      `unknown configuration policy ${String(identity.policy)}`,
    )
  return coordinationKey(
    namespace,
    `${prefix}:${identity.name}`,
    identity.operation,
    identity.scope,
    "configuration",
  )
}

interface DecodedReply {
  readonly status: 0 | 1 | 2
  readonly revision: number
  readonly changedAt: number
  readonly body: string
}

function decodeReply(reply: unknown): DecodedReply {
  if (!Array.isArray(reply) || reply.length !== 4)
    throw new ConfigurationError(
      "corrupt-record",
      "configuration update returned an unexpected reply",
    )
  const [status, revision, changedAt, body] = reply as unknown[]
  if (
    (status !== 0 && status !== 1 && status !== 2) ||
    typeof revision !== "number" ||
    !Number.isSafeInteger(revision) ||
    typeof changedAt !== "number" ||
    !Number.isSafeInteger(changedAt) ||
    typeof body !== "string"
  )
    throw new ConfigurationError(
      "corrupt-record",
      "configuration update returned an unexpected reply",
    )
  return { status, revision, changedAt, body }
}

function decode(
  identity: ConfigurationIdentity,
  serialized: string,
): ConfigurationSnapshot {
  const snapshot = decodeConfigurationRecord(serialized)
  if (snapshot.settings.policy !== identity.policy)
    throw new ConfigurationError(
      "corrupt-record",
      "configuration record policy does not match the identity",
    )
  return snapshot
}

/** Creates the configuration manager for one namespace. */
export function redisConfigurationManager(
  client: ScriptClient,
  options: { namespace: string },
): ConfigurationManager {
  const { namespace } = options
  const manager: ConfigurationManager = {
    async read(identity) {
      const key = recordKey(namespace, identity)
      try {
        const reply = await evalScript(client, configurationReadV1, 1, key)
        if (reply === null || reply === undefined || reply === false)
          return null
        if (typeof reply !== "string")
          throw new ConfigurationError(
            "corrupt-record",
            "configuration read returned an unexpected reply",
          )
        return decode(identity, reply)
      } catch (error) {
        if (error instanceof ConfigurationError) throw error
        throw new CoordinatorUnavailableError(error)
      }
    },
    async update(identity, expectedRevision, settings) {
      if (!Number.isSafeInteger(expectedRevision) || expectedRevision < 0)
        throw new RangeError("expectedRevision must be a non-negative integer")
      if (settings.policy !== identity.policy)
        throw new TypeError("settings policy must match the identity policy")
      const effective = resolveConfiguration(settings)
      const template = encodeConfigurationRecord({
        schemaVersion: CONFIGURATION_SCHEMA_VERSION,
        revision: 0,
        changedAt: 0,
        settings,
        effective,
      })
      const fingerprint = configurationFingerprint(effective)
      const key = recordKey(namespace, identity)
      try {
        const reply = await evalScript(
          client,
          configurationUpdateV1,
          1,
          key,
          expectedRevision,
          template,
          fingerprint,
        )
        const decoded = decodeReply(reply)
        if (decoded.body === "") {
          if (decoded.status === 1)
            return { status: "conflict", snapshot: null }
          throw new ConfigurationError(
            "corrupt-record",
            "configuration update returned no record",
          )
        }
        const stored = decode(identity, decoded.body)
        if (stored.revision !== decoded.revision)
          throw new ConfigurationError(
            "corrupt-record",
            "configuration revision does not match the reply",
          )
        if (decoded.status === 2) return { status: "applied", snapshot: stored }
        if (decoded.status === 0)
          return { status: "unchanged", snapshot: stored }
        return { status: "conflict", snapshot: stored }
      } catch (error) {
        if (error instanceof ConfigurationError) throw error
        throw new CoordinatorUnavailableError(error)
      }
    },
  }
  return Object.freeze(manager)
}
