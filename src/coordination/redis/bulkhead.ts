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
import { bulkheadAllocationV1, bulkheadLeaseV1 } from "./scripts.js"

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

const ALLOCATION_REFUSAL_REASONS = [
  "none",
  "limit",
  "revision",
  "expired",
  "exists",
] as const

export type BulkheadAllocationRefusalReason =
  | "limit"
  | "revision"
  | "expired"
  | "exists"

export type BulkheadAllocationAction = "allocate" | "renew" | "resize" | "read"

/**
 * One allocation modification. `operationId` identifies the modification,
 * including all of its retries, so an ambiguous reply is recoverable: the retry
 * returns the recorded result instead of applying the change twice.
 */
export interface BulkheadAllocationRequest {
  readonly identity: {
    readonly name: string
    readonly operation: string
    readonly scope: string
  }
  readonly replicaId: string
  readonly operationId: string
  readonly action: BulkheadAllocationAction
  /** Requested slots for `allocate`; a signed delta for `resize`. */
  readonly slots?: number
  readonly validityMs?: number
  /** Compare-and-swap against the allocation's current lease revision. */
  readonly expectedRevision?: number
  /** The policy's fleet-wide ceiling; a configuration record overrides it. */
  readonly limit: number
  /** How long the reservation outlives its validity, so a lost reply is reconcilable. */
  readonly graceMs?: number
}

export type BulkheadAllocationReply =
  | {
      readonly type: "committed"
      readonly slots: number
      readonly revision: number
      /** Server-clock expiry of the allocation's validity. */
      readonly expiresAt: number
      /** Server-clock reading, so the caller derives a duration, never a deadline. */
      readonly serverTime: number
      readonly duplicate: boolean
    }
  | {
      readonly type: "refused"
      readonly reason: BulkheadAllocationRefusalReason
      readonly slots: number
      readonly revision: number
      readonly expiresAt: number
      readonly serverTime: number
    }

/**
 * The allocation capability of a delegated bulkhead.
 *
 * Every modification carries the `leaseRevision` it expects, so a reply that lost a
 * race can never regress capacity, spendability, expiry or frozen state.
 */
export interface BulkheadAllocationIssuer {
  modify(request: BulkheadAllocationRequest): Promise<BulkheadAllocationReply>
}

interface AllocationReply {
  readonly status: number
  readonly reason: number
  readonly slots: number
  readonly revision: number
  readonly expiresAt: number
  readonly serverTime: number
  readonly duplicate: boolean
  readonly configRevision: number
  readonly body: string | undefined
}

function decodeAllocationReply(result: unknown): AllocationReply {
  if (!Array.isArray(result) || (result.length !== 10 && result.length !== 11))
    throw new Error("Invalid bulkhead allocation reply")
  const [
    envelope,
    status,
    reason,
    slots,
    revision,
    expiresAt,
    serverTime,
    duplicate,
    configRevision,
    changedAt,
    body,
  ] = result
  if (
    envelope !== ENVELOPE_VERSION ||
    (status !== 0 && status !== 1) ||
    !Number.isSafeInteger(reason) ||
    (reason as number) < 0 ||
    (reason as number) >= ALLOCATION_REFUSAL_REASONS.length ||
    !Number.isSafeInteger(slots) ||
    (slots as number) < 0 ||
    !Number.isSafeInteger(revision) ||
    (revision as number) < 0 ||
    !Number.isSafeInteger(expiresAt) ||
    (expiresAt as number) < 0 ||
    !Number.isSafeInteger(serverTime) ||
    (serverTime as number) < 0 ||
    (duplicate !== 0 && duplicate !== 1) ||
    !Number.isSafeInteger(configRevision) ||
    (configRevision as number) < 0 ||
    !Number.isSafeInteger(changedAt) ||
    (body !== undefined && typeof body !== "string")
  )
    throw new Error("Invalid bulkhead allocation reply")
  return {
    status: status as number,
    reason: reason as number,
    slots: slots as number,
    revision: revision as number,
    expiresAt: expiresAt as number,
    serverTime: serverTime as number,
    duplicate: duplicate === 1,
    configRevision: configRevision as number,
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

/**
 * Redis-backed allocation authority for a delegated bulkhead.
 *
 * The fleet-wide limit is a sum of allocations, so this is the only place that can
 * grant capacity: a replica asks for slots and spends them locally, and the server
 * accounts for the sum.
 */
export function redisBulkheadAllocationIssuer(
  client: RedisScriptClient,
  options: { namespace: string },
): BulkheadAllocationIssuer {
  const namespace = options.namespace
  const revisions = createRevisionCache()

  const issuer: BulkheadAllocationIssuer = {
    async modify(request) {
      const { identity } = request
      if (
        typeof request.replicaId !== "string" ||
        !request.replicaId.trim() ||
        // Reservation members are `a|replicaId|grantId|slots` and the pruning pass
        // splits them on `|`, so a component that carries the delimiter would make
        // its own reservation unreadable and strand the capacity it holds.
        request.replicaId.includes("|") ||
        Buffer.byteLength(request.replicaId) > 256
      )
        throw new TypeError("Invalid bulkhead replica id")
      if (
        typeof request.operationId !== "string" ||
        !request.operationId.trim() ||
        request.operationId.includes("|") ||
        Buffer.byteLength(request.operationId) > 1024
      )
        throw new TypeError("Invalid bulkhead allocation operation id")
      if (!Number.isSafeInteger(request.limit) || request.limit < 1)
        throw new RangeError("Invalid bulkhead limit")

      const slots = request.slots ?? 0
      if (
        request.action === "allocate" &&
        (!Number.isSafeInteger(slots) || slots < 1)
      )
        throw new RangeError("Invalid bulkhead allocation size")
      if (
        request.action === "resize" &&
        (!Number.isSafeInteger(slots) || slots === 0)
      )
        throw new RangeError("Invalid bulkhead resize delta")
      const validityMs = request.validityMs ?? 0
      if (
        (request.action === "allocate" || request.action === "renew") &&
        (!Number.isSafeInteger(validityMs) || validityMs < 1)
      )
        throw new RangeError("Invalid bulkhead allocation validity")
      const graceMs = request.graceMs ?? 0
      if (!Number.isSafeInteger(graceMs) || graceMs < 0)
        throw new RangeError("Invalid bulkhead reclamation grace")

      const policy = `bulkhead:${identity.name}`
      // A suffix of its own: the default `leases` key belongs to the strict
      // coordinator's token set, and an allocation hash must never land on it.
      const key = coordinationKey(
        namespace,
        policy,
        identity.operation,
        identity.scope,
        "allocation",
      )
      const expiryKey = coordinationKey(
        namespace,
        policy,
        identity.operation,
        identity.scope,
        "allocation-expiry",
      )
      const counterKey = coordinationKey(
        namespace,
        policy,
        identity.operation,
        identity.scope,
        "allocated",
      )
      const ledgerKey = coordinationKey(
        namespace,
        policy,
        identity.operation,
        identity.scope,
        "allocation-ops",
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
          bulkheadAllocationV1,
          5,
          key,
          expiryKey,
          counterKey,
          ledgerKey,
          configurationKey,
          request.action,
          request.replicaId,
          request.operationId,
          slots,
          validityMs,
          request.expectedRevision ?? -1,
          request.limit,
          graceMs,
          revisions.get(configurationKey),
        )
        const reply = decodeAllocationReply(result)
        if (reply.configRevision > 0) {
          revisions.set(configurationKey, reply.configRevision)
        }
        if (reply.body !== undefined) {
          const snapshot = decodeConfigurationRecord(reply.body)
          if (snapshot.settings.policy !== "bulkhead")
            throw new ConfigurationError(
              "corrupt-record",
              "configuration record policy does not match the identity",
            )
        }

        const shared = {
          slots: reply.slots,
          revision: reply.revision,
          expiresAt: reply.expiresAt,
          serverTime: reply.serverTime,
        }
        if (reply.status === 1) {
          return { type: "committed", ...shared, duplicate: reply.duplicate }
        }
        return {
          type: "refused",
          reason: ALLOCATION_REFUSAL_REASONS[
            reply.reason
          ] as BulkheadAllocationRefusalReason,
          ...shared,
        }
      } catch (error) {
        const configuration = configurationError(error)
        if (configuration !== undefined) throw configuration
        throw new CoordinatorUnavailableError(error)
      }
    },
  }
  return Object.freeze(issuer)
}
