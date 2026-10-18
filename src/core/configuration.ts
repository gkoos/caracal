import { isBulkheadLimit } from "./bulkhead.js"
import {
  assertResolvableThreshold,
  MAX_WINDOW_SIZE,
  resolveFailureThresholdNumerator,
} from "./circuit-breaker.js"
import { resolveRateLimitKnobs } from "./rate-limit.js"

/**
 * A shared configuration record for one distributed policy identity.
 *
 * The record holds the settings the application asked for beside the
 * normalized values every decision uses. Reads report the request; equality,
 * fingerprints and comparisons use the normalized values only. Constructor
 * settings are revision 0, so a record exists only after an explicit update.
 */

export const CONFIGURATION_SCHEMA_VERSION = 1

export type ConfigurationPolicy = "bulkhead" | "rateLimit" | "circuitBreaker"

/** One policy identity: the coordinator identity plus the policy kind. */
export interface ConfigurationIdentity {
  readonly policy: ConfigurationPolicy
  readonly name: string
  readonly operation: string
  readonly scope: string
}

export type BulkheadSettings = Readonly<{
  policy: "bulkhead"
  limit: number
}>

export type RateLimitSettings = Readonly<{
  policy: "rateLimit"
  rate: number
  burst: number
}>

export type CircuitBreakerSettings = Readonly<{
  policy: "circuitBreaker"
  failureThreshold: number
  minimumThroughput: number
  windowSize: number
  openMs: number
  halfOpenProbes: number
  halfOpenSuccesses: number
}>

export type ConfigurationSettings =
  | BulkheadSettings
  | RateLimitSettings
  | CircuitBreakerSettings

export type BulkheadEffective = Readonly<{
  policy: "bulkhead"
  limit: number
}>

export type RateLimitEffective = Readonly<{
  policy: "rateLimit"
  emissionIntervalMs: number
  burstDelayMs: number
}>

export type CircuitBreakerEffective = Readonly<{
  policy: "circuitBreaker"
  failureThresholdNumerator: number
  minimumThroughput: number
  windowSize: number
  openMs: number
  halfOpenProbes: number
  halfOpenSuccesses: number
}>

export type ConfigurationEffective =
  | BulkheadEffective
  | RateLimitEffective
  | CircuitBreakerEffective

export interface ConfigurationSnapshot {
  readonly schemaVersion: number
  readonly revision: number
  readonly changedAt: number
  readonly settings: ConfigurationSettings
  readonly effective: ConfigurationEffective
}

export type ConfigurationUpdateResult =
  | Readonly<{ status: "applied"; snapshot: ConfigurationSnapshot }>
  | Readonly<{ status: "unchanged"; snapshot: ConfigurationSnapshot }>
  | Readonly<{ status: "conflict"; snapshot: ConfigurationSnapshot | null }>

/** A stored record that exists but cannot be used. */
export class ConfigurationError extends Error {
  constructor(
    readonly reason: "corrupt-record" | "unsupported-schema",
    message: string,
  ) {
    super(message)
    this.name = "ConfigurationError"
  }
}

/** Validates a complete snapshot and returns the values decisions use. */
export function resolveConfiguration(
  settings: ConfigurationSettings,
): ConfigurationEffective {
  switch (settings.policy) {
    case "bulkhead":
      if (!isBulkheadLimit(settings.limit))
        throw new RangeError("limit must be a positive integer")
      return { policy: "bulkhead", limit: settings.limit }
    case "rateLimit": {
      const { emissionIntervalMs, burstDelayMs } = resolveRateLimitKnobs(
        settings.rate,
        settings.burst,
      )
      return { policy: "rateLimit", emissionIntervalMs, burstDelayMs }
    }
    case "circuitBreaker":
      return resolveCircuitBreaker(settings)
  }
}

function resolveCircuitBreaker(
  settings: CircuitBreakerSettings,
): CircuitBreakerEffective {
  const {
    failureThreshold,
    minimumThroughput,
    windowSize,
    openMs,
    halfOpenProbes,
    halfOpenSuccesses,
  } = settings
  if (!Number.isInteger(minimumThroughput) || minimumThroughput < 1)
    throw new RangeError("minimumThroughput must be a positive integer")
  if (
    !Number.isFinite(failureThreshold) ||
    failureThreshold <= 0 ||
    failureThreshold >= 1
  )
    throw new RangeError("failureThreshold must be a number in (0, 1)")
  assertResolvableThreshold(failureThreshold)
  if (
    !Number.isInteger(windowSize) ||
    windowSize < 1 ||
    windowSize > MAX_WINDOW_SIZE
  )
    throw new RangeError(
      `windowSize must be an integer between 1 and ${MAX_WINDOW_SIZE}`,
    )
  // The constructor permits this legacy combination; an override does not,
  // because the count cap holds the window below the opening threshold.
  if (minimumThroughput > windowSize)
    throw new RangeError("minimumThroughput must not exceed windowSize")
  if (!Number.isInteger(openMs) || openMs < 1)
    throw new RangeError("openMs must be a positive integer")
  if (!Number.isInteger(halfOpenProbes) || halfOpenProbes < 1)
    throw new RangeError("halfOpenProbes must be a positive integer")
  if (!Number.isInteger(halfOpenSuccesses) || halfOpenSuccesses < 1)
    throw new RangeError("halfOpenSuccesses must be a positive integer")
  return {
    policy: "circuitBreaker",
    failureThresholdNumerator:
      resolveFailureThresholdNumerator(failureThreshold),
    minimumThroughput,
    windowSize,
    openMs,
    halfOpenProbes,
    halfOpenSuccesses,
  }
}

/** A stable identity for the effective settings, compared as a string. */
export function configurationFingerprint(
  effective: ConfigurationEffective,
): string {
  switch (effective.policy) {
    case "bulkhead":
      return `bulkhead|limit=${effective.limit}`
    case "rateLimit":
      return `ratelimit|interval=${effective.emissionIntervalMs}|burst=${effective.burstDelayMs}`
    case "circuitBreaker":
      return [
        "breaker",
        `threshold=${effective.failureThresholdNumerator}`,
        `minimumThroughput=${effective.minimumThroughput}`,
        `windowSize=${effective.windowSize}`,
        `openMs=${effective.openMs}`,
        `halfOpenProbes=${effective.halfOpenProbes}`,
        `halfOpenSuccesses=${effective.halfOpenSuccesses}`,
      ].join("|")
  }
}

type EncodedValues = Record<string, string>

function encodeSettings(settings: ConfigurationSettings): EncodedValues {
  switch (settings.policy) {
    case "bulkhead":
      return { policy: "bulkhead", limit: String(settings.limit) }
    case "rateLimit":
      return {
        policy: "rateLimit",
        rate: String(settings.rate),
        burst: String(settings.burst),
      }
    case "circuitBreaker":
      return {
        policy: "circuitBreaker",
        failureThreshold: String(settings.failureThreshold),
        minimumThroughput: String(settings.minimumThroughput),
        windowSize: String(settings.windowSize),
        openMs: String(settings.openMs),
        halfOpenProbes: String(settings.halfOpenProbes),
        halfOpenSuccesses: String(settings.halfOpenSuccesses),
      }
  }
}

function encodeEffective(effective: ConfigurationEffective): EncodedValues {
  switch (effective.policy) {
    case "bulkhead":
      return { policy: "bulkhead", limit: String(effective.limit) }
    case "rateLimit":
      return {
        policy: "rateLimit",
        emissionIntervalMs: String(effective.emissionIntervalMs),
        burstDelayMs: String(effective.burstDelayMs),
      }
    case "circuitBreaker":
      return {
        policy: "circuitBreaker",
        failureThresholdNumerator: String(effective.failureThresholdNumerator),
        minimumThroughput: String(effective.minimumThroughput),
        windowSize: String(effective.windowSize),
        openMs: String(effective.openMs),
        halfOpenProbes: String(effective.halfOpenProbes),
        halfOpenSuccesses: String(effective.halfOpenSuccesses),
      }
  }
}

/** Serializes a snapshot with every setting as a decimal string. */
export function encodeConfigurationRecord(
  snapshot: ConfigurationSnapshot,
): string {
  return JSON.stringify({
    schemaVersion: snapshot.schemaVersion,
    revision: snapshot.revision,
    changedAt: snapshot.changedAt,
    fingerprint: configurationFingerprint(snapshot.effective),
    settings: encodeSettings(snapshot.settings),
    effective: encodeEffective(snapshot.effective),
  })
}

function recordObject(value: unknown, label: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value))
    throw new ConfigurationError("corrupt-record", `${label} must be an object`)
  return value as Record<string, unknown>
}

function decimal(value: unknown, label: string): number {
  if (typeof value !== "string" || value.trim() === "")
    throw new ConfigurationError(
      "corrupt-record",
      `${label} must be a decimal string`,
    )
  const parsed = Number(value)
  if (!Number.isFinite(parsed))
    throw new ConfigurationError("corrupt-record", `${label} is not a number`)
  return parsed
}

function integer(value: unknown, label: string): number {
  const parsed = decimal(value, label)
  if (!Number.isSafeInteger(parsed))
    throw new ConfigurationError(
      "corrupt-record",
      `${label} is not a safe integer`,
    )
  return parsed
}

function decodeSettings(
  record: Record<string, unknown>,
): ConfigurationSettings {
  switch (record.policy) {
    case "bulkhead":
      return { policy: "bulkhead", limit: integer(record.limit, "limit") }
    case "rateLimit":
      return {
        policy: "rateLimit",
        rate: decimal(record.rate, "rate"),
        burst: integer(record.burst, "burst"),
      }
    case "circuitBreaker":
      return {
        policy: "circuitBreaker",
        failureThreshold: decimal(record.failureThreshold, "failureThreshold"),
        minimumThroughput: integer(
          record.minimumThroughput,
          "minimumThroughput",
        ),
        windowSize: integer(record.windowSize, "windowSize"),
        openMs: integer(record.openMs, "openMs"),
        halfOpenProbes: integer(record.halfOpenProbes, "halfOpenProbes"),
        halfOpenSuccesses: integer(
          record.halfOpenSuccesses,
          "halfOpenSuccesses",
        ),
      }
    default:
      throw new ConfigurationError(
        "unsupported-schema",
        `unknown configuration policy ${String(record.policy)}`,
      )
  }
}

function decodeEffective(
  record: Record<string, unknown>,
): ConfigurationEffective {
  switch (record.policy) {
    case "bulkhead":
      return { policy: "bulkhead", limit: integer(record.limit, "limit") }
    case "rateLimit":
      return {
        policy: "rateLimit",
        emissionIntervalMs: integer(
          record.emissionIntervalMs,
          "emissionIntervalMs",
        ),
        burstDelayMs: integer(record.burstDelayMs, "burstDelayMs"),
      }
    case "circuitBreaker":
      return {
        policy: "circuitBreaker",
        failureThresholdNumerator: integer(
          record.failureThresholdNumerator,
          "failureThresholdNumerator",
        ),
        minimumThroughput: integer(
          record.minimumThroughput,
          "minimumThroughput",
        ),
        windowSize: integer(record.windowSize, "windowSize"),
        openMs: integer(record.openMs, "openMs"),
        halfOpenProbes: integer(record.halfOpenProbes, "halfOpenProbes"),
        halfOpenSuccesses: integer(
          record.halfOpenSuccesses,
          "halfOpenSuccesses",
        ),
      }
    default:
      throw new ConfigurationError(
        "unsupported-schema",
        `unknown configuration policy ${String(record.policy)}`,
      )
  }
}

/** Parses and validates a stored record. */
export function decodeConfigurationRecord(
  serialized: string,
): ConfigurationSnapshot {
  let parsed: unknown
  try {
    parsed = JSON.parse(serialized)
  } catch {
    throw new ConfigurationError(
      "corrupt-record",
      "configuration record is not valid JSON",
    )
  }
  const record = recordObject(parsed, "configuration record")
  if (record.schemaVersion !== CONFIGURATION_SCHEMA_VERSION)
    throw new ConfigurationError(
      "unsupported-schema",
      `unsupported configuration schema ${String(record.schemaVersion)}`,
    )
  const revision = record.revision
  if (
    typeof revision !== "number" ||
    !Number.isSafeInteger(revision) ||
    revision < 1
  )
    throw new ConfigurationError(
      "corrupt-record",
      "revision must be a positive integer",
    )
  const changedAt = record.changedAt
  if (
    typeof changedAt !== "number" ||
    !Number.isSafeInteger(changedAt) ||
    changedAt < 0
  )
    throw new ConfigurationError(
      "corrupt-record",
      "changedAt must be a non-negative integer",
    )
  const settings = decodeSettings(recordObject(record.settings, "settings"))
  const effective = decodeEffective(recordObject(record.effective, "effective"))
  if (settings.policy !== effective.policy)
    throw new ConfigurationError(
      "corrupt-record",
      "settings and effective policies differ",
    )
  let resolved: ConfigurationEffective
  try {
    resolved = resolveConfiguration(settings)
  } catch {
    throw new ConfigurationError(
      "corrupt-record",
      "stored settings fail current validation",
    )
  }
  const fingerprint = record.fingerprint
  if (
    typeof fingerprint !== "string" ||
    fingerprint !== configurationFingerprint(resolved) ||
    fingerprint !== configurationFingerprint(effective)
  )
    throw new ConfigurationError(
      "corrupt-record",
      "stored fingerprint does not match the settings",
    )
  return {
    schemaVersion: CONFIGURATION_SCHEMA_VERSION,
    revision,
    changedAt,
    settings,
    effective: resolved,
  }
}
