import { describe, expect, it } from "vitest"
import type {
  CircuitBreakerSettings,
  ConfigurationSettings,
} from "../../src/core/configuration.js"
import {
  CONFIGURATION_SCHEMA_VERSION,
  ConfigurationError,
  configurationFingerprint,
  decodeConfigurationRecord,
  encodeConfigurationRecord,
  resolveConfiguration,
} from "../../src/core/configuration.js"
import type { BreakerCoordinator } from "../../src/index.js"
import { circuitBreaker, rateLimit } from "../../src/index.js"

const breaker = (
  overrides: Partial<CircuitBreakerSettings> = {},
): CircuitBreakerSettings => ({
  policy: "circuitBreaker",
  failureThreshold: 0.5,
  minimumThroughput: 5,
  windowSize: 100,
  openMs: 10_000,
  halfOpenProbes: 3,
  halfOpenSuccesses: 2,
  ...overrides,
})

function snapshot(settings: ConfigurationSettings) {
  return {
    schemaVersion: CONFIGURATION_SCHEMA_VERSION,
    revision: 1,
    changedAt: 1_700_000_000_000,
    settings,
    effective: resolveConfiguration(settings),
  }
}

function throws(run: () => unknown): boolean {
  try {
    run()
    return false
  } catch {
    return true
  }
}

describe("configuration normalization", () => {
  it("normalizes a rate to a whole-millisecond interval", () => {
    expect(
      resolveConfiguration({ policy: "rateLimit", rate: 3, burst: 3 }),
    ).toEqual({
      policy: "rateLimit",
      emissionIntervalMs: 333,
      burstDelayMs: 666,
    })
  })

  it("treats equivalent requested rates as the same effective settings", () => {
    const first = resolveConfiguration({
      policy: "rateLimit",
      rate: 3,
      burst: 1,
    })
    const second = resolveConfiguration({
      policy: "rateLimit",
      rate: 3.003,
      burst: 1,
    })
    expect(configurationFingerprint(first)).toBe(
      configurationFingerprint(second),
    )
  })

  it("normalizes a failure threshold to thousandths", () => {
    const effective = resolveConfiguration(
      breaker({ failureThreshold: 0.3335 }),
    )
    expect(effective).toMatchObject({ failureThresholdNumerator: 334 })
  })

  it("keeps the requested settings beside the effective ones", () => {
    const settings = { policy: "rateLimit" as const, rate: 3.003, burst: 1 }
    const decoded = decodeConfigurationRecord(
      encodeConfigurationRecord(snapshot(settings)),
    )
    expect(decoded.settings).toEqual(settings)
    expect(decoded.effective).toEqual({
      policy: "rateLimit",
      emissionIntervalMs: 333,
      burstDelayMs: 0,
    })
  })
})

describe("configuration validation", () => {
  it("rejects a bulkhead limit the bulkhead would reject", () => {
    for (const limit of [0, -1, 1.5, Number.NaN]) {
      expect(() => resolveConfiguration({ policy: "bulkhead", limit })).toThrow(
        RangeError,
      )
    }
    expect(resolveConfiguration({ policy: "bulkhead", limit: 1 })).toEqual({
      policy: "bulkhead",
      limit: 1,
    })
  })

  it("rejects breaker settings no constructor would accept", () => {
    for (const settings of [
      breaker({ minimumThroughput: 0 }),
      breaker({ failureThreshold: 0 }),
      breaker({ failureThreshold: 0.9995 }),
      breaker({ windowSize: 0 }),
      breaker({ windowSize: 10_001 }),
      breaker({ openMs: 0 }),
      breaker({ halfOpenProbes: 0 }),
      breaker({ halfOpenSuccesses: 0 }),
    ]) {
      expect(() => resolveConfiguration(settings)).toThrow(RangeError)
    }
  })

  it("rejects a window that cannot reach its own throughput", () => {
    expect(() =>
      resolveConfiguration(breaker({ minimumThroughput: 100, windowSize: 10 })),
    ).toThrow(/minimumThroughput must not exceed windowSize/)
  })

  it("accepts and rejects the same rate settings as the constructor", () => {
    for (const rate of [0, 0.0001, 1, 3, 1000, 1001, Number.NaN]) {
      for (const burst of [1, 2]) {
        const constructRate = () => rateLimit.local({ name: "x", rate, burst })
        const update = () =>
          resolveConfiguration({ policy: "rateLimit", rate, burst })
        expect(throws(update), `rate ${rate}, burst ${burst}`).toBe(
          throws(constructRate),
        )
      }
    }
  })

  it("keeps constructor and override parity for breaker fields", () => {
    const coordinator = {} as BreakerCoordinator
    const construct = (settings: CircuitBreakerSettings) => () =>
      circuitBreaker.distributed({
        name: "x",
        coordinator,
        scope: () => "scope",
        ...settings,
      })
    for (const settings of [
      breaker(),
      breaker({ minimumThroughput: 0 }),
      breaker({ windowSize: 10_001 }),
      breaker({ openMs: 0 }),
      breaker({ halfOpenProbes: 0 }),
      breaker({ halfOpenSuccesses: 0 }),
      breaker({ failureThreshold: 0.9995 }),
    ]) {
      expect(throws(() => resolveConfiguration(settings))).toBe(
        throws(construct(settings)),
      )
    }
  })
})

describe("configuration records", () => {
  it("round-trips every policy kind", () => {
    for (const settings of [
      { policy: "bulkhead" as const, limit: 40 },
      { policy: "rateLimit" as const, rate: 3, burst: 3 },
      breaker(),
    ]) {
      const decoded = decodeConfigurationRecord(
        encodeConfigurationRecord(snapshot(settings)),
      )
      expect(decoded.settings).toEqual(settings)
      expect(decoded.revision).toBe(1)
      expect(decoded.changedAt).toBe(1_700_000_000_000)
    }
  })

  it("stores every setting as a decimal string", () => {
    const encoded = JSON.parse(
      encodeConfigurationRecord(
        snapshot({ policy: "rateLimit", rate: 3, burst: 1 }),
      ),
    ) as { settings: Record<string, unknown> }
    expect(encoded.settings).toEqual({
      policy: "rateLimit",
      rate: "3",
      burst: "1",
    })
  })

  it("rejects a record whose schema version it does not know", () => {
    const record = JSON.parse(
      encodeConfigurationRecord(snapshot({ policy: "bulkhead", limit: 2 })),
    ) as Record<string, unknown>
    record.schemaVersion = 99
    expect(() => decodeConfigurationRecord(JSON.stringify(record))).toThrow(
      ConfigurationError,
    )
  })

  it("rejects a fingerprint that does not match the settings", () => {
    const record = JSON.parse(
      encodeConfigurationRecord(snapshot({ policy: "bulkhead", limit: 2 })),
    ) as Record<string, unknown>
    record.fingerprint = "bulkhead|limit=3"
    expect(() => decodeConfigurationRecord(JSON.stringify(record))).toThrow(
      ConfigurationError,
    )
  })

  it("rejects stored settings the current validators would not accept", () => {
    const record = JSON.parse(
      encodeConfigurationRecord(snapshot(breaker())),
    ) as { settings: Record<string, unknown> }
    record.settings.windowSize = "0"
    expect(() => decodeConfigurationRecord(JSON.stringify(record))).toThrow(
      ConfigurationError,
    )
  })

  it("rejects malformed JSON", () => {
    expect(() => decodeConfigurationRecord("{")).toThrow(ConfigurationError)
  })
})
