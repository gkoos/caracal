/**
 * Property tests for the configuration record format.
 *
 * The record is the wire contract between the manager and the Redis scripts:
 * requested settings beside the normalized values every decision uses. These
 * properties pin that a record written by the manager reads back as the same
 * request and the same effective values, for any settings the validators accept.
 */

import * as fc from "fast-check"
import { describe, expect } from "vitest"
import {
  CONFIGURATION_SCHEMA_VERSION,
  decodeConfigurationRecord,
  encodeConfigurationRecord,
  resolveConfiguration,
} from "../../src/core/configuration.js"
import type { ConfigurationSettings } from "../../src/core/configuration.js"
import { createGeneratedSuite } from "../support/seed.js"

const suite = createGeneratedSuite({
  name: "runtime-configuration",
  command: "npm run test:property",
  maxCases: 2_000,
})

const bulkhead = fc
  .integer({ min: 1, max: 10_000 })
  .map((limit) => ({ policy: "bulkhead" as const, limit }))

const rateLimit = fc
  .tuple(
    fc.double({ min: 0.001, max: 1000, noNaN: true }),
    fc.integer({ min: 1, max: 100 }),
  )
  .map(([rate, burst]) => ({ policy: "rateLimit" as const, rate, burst }))

const breaker = fc
  .tuple(
    fc.double({ min: 0.0005, max: 0.9994, noNaN: true }),
    fc.integer({ min: 1, max: 10_000 }),
    fc.integer({ min: 1, max: 10_000 }),
    fc.integer({ min: 1, max: 86_400_000 }),
    fc.integer({ min: 1, max: 10 }),
    fc.integer({ min: 1, max: 10 }),
  )
  .map(([failureThreshold, first, second, openMs, probes, successes]) => ({
    policy: "circuitBreaker" as const,
    failureThreshold,
    minimumThroughput: Math.min(first, second),
    windowSize: Math.max(first, second),
    openMs,
    halfOpenProbes: probes,
    halfOpenSuccesses: successes,
  }))

const settings = fc.oneof(bulkhead, rateLimit, breaker)

describe(`runtime configuration record - generated settings (seed=${suite.seed})`, () => {
  suite.itProperty(
    "round-trips the requested and effective settings",
    settings,
    (value) => {
      const requested = value as ConfigurationSettings
      const snapshot = {
        schemaVersion: CONFIGURATION_SCHEMA_VERSION,
        revision: 3,
        changedAt: 1_700_000_000_000,
        settings: requested,
        effective: resolveConfiguration(requested),
      }
      const decoded = decodeConfigurationRecord(
        encodeConfigurationRecord(snapshot),
      )
      expect(decoded.settings).toEqual(requested)
      expect(decoded.effective).toEqual(snapshot.effective)
      expect(decoded.revision).toBe(3)
      expect(decoded.changedAt).toBe(1_700_000_000_000)
    },
    { runs: 200 },
  )

  suite.itProperty(
    "normalizes once and stays stable across a second write",
    settings,
    (value) => {
      const requested = value as ConfigurationSettings
      const first = resolveConfiguration(requested)
      const snapshot = {
        schemaVersion: CONFIGURATION_SCHEMA_VERSION,
        revision: 1,
        changedAt: 1,
        settings: requested,
        effective: first,
      }
      const decoded = decodeConfigurationRecord(
        encodeConfigurationRecord(snapshot),
      )
      expect(resolveConfiguration(decoded.settings)).toEqual(first)
      expect(encodeConfigurationRecord(decoded)).toBe(
        encodeConfigurationRecord(snapshot),
      )
    },
    { runs: 200 },
  )
})
