/**
 * Fuzz tests for configuration record decoding.
 *
 * A record can be written by another release, restored from a dump, or edited
 * by hand. Whatever arrives, decoding must either produce a valid snapshot or
 * raise `ConfigurationError`; it must never surface a `TypeError` or a
 * `RangeError` that callers would not know how to classify.
 */

import * as fc from "fast-check"
import { describe, expect } from "vitest"
import {
  ConfigurationError,
  decodeConfigurationRecord,
  encodeConfigurationRecord,
} from "../../src/core/configuration.js"
import { createGeneratedSuite } from "../support/seed.js"

const suite = createGeneratedSuite({
  name: "runtime-configuration-fuzz",
  command: "npm run test:fuzz",
  maxCases: 2_000,
})

describe(`configuration record fuzz (seed=${suite.seed})`, () => {
  suite.itProperty(
    "rejects arbitrary input only as a configuration error",
    fc.string(),
    (value) => {
      try {
        const snapshot = decodeConfigurationRecord(value)
        expect(
          decodeConfigurationRecord(encodeConfigurationRecord(snapshot))
            .settings,
        ).toEqual(snapshot.settings)
      } catch (error) {
        expect(error).toBeInstanceOf(ConfigurationError)
      }
    },
    { runs: 300 },
  )
})
