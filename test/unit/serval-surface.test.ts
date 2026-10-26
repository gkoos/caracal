/**
 * The public surface of the experimental Serval entry point.
 *
 * A published entry point is a promise: these tests pin the promise's shape - the
 * exact runtime exports, the error classes a caller may catch, and the fact that a
 * coordinator built only from the entry point validates its options and refuses
 * nonsense - so an accidental rename or a missing export fails here rather than in a
 * user's build.
 */

import { describe, expect, it } from "vitest"
import {
  bulkhead,
  circuitBreaker,
  CoordinatorRefusalError,
  operation,
  rateLimit,
} from "../../src/index.js"
import * as serval from "../../src/serval.js"
import { ServalRegistryPressureError } from "../../src/coordination/serval/cache.js"
import { ServalAuthorityRefusalError } from "../../src/coordination/serval/breaker.js"
import { ServalSessionClosedError } from "../../src/coordination/serval/session.js"

const IDENTITY = { name: "payments", operation: "charge", scope: "tenant-a" }

describe("serval public surface", () => {
  it("exports exactly the documented runtime names", () => {
    expect(Object.keys(serval).sort()).toEqual(
      [
        "ServalAuthorityRefusalError",
        "ServalRegistryPressureError",
        "ServalSessionClosedError",
        "createServalBreaker",
        "createServalBulkheadCoordinator",
        "createServalRateLimitCoordinator",
        "createServalSession",
        "redisBulkheadAllocationIssuer",
        "redisRateLimitGrantIssuer",
      ].sort(),
    )
  })

  it("exports the error classes a caller catches", () => {
    expect(serval.ServalSessionClosedError).toBe(ServalSessionClosedError)
    expect(serval.ServalRegistryPressureError).toBe(ServalRegistryPressureError)
    expect(serval.ServalAuthorityRefusalError).toBe(ServalAuthorityRefusalError)
    expect(new ServalSessionClosedError()).toBeInstanceOf(Error)
    expect(new ServalRegistryPressureError("full")).toBeInstanceOf(Error)
    expect(new ServalAuthorityRefusalError("stale")).toBeInstanceOf(Error)
    // The policy branches on the core type, so a local refusal must be one: this is the link the
    // stale-CLOSED fail-closed path depends on to reach the caller as a rejection.
    expect(new ServalAuthorityRefusalError("stale")).toBeInstanceOf(
      CoordinatorRefusalError,
    )
  })

  it("validates the configuration of a coordinator built from the entry point", () => {
    const issuer = {
      async issue() {
        return {
          granted: 0,
          duplicate: false,
          validForMs: 1_000,
          retryAfterMs: 1,
          revision: 0,
        }
      },
    }
    expect(() =>
      serval.createServalRateLimitCoordinator({
        authority: issuer,
        grantSize: 0,
      }),
    ).toThrow("grantSize")
    expect(() =>
      serval.createServalRateLimitCoordinator({ authority: issuer }),
    ).not.toThrow()

    const allocations = {
      async modify() {
        return {
          type: "refused" as const,
          reason: "expired" as const,
          slots: 0,
          revision: 0,
          expiresAt: 0,
          serverTime: 0,
        }
      },
    }
    expect(() =>
      serval.createServalBulkheadCoordinator({
        authority: allocations,
        replicaId: "",
      }),
    ).toThrow("replica id")
    expect(() =>
      serval.createServalBulkheadCoordinator({
        authority: allocations,
        replicaId: "replica-1",
        validityMs: 0,
      }),
    ).toThrow("validityMs")
  })

  it("plugs into the existing policy factories without changing them", async () => {
    // The surface's whole claim: a delegated coordinator is a coordinator, so the
    // policy path is the one already documented for a strict one.
    const coordinator = serval.createServalRateLimitCoordinator({
      authority: {
        async issue() {
          return {
            granted: 2,
            duplicate: false,
            validForMs: 1_000,
            retryAfterMs: 0,
            revision: 0,
          }
        },
      },
    })
    const op = operation({
      name: IDENTITY.operation,
      adapter: {
        capabilities: () => ({ abort: "supported", replay: "safe" }) as const,
        execute: async () => "ok",
      },
      policies: [
        rateLimit.distributed({
          name: IDENTITY.name,
          rate: 1_000,
          burst: 2,
          coordinator,
          scope: () => IDENTITY.scope,
        }),
      ],
    })

    await expect(op.execute(undefined)).resolves.toBe("ok")
    await expect(op.execute(undefined)).resolves.toBe("ok")
    expect(coordinator.metrics().admissions).toBe(2)
    // The strict policy objects are untouched by the delegated path.
    expect(typeof bulkhead.distributed).toBe("function")
    expect(typeof circuitBreaker.distributed).toBe("function")
  })
})
