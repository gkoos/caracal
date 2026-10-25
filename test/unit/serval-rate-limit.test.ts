/**
 * Unit tests for the delegated rate limiter (Phase 3).
 *
 * `docs/serval.md` decision 8 is the contract: the server issues bounded quota
 * grants from a token bucket, the process spends them locally within a
 * send-time-anchored horizon, and unused stock expires rather than being carried
 * forward. The authority here is the Phase 0 reference model, so these cases check
 * the implementation against the specification's own oracle.
 */

import { describe, expect, it } from "vitest"
import {
  operation,
  rateLimit,
  RateLimitExceededError,
} from "../../src/index.js"
import { ServalRegistryPressureError } from "../../src/coordination/serval/cache.js"
import {
  createServalRateLimitCoordinator,
  type ServalRateLimitCoordinatorOptions,
} from "../../src/coordination/serval/rate-limit.js"
import { createFakeGrantAuthority } from "../support/serval/fake-grant-authority.js"
import {
  TEST_RATE_IDENTITY,
  createRateLimitHarness,
  type RateLimitHarness,
} from "../support/serval/rate-limit-harness.js"

function command(harness: RateLimitHarness) {
  return harness.coordinator.command(harness.identity, harness.params)
}

/** A coordinator with the reference-model authority, for construction checks. */
function coordinatorWith(
  options: Partial<ServalRateLimitCoordinatorOptions> = {},
) {
  const authority = createFakeGrantAuthority({
    now: () => 1_000,
    ratePerSecond: 1_000,
    burst: 4,
    spendHorizonMs: 500,
  })
  return createServalRateLimitCoordinator({
    authority: authority.issuer,
    now: () => 1_000,
    ...options,
  })
}

describe("serval rate limiter local stock", () => {
  it("admits a whole grant with one issuance round trip", async () => {
    const harness = createRateLimitHarness({ burst: 4, grantSize: 4 })

    for (let index = 0; index < 4; index += 1) {
      expect(await command(harness)).toEqual({ allowed: true, retryAfterMs: 0 })
    }
    await harness.whenIdle()

    // One grant for four admissions, plus the background refill the last permit
    // triggered - which the empty bucket refused.
    expect(harness.authority.calls.issue).toBe(2)
    expect(harness.coordinator.metrics()).toMatchObject({
      admissions: 4,
      permitsGranted: 4,
      permitsSpent: 4,
      refused: 1,
    })
  })

  it("coalesces concurrent admissions onto one issuance", async () => {
    const harness = createRateLimitHarness({ burst: 4, grantSize: 4 })

    const decisions = await Promise.all([
      command(harness),
      command(harness),
      command(harness),
      command(harness),
    ])

    expect(decisions.every((decision) => decision.allowed)).toBe(true)
    // One issuance served all four admissions. The second call is the background
    // refill the fourth permit triggered, which is what keeps the next admission
    // off the round trip - not a per-admission read.
    expect(harness.authority.calls.issue).toBe(2)
    expect(harness.coordinator.metrics().permitsSpent).toBe(4)
    expect(harness.coordinator.metrics().issuances).toBe(2)
  })

  it("never spends a grant twice under concurrent load", async () => {
    const harness = createRateLimitHarness({ burst: 4, grantSize: 4 })

    const decisions = await Promise.all(
      Array.from({ length: 10 }, () => command(harness)),
    )
    await harness.whenIdle()

    const admitted = decisions.filter((decision) => decision.allowed).length
    expect(admitted).toBe(4)
    // Every refusal carries a positive wait, and no permit was invented to serve
    // the callers the bucket could not cover.
    for (const decision of decisions) {
      if (!decision.allowed) expect(decision.retryAfterMs).toBeGreaterThan(0)
    }
    const metrics = harness.coordinator.metrics()
    expect(metrics.permitsSpent).toBe(4)
    expect(metrics.admissions).toBe(4)
    expect(metrics.permitsGranted).toBe(4)
  })

  it("discards unused stock at expiry instead of carrying it forward", async () => {
    const harness = createRateLimitHarness({
      burst: 4,
      grantSize: 4,
      validForMs: 500,
    })

    await command(harness)
    await harness.whenIdle()
    expect(harness.coordinator.diagnostics(harness.identity)?.stock).toBe(3)

    harness.advanceBy(600)
    expect(await command(harness)).toEqual({ allowed: true, retryAfterMs: 0 })
    await harness.whenIdle()

    const metrics = harness.coordinator.metrics()
    // Three permits were thrown away, and the new grant starts from zero.
    expect(metrics.permitsExpired).toBe(3)
    expect(metrics.permitsGranted).toBe(8)
    expect(metrics.admissions).toBe(2)
    expect(metrics.permitsSpent + metrics.permitsExpired).toBe(5)
  })

  it("refuses without a round trip while the server's hint is live", async () => {
    const harness = createRateLimitHarness({ burst: 1, grantSize: 1 })

    expect((await command(harness)).allowed).toBe(true)
    await harness.whenIdle()
    const refused = await command(harness)
    await harness.whenIdle()
    expect(refused.allowed).toBe(false)
    expect(refused.retryAfterMs).toBeGreaterThan(0)

    harness.authority.reset()
    for (let index = 0; index < 50; index += 1) {
      const decision = await command(harness)
      expect(decision.allowed).toBe(false)
      expect(decision.retryAfterMs).toBeGreaterThan(0)
    }
    // A hot replica must not poll Redis once the server has said when to ask again.
    expect(harness.authority.calls.issue).toBe(0)
    expect(harness.coordinator.metrics().shortCircuits).toBeGreaterThanOrEqual(
      50,
    )
  })
})

describe("serval rate limiter issuance operations", () => {
  it("cannot extend a grant's window by retrying after an ambiguous reply", async () => {
    const harness = createRateLimitHarness({
      burst: 4,
      grantSize: 4,
      validForMs: 500,
    })

    const sentAt = harness.now()
    harness.authority.commitThenFailNext()
    await expect(command(harness)).rejects.toThrow("unavailable")
    // The permits were deducted server-side and this process never saw the reply.
    expect(harness.coordinator.metrics()).toMatchObject({
      failures: 1,
      permitsGranted: 0,
    })
    expect(harness.coordinator.diagnostics(harness.identity)?.state).toBe(
      "uncertain",
    )

    harness.advanceBy(100)
    const retried = await command(harness)
    expect(retried.allowed).toBe(true)
    await harness.whenIdle()

    // The retry was deduplicated: the server returned its recorded grant, so the
    // bucket was not deducted twice, and the deadline is still the original send
    // time plus the horizon - not the retry's.
    expect(harness.authority.calls.duplicates).toBe(1)
    expect(harness.authority.permitsIssued()).toBe(4)
    expect(harness.coordinator.diagnostics(harness.identity)?.deadline).toBe(
      sentAt + 500 - 25,
    )
  })

  it("starts a fresh operation once a retained one can no longer be adopted", async () => {
    const harness = createRateLimitHarness({
      burst: 4,
      grantSize: 4,
      validForMs: 500,
    })

    harness.authority.failNext(1)
    await expect(command(harness)).rejects.toThrow("unavailable")

    // Past the original window a retry could only return a grant nobody can spend.
    harness.advanceBy(500)
    expect((await command(harness)).allowed).toBe(true)
    await harness.whenIdle()

    expect(harness.authority.calls.issue).toBe(2)
    expect(harness.coordinator.metrics().failures).toBe(1)
  })

  it("counts the permits of a reply it could not adopt as lost", async () => {
    const harness = createRateLimitHarness({
      burst: 4,
      grantSize: 4,
      validForMs: 200,
    })

    harness.authority.holdNextIssue()
    const pending = command(harness)
    // The reply outlives the window it was anchored to.
    harness.advanceBy(400)
    harness.authority.releaseIssue()
    const decision = await pending

    expect(decision.allowed).toBe(false)
    expect(decision.retryAfterMs).toBeGreaterThan(0)
    const metrics = harness.coordinator.metrics()
    expect(metrics.permitsLost).toBe(4)
    expect(metrics.permitsGranted).toBe(0)
    expect(metrics.admissions).toBe(0)
    expect(harness.coordinator.diagnostics(harness.identity)?.stock).toBe(0)
  })

  it("spends held stock while the authority is unreachable", async () => {
    const harness = createRateLimitHarness({ burst: 4, grantSize: 4 })

    await command(harness)
    await harness.whenIdle()
    harness.authority.reset()
    harness.authority.failNext(10)

    // Within the remaining conservative validity, stock is still spendable.
    for (let index = 0; index < 3; index += 1) {
      expect((await command(harness)).allowed).toBe(true)
    }
    // Spending the last permit triggers a refill, which the unreachable authority
    // fails; nothing was granted, and nothing was invented.
    expect(harness.authority.permitsIssued()).toBe(4)
    expect(harness.coordinator.metrics().permitsGranted).toBe(4)

    // With no stock left there is no authority to spend, and none is invented.
    await expect(command(harness)).rejects.toThrow("unavailable")
    expect(harness.coordinator.metrics().failures).toBe(1)
  })
})

describe("serval rate limiter boundaries", () => {
  it("refuses a scope its registry cannot hold rather than dropping live quota", async () => {
    const harness = createRateLimitHarness({ maximumScopes: 1 })
    await command(harness)
    expect(harness.coordinator.diagnostics(harness.identity)?.stock).toBe(3)

    const other = { ...TEST_RATE_IDENTITY, scope: "tenant-b" }
    // The held scope has spendable stock, so it is pinned: evicting it would
    // silently discard quota this process is still entitled to spend.
    await expect(
      harness.coordinator.command(other, harness.params),
    ).rejects.toBeInstanceOf(ServalRegistryPressureError)
    expect(harness.coordinator.metrics().registryRefusals).toBe(1)
    expect(harness.coordinator.metrics().scopes).toBe(1)
  })

  it("reclaims a scope that holds no accounting", async () => {
    const harness = createRateLimitHarness({
      burst: 1,
      grantSize: 1,
      maximumScopes: 1,
    })
    expect((await command(harness)).allowed).toBe(true)
    await harness.whenIdle()

    // The scope is depleted and its retry hint has lapsed, so it holds nothing and
    // can be reclaimed without losing quota.
    harness.advanceBy(50)
    const other = { ...TEST_RATE_IDENTITY, scope: "tenant-b" }
    expect(await harness.coordinator.command(other, harness.params)).toEqual({
      allowed: true,
      retryAfterMs: 0,
    })
    expect(harness.coordinator.metrics().scopes).toBe(1)
    expect(harness.coordinator.diagnostics(harness.identity)).toBeUndefined()
  })

  it("rejects an invalid configuration", () => {
    expect(() => coordinatorWith({ grantSize: 0 })).toThrow("grantSize")
    expect(() => coordinatorWith({ validForMs: 0 })).toThrow("validForMs")
    expect(() => coordinatorWith({ maximumScopes: -1 })).toThrow(
      "maximumScopes",
    )
    expect(() => coordinatorWith({ maximumOutstandingPermits: 0 })).toThrow(
      "maximumOutstandingPermits",
    )
    expect(() => coordinatorWith({ safetyMarginMs: -1 })).toThrow(
      "safetyMarginMs",
    )
    expect(() => coordinatorWith({ grantSize: 2 })).not.toThrow()
  })

  it("rejects invalid admission parameters as the strict coordinator does", async () => {
    const harness = createRateLimitHarness()
    await expect(
      harness.coordinator.command(harness.identity, {
        emissionIntervalMs: 0,
        burstDelayMs: 0,
      }),
    ).rejects.toThrow("emission interval")
    await expect(
      harness.coordinator.command(harness.identity, {
        emissionIntervalMs: 1.5,
        burstDelayMs: 0,
      }),
    ).rejects.toThrow("emission interval")
    await expect(
      harness.coordinator.command(harness.identity, {
        emissionIntervalMs: 1,
        burstDelayMs: -1,
      }),
    ).rejects.toThrow("burst delay")
  })
})

describe("serval rate limiter in the policy path", () => {
  it("admits from stock and reports the server's wait on refusal", async () => {
    const harness = createRateLimitHarness({ burst: 4, grantSize: 4 })
    const policy = rateLimit.distributed({
      name: TEST_RATE_IDENTITY.name,
      rate: 1_000,
      burst: 4,
      coordinator: harness.coordinator,
      scope: () => TEST_RATE_IDENTITY.scope,
    })
    const op = operation({
      name: TEST_RATE_IDENTITY.operation,
      adapter: {
        capabilities: () => ({ abort: "supported", replay: "safe" }) as const,
        execute: async () => "ok",
      },
      policies: [policy],
    })

    for (let index = 0; index < 4; index += 1) {
      await expect(op.execute(undefined)).resolves.toBe("ok")
    }
    await harness.whenIdle()

    // Four admissions for at most two issuances: the round trip is not per admission.
    expect(harness.coordinator.metrics().admissions).toBe(4)
    expect(harness.authority.calls.issue).toBeLessThanOrEqual(2)

    const rejected = await op.execute(undefined).catch((error) => error)
    expect(rejected).toBeInstanceOf(RateLimitExceededError)
    expect((rejected as RateLimitExceededError).coordination).toBe(
      "distributed",
    )
    expect((rejected as RateLimitExceededError).retryAfterMs).toBeGreaterThan(0)
  })
})
