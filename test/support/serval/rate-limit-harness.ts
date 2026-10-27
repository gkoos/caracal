import { resolveRateLimitKnobs } from "../../../src/core/rate-limit.js"
import {
  createServalRateLimitCoordinator,
  type ServalRateLimitCoordinator,
} from "../../../src/serval/rate-limit.js"
import {
  createFakeGrantAuthority,
  type FakeGrantAuthority,
} from "./fake-grant-authority.js"

/**
 * The delegated rate limiter wired to a manual clock and the reference-model
 * authority.
 *
 * The clock is a plain counter: the GCRA authority refills from elapsed time, so
 * advancing it is exactly what moves the bucket. Nothing here schedules a timer,
 * which keeps every case deterministic and instantaneous.
 */

/** The rate-limit identity most tests use. */
export const TEST_RATE_IDENTITY = Object.freeze({
  name: "payments-rate",
  operation: "charge",
  scope: "tenant-a",
})

export interface RateLimitHarnessOptions {
  readonly ratePerSecond?: number
  readonly burst?: number
  readonly validForMs?: number
  readonly grantSize?: number
  readonly maximumOutstandingPermits?: number
  readonly maximumScopes?: number
  readonly safetyMarginMs?: number
  readonly initialNow?: number
}

export interface RateLimitHarness {
  readonly authority: FakeGrantAuthority
  readonly coordinator: ServalRateLimitCoordinator
  readonly identity: {
    readonly name: string
    readonly operation: string
    readonly scope: string
  }
  readonly params: {
    readonly emissionIntervalMs: number
    readonly burstDelayMs: number
  }
  now(): number
  advanceBy(ms: number): void
  whenIdle(): Promise<void>
}

export function createRateLimitHarness(
  options: RateLimitHarnessOptions = {},
): RateLimitHarness {
  const ratePerSecond = options.ratePerSecond ?? 1_000
  const burst = options.burst ?? 4
  const validForMs = options.validForMs ?? 500
  const { emissionIntervalMs, burstDelayMs } = resolveRateLimitKnobs(
    ratePerSecond,
    burst,
  )

  let clock = options.initialNow ?? 1_000
  const authority = createFakeGrantAuthority({
    now: () => clock,
    ratePerSecond,
    burst,
    spendHorizonMs: validForMs,
    ...(options.maximumOutstandingPermits === undefined
      ? {}
      : { maximumOutstandingStock: options.maximumOutstandingPermits }),
  })

  const coordinator = createServalRateLimitCoordinator({
    authority: authority.issuer,
    now: () => clock,
    validForMs,
    grantSize: options.grantSize,
    maximumOutstandingPermits: options.maximumOutstandingPermits,
    maximumScopes: options.maximumScopes,
    safetyMarginMs: options.safetyMarginMs,
  })

  return {
    authority,
    coordinator,
    identity: TEST_RATE_IDENTITY,
    params: { emissionIntervalMs, burstDelayMs },
    now: () => clock,
    advanceBy(ms) {
      clock += ms
    },
    whenIdle: () => coordinator.whenIdle(),
  }
}
