/**
 * Property tests for the local GCRA rate limiter (`rateLimit.local`).
 *
 * `docs/rate-limit.md` is the contract under test: the emission interval is
 * `round(1000 / rate)`, the burst tolerance is `(burst - 1) * emissionIntervalMs`,
 * one scalar of state (the theoretical arrival time) enforces both, and
 * `retryAfterMs` on a rejection is the time until the next admissible call.
 *
 * Admission is immediate-reject only, so a case is a pure function of an
 * arrival timeline on the clock - no timers, no concurrency, no coordinator.
 * Every timeline is drawn from the suite seed, so a failing case replays.
 */

import * as fc from "fast-check"
import { describe, expect, it, vi } from "vitest"
import type { OperationEvent } from "../../src/index.js"
import {
  operation,
  RateLimitExceededError,
  rateLimit,
} from "../../src/index.js"
import { createGeneratedSuite } from "../support/seed.js"

const suite = createGeneratedSuite({
  name: "rate-limit-gcra",
  command: "npm run test:property",
  maxCases: 4000,
})

const NAME = "partners"

/** Absolute arrival times on the fake clock; equal consecutive times are a burst. */
const timelineArb = fc
  .array(fc.integer({ min: 0, max: 40 }), { minLength: 1, maxLength: 12 })
  .map((advances) => {
    const times: number[] = []
    let at = 0
    for (const advance of advances) {
      at += advance
      times.push(at)
    }
    return times
  })

const specArb = fc.record({
  /**
   * Rates whose emission interval is not a whole millisecond also pin the
   * rounding rule: 7 rps resolves to 143ms and 3 rps to 333ms, so `floor`
   * instead of `round` changes which arrivals are admitted.
   */
  rate: fc.constantFrom(2, 3, 4, 5, 7, 9, 13, 20, 50, 100, 333, 999, 1000),
  burst: fc.integer({ min: 1, max: 6 }),
  times: timelineArb,
  window: fc.record({
    from: fc.integer({ min: 0, max: 120 }),
    span: fc.integer({ min: 0, max: 60 }),
  }),
})

type Spec = {
  readonly rate: number
  readonly burst: number
  readonly times: readonly number[]
  readonly window: { readonly from: number; readonly span: number }
}

/**
 * The documented arithmetic, written from `docs/rate-limit.md` rather than
 * from the policy, so the differential compares two independent transcriptions
 * of the same contract.
 */
function reference(rate: number, burst: number) {
  const emissionIntervalMs = Math.round(1000 / rate)
  const burstDelayMs = (burst - 1) * emissionIntervalMs
  let tat = 0

  return {
    emissionIntervalMs,
    burstDelayMs,
    admit(now: number): { allowed: boolean; retryAfterMs: number } {
      const anchored = Math.max(tat, now)
      if (anchored - now > burstDelayMs) {
        return { allowed: false, retryAfterMs: anchored - burstDelayMs - now }
      }
      tat = anchored + emissionIntervalMs
      return { allowed: true, retryAfterMs: 0 }
    },
  }
}

interface Attempt {
  /** The clock time of the call. */
  readonly at: number
  readonly allowed: boolean
  /** From the thrown `RateLimitExceededError`; 0 when admitted. */
  readonly retryAfterMs: number
  /** `snapshot().nextAllowedAt` immediately before the call. */
  readonly nextAllowedAt: number
}

/** A fresh policy sharing one GCRA cell with one operation. */
function subject(
  rate: number,
  burst: number,
  events?: OperationEvent[],
): {
  policy: ReturnType<typeof rateLimit.local>
  execute: () => Promise<void>
} {
  const policy = rateLimit.local({ name: NAME, rate, burst })
  const op = operation({
    name: "work",
    adapter: {
      capabilities: () => ({
        abort: "unsupported" as const,
        replay: "safe" as const,
      }),
      execute: async () => "ok",
    },
    policies: [policy],
    events: events === undefined ? undefined : { emit: (e) => events.push(e) },
  })

  return {
    policy,
    execute: async () => {
      await op.execute(undefined)
    },
  }
}

/** Calls `at` on the shared clock and records everything a property needs. */
async function attempt(
  run: {
    policy: ReturnType<typeof rateLimit.local>
    execute: () => Promise<void>
  },
  at: number,
): Promise<Attempt> {
  vi.setSystemTime(at)
  const nextAllowedAt = run.policy.snapshot().nextAllowedAt

  try {
    await run.execute()
    return { at, allowed: true, retryAfterMs: 0, nextAllowedAt }
  } catch (error) {
    expect(error).toBeInstanceOf(RateLimitExceededError)
    const rejection = error as RateLimitExceededError
    expect(rejection.retryAfterMs).toBeGreaterThan(0)
    return {
      at,
      allowed: false,
      retryAfterMs: rejection.retryAfterMs,
      nextAllowedAt,
    }
  }
}

async function replay(
  rate: number,
  burst: number,
  times: readonly number[],
  events?: OperationEvent[],
): Promise<Attempt[]> {
  const run = subject(rate, burst, events)
  const attempts: Attempt[] = []
  for (const at of times) {
    attempts.push(await attempt(run, at))
  }
  return attempts
}

/**
 * Replays `prefix` on a fresh policy, then calls once at `at`. This is how the
 * properties observe state the policy does not expose: one timeline, many
 * independent probes. Only the call at `at` can be admitted, so a probe never
 * perturbs the state the property is asking about.
 */
async function probe(
  rate: number,
  burst: number,
  prefix: readonly number[],
  at: number,
): Promise<Attempt> {
  const run = subject(rate, burst)
  for (const time of prefix) {
    await attempt(run, time)
  }
  return await attempt(run, at)
}

/**
 * Only `Date` is faked: the limiter reads the clock, it never schedules, and
 * faking the timer functions would only add a way for a case to hang.
 */
function withFakeClock<Result>(body: () => Promise<Result>): Promise<Result> {
  vi.useFakeTimers({ toFake: ["Date"] })
  return body().finally(() => {
    vi.useRealTimers()
  })
}

describe(`local GCRA rate limiter - generated arrivals (seed=${suite.seed} replay="${suite.replay}")`, () => {
  suite.itProperty(
    "admits exactly the arrivals the documented arithmetic admits",
    specArb,
    async (spec: Spec) => {
      await withFakeClock(async () => {
        const model = reference(spec.rate, spec.burst)
        const attempts = await replay(spec.rate, spec.burst, spec.times)
        for (const entry of attempts) {
          const expected = model.admit(entry.at)
          expect(
            entry.allowed,
            `at=${entry.at} rate=${spec.rate} burst=${spec.burst}`,
          ).toBe(expected.allowed)
          expect(
            entry.retryAfterMs,
            `at=${entry.at} rate=${spec.rate} burst=${spec.burst}`,
          ).toBe(expected.retryAfterMs)
        }
      })
    },
    { runs: 200 },
  )

  suite.itProperty(
    "holds the burst envelope: admission k is never before the first plus k intervals minus the burst tolerance",
    specArb,
    async (spec: Spec) => {
      await withFakeClock(async () => {
        const { emissionIntervalMs, burstDelayMs } = reference(
          spec.rate,
          spec.burst,
        )
        const attempts = await replay(spec.rate, spec.burst, spec.times)
        const admitted = attempts.filter((entry) => entry.allowed)
        const first = admitted[0]

        for (const [index, entry] of admitted.entries()) {
          if (index === 0 || first === undefined) {
            continue
          }
          expect(
            entry.at - first.at,
            `admission ${index} at=${entry.at} rate=${spec.rate} burst=${spec.burst}`,
          ).toBeGreaterThanOrEqual(index * emissionIntervalMs - burstDelayMs)
        }
      })
    },
    { runs: 200 },
  )

  suite.itProperty(
    "never admits more than burst plus one window's worth of intervals",
    specArb,
    async (spec: Spec) => {
      await withFakeClock(async () => {
        const { emissionIntervalMs } = reference(spec.rate, spec.burst)
        const attempts = await replay(spec.rate, spec.burst, spec.times)
        const { from, span } = spec.window
        const inside = attempts.filter(
          (entry) =>
            entry.allowed && entry.at >= from && entry.at <= from + span,
        ).length

        expect(
          inside,
          `window=[${from},${from + span}] span=${span} rate=${spec.rate} burst=${spec.burst}`,
        ).toBeLessThanOrEqual(
          spec.burst + Math.floor(span / emissionIntervalMs),
        )
      })
    },
    { runs: 200 },
  )

  suite.itProperty(
    "reports a retryAfterMs that is exactly the wait to the next admission",
    specArb,
    async (spec: Spec) => {
      await withFakeClock(async () => {
        const attempts = await replay(spec.rate, spec.burst, spec.times)

        for (const [index, entry] of attempts.entries()) {
          if (entry.allowed) {
            continue
          }
          const prefix = spec.times.slice(0, index)
          const context = `at=${entry.at} retryAfterMs=${entry.retryAfterMs} rate=${spec.rate} burst=${spec.burst}`

          // Waiting exactly the reported time admits the call...
          const exact = await probe(
            spec.rate,
            spec.burst,
            prefix,
            entry.at + entry.retryAfterMs,
          )
          expect(exact.allowed, `one ms too late: ${context}`).toBe(true)

          // ...and one millisecond less is still one millisecond short.
          const early = await probe(
            spec.rate,
            spec.burst,
            prefix,
            entry.at + entry.retryAfterMs - 1,
          )
          expect(early.allowed, `one ms too early: ${context}`).toBe(false)
          expect(early.retryAfterMs, `one ms too early: ${context}`).toBe(1)
        }
      })
    },
    { runs: 60 },
  )

  suite.itProperty(
    "reports a snapshot that is never in the past and never overstates by more than a burst",
    specArb,
    async (spec: Spec) => {
      await withFakeClock(async () => {
        const { burstDelayMs } = reference(spec.rate, spec.burst)
        const attempts = await replay(spec.rate, spec.burst, spec.times)

        for (const [index, entry] of attempts.entries()) {
          const context = `at=${entry.at} nextAllowedAt=${entry.nextAllowedAt} rate=${spec.rate} burst=${spec.burst}`

          // A snapshot never points at a time that has already passed.
          expect(entry.nextAllowedAt, context).toBeGreaterThanOrEqual(entry.at)

          // Conservative: a call at nextAllowedAt is admitted, so the reported
          // timestamp is a safe retry instant.
          const atSnapshot = await probe(
            spec.rate,
            spec.burst,
            spec.times.slice(0, index),
            entry.nextAllowedAt,
          )
          expect(atSnapshot.allowed, context).toBe(true)

          // Tight: on a rejection it overstates the exact wait by at most one
          // burst tolerance, so it is not a uselessly distant hint either.
          if (!entry.allowed) {
            const overstatement =
              entry.nextAllowedAt - (entry.at + entry.retryAfterMs)
            expect(overstatement, context).toBeGreaterThanOrEqual(0)
            expect(overstatement, context).toBeLessThanOrEqual(burstDelayMs)
          }
        }
      })
    },
    { runs: 60 },
  )

  suite.itProperty(
    "emits one rate-limit event per attempt, in arrival order, with the same retry hint",
    specArb,
    async (spec: Spec) => {
      await withFakeClock(async () => {
        const events: OperationEvent[] = []
        const attempts = await replay(spec.rate, spec.burst, spec.times, events)
        const rateEvents = events.filter((event) =>
          event.type.startsWith("ratelimit."),
        )

        expect(
          rateEvents.map((event) => event.type),
          `rate=${spec.rate} burst=${spec.burst}`,
        ).toEqual(
          attempts.map((entry) =>
            entry.allowed ? "ratelimit.admitted" : "ratelimit.rejected",
          ),
        )

        const rejections = rateEvents.filter(
          (event) => event.type === "ratelimit.rejected",
        )
        const rejected = attempts.filter((entry) => !entry.allowed)
        expect(rejections).toHaveLength(rejected.length)
        for (const [index, entry] of rejected.entries()) {
          expect(rejections[index]).toMatchObject({
            coordination: "local",
            policyName: NAME,
            scope: "process",
            reason: "rate-exceeded",
            retryAfterMs: entry.retryAfterMs,
          })
        }
      })
    },
    { runs: 200 },
  )
})

/**
 * Pinned histories. The generator rarely draws an exact burst boundary, so the
 * arithmetic a mutation is most likely to break is also written out by hand.
 */
describe("local GCRA rate limiter - pinned histories", () => {
  it("history: a burst admits exactly `burst` arrivals at one instant, then reports the exact wait", async () => {
    await withFakeClock(async () => {
      // 4 rps resolves to a 250ms emission interval and burst 3 to a 500ms
      // tolerance, so three arrivals may land on the same instant.
      const attempts = await replay(4, 3, [0, 0, 0, 0, 249, 250])

      expect(attempts.map((entry) => entry.allowed)).toEqual([
        true,
        true,
        true,
        false,
        false,
        true,
      ])
      // The fourth arrival waits one emission interval, and one millisecond
      // less is still one millisecond short of the catch-up time.
      expect(attempts[3]?.retryAfterMs).toBe(250)
      expect(attempts[4]?.retryAfterMs).toBe(1)
    })
  })
})
