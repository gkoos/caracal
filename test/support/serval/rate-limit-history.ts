import { ServalRegistryPressureError } from "../../../src/coordination/serval/cache.js"
import { mulberry32, randInt } from "../seed.js"
import {
  TEST_RATE_IDENTITY,
  createRateLimitHarness,
  type RateLimitHarness,
} from "./rate-limit-harness.js"

/**
 * A seeded adversarial history for the delegated rate limiter.
 *
 * It mixes clock advances, single and concurrent admissions, ambiguous replies,
 * outages, late replies and several scopes, and checks Contract 8's invariants
 * after every step against the authority the reference model implements:
 *
 * - No local admission beyond granted quota, and no permit created locally: every
 *   admitted, held or discarded permit came from an issuance.
 * - An admission never uses stock past its send-time-anchored deadline.
 * - A refusal always carries a positive retry-after hint, an admission never does.
 * - Admissions satisfy the rolling-window bound `R * (L + V) + B`, including
 *   permits issued earlier and spent just before they expired.
 * - The registry stays bounded, and no work is left pending.
 *
 * The model's own token bucket bounds *issuance*; the bound on admissions follows
 * from it only because no permit outlives its horizon, which is exactly what this
 * history is built to break.
 */

export interface RateLimitHistoryOptions {
  readonly seed: number
  readonly steps: number
}

const RATE_PER_SECOND = 200
const BURST = 4
const GRANT_SIZE = 2
const VALID_FOR_MS = 150
const SCOPES = ["tenant-a", "tenant-b", "tenant-c"] as const
const MAXIMUM_SCOPES = SCOPES.length

/** The largest number of admissions in any window of length `intervalMs`. */
function maxAdmissionsInWindow(
  admissions: readonly number[],
  intervalMs: number,
): number {
  let maximum = 0
  for (const end of admissions) {
    for (const start of [end - intervalMs, end]) {
      let count = 0
      for (const at of admissions) {
        if (at >= start && at <= start + intervalMs) count += 1
      }
      maximum = Math.max(maximum, count)
    }
  }
  return maximum
}

export async function runRateLimitHistory(
  options: RateLimitHistoryOptions,
): Promise<readonly string[]> {
  const { seed, steps } = options
  const rng = mulberry32(seed)
  const violations: string[] = []

  const harness: RateLimitHarness = createRateLimitHarness({
    ratePerSecond: RATE_PER_SECOND,
    burst: BURST,
    grantSize: GRANT_SIZE,
    validForMs: VALID_FOR_MS,
    maximumScopes: MAXIMUM_SCOPES,
  })
  const admissions: number[] = []
  const identities = SCOPES.map((scope) => ({ ...TEST_RATE_IDENTITY, scope }))

  for (let step = 0; step < steps; step += 1) {
    const identity = identities[randInt(rng, 0, identities.length - 1)]
    // The first step always admits or is refused, so every history exercises the
    // quota path whatever the seed is.
    const action = step === 0 ? 5 : randInt(rng, 0, 9)
    const startedAt = harness.now()
    const metricsBefore = harness.coordinator.metrics()
    const errors: unknown[] = []
    const decisions: {
      readonly allowed: boolean
      readonly retryAfterMs: number
    }[] = []

    if (action === 0) {
      harness.advanceBy(randInt(rng, 0, 40))
    } else if (action === 1) {
      harness.authority.failNext(1)
    } else if (action === 2) {
      harness.authority.commitThenFailNext()
    } else if (action === 3) {
      // A reply held past the window it was anchored to.
      harness.authority.holdNextIssue()
      const pending = harness.coordinator
        .command(identity, harness.params)
        .catch((error: unknown) => {
          errors.push(error)
          return undefined
        })
      harness.advanceBy(VALID_FOR_MS + randInt(rng, 0, 50))
      harness.authority.releaseIssue()
      const held = await pending
      if (held !== undefined) decisions.push(held)
    } else {
      // Single admissions, and concurrent bursts that have to share one issuance.
      const burst = randInt(rng, 1, 5)
      const settled = await Promise.all(
        Array.from({ length: burst }, () =>
          harness.coordinator
            .command(identity, harness.params)
            .catch((error: unknown) => {
              errors.push(error)
              return undefined
            }),
        ),
      )
      for (const decision of settled) {
        if (decision !== undefined) decisions.push(decision)
      }
    }

    await harness.whenIdle()
    const metrics = harness.coordinator.metrics()

    // A command throws for exactly two reasons: the registry could not hold the
    // scope, or the authority could not decide. Anything else would be local state
    // turning into an exception, which is what the other invariants assume cannot
    // happen. The converse of the second case does not hold: a background refill
    // nobody is waiting on can fail silently, and it is counted either way.
    const newFailures = metrics.failures - metricsBefore.failures
    const pressure = errors.filter(
      (error) => error instanceof ServalRegistryPressureError,
    ).length
    if (errors.length - pressure > 0 && newFailures < 1) {
      const first = errors.find(
        (error) => !(error instanceof ServalRegistryPressureError),
      )
      violations.push(
        `step ${step}: ${errors.length - pressure} commands threw with ${newFailures} issuance failures and ${metrics.issuances - metricsBefore.issuances} issuances (${first instanceof Error ? first.message : String(first)})`,
      )
    }
    if (
      pressure !==
      metrics.registryRefusals - metricsBefore.registryRefusals
    ) {
      violations.push(
        `step ${step}: ${pressure} registry refusals reported as ${metrics.registryRefusals - metricsBefore.registryRefusals}`,
      )
    }

    for (const decision of decisions) {
      if (decision.allowed) {
        if (decision.retryAfterMs !== 0) {
          violations.push(
            `step ${step}: an admission carried a retry-after hint`,
          )
        }
        admissions.push(startedAt)
        const diagnostics = harness.coordinator.diagnostics(identity)
        if (diagnostics === undefined || diagnostics.deadline <= startedAt) {
          violations.push(
            `step ${step}: admitted from stock whose deadline had passed`,
          )
        }
      } else if (decision.retryAfterMs < 1) {
        violations.push(
          `step ${step}: a refusal without a positive retry-after hint`,
        )
      }
    }

    // Conservation: every adopted permit is spent, still held, or discarded. A
    // permit that is none of those was created locally.
    const held = identities.reduce(
      (total, candidate) =>
        total + (harness.coordinator.diagnostics(candidate)?.stock ?? 0),
      0,
    )
    if (
      metrics.admissions + held + metrics.permitsExpired !==
      metrics.permitsGranted
    ) {
      violations.push(
        `step ${step}: ${metrics.admissions} admitted + ${held} held + ${metrics.permitsExpired} discarded != ${metrics.permitsGranted} granted`,
      )
    }

    // No admission beyond granted quota, counting what the authority actually
    // issued: a duplicate reply returns a recorded grant and mints nothing.
    if (metrics.admissions > harness.authority.permitsIssued()) {
      violations.push(
        `step ${step}: ${metrics.admissions} admissions from ${harness.authority.permitsIssued()} issued permits`,
      )
    }

    if (metrics.scopes > MAXIMUM_SCOPES) {
      violations.push(`step ${step}: the registry grew past its capacity`)
    }

    // The documented rolling-window envelope, over windows the history actually
    // spans: permits issued before the window can be spent inside it, so the bound
    // is stated over an interval of length L + V.
    for (const intervalMs of [50, 150, 300]) {
      const observed = maxAdmissionsInWindow(admissions, intervalMs)
      const bound =
        (RATE_PER_SECOND * (intervalMs + VALID_FOR_MS)) / 1000 + BURST
      if (observed > bound) {
        violations.push(
          `step ${step}: ${observed} admissions in ${intervalMs}ms exceed the bound ${bound}`,
        )
      }
    }
  }

  await harness.whenIdle()
  const finalMetrics = harness.coordinator.metrics()
  if (finalMetrics.admissions === 0 && finalMetrics.refused === 0) {
    violations.push(
      "the history neither admitted nor refused, so it proved nothing",
    )
  }

  return violations
}
