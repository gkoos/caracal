import type { BreakerIdentity } from "../../../src/core/circuit-breaker.js"
import { mulberry32, randInt } from "../seed.js"
import {
  createDeterministicScheduler,
  type DeterministicScheduler,
} from "./deterministic-scheduler.js"
import {
  createFakeBreakerAuthority,
  type FakeBreakerAuthority,
} from "./fake-breaker-authority.js"
import { createBreakerHarness, type BreakerHarness } from "./session-harness.js"

/**
 * A seeded adversarial history for the cached-verdict breaker.
 *
 * It mixes clock advances, out-of-band transitions, reads, probe attempts,
 * settlements and refresh cycles, and checks the invariants Contract 2 promises
 * after every step against the authority's own state:
 *
 * - An admission is never fabricated: only a reply from the authority admits one.
 * - A local CLOSED admission never happens while the authority is OPEN and the
 *   client's own allowance has lapsed.
 * - A cached verdict's version never regresses.
 * - Local state stays bounded, and closing releases it.
 */

export interface BreakerHistoryOptions {
  readonly seed: number
  readonly steps: number
}

const OPEN_MS = 400
const FRESHNESS_MS = 250
const CAPACITY = 4

export async function runBreakerHistory(
  options: BreakerHistoryOptions,
): Promise<readonly string[]> {
  const { seed, steps } = options
  const rng = mulberry32(seed)
  const violations: string[] = []

  const scheduler: DeterministicScheduler = createDeterministicScheduler(1_000)
  const authority: FakeBreakerAuthority = createFakeBreakerAuthority({
    now: () => scheduler.now(),
  })
  const harness: BreakerHarness = createBreakerHarness({
    scheduler,
    authority,
    namespace: `serval-breaker-history-${seed}`,
    freshnessMs: FRESHNESS_MS,
    reconciliationIntervalMs: 2_000,
    registryCapacity: CAPACITY,
  })
  const identity: BreakerIdentity = harness.identity

  await harness.start()
  await harness.scheduler.advanceBy(0)
  await harness.session.whenIdle()

  let admitted = 0
  let lastReadAt = scheduler.now()
  let highestVersion = 0

  for (let step = 0; step < steps; step += 1) {
    const action = randInt(rng, 0, 7)
    const callsBefore = { ...authority.calls }

    if (action === 0) {
      await scheduler.advanceBy(randInt(rng, 1, FRESHNESS_MS))
    } else if (action === 1) {
      // An out-of-band transition: no hint reaches this replica.
      authority.setState(rng() < 0.5 ? "open" : "closed")
    } else if (action === 2) {
      const state = await harness.coordinator.readState(identity)
      if (state?.state === "closed") {
        // A local CLOSED admission is only sound while the client's own allowance
        // is current, or while the authority itself says CLOSED.
        const fresh = scheduler.now() - lastReadAt <= FRESHNESS_MS
        if (authority.state() !== "closed" && !fresh) {
          violations.push(
            `step ${step}: admitted from a stale CLOSED verdict while the authority was ${authority.state()}`,
          )
        }
      }
    } else if (action === 3) {
      const reply = await harness.coordinator.admitProbe(identity, {
        probeToken: `probe-${step}`,
        openMs: OPEN_MS,
        halfOpenProbes: 2,
        probeLeaseTtlMs: 200,
      })
      if (reply.type === "admitted") {
        admitted += 1
      }
    } else if (action === 4) {
      await harness.coordinator.settleProbe(identity, {
        probeToken: `probe-${step}`,
        outcome: rng() < 0.5 ? "success" : "failure",
        generation: authority.generation(),
        halfOpenSuccesses: 1,
        openMs: OPEN_MS,
        closedCleanupMs: 2_000,
        windowTtlMs: 2_000,
      })
    } else if (action === 5) {
      await harness.coordinator.observe(identity, {
        generation: authority.generation(),
        outcome: rng() < 0.5 ? "failure" : "success",
        uuid: `uuid-${step}`,
        windowTtlMs: 2_000,
        minimumThroughput: 2,
        failureThresholdNumerator: 500,
        windowSize: 20,
        openMs: OPEN_MS,
        closedCleanupMs: 2_000,
      })
    } else if (action === 6) {
      await harness.session.refresh("invalidation")
    } else {
      harness.notifications.disconnect()
      await scheduler.advanceBy(randInt(rng, 0, 100))
      harness.notifications.reconnect()
    }

    await scheduler.advanceBy(randInt(rng, 0, 20))
    await harness.session.whenIdle()

    // Any interaction with the authority can refresh the local verdict: a read, a
    // settlement reply, a probe, or a reconciliation through the reader.
    const interacted =
      authority.calls.readState > callsBefore.readState ||
      authority.calls.observe > callsBefore.observe ||
      authority.calls.admitProbe > callsBefore.admitProbe ||
      authority.calls.settleProbe > callsBefore.settleProbe
    if (interacted) {
      lastReadAt = scheduler.now()
    }

    // An admission always came from the authority.
    if (admitted > authority.calls.admitProbe) {
      violations.push(
        `step ${step}: ${admitted} admissions from ${authority.calls.admitProbe} probes`,
      )
    }

    const cached = harness.coordinator.cached(identity)
    if (cached !== undefined) {
      const version =
        cached.generation * 2 + (cached.state === "half-open" ? 1 : 0)
      if (version < highestVersion) {
        violations.push(
          `step ${step}: the cached verdict regressed to ${version}`,
        )
      }
      highestVersion = version
    }

    if (harness.session.scopeCount > CAPACITY) {
      violations.push(`step ${step}: the registry grew past its capacity`)
    }
    if (scheduler.pending() > 2) {
      violations.push(`step ${step}: ${scheduler.pending()} timers pending`)
    }
  }

  await harness.close()
  if (scheduler.pending() !== 0) {
    violations.push("close left timers pending")
  }
  if (harness.session.scopeCount !== 0) {
    violations.push("close left scopes registered")
  }

  return violations
}
