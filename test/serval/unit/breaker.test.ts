/**
 * Unit tests for the cached-verdict circuit breaker.
 *
 * `docs/serval/README.md` Contract 2 is the contract: a warm valid CLOSED verdict admits
 * with no authority read, a cached OPEN rejects locally until its cooldown
 * elapses, and only then does one coalesced probe reach the authority - which
 * stays authoritative for every transition. The strict coordinator is a scriptable
 * stand-in here, so every test can count the round trips a path actually costs.
 */

import { describe, expect, it } from "vitest"
import {
  circuitBreaker,
  CircuitOpenError,
  operation,
} from "../../../src/index.js"
import type { BreakerIdentity, OperationEvent } from "../../../src/index.js"
import { ServalRegistryPressureError } from "../../../src/serval/cache.js"
import { ServalAuthorityRefusalError } from "../../../src/serval/breaker.js"
import { createDeterministicScheduler } from "../../support/serval/deterministic-scheduler.js"
import { createFakeBreakerAuthority } from "../../support/serval/fake-breaker-authority.js"
import {
  TEST_BREAKER_IDENTITY,
  createBreakerHarness,
  type BreakerHarness,
} from "../../support/serval/session-harness.js"

const OPEN_MS = 5_000
const PROBE_LEASE_MS = 1_000

let probeSequence = 0

function probeParams(overrides: { readonly openMs?: number } = {}) {
  probeSequence += 1
  return {
    probeToken: `probe-${probeSequence}`,
    openMs: overrides.openMs ?? OPEN_MS,
    halfOpenProbes: 3,
    probeLeaseTtlMs: PROBE_LEASE_MS,
  }
}

let observeSequence = 0

function observeParams(
  identity: BreakerIdentity,
  generation: number,
  overrides: { readonly outcome?: "success" | "failure" } = {},
) {
  observeSequence += 1
  void identity
  return {
    generation,
    outcome: overrides.outcome ?? "failure",
    uuid: `uuid-${observeSequence}`,
    windowTtlMs: 60_000,
    minimumThroughput: 1,
    failureThresholdNumerator: 500,
    windowSize: 20,
    openMs: OPEN_MS,
    closedCleanupMs: 60_000,
  } as const
}

describe("serval breaker fast path", () => {
  it("admits from a warm valid CLOSED verdict with no authority read", async () => {
    const harness = createBreakerHarness({ freshnessMs: 10_000 })
    await harness.start()
    harness.authority.reset()

    for (let index = 0; index < 25; index += 1) {
      expect(
        await harness.coordinator.readState(harness.identity),
      ).toMatchObject({ state: "closed" })
    }

    // The whole point of the cached verdict: the ordinary admission path costs nothing.
    expect(harness.authority.calls.readState).toBe(0)
  })

  it("reads the authority while no incarnation has been adopted", async () => {
    const harness = createBreakerHarness({ freshnessMs: 10_000 })
    // Not started: the session has adopted no authority, so nothing may be cached.
    expect(await harness.coordinator.readState(harness.identity)).toMatchObject(
      {
        state: "closed",
      },
    )
    expect(harness.authority.calls.readState).toBe(1)

    await harness.coordinator.readState(harness.identity)
    expect(harness.authority.calls.readState).toBe(2)
  })

  it("rejects a cached OPEN locally before its cooldown elapses", async () => {
    const harness = createBreakerHarness({ freshnessMs: 1_000 })
    await harness.start()

    harness.authority.setState("open")
    await harness.session.refresh("periodic")
    harness.authority.reset()

    expect(await harness.coordinator.readState(harness.identity)).toMatchObject(
      {
        state: "open",
        generation: 1,
      },
    )
    expect(
      await harness.coordinator.admitProbe(harness.identity, probeParams()),
    ).toMatchObject({ type: "rejected", reason: "open", generation: 1 })

    // A protective rejection is local: neither the read nor the probe was issued.
    expect(harness.authority.calls.readState).toBe(0)
    expect(harness.authority.calls.admitProbe).toBe(0)
  })

  it("probes authoritatively after the cooldown and coalesces while it is in flight", async () => {
    const harness = createBreakerHarness({ freshnessMs: 1_000 })
    await harness.start()

    harness.authority.setState("open")
    await harness.session.refresh("periodic")
    harness.authority.reset()

    await harness.scheduler.advanceBy(OPEN_MS + 100)

    harness.authority.holdNextProbe()
    const first = harness.coordinator.admitProbe(
      harness.identity,
      probeParams(),
    )
    const second = await harness.coordinator.admitProbe(
      harness.identity,
      probeParams(),
    )

    expect(second).toMatchObject({ type: "rejected", reason: "probe-limit" })
    harness.authority.releaseProbe()
    expect(await first).toMatchObject({ type: "admitted" })

    // One authoritative probe, not three.
    expect(harness.authority.calls.admitProbe).toBe(1)
    // The admitted probe refines the cached verdict to HALF_OPEN.
    expect(harness.coordinator.cached(harness.identity)).toMatchObject({
      state: "half-open",
      generation: 1,
    })
  })

  it("delegates a cached HALF_OPEN to the authority, which owns the probe budget", async () => {
    const harness = createBreakerHarness({ freshnessMs: 1_000 })
    await harness.start()

    harness.authority.setState("open")
    await harness.session.refresh("periodic")
    await harness.scheduler.advanceBy(OPEN_MS + 100)

    expect(
      await harness.coordinator.admitProbe(harness.identity, probeParams()),
    ).toMatchObject({ type: "admitted" })
    // The verdict is HALF_OPEN now, so the next attempt is authoritative: the strict
    // probe budget, not a local flag, decides how many probes may be in flight.
    expect(
      await harness.coordinator.admitProbe(harness.identity, probeParams()),
    ).toMatchObject({ type: "admitted" })
    expect(harness.authority.calls.admitProbe).toBe(2)
  })

  it("adopts the reopen after a failed probe, so it stops probing until the cooldown", async () => {
    const harness = createBreakerHarness({ freshnessMs: 1_000 })
    await harness.start()

    harness.authority.setState("open")
    await harness.session.refresh("periodic")
    await harness.scheduler.advanceBy(OPEN_MS + 100)
    harness.authority.reset()

    // One probe is admitted, which refines the cached verdict to HALF_OPEN.
    const admitted = await harness.coordinator.admitProbe(
      harness.identity,
      probeParams(),
    )
    expect(admitted).toMatchObject({ type: "admitted" })
    expect(harness.coordinator.cached(harness.identity)).toMatchObject({
      state: "half-open",
    })

    // The probe fails, so the authority reopens the breaker in a new generation -
    // the script does this with `gen = gen + 1` and discards the probe set.
    harness.authority.setProbeOutcome("failure")
    const settled = await harness.coordinator.settleProbe(harness.identity, {
      probeToken: `probe-${probeSequence}`,
      outcome: "failure",
      generation: (admitted as { generation: number }).generation,
      halfOpenSuccesses: 1,
      openMs: OPEN_MS,
      closedCleanupMs: 60_000,
      windowTtlMs: 60_000,
    })
    expect(settled).toMatchObject({ type: "transitioned", newState: "open" })

    // The replica must adopt the reopen. If it does not, the cached verdict stays
    // HALF_OPEN, every later attempt is delegated, and a failing dependency mints a
    // new generation per failed probe - which is a probe storm, not a recovery.
    expect(harness.coordinator.cached(harness.identity)).toMatchObject({
      state: "open",
    })

    // And with OPEN cached, the next attempt is refused locally.
    harness.authority.reset()
    expect(
      await harness.coordinator.admitProbe(harness.identity, probeParams()),
    ).toMatchObject({ type: "rejected", reason: "open" })
    expect(harness.authority.calls.admitProbe).toBe(0)
  })
})

describe("serval breaker recovery and convergence", () => {
  it("recovers every cached-OPEN replica through one authoritative probe, with no observation", async () => {
    const scheduler = createDeterministicScheduler(1_000)
    const authority = createFakeBreakerAuthority({ now: () => scheduler.now() })
    const namespace = "serval-breaker-replicas"
    const replicas = ["a", "b", "c"].map((id) =>
      createBreakerHarness({
        scheduler,
        authority,
        namespace,
        replicaId: `replica-${id}`,
        freshnessMs: 1_000,
      }),
    )
    for (const replica of replicas) {
      await replica.start()
    }

    // The breaker opens out of band: every replica converges through its reader,
    // and not one of them observed the failure that opened it.
    authority.setState("open")
    for (const replica of replicas) {
      await replica.session.refresh("periodic")
    }
    for (const replica of replicas) {
      expect(
        await replica.coordinator.readState(replica.identity),
      ).toMatchObject({
        state: "open",
      })
    }

    // Before the cooldown every replica refuses locally.
    const first = replicas[0] as BreakerHarness
    const second = replicas[1] as BreakerHarness
    for (const replica of replicas) {
      authority.reset()
      expect(
        await replica.coordinator.admitProbe(replica.identity, probeParams()),
      ).toMatchObject({ type: "rejected", reason: "open" })
      expect(authority.calls.admitProbe).toBe(0)
    }

    // After it, one replica's probe reaches the authority and closes the breaker.
    await scheduler.advanceBy(OPEN_MS + 100)
    authority.reset()
    expect(
      await first.coordinator.admitProbe(first.identity, probeParams()),
    ).toMatchObject({ type: "admitted" })
    expect(
      await first.coordinator.settleProbe(first.identity, {
        probeToken: "probe-recovery",
        outcome: "success",
        generation: 1,
        halfOpenSuccesses: 1,
        openMs: OPEN_MS,
        closedCleanupMs: 60_000,
        windowTtlMs: 60_000,
      }),
    ).toMatchObject({ type: "transitioned", newState: "closed" })
    expect(authority.state()).toBe("closed")

    // The transition was announced, so the other replicas converge without waiting
    // for the reconciliation interval.
    await first.session.whenIdle()
    expect(first.published).toHaveLength(1)
    second.notifications.deliverInvalidation(first.published[0] as never)
    await scheduler.advanceBy(0)
    await second.session.whenIdle()

    authority.reset()
    expect(await second.coordinator.readState(second.identity)).toMatchObject({
      state: "closed",
    })
    expect(authority.calls.readState).toBe(0)
  })

  it("keeps a known OPEN protective through an authority outage", async () => {
    const harness = createBreakerHarness({ freshnessMs: 1_000 })
    await harness.start()

    harness.authority.setState("open")
    await harness.session.refresh("periodic")
    harness.authority.reset()
    harness.authority.failNext(100)

    // The cached verdict is protective: it is served without a call, so the policy
    // rejects instead of admitting into a known-open dependency.
    expect(await harness.coordinator.readState(harness.identity)).toMatchObject(
      {
        state: "open",
        generation: 1,
      },
    )
    expect(harness.authority.calls.readState).toBe(0)

    // A probe cannot be answered during the outage, so the failure reaches the
    // policy, which applies its own degraded behaviour.
    await harness.scheduler.advanceBy(OPEN_MS + 100)
    await expect(
      harness.coordinator.admitProbe(harness.identity, probeParams()),
    ).rejects.toThrow("unavailable")
  })

  it("propagates an outage for a scope it holds no verdict for", async () => {
    const harness = createBreakerHarness({ freshnessMs: 1_000 })
    await harness.start()
    harness.authority.failNext(100)

    const cold: BreakerIdentity = { ...TEST_BREAKER_IDENTITY, scope: "cold" }
    await expect(harness.coordinator.readState(cold)).rejects.toThrow(
      "unavailable",
    )
  })

  it("converges to the authority's verdict once it answers again", async () => {
    const harness = createBreakerHarness({ freshnessMs: 1_000 })
    await harness.start()
    // Let the refresh that registration asked for run, so the sequence below is
    // about the stale-CLOSED path and not about that pending read.
    await harness.scheduler.advanceBy(0)
    await harness.session.whenIdle()

    // An out-of-band transition with no hint: the cached CLOSED verdict is served
    // until its allowance lapses.
    harness.authority.setState("open")
    await harness.scheduler.advanceBy(1_100)

    harness.authority.failNext(1)
    await expect(
      harness.coordinator.readState(harness.identity),
    ).rejects.toThrow("unavailable")

    harness.authority.reset()
    expect(await harness.coordinator.readState(harness.identity)).toMatchObject(
      {
        state: "open",
        generation: 1,
      },
    )
    expect(harness.authority.calls.readState).toBe(1)
    // Cached now: the next admission costs nothing.
    expect(await harness.coordinator.readState(harness.identity)).toMatchObject(
      {
        state: "open",
      },
    )
    expect(harness.authority.calls.readState).toBe(1)
  })
})

describe("serval breaker settlement and degraded behaviour", () => {
  it("keeps settlement awaited and authoritative, and refreshes from its reply", async () => {
    const harness = createBreakerHarness({ freshnessMs: 10_000 })
    await harness.start()
    harness.authority.reset()

    await harness.coordinator.observe(
      harness.identity,
      observeParams(harness.identity, 0, { outcome: "success" }),
    )
    expect(harness.authority.calls.observe).toBe(1)

    // The settlement's own reply refreshed the verdict, so the admission is local.
    expect(await harness.coordinator.readState(harness.identity)).toMatchObject(
      {
        state: "closed",
      },
    )
    expect(harness.authority.calls.readState).toBe(0)
  })

  it("caches an opening observation and announces the transition", async () => {
    const harness = createBreakerHarness({ freshnessMs: 1_000 })
    await harness.start()

    const reply = await harness.coordinator.observe(
      harness.identity,
      observeParams(harness.identity, 0),
    )
    expect(reply).toMatchObject({ type: "opened" })
    expect(harness.coordinator.cached(harness.identity)).toMatchObject({
      state: "open",
      generation: 1,
    })

    await harness.session.whenIdle()
    expect(harness.published).toHaveLength(1)
    expect(harness.published[0]).toMatchObject({
      kind: "authority",
      version: { version: 2 },
    })
  })

  it("follows the configured stale-CLOSED behaviour", async () => {
    // refresh (the default): one read, and the refreshed verdict is then local.
    const refreshing = createBreakerHarness({ freshnessMs: 100 })
    await refreshing.start()
    await refreshing.scheduler.advanceBy(1_000)
    refreshing.authority.reset()
    expect(
      await refreshing.coordinator.readState(refreshing.identity),
    ).toMatchObject({ state: "closed" })
    expect(refreshing.authority.calls.readState).toBe(1)
    expect(
      await refreshing.coordinator.readState(refreshing.identity),
    ).toMatchObject({ state: "closed" })
    expect(refreshing.authority.calls.readState).toBe(1)

    // fail-open: admits from the cached verdict with no call at all.
    const failingOpen = createBreakerHarness({
      freshnessMs: 100,
      staleClosed: "fail-open",
    })
    await failingOpen.start()
    await failingOpen.scheduler.advanceBy(1_000)
    failingOpen.authority.reset()
    expect(
      await failingOpen.coordinator.readState(failingOpen.identity),
    ).toMatchObject({ state: "closed" })
    expect(failingOpen.authority.calls.readState).toBe(0)

    // fail-closed: refuses with no call, so the policy applies its own behaviour.
    const failingClosed = createBreakerHarness({
      freshnessMs: 100,
      staleClosed: "fail-closed",
    })
    await failingClosed.start()
    await failingClosed.scheduler.advanceBy(1_000)
    failingClosed.authority.reset()
    await expect(
      failingClosed.coordinator.readState(failingClosed.identity),
    ).rejects.toThrow("freshness allowance")
    expect(failingClosed.authority.calls.readState).toBe(0)
  })

  it("measures a delayed settlement's freshness from the send time", async () => {
    // Every settlement writes the local window, and it must be measured from the request's send
    // time. Measured from the reply's arrival instead, a delayed settlement leaves the replica
    // trusting the verdict for longer than the authority granted, which is the direction that
    // matters: the window is what decides whether a cached all-clear may still be served.
    const scheduler = createDeterministicScheduler(1_000)
    const authority = createFakeBreakerAuthority({ now: () => scheduler.now() })
    const delay = async <T>(pending: Promise<T>): Promise<T> => {
      await scheduler.advanceBy(50)
      return pending
    }
    const harness = createBreakerHarness({
      scheduler,
      freshnessMs: 100,
      staleClosed: "fail-closed",
      authority: {
        ...authority,
        coordinator: {
          ...authority.coordinator,
          readState: (
            ...args: Parameters<typeof authority.coordinator.readState>
          ) => delay(authority.coordinator.readState(...args)),
          observe: (
            ...args: Parameters<typeof authority.coordinator.observe>
          ) => delay(authority.coordinator.observe(...args)),
        },
      },
    })
    await harness.start()

    const policy = circuitBreaker.distributed({
      name: TEST_BREAKER_IDENTITY.name,
      coordinator: harness.coordinator,
      scope: () => TEST_BREAKER_IDENTITY.scope,
    })
    const op = operation({
      name: TEST_BREAKER_IDENTITY.operation,
      adapter: {
        capabilities: () => ({ abort: "supported", replay: "safe" }) as const,
        execute: async () => "ok",
      },
      policies: [policy],
    })

    // One successful operation. The admission is served from the warm verdict, and the
    // settlement is sent at this instant and answered 50ms later.
    const sentAt = harness.scheduler.now()
    await expect(op.execute(undefined)).resolves.toBe("ok")
    expect(harness.scheduler.now()).toBe(sentAt + 50)

    // The settlement's window ends at sentAt + 100 - 25 = sentAt + 75. At sentAt + 100 it has
    // lapsed, so the configured behaviour applies and nothing is read. A window measured from
    // the reply's arrival would run to sentAt + 150 and would still be served here.
    await harness.scheduler.advanceBy(50)
    harness.authority.reset()
    await expect(
      harness.coordinator.readState(harness.identity),
    ).rejects.toThrow("freshness allowance")
    expect(harness.authority.calls.readState).toBe(0)
  })

  it("refuses a scope the session's registry cannot hold", async () => {
    const harness = createBreakerHarness({
      freshnessMs: 1_000,
      registryCapacity: 1,
    })
    await harness.start()
    await harness.coordinator.readState(harness.identity)

    const second: BreakerIdentity = { ...TEST_BREAKER_IDENTITY, scope: "other" }
    // The registry's refusal is a decision, so the coordinator reports it as the refusal a policy
    // reads, keeping the registry error as the cause.
    await expect(harness.coordinator.readState(second)).rejects.toMatchObject({
      name: "ServalAuthorityRefusalError",
      cause: expect.any(ServalRegistryPressureError),
    })
  })
})

describe("serval breaker in the policy path", () => {
  it("rejects a call after the session is closed", async () => {
    // A closed session is a decision, not an outage: the coordinator refuses, and the policy must
    // reject rather than apply onCoordinatorError, whose default would run the adapter.
    const harness = createBreakerHarness({ freshnessMs: 100 })
    await harness.start()
    await harness.coordinator.readState(harness.identity)
    await harness.close()

    await expect(
      harness.coordinator.readState(harness.identity),
    ).rejects.toBeInstanceOf(ServalAuthorityRefusalError)

    const policy = circuitBreaker.distributed({
      name: TEST_BREAKER_IDENTITY.name,
      coordinator: harness.coordinator,
      scope: () => TEST_BREAKER_IDENTITY.scope,
    })
    let ran = 0
    const op = operation({
      name: TEST_BREAKER_IDENTITY.operation,
      adapter: {
        capabilities: () => ({ abort: "supported", replay: "safe" }) as const,
        execute: async () => {
          ran += 1
          return "ok"
        },
      },
      policies: [policy],
    })

    await expect(op.execute(undefined)).rejects.toBeInstanceOf(CircuitOpenError)
    expect(ran).toBe(0)
  })

  it("rejects a call when the registry cannot hold the scope", async () => {
    // The registry refuses rather than dropping live authority, and that refusal is a decision:
    // the policy rejects instead of admitting a call the coordinator could not track.
    const harness = createBreakerHarness({
      freshnessMs: 100,
      registryCapacity: 1,
    })
    await harness.start()
    // Registers the first scope, so the registry has no room for a second.
    await harness.coordinator.readState(harness.identity)

    const other = { ...TEST_BREAKER_IDENTITY, scope: "tenant-b" }
    const policy = circuitBreaker.distributed({
      name: TEST_BREAKER_IDENTITY.name,
      coordinator: harness.coordinator,
      scope: () => other.scope,
    })
    let ran = 0
    const op = operation({
      name: TEST_BREAKER_IDENTITY.operation,
      adapter: {
        capabilities: () => ({ abort: "supported", replay: "safe" }) as const,
        execute: async () => {
          ran += 1
          return "ok"
        },
      },
      policies: [policy],
    })

    await expect(op.execute(undefined)).rejects.toBeInstanceOf(CircuitOpenError)
    expect(ran).toBe(0)
  })

  it("rejects a call when its read arrives after the freshness allowance", async () => {
    // A late reply is not authority: the session refuses to adopt it, so the admission must not
    // borrow its freshness. Measured from the send time, the reply below outlives the window the
    // read was granted - and the cached verdict it would have refreshed is already stale.
    const scheduler = createDeterministicScheduler(1_000)
    const authority = createFakeBreakerAuthority({ now: () => scheduler.now() })
    let reads = 0
    const harness = createBreakerHarness({
      scheduler,
      freshnessMs: 100,
      staleClosed: "refresh",
      authority: {
        ...authority,
        coordinator: {
          ...authority.coordinator,
          readState: async (
            ...args: Parameters<typeof authority.coordinator.readState>
          ) => {
            reads += 1
            const reply = await authority.coordinator.readState(...args)
            // Reads 1 and 2 are the start's and the warming read. The third is the one an
            // admission needs, and it lands after the allowance has passed.
            if (reads > 2) await scheduler.advanceBy(200)
            return reply
          },
        },
      },
    })
    await harness.start()

    // Warm the cache, then let the window lapse so the admission has to read.
    await harness.coordinator.readState(harness.identity)
    await scheduler.advanceBy(2_000)

    const policy = circuitBreaker.distributed({
      name: TEST_BREAKER_IDENTITY.name,
      coordinator: harness.coordinator,
      scope: () => TEST_BREAKER_IDENTITY.scope,
    })
    let ran = 0
    const op = operation({
      name: TEST_BREAKER_IDENTITY.operation,
      adapter: {
        capabilities: () => ({ abort: "supported", replay: "safe" }) as const,
        execute: async () => {
          ran += 1
          return "ok"
        },
      },
      policies: [policy],
    })

    await expect(op.execute(undefined)).rejects.toBeInstanceOf(CircuitOpenError)
    expect(ran).toBe(0)
  })

  it("rejects a call when a newer protective verdict supersedes its read", async () => {
    // The read was in flight when the breaker opened, so the authority's answer describes a past
    // state that a verdict this replica already holds supersedes. The session refuses to adopt it,
    // and the admission must respect the cached OPEN rather than the reply it just rejected.
    const scheduler = createDeterministicScheduler(1_000)
    const authority = createFakeBreakerAuthority({ now: () => scheduler.now() })
    const older = await authority.coordinator.readState({
      name: TEST_BREAKER_IDENTITY.name,
      operation: TEST_BREAKER_IDENTITY.operation,
      scope: TEST_BREAKER_IDENTITY.scope,
    })
    let reads = 0
    let readStarted: (() => void) | undefined
    const started = new Promise<void>((resolve) => {
      readStarted = resolve
    })
    let release: (() => void) | undefined
    const harness = createBreakerHarness({
      scheduler,
      freshnessMs: 100,
      staleClosed: "refresh",
      authority: {
        ...authority,
        coordinator: {
          ...authority.coordinator,
          readState: async (
            ...args: Parameters<typeof authority.coordinator.readState>
          ) => {
            reads += 1
            // Reads 1 and 2 are the start's and the warming read. The third is the one the
            // admission needs: it is held so the opening can land while it is in flight, and it
            // then answers with the state it was sent with.
            if (reads <= 2) return authority.coordinator.readState(...args)
            readStarted?.()
            await new Promise<void>((resolve) => {
              release = resolve
            })
            return older
          },
        },
      },
    })
    await harness.start()

    await harness.coordinator.readState(harness.identity)
    await scheduler.advanceBy(2_000)

    const policy = circuitBreaker.distributed({
      name: TEST_BREAKER_IDENTITY.name,
      coordinator: harness.coordinator,
      scope: () => TEST_BREAKER_IDENTITY.scope,
    })
    let ran = 0
    const op = operation({
      name: TEST_BREAKER_IDENTITY.operation,
      adapter: {
        capabilities: () => ({ abort: "supported", replay: "safe" }) as const,
        execute: async () => {
          ran += 1
          return "ok"
        },
      },
      policies: [policy],
    })

    const pending = op.execute(undefined)
    await started

    // A protective update lands while the admission's read is in flight.
    await harness.coordinator.observe(
      harness.identity,
      observeParams(harness.identity, 0),
    )
    expect(harness.coordinator.cached(harness.identity)).toMatchObject({
      state: "open",
      generation: 1,
    })

    release?.()
    await expect(pending).rejects.toBeInstanceOf(CircuitOpenError)
    expect(ran).toBe(0)
    // The reply was superseded, so the cached verdict stands.
    expect(harness.coordinator.cached(harness.identity)).toMatchObject({
      state: "open",
      generation: 1,
    })
  })

  it("does not move the deadline for a settlement the authority has superseded", async () => {
    // A settlement whose verdict is older than the one this replica holds verified nothing about
    // now, so it must not move the deadline. Extending it there is what lets a replica admit past
    // the window the authority granted.
    const scheduler = createDeterministicScheduler(1_000)
    const authority = createFakeBreakerAuthority({
      now: () => scheduler.now(),
      state: "closed",
      generation: 1,
    })
    const harness = createBreakerHarness({
      scheduler,
      freshnessMs: 100,
      staleClosed: "fail-closed",
      authority: {
        ...authority,
        coordinator: {
          ...authority.coordinator,
          // This replica's view is ahead, so its own settlement reports the older generation.
          async observe() {
            return {
              type: "observed",
              generation: 0,
              windowTotal: 1,
              windowFailures: 0,
            } as const
          },
        },
      },
    })
    await harness.start()

    const policy = circuitBreaker.distributed({
      name: TEST_BREAKER_IDENTITY.name,
      coordinator: harness.coordinator,
      scope: () => TEST_BREAKER_IDENTITY.scope,
    })
    const op = operation({
      name: TEST_BREAKER_IDENTITY.operation,
      adapter: {
        capabilities: () => ({ abort: "supported", replay: "safe" }) as const,
        execute: async () => "ok",
      },
      policies: [policy],
    })

    // Admitted from the warm verdict, and settled at 1060 with the superseded reply. The cold
    // start read at 1000 earned a window ending at 1075, and the settlement must not move it.
    await harness.scheduler.advanceBy(60)
    await expect(op.execute(undefined)).resolves.toBe("ok")

    await harness.scheduler.advanceBy(40)
    harness.authority.reset()
    await expect(
      harness.coordinator.readState(harness.identity),
    ).rejects.toThrow("freshness allowance")
    expect(harness.authority.calls.readState).toBe(0)
  })

  it("rejects the call when the cached verdict is stale and staleClosed is fail-closed", async () => {
    // The refusal is a decision, not an outage. onCoordinatorError defaults to fail-open, so
    // reading the refusal as unavailability would run the adapter the coordinator just refused -
    // the integration failure a coordinator-only test cannot see.
    const harness = createBreakerHarness({
      freshnessMs: 100,
      staleClosed: "fail-closed",
    })
    await harness.start()
    await harness.scheduler.advanceBy(1_000)

    const events: OperationEvent[] = []
    let ran = 0
    const policy = circuitBreaker.distributed({
      name: TEST_BREAKER_IDENTITY.name,
      coordinator: harness.coordinator,
      scope: () => TEST_BREAKER_IDENTITY.scope,
    })
    const op = operation({
      name: TEST_BREAKER_IDENTITY.operation,
      adapter: {
        capabilities: () => ({ abort: "supported", replay: "safe" }) as const,
        execute: async () => {
          ran += 1
          return "ok"
        },
      },
      policies: [policy],
      events: { emit: (event) => events.push(event) },
    })

    await expect(op.execute(undefined)).rejects.toBeInstanceOf(CircuitOpenError)
    expect(ran).toBe(0)
    expect(events.some((event) => event.type === "breaker.rejected")).toBe(true)
    // The coordinator answered, so nothing degraded.
    expect(events.some((event) => event.type === "breaker.degraded")).toBe(
      false,
    )
  })

  it("prefers the caller's cancellation over the refusal", async () => {
    // The refusal and the cancellation land in the same window: the read is in flight when the
    // caller gives up, and the coordinator refuses anyway. The caller is gone, so there is no
    // admission to refuse and no rejection to report for it.
    const harness = createBreakerHarness({
      freshnessMs: 100,
      staleClosed: "fail-closed",
    })
    await harness.start()
    await harness.scheduler.advanceBy(1_000)

    const events: OperationEvent[] = []
    const controller = new AbortController()
    const reason = new Error("caller cancelled")
    let ran = 0
    const policy = circuitBreaker.distributed({
      name: TEST_BREAKER_IDENTITY.name,
      coordinator: {
        ...harness.coordinator,
        async readState(
          ...args: Parameters<typeof harness.coordinator.readState>
        ) {
          const pending = harness.coordinator.readState(...args)
          controller.abort(reason)
          return pending
        },
      },
      scope: () => TEST_BREAKER_IDENTITY.scope,
    })
    const op = operation({
      name: TEST_BREAKER_IDENTITY.operation,
      adapter: {
        capabilities: () => ({ abort: "supported", replay: "safe" }) as const,
        execute: async () => {
          ran += 1
          return "ok"
        },
      },
      policies: [policy],
      events: { emit: (event) => events.push(event) },
    })

    await expect(
      op.execute(undefined, { signal: controller.signal }),
    ).rejects.toBe(reason)
    expect(ran).toBe(0)
    expect(events.some((event) => event.type === "breaker.rejected")).toBe(
      false,
    )
  })

  it("removes the admission read while keeping the settlement awaited", async () => {
    const harness = createBreakerHarness({ freshnessMs: 10_000 })
    await harness.start()

    const policy = circuitBreaker.distributed({
      name: TEST_BREAKER_IDENTITY.name,
      coordinator: harness.coordinator,
      scope: () => TEST_BREAKER_IDENTITY.scope,
    })
    const op = operation({
      name: TEST_BREAKER_IDENTITY.operation,
      adapter: {
        capabilities: () => ({ abort: "supported", replay: "safe" }) as const,
        execute: async () => "ok",
      },
      policies: [policy],
    })

    await op.execute(undefined)
    harness.authority.reset()

    for (let index = 0; index < 10; index += 1) {
      await expect(op.execute(undefined)).resolves.toBe("ok")
    }

    // Warm and valid: no admission reads at all, and every settlement still went.
    expect(harness.authority.calls.readState).toBe(0)
    expect(harness.authority.calls.observe).toBe(10)
  })
})
