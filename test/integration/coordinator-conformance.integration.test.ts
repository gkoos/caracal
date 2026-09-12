import { randomUUID } from "node:crypto"
import { afterAll, beforeAll, describe, expect, it } from "vitest"
import {
  evalScript,
  scriptSha,
} from "../../src/coordination/redis/eval-script.js"
import { coordinationKey } from "../../src/coordination/redis/keys.js"
import type { RedisScriptClient } from "../../src/coordination/redis/leases.js"
import { breakerObserveV1 } from "../../src/coordination/redis/scripts.js"
import type { BreakerCoordinator, BreakerIdentity } from "../../src/index.js"
import {
  createCoordinationClient,
  redisCircuitBreakerCoordinator,
} from "../../src/redis.js"
import { memoryBreakerCoordinator } from "../support/memory-coordinator/memory-circuit-breaker.js"
import { replayInstruction, resolveTestSeed } from "../support/seed.js"

const url = process.env.CARACAL_REDIS_URL

/**
 * Coordinator conformance suite.
 *
 * Two gaps the other suites leave open:
 * 1. Targeted edge cases of the shipped Lua state machine: threshold boundary,
 *    probe accounting, TTL policy, and probe-success accumulation.
 * 2. Equivalence between the in-memory coordinator used by the unit and fuzz
 *    suites and the Redis coordinator, driven by generated command sequences.
 *    That is what can catch the two implementations drifting apart.
 */

// observe/settle openMs is deliberately large so a CLOSED state hash never
// expires mid-run. admitProbe uses 0 so OPEN -> HALF_OPEN needs no wall clock.
const OBSERVE_OPEN_MS = 999_999
const ADMIT_OPEN_MS = 0
const PROBE_LEASE_TTL_MS = 60_000
const WINDOW_TTL_MS = 60_000

const IDENTITY: BreakerIdentity = {
  name: "conformance",
  operation: "work",
  scope: "shared",
}

function breakerKeys(namespace: string): string[] {
  return (["breaker", "observations", "probes"] as const).map((suffix) =>
    coordinationKey(
      namespace,
      `breaker:${IDENTITY.name}`,
      IDENTITY.operation,
      IDENTITY.scope,
      suffix,
    ),
  )
}

describe.skipIf(!url)("coordinator conformance — Lua edge cases", () => {
  const client = createCoordinationClient(url ?? "redis://127.0.0.1:6379")
  const cleanup: string[] = []

  beforeAll(() => client.connect())
  afterAll(async () => {
    if (cleanup.length > 0) await client.del(...cleanup)
    client.disconnect()
  })

  function coordinatorFor(): {
    coordinator: BreakerCoordinator
    keys: string[]
  } {
    const namespace = `conformance-${randomUUID()}`
    const keys = breakerKeys(namespace)
    cleanup.push(...keys)
    return {
      coordinator: redisCircuitBreakerCoordinator(client, { namespace }),
      keys,
    }
  }

  async function openBreaker(coordinator: BreakerCoordinator): Promise<number> {
    let generation = 0
    for (let attempt = 0; attempt < 10; attempt++) {
      const result = await coordinator.observe(IDENTITY, {
        generation,
        outcome: "failure",
        uuid: randomUUID(),
        windowTtlMs: WINDOW_TTL_MS,
        minimumThroughput: 2,
        failureThresholdNumerator: 500,
        windowSize: 8,
        openMs: OBSERVE_OPEN_MS,
      })
      if (result.type === "opened") return result.newGeneration
      if (result.type !== "observed") {
        throw new Error(`unexpected observe result: ${result.type}`)
      }
      generation = result.generation
    }
    throw new Error("breaker did not open")
  }

  it("opens exactly at the failure-threshold boundary", async () => {
    const { coordinator } = coordinatorFor()
    const params = {
      windowTtlMs: WINDOW_TTL_MS,
      minimumThroughput: 2,
      failureThresholdNumerator: 500,
      windowSize: 8,
      openMs: OBSERVE_OPEN_MS,
    }

    const first = await coordinator.observe(IDENTITY, {
      ...params,
      generation: 0,
      outcome: "failure",
      uuid: randomUUID(),
    })
    expect(first).toMatchObject({
      type: "observed",
      windowTotal: 1,
      windowFailures: 1,
    })

    // 1 failure / 2 observations === 0.5 => opens.
    const second = await coordinator.observe(IDENTITY, {
      ...params,
      generation: 0,
      outcome: "success",
      uuid: randomUUID(),
    })
    expect(second).toMatchObject({
      type: "opened",
      windowTotal: 2,
      windowFailures: 1,
    })
  })

  it("stays closed below the failure-threshold boundary", async () => {
    const { coordinator } = coordinatorFor()
    const params = {
      windowTtlMs: WINDOW_TTL_MS,
      minimumThroughput: 4,
      failureThresholdNumerator: 500,
      windowSize: 8,
      openMs: OBSERVE_OPEN_MS,
    }

    const results = [
      await coordinator.observe(IDENTITY, {
        ...params,
        generation: 0,
        outcome: "failure",
        uuid: randomUUID(),
      }),
    ]
    for (let index = 0; index < 3; index++) {
      results.push(
        await coordinator.observe(IDENTITY, {
          ...params,
          generation: 0,
          outcome: "success",
          uuid: randomUUID(),
        }),
      )
    }

    // 1 failure / 4 observations === 0.25 < 0.5 => stays CLOSED.
    expect(results.every((result) => result.type === "observed")).toBe(true)
    expect(results.at(-1)).toMatchObject({ windowTotal: 4, windowFailures: 1 })
  })

  it("keeps the state-hash probeCount consistent with live probe tokens", async () => {
    const { coordinator, keys } = coordinatorFor()
    const hashKey = keys[0]
    const probeKey = keys[2]

    await openBreaker(coordinator)

    const halfOpenProbes = 2
    let admittedCount = 0
    for (let index = 0; index < halfOpenProbes + 1; index++) {
      const result = await coordinator.admitProbe(IDENTITY, {
        probeToken: randomUUID(),
        openMs: ADMIT_OPEN_MS,
        halfOpenProbes,
        probeLeaseTtlMs: PROBE_LEASE_TTL_MS,
      })
      if (result.type === "admitted") admittedCount += 1

      // The hash field and the sorted set must agree after every attempt,
      // including the rejected one.
      expect(Number(await client.hget(hashKey, "probeCount"))).toBe(
        await client.zcard(probeKey),
      )
    }

    expect(admittedCount).toBe(halfOpenProbes)
    expect(await client.zcard(probeKey)).toBe(halfOpenProbes)
  })

  it("persists OPEN and HALF_OPEN state and expires CLOSED state", async () => {
    const { coordinator, keys } = coordinatorFor()
    const hashKey = keys[0]
    const probeKey = keys[2]

    await openBreaker(coordinator)
    expect(await client.pttl(hashKey)).toBe(-1)

    const probeToken = randomUUID()
    const admitted = await coordinator.admitProbe(IDENTITY, {
      probeToken,
      openMs: ADMIT_OPEN_MS,
      halfOpenProbes: 1,
      probeLeaseTtlMs: PROBE_LEASE_TTL_MS,
    })
    if (admitted.type !== "admitted") {
      throw new Error(`probe not admitted: ${admitted.type}`)
    }

    // HALF_OPEN must not expire while recovery is in progress, but the probe
    // token lease is bounded so a dead worker's slot recovers.
    expect(await client.pttl(hashKey)).toBe(-1)
    expect(await client.pttl(probeKey)).toBeGreaterThan(0)

    // A failing probe re-opens and keeps the hash persistent.
    const reopened = await coordinator.settleProbe(IDENTITY, {
      probeToken,
      outcome: "failure",
      generation: admitted.generation,
      halfOpenSuccesses: 2,
      openMs: OBSERVE_OPEN_MS,
    })
    expect(reopened).toMatchObject({ type: "transitioned", newState: "open" })
    expect(await client.pttl(hashKey)).toBe(-1)

    // A closing success restores a finite cleanup TTL.
    const closingToken = randomUUID()
    const readmitted = await coordinator.admitProbe(IDENTITY, {
      probeToken: closingToken,
      openMs: ADMIT_OPEN_MS,
      halfOpenProbes: 1,
      probeLeaseTtlMs: PROBE_LEASE_TTL_MS,
    })
    if (readmitted.type !== "admitted") {
      throw new Error(`probe not admitted: ${readmitted.type}`)
    }

    const closed = await coordinator.settleProbe(IDENTITY, {
      probeToken: closingToken,
      outcome: "success",
      generation: readmitted.generation,
      halfOpenSuccesses: 1,
      openMs: 30_000,
    })
    expect(closed).toMatchObject({ type: "transitioned", newState: "closed" })
    expect(await client.pttl(hashKey)).toBeGreaterThan(0)
  })

  it("releases an ignored probe without recording an outcome", async () => {
    const { coordinator, keys } = coordinatorFor()
    const [hashKey, , probeKey] = keys
    const generation = await openBreaker(coordinator)
    const token = randomUUID()

    const admitted = await coordinator.admitProbe(IDENTITY, {
      probeToken: token,
      openMs: ADMIT_OPEN_MS,
      halfOpenProbes: 1,
      probeLeaseTtlMs: PROBE_LEASE_TTL_MS,
    })
    expect(admitted).toMatchObject({ type: "admitted", stateChanged: true })

    const released = await coordinator.settleProbe(IDENTITY, {
      probeToken: token,
      outcome: "ignored",
      generation,
      halfOpenSuccesses: 1,
      openMs: OBSERVE_OPEN_MS,
    })

    // Released, not recorded: the slot is free, the counters are untouched and
    // the recovery window has not advanced.
    expect(released).toMatchObject({ type: "settled", state: "half-open" })
    expect(await client.zcard(probeKey)).toBe(0)
    expect(Number(await client.hget(hashKey, "probeCount"))).toBe(0)
    expect(Number(await client.hget(hashKey, "probeSuccesses"))).toBe(0)
    // Still HALF_OPEN, so the state hash stays persistent.
    expect(await client.pttl(hashKey)).toBe(-1)

    // The released slot is immediately reusable.
    const readmitted = await coordinator.admitProbe(IDENTITY, {
      probeToken: randomUUID(),
      openMs: ADMIT_OPEN_MS,
      halfOpenProbes: 1,
      probeLeaseTtlMs: PROBE_LEASE_TTL_MS,
    })
    expect(readmitted).toMatchObject({ type: "admitted", probeCount: 1 })
  })

  it("discards in-flight probe tokens when a probe failure re-opens the breaker", async () => {
    const { coordinator, keys } = coordinatorFor()
    const hashKey = keys[0]
    const probeKey = keys[2]

    await openBreaker(coordinator)

    const failingToken = randomUUID()
    const orphanToken = randomUUID()
    const failingAdmit = await coordinator.admitProbe(IDENTITY, {
      probeToken: failingToken,
      openMs: ADMIT_OPEN_MS,
      halfOpenProbes: 2,
      probeLeaseTtlMs: PROBE_LEASE_TTL_MS,
    })
    const orphanAdmit = await coordinator.admitProbe(IDENTITY, {
      probeToken: orphanToken,
      openMs: ADMIT_OPEN_MS,
      halfOpenProbes: 2,
      probeLeaseTtlMs: PROBE_LEASE_TTL_MS,
    })
    if (failingAdmit.type !== "admitted" || orphanAdmit.type !== "admitted") {
      throw new Error("expected two admitted probes")
    }
    expect(await client.zcard(probeKey)).toBe(2)

    const reopened = await coordinator.settleProbe(IDENTITY, {
      probeToken: failingToken,
      outcome: "failure",
      generation: failingAdmit.generation,
      halfOpenSuccesses: 2,
      openMs: OBSERVE_OPEN_MS,
    })
    expect(reopened).toMatchObject({ type: "transitioned", newState: "open" })

    // Tokens of the superseded window must not consume slots in the next one,
    // and probeCount must stay consistent with the probe set.
    expect(await client.zcard(probeKey)).toBe(0)
    expect(await client.hget(hashKey, "probeCount")).toBe("0")

    // A late settle from the dead window is dropped rather than mutating state.
    const orphanSettle = await coordinator.settleProbe(IDENTITY, {
      probeToken: orphanToken,
      outcome: "success",
      generation: reopened.type === "transitioned" ? reopened.newGeneration : 0,
      halfOpenSuccesses: 1,
      openMs: OBSERVE_OPEN_MS,
    })
    expect(orphanSettle).toMatchObject({ type: "stale" })

    // Full capacity is available to the next recovery window.
    const readmitted = await coordinator.admitProbe(IDENTITY, {
      probeToken: randomUUID(),
      openMs: ADMIT_OPEN_MS,
      halfOpenProbes: 2,
      probeLeaseTtlMs: PROBE_LEASE_TTL_MS,
    })
    expect(readmitted).toMatchObject({ type: "admitted", probeCount: 1 })
  })

  it("accumulates probe successes across probes and lets a failure take priority", async () => {
    const success = coordinatorFor()
    await openBreaker(success.coordinator)

    const firstToken = randomUUID()
    const secondToken = randomUUID()
    const firstAdmit = await success.coordinator.admitProbe(IDENTITY, {
      probeToken: firstToken,
      openMs: ADMIT_OPEN_MS,
      halfOpenProbes: 2,
      probeLeaseTtlMs: PROBE_LEASE_TTL_MS,
    })
    const secondAdmit = await success.coordinator.admitProbe(IDENTITY, {
      probeToken: secondToken,
      openMs: ADMIT_OPEN_MS,
      halfOpenProbes: 2,
      probeLeaseTtlMs: PROBE_LEASE_TTL_MS,
    })
    if (firstAdmit.type !== "admitted" || secondAdmit.type !== "admitted") {
      throw new Error("expected two admitted probes")
    }
    const generation = firstAdmit.generation

    const firstSettled = await success.coordinator.settleProbe(IDENTITY, {
      probeToken: firstToken,
      outcome: "success",
      generation,
      halfOpenSuccesses: 2,
      openMs: OBSERVE_OPEN_MS,
    })
    expect(firstSettled).toMatchObject({ type: "settled", state: "half-open" })
    expect(await client.hget(success.keys[0], "probeSuccesses")).toBe("1")

    const secondSettled = await success.coordinator.settleProbe(IDENTITY, {
      probeToken: secondToken,
      outcome: "success",
      generation,
      halfOpenSuccesses: 2,
      openMs: OBSERVE_OPEN_MS,
    })
    expect(secondSettled).toMatchObject({
      type: "transitioned",
      newState: "closed",
    })

    // Failure priority: a success followed by a failure re-opens.
    const failure = coordinatorFor()
    await openBreaker(failure.coordinator)

    const successToken = randomUUID()
    const failureToken = randomUUID()
    await failure.coordinator.admitProbe(IDENTITY, {
      probeToken: successToken,
      openMs: ADMIT_OPEN_MS,
      halfOpenProbes: 2,
      probeLeaseTtlMs: PROBE_LEASE_TTL_MS,
    })
    const failureAdmit = await failure.coordinator.admitProbe(IDENTITY, {
      probeToken: failureToken,
      openMs: ADMIT_OPEN_MS,
      halfOpenProbes: 2,
      probeLeaseTtlMs: PROBE_LEASE_TTL_MS,
    })
    if (failureAdmit.type !== "admitted") {
      throw new Error("expected a second admitted probe")
    }
    const failureGeneration = failureAdmit.generation

    await failure.coordinator.settleProbe(IDENTITY, {
      probeToken: successToken,
      outcome: "success",
      generation: failureGeneration,
      halfOpenSuccesses: 2,
      openMs: OBSERVE_OPEN_MS,
    })
    const reopened = await failure.coordinator.settleProbe(IDENTITY, {
      probeToken: failureToken,
      outcome: "failure",
      generation: failureGeneration,
      halfOpenSuccesses: 2,
      openMs: OBSERVE_OPEN_MS,
    })
    expect(reopened).toMatchObject({ type: "transitioned", newState: "open" })
  })
})

/**
 * Model-based equivalence.
 *
 * The unit and fuzz suites drive the distributed policy against the in-memory
 * coordinator. That mirror is only trustworthy while it agrees with the Lua
 * state machine, so here the same generated command sequence is replayed
 * against both coordinators and every observable result is compared:
 * per-call results, then the final state (state, generation, probeSuccesses,
 * live probe tokens).
 *
 * Divergence is reported with the exact script plus a replay instruction.
 */
describe.skipIf(!url)(
  "coordinator conformance — memory vs Redis equivalence",
  () => {
    const client = createCoordinationClient(url ?? "redis://127.0.0.1:6379")
    const cleanup: string[] = []
    const SEED = resolveTestSeed()
    const REPLAY = replayInstruction("npm run test:integration", SEED)

    beforeAll(() => client.connect())
    afterAll(async () => {
      if (cleanup.length > 0) await client.del(...cleanup)
      client.disconnect()
    })

    type Outcome = "success" | "failure"

    type Step =
      | { kind: "observe"; outcome: Outcome }
      | { kind: "admit" }
      | { kind: "settle"; outcome: Outcome }
      | { kind: "settle-stale" }

    interface Config {
      windowTtlMs: number
      minimumThroughput: number
      failureThresholdNumerator: number
      windowSize: number
      observeOpenMs: number
      admitOpenMs: number
      settleOpenMs: number
      halfOpenProbes: number
      halfOpenSuccesses: number
      probeLeaseTtlMs: number
    }

    function mulberry32(seed: number): () => number {
      let state = seed >>> 0
      return () => {
        state = (state + 0x6d2b79f5) >>> 0
        let t = state
        t = Math.imul(t ^ (t >>> 15), t | 1)
        t ^= t + Math.imul(t ^ (t >>> 7), t | 61)
        return ((t ^ (t >>> 14)) >>> 0) / 4294967296
      }
    }

    function randInt(rng: () => number, min: number, max: number): number {
      return min + Math.floor(rng() * (max - min + 1))
    }

    /**
     * Replays one generated script against one coordinator.  Each side tracks the
     * generation it observed, exactly like the policy does, so a divergence in
     * results also shows up as a divergence in the generation the next call uses.
     */
    async function runSequence(
      coordinator: BreakerCoordinator,
      identity: BreakerIdentity,
      config: Config,
      steps: Step[],
    ): Promise<unknown[]> {
      let generation = 0
      const pendingTokens: string[] = []
      let lastSettledToken: string | undefined
      const results: unknown[] = []

      for (const [index, step] of steps.entries()) {
        if (step.kind === "observe") {
          const result = await coordinator.observe(identity, {
            generation,
            outcome: step.outcome,
            uuid: `u-${index}`,
            windowTtlMs: config.windowTtlMs,
            minimumThroughput: config.minimumThroughput,
            failureThresholdNumerator: config.failureThresholdNumerator,
            windowSize: config.windowSize,
            openMs: config.observeOpenMs,
          })
          if (result.type === "opened") generation = result.newGeneration
          else if (result.type === "observed") generation = result.generation
          else generation = result.currentGeneration
          results.push(result)
          continue
        }

        if (step.kind === "admit") {
          const probeToken = `t-${index}`
          const result = await coordinator.admitProbe(identity, {
            probeToken,
            openMs: config.admitOpenMs,
            halfOpenProbes: config.halfOpenProbes,
            probeLeaseTtlMs: config.probeLeaseTtlMs,
          })
          generation = result.generation
          if (result.type === "admitted") pendingTokens.push(probeToken)
          results.push(result)
          continue
        }

        // Replaying an already-consumed token must come back stale from both
        // coordinators, which also gives the run a guaranteed stale path.
        const probeToken =
          step.kind === "settle-stale"
            ? lastSettledToken
            : pendingTokens.shift()
        if (probeToken === undefined) {
          results.push({ type: "no-token" })
          continue
        }
        const result = await coordinator.settleProbe(identity, {
          probeToken,
          outcome: step.kind === "settle-stale" ? "success" : step.outcome,
          generation,
          halfOpenSuccesses: config.halfOpenSuccesses,
          openMs: config.settleOpenMs,
        })
        generation =
          result.type === "transitioned"
            ? result.newGeneration
            : result.generation
        lastSettledToken = probeToken
        results.push(result)
      }

      return results
    }

    /** Canonical, key-order independent view of a coordinator result. */
    function normalize(result: unknown): string {
      const fields = result as Record<string, unknown>
      return [
        fields.type,
        fields.reason,
        fields.state,
        fields.newState,
        fields.previousState,
        fields.generation,
        fields.newGeneration,
        fields.currentGeneration,
        fields.windowTotal,
        fields.windowFailures,
        fields.probeCount,
        fields.stateChanged,
      ]
        .map((value) => (value === undefined ? "" : String(value)))
        .join("|")
    }

    it("produces identical results for generated command sequences", async () => {
      const rng = mulberry32(SEED ^ 0x1234abcd)
      const CASES = 20
      const failures: string[] = []
      // Guard against a degenerate generator: equivalence only means something if
      // the script actually drives the interesting paths.
      const coverage = {
        opened: 0,
        admitted: 0,
        probeLimitRejected: 0,
        settled: 0,
        transitioned: 0,
        stale: 0,
      }

      for (let caseIndex = 0; caseIndex < CASES; caseIndex++) {
        const windowSize = randInt(rng, 2, 6)
        const config: Config = {
          windowTtlMs: WINDOW_TTL_MS,
          // Never above windowSize, otherwise the breaker can never open.
          minimumThroughput: randInt(rng, 1, windowSize),
          failureThresholdNumerator: randInt(rng, 200, 900),
          windowSize,
          observeOpenMs: OBSERVE_OPEN_MS,
          admitOpenMs: ADMIT_OPEN_MS,
          settleOpenMs: OBSERVE_OPEN_MS,
          halfOpenProbes: randInt(rng, 1, 3),
          // >= 2 so the prologue below always sees a non-closing success.
          halfOpenSuccesses: randInt(rng, 2, 3),
          probeLeaseTtlMs: PROBE_LEASE_TTL_MS,
        }

        const steps: Step[] = []
        // Deterministic prologue: open, probe and fail to re-open, oversubscribe
        // the probe budget, accumulate successes until it closes, then replay a
        // consumed token.  That covers every result type the comparison cares
        // about regardless of the seed; the random tail below explores from there.
        for (let index = 0; index < config.minimumThroughput; index++) {
          steps.push({ kind: "observe", outcome: "failure" })
        }
        steps.push({ kind: "admit" })
        steps.push({ kind: "settle", outcome: "failure" })
        for (let index = 0; index <= config.halfOpenProbes; index++) {
          steps.push({ kind: "admit" })
        }
        for (let index = 0; index < config.halfOpenSuccesses; index++) {
          steps.push({ kind: "admit" })
          steps.push({ kind: "settle", outcome: "success" })
        }
        steps.push({ kind: "settle-stale" })

        const tailCount = randInt(rng, 6, 20)
        for (let index = 0; index < tailCount; index++) {
          const roll = rng()
          if (roll < 0.6) {
            steps.push({
              kind: "observe",
              outcome: rng() < 0.6 ? "failure" : "success",
            })
          } else if (roll < 0.85) {
            steps.push({ kind: "admit" })
          } else {
            steps.push({
              kind: "settle",
              outcome: rng() < 0.5 ? "failure" : "success",
            })
          }
        }

        const namespace = `conformance-${randomUUID()}`
        const keys = breakerKeys(namespace)
        cleanup.push(...keys)

        const memory = memoryBreakerCoordinator()
        const redis = redisCircuitBreakerCoordinator(client, { namespace })

        const memoryResults = await runSequence(memory, IDENTITY, config, steps)
        const redisResults = await runSequence(redis, IDENTITY, config, steps)

        for (const result of memoryResults) {
          const view = result as { type?: string; reason?: string }
          if (view.type === "opened") coverage.opened += 1
          else if (view.type === "admitted") coverage.admitted += 1
          else if (view.type === "settled") coverage.settled += 1
          else if (view.type === "transitioned") coverage.transitioned += 1
          else if (view.type === "stale") coverage.stale += 1
          else if (view.type === "rejected" && view.reason === "probe-limit") {
            coverage.probeLimitRejected += 1
          }
        }

        for (const [index, step] of steps.entries()) {
          const memoryView = normalize(memoryResults[index])
          const redisView = normalize(redisResults[index])
          if (memoryView !== redisView) {
            failures.push(
              `case ${caseIndex} step ${index} (${JSON.stringify(step)})\n` +
                `  config=${JSON.stringify(config)}\n` +
                `  steps=${JSON.stringify(steps)}\n` +
                `  memory=${memoryView}\n` +
                `  redis =${redisView}\n${REPLAY}`,
            )
            break
          }
        }
        if (failures.length > 0) break

        // Final observable state must match too.
        const record = memory.inspect(IDENTITY)
        const hash = await client.hmget(
          keys[0],
          "state",
          "generation",
          "probeSuccesses",
          "probeCount",
        )
        const liveTokens = await client.zcard(keys[2])
        expect(
          Number(hash[3] ?? 0),
          "Lua probeCount drifted from the probe set",
        ).toBe(liveTokens)
        expect({
          state: record?.state ?? "closed",
          generation: record?.generation ?? 0,
          probeSuccesses: record?.probeSuccesses ?? 0,
          probeTokens: record?.probeTokens.size ?? 0,
        }).toEqual({
          state: hash[0] ?? "closed",
          generation: Number(hash[1] ?? 0),
          probeSuccesses: Number(hash[2] ?? 0),
          probeTokens: liveTokens,
        })
      }

      expect(failures.join("\n\n"), `memory/Redis divergence\n${REPLAY}`).toBe(
        "",
      )

      // The prologue pins these paths; if the generator stops reaching them, the
      // equivalence check above has silently stopped proving anything.
      expect(
        coverage.opened,
        "generator never opened the breaker",
      ).toBeGreaterThan(0)
      expect(
        coverage.admitted,
        "generator never admitted a probe",
      ).toBeGreaterThan(0)
      expect(
        coverage.settled,
        "generator never settled a probe",
      ).toBeGreaterThan(0)
      expect(
        coverage.transitioned,
        "generator never transitioned state",
      ).toBeGreaterThan(0)
      expect(
        coverage.probeLimitRejected,
        "generator never hit the probe limit",
      ).toBeGreaterThan(0)
      expect(
        coverage.stale,
        "generator never produced a stale result",
      ).toBeGreaterThan(0)
    })
  },
)

/**
 * Script transport.
 *
 * The coordinators send EVALSHA in steady state and only ship the full Lua body
 * when the server has forgotten it.  These tests pin both halves against a real
 * server, using a recording wrapper so the assertions attribute commands to our
 * own code path rather than to whatever else is running against the instance.
 */
describe.skipIf(!url)("coordinator conformance — script transport", () => {
  const client = createCoordinationClient(url ?? "redis://127.0.0.1:6379")
  const cleanup: string[] = []

  beforeAll(() => client.connect())
  afterAll(async () => {
    if (cleanup.length > 0) await client.del(...cleanup)
    client.disconnect()
  })

  /** Wraps the real client and records which script command was issued. */
  function recordingClient(): { client: RedisScriptClient; issued: string[] } {
    const issued: string[] = []
    return {
      issued,
      client: {
        eval: (script, numberOfKeys, ...args) => {
          issued.push("eval")
          return client.eval(script, numberOfKeys, ...args)
        },
        evalsha: (sha, numberOfKeys, ...args) => {
          issued.push("evalsha")
          return client.evalsha(sha, numberOfKeys, ...args)
        },
        hmget: (key, ...fields) => client.hmget(key, ...fields),
      },
    }
  }

  const observeParams = {
    windowTtlMs: WINDOW_TTL_MS,
    minimumThroughput: 2,
    failureThresholdNumerator: 500,
    windowSize: 8,
    openMs: OBSERVE_OPEN_MS,
  }

  it("sends the body once per cache miss, then only the SHA1", async () => {
    // A body no other process can have cached, so the first call is a
    // deterministic cache miss.
    const uniqueScript = `return ${Date.now()}`
    const recording = recordingClient()

    await client.script("FLUSH")

    const first = await evalScript(recording.client, uniqueScript, 0)
    expect(recording.issued).toEqual(["evalsha", "eval"])
    expect(Number(first)).toBeGreaterThan(0)

    const second = await evalScript(recording.client, uniqueScript, 0)
    expect(recording.issued).toEqual(["evalsha", "eval", "evalsha"])
    expect(second).toBe(first)
  })

  it("keeps a shipped script's SHA1 in sync with the server cache", async () => {
    const namespace = `conformance-${randomUUID()}`
    cleanup.push(...breakerKeys(namespace))
    const recording = recordingClient()
    const coordinator = redisCircuitBreakerCoordinator(recording.client, {
      namespace,
    })

    // Warm up: the first call may take the EVAL path on a cold cache.
    await coordinator.observe(IDENTITY, {
      ...observeParams,
      generation: 0,
      outcome: "success",
      uuid: randomUUID(),
    })
    recording.issued.length = 0

    for (let index = 0; index < 5; index++) {
      await coordinator.observe(IDENTITY, {
        ...observeParams,
        generation: 0,
        outcome: "success",
        uuid: randomUUID(),
      })
    }

    // EVALSHA only means our SHA1 is the one the server has stored.
    expect(recording.issued).toEqual(Array.from({ length: 5 }, () => "evalsha"))
    const existsReply = (await client.script(
      "EXISTS",
      scriptSha(breakerObserveV1),
    )) as number[]
    expect(Number(existsReply[0])).toBe(1)
  })

  it("does not surface NOSCRIPT when the script cache is flushed", async () => {
    const namespace = `conformance-${randomUUID()}`
    cleanup.push(...breakerKeys(namespace))
    const recording = recordingClient()
    const coordinator = redisCircuitBreakerCoordinator(recording.client, {
      namespace,
    })

    await coordinator.observe(IDENTITY, {
      ...observeParams,
      generation: 0,
      outcome: "success",
      uuid: randomUUID(),
    })

    await client.script("FLUSH")
    recording.issued.length = 0

    // A flushed cache must not become an error for the caller.  (The fallback
    // path itself is pinned deterministically by the first test; another suite
    // running in parallel could legitimately re-populate this shared cache
    // between the flush and the call.)
    const afterFlush = await coordinator.observe(IDENTITY, {
      ...observeParams,
      generation: 0,
      outcome: "success",
      uuid: randomUUID(),
    })
    expect(afterFlush).toMatchObject({ type: "observed" })

    // The script is cached again, so the transport returns to EVALSHA.
    recording.issued.length = 0
    await coordinator.observe(IDENTITY, {
      ...observeParams,
      generation: 0,
      outcome: "success",
      uuid: randomUUID(),
    })
    expect(recording.issued).toEqual(["evalsha"])
  })
})

/**
 * Epochs and cleanup.
 *
 * The state hash is the only thing that records which generation a window
 * belongs to; `generation` doubles as the window's epoch.  Expiring it while
 * members survive - which the coupled TTL makes impossible through the normal
 * path, but an eviction policy or an admin cleanup can still do - must start a
 * fresh epoch rather than reuse a value those members would match.
 *
 * Regression test for the reported high: open -> recover -> close -> state lost
 * -> a stale generation was accepted, and the resulting window counted the
 * superseded epoch's failures.
 */
describe.skipIf(!url)("coordinator conformance — epochs and cleanup", () => {
  const client = createCoordinationClient(url ?? "redis://127.0.0.1:6379")
  const cleanup: string[] = []

  beforeAll(() => client.connect())
  afterAll(async () => {
    if (cleanup.length > 0) await client.del(...cleanup)
    client.disconnect()
  })

  const params = {
    windowTtlMs: WINDOW_TTL_MS,
    minimumThroughput: 3,
    failureThresholdNumerator: 500,
    windowSize: 10,
    openMs: 50,
  }

  function namespace(): string {
    const value = `conformance-${randomUUID()}`
    cleanup.push(...breakerKeys(value))
    return value
  }

  function coordinatorFor(value: string): BreakerCoordinator {
    return redisCircuitBreakerCoordinator(client, { namespace: value })
  }

  /** Observes the way the policy does: read the state, then report with it. */
  async function observe(
    coordinator: BreakerCoordinator,
    outcome: "success" | "failure",
  ) {
    const state = await coordinator.readState(IDENTITY)
    return await coordinator.observe(IDENTITY, {
      ...params,
      generation: state?.generation ?? 0,
      outcome,
      uuid: randomUUID(),
    })
  }

  /** Waits out openMs, then closes the breaker with one successful probe. */
  async function recover(coordinator: BreakerCoordinator): Promise<number> {
    await new Promise((resolve) => setTimeout(resolve, params.openMs + 30))
    const token = randomUUID()
    const admitted = await coordinator.admitProbe(IDENTITY, {
      probeToken: token,
      openMs: params.openMs,
      halfOpenProbes: 1,
      probeLeaseTtlMs: 5_000,
    })
    if (admitted.type !== "admitted") {
      throw new Error(`probe not admitted: ${admitted.type}`)
    }
    const settled = await coordinator.settleProbe(IDENTITY, {
      probeToken: token,
      outcome: "success",
      generation: admitted.generation,
      halfOpenSuccesses: 1,
      openMs: params.openMs,
      windowTtlMs: params.windowTtlMs,
    })
    if (settled.type !== "transitioned" || settled.newState !== "closed") {
      throw new Error(`probe did not close the breaker: ${settled.type}`)
    }
    return settled.newGeneration
  }

  it("records the window epoch from the first observation", async () => {
    const value = namespace()
    const coordinator = coordinatorFor(value)

    const first = await observe(coordinator, "success")

    expect(first).toMatchObject({
      type: "observed",
      generation: 0,
      windowTotal: 1,
    })
    // The epoch is on record from the first observation, which is what makes a
    // later state loss detectable at all.
    const [hashKey] = breakerKeys(value)
    expect(await client.exists(hashKey)).toBe(1)
  })

  it("expires the state hash no earlier than the window it governs", async () => {
    const value = namespace()
    const coordinator = coordinatorFor(value)

    for (let index = 0; index < params.minimumThroughput; index++) {
      await observe(coordinator, "failure")
    }
    await recover(coordinator)

    const [hashKey, obsKey] = breakerKeys(value)
    const hashTtl = await client.pttl(hashKey)
    const obsTtl = await client.pttl(obsKey)

    // Not the historical openMs x 2: the window outlives it.
    expect(hashTtl).toBeGreaterThan(params.openMs * 2)
    expect(hashTtl).toBeLessThanOrEqual(params.windowTtlMs)
    expect(hashTtl).toBeGreaterThanOrEqual(obsTtl)
  })

  it("mints a fresh epoch when the state hash is lost while its window survives", async () => {
    const value = namespace()
    const coordinator = coordinatorFor(value)

    for (let index = 0; index < params.minimumThroughput; index++) {
      await observe(coordinator, "failure")
    }
    const closedGeneration = await recover(coordinator)

    const [hashKey, obsKey] = breakerKeys(value)
    expect(await client.zcard(obsKey)).toBe(params.minimumThroughput)
    expect(closedGeneration).toBeGreaterThan(0)

    // A loss the coupled TTL cannot prevent: eviction policy, admin cleanup.
    await client.del(hashKey)

    const fresh = await observe(coordinator, "failure")
    const epoch = (fresh as { generation: number }).generation

    // Only the new observation counts, even though the old members are present.
    expect(fresh).toMatchObject({
      type: "observed",
      windowTotal: 1,
      windowFailures: 1,
    })
    expect(epoch).not.toBe(closedGeneration)
    expect(epoch).toBeGreaterThan(0)
    expect(await client.zcard(obsKey)).toBe(params.minimumThroughput + 1)

    // An attempt admitted before the loss can no longer pass the epoch check...
    await expect(
      coordinator.observe(IDENTITY, {
        ...params,
        generation: closedGeneration,
        outcome: "failure",
        uuid: randomUUID(),
      }),
    ).resolves.toMatchObject({ type: "stale", currentGeneration: epoch })

    // ... and neither can one that never saw the state at all.
    await expect(
      coordinator.observe(IDENTITY, {
        ...params,
        generation: 0,
        outcome: "failure",
        uuid: randomUUID(),
      }),
    ).resolves.toMatchObject({ type: "stale" })

    // The epoch survives the decimal round-trip through the hash field, so the
    // window keeps accumulating in the new epoch.
    expect((await coordinator.readState(IDENTITY))?.generation).toBe(epoch)
    expect(await observe(coordinator, "failure")).toMatchObject({
      type: "observed",
      generation: epoch,
      windowTotal: 2,
      windowFailures: 2,
    })
  })
})

/**
 * Probe leases.
 *
 * A probe token carries its deadline as its sorted-set score, and its slot is
 * recoverable from the moment that deadline passes - `admitProbe` prunes expired
 * tokens and re-issues the slot.  A result that arrives after the deadline must
 * therefore be dropped, exactly as the in-memory coordinator drops it;
 * otherwise a dead probe can still close or re-open the breaker.
 */
describe.skipIf(!url)("coordinator conformance — probe leases", () => {
  const client = createCoordinationClient(url ?? "redis://127.0.0.1:6379")
  const cleanup: string[] = []

  beforeAll(() => client.connect())
  afterAll(async () => {
    if (cleanup.length > 0) await client.del(...cleanup)
    client.disconnect()
  })

  const leaseMs = 100

  interface LeaseParams {
    windowTtlMs: number
    minimumThroughput: number
    failureThresholdNumerator: number
    windowSize: number
    openMs: number
  }

  function leaseParams(namespace: string): LeaseParams {
    cleanup.push(...breakerKeys(namespace))
    return {
      windowTtlMs: WINDOW_TTL_MS,
      minimumThroughput: 2,
      failureThresholdNumerator: 500,
      windowSize: 10,
      openMs: 50,
    }
  }

  /** Opens the breaker, then admits one probe once openMs has elapsed. */
  async function openAndAdmit(
    coordinator: BreakerCoordinator,
    params: LeaseParams,
  ): Promise<{ probeToken: string; generation: number }> {
    let generation = 0
    for (let index = 0; index < params.minimumThroughput; index++) {
      const result = await coordinator.observe(IDENTITY, {
        ...params,
        generation,
        outcome: "failure",
        uuid: randomUUID(),
      })
      if (result.type === "opened") generation = result.newGeneration
      else if (result.type === "observed") generation = result.generation
      else generation = result.currentGeneration
    }
    await new Promise((resolve) => setTimeout(resolve, params.openMs + 30))
    const probeToken = randomUUID()
    const admission = await coordinator.admitProbe(IDENTITY, {
      probeToken,
      openMs: params.openMs,
      halfOpenProbes: 1,
      probeLeaseTtlMs: leaseMs,
    })
    if (admission.type !== "admitted") {
      throw new Error(`probe not admitted: ${admission.type}`)
    }
    return { probeToken, generation: admission.generation }
  }

  function settleExpired(
    coordinator: BreakerCoordinator,
    params: LeaseParams,
    run: { probeToken: string; generation: number },
  ) {
    return coordinator.settleProbe(IDENTITY, {
      probeToken: run.probeToken,
      outcome: "success",
      generation: run.generation,
      halfOpenSuccesses: 1,
      openMs: params.openMs,
      windowTtlMs: params.windowTtlMs,
    })
  }

  it("drops a probe result whose lease elapsed instead of closing the breaker", async () => {
    const namespace = `conformance-${randomUUID()}`
    const params = leaseParams(namespace)
    const coordinator = redisCircuitBreakerCoordinator(client, { namespace })
    const run = await openAndAdmit(coordinator, params)
    const [hashKey, , probeKey] = breakerKeys(namespace)

    // The score is the deadline, and nothing has pruned the token yet.
    const deadline = await client.zscore(probeKey, run.probeToken)
    expect(deadline).not.toBeNull()

    await new Promise((resolve) => setTimeout(resolve, leaseMs + 50))
    expect(Number(deadline)).toBeLessThan(Date.now())
    expect(await client.zcard(probeKey)).toBe(1)

    const settled = await settleExpired(coordinator, params, run)

    expect(settled).toMatchObject({ type: "stale" })
    // Still recovering: a dead probe does not get to close the breaker.
    expect(await client.hmget(hashKey, "state", "generation")).toEqual([
      "half-open",
      String(run.generation),
    ])
    // The consumed token releases the slot it still occupied.
    expect(await client.zcard(probeKey)).toBe(0)
    expect(Number(await client.hget(hashKey, "probeCount"))).toBe(0)
  })

  it("agrees with the in-memory coordinator about an expired probe", async () => {
    const namespace = `conformance-${randomUUID()}`
    const params = leaseParams(namespace)
    const redis = redisCircuitBreakerCoordinator(client, { namespace })
    const memory = memoryBreakerCoordinator()

    const redisRun = await openAndAdmit(redis, params)
    const memoryRun = await openAndAdmit(memory, params)
    await new Promise((resolve) => setTimeout(resolve, leaseMs + 50))

    const redisSettled = await settleExpired(redis, params, redisRun)
    const memorySettled = await settleExpired(memory, params, memoryRun)

    expect(redisSettled).toMatchObject({ type: "stale" })
    expect(memorySettled).toMatchObject({ type: "stale" })
    expect(redisSettled).toEqual(memorySettled)
  })
})
