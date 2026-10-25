import { describe, expect, it } from "vitest"
import {
  CONFIGURATION_SCHEMA_VERSION,
  ConfigurationError,
  encodeConfigurationRecord,
  resolveConfiguration,
} from "../../src/core/configuration.js"
import type { ConfigurationSettings } from "../../src/core/configuration.js"
import { coordinationKey } from "../../src/coordination/redis/keys.js"
import {
  CoordinatorUnavailableError,
  redisCircuitBreakerCoordinator,
} from "../../src/redis.js"

const identity = { name: "breaker", operation: "work", scope: "shared" }
const settings: ConfigurationSettings = {
  policy: "circuitBreaker",
  failureThreshold: 0.5,
  minimumThroughput: 1,
  windowSize: 20,
  openMs: 10_000,
  halfOpenProbes: 2,
  halfOpenSuccesses: 1,
}
const observeParams = {
  generation: 0,
  outcome: "success" as const,
  uuid: "uuid-1",
  windowTtlMs: 60_000,
  minimumThroughput: 5,
  failureThresholdNumerator: 500,
  windowSize: 100,
  openMs: 10_000,
  closedCleanupMs: 60_000,
}
const configKey = coordinationKey(
  "test",
  "breaker:breaker",
  "work",
  "shared",
  "configuration",
)

function record(revision: number): string {
  return encodeConfigurationRecord({
    schemaVersion: CONFIGURATION_SCHEMA_VERSION,
    revision,
    changedAt: 111,
    settings,
    effective: resolveConfiguration(settings),
  })
}

function coordinator(reply: unknown) {
  const calls: (string | number)[][] = []
  const keyCounts: number[] = []
  const client = {
    async eval(
      _script: string,
      numberOfKeys: number,
      ...args: (string | number)[]
    ) {
      keyCounts.push(numberOfKeys)
      calls.push(args)
      return reply
    },
    async hmget() {
      return []
    },
  }
  return {
    subject: redisCircuitBreakerCoordinator(client, { namespace: "test" }),
    calls,
    keyCounts,
  }
}

describe("breaker observe protocol", () => {
  it("declares both keys and the baseline revision", async () => {
    const { subject, calls, keyCounts } = coordinator([
      1, 3, 111, 1, 0, 0, 4, 1, 0,
    ])
    await expect(subject.observe(identity, observeParams)).resolves.toEqual({
      type: "observed",
      generation: 0,
      windowTotal: 4,
      windowFailures: 1,
    })
    expect(keyCounts[0]).toBe(3)
    expect(calls[0]?.[2]).toBe(configKey)
    expect(calls[0]?.[11]).toBe(0)
  })

  it("reports an opening observation with its authoritative open time", async () => {
    const { subject } = coordinator([1, 3, 111, 2, 1, 1, 4, 2, 1_700_000])
    await expect(subject.observe(identity, observeParams)).resolves.toEqual({
      type: "opened",
      newGeneration: 1,
      windowTotal: 4,
      windowFailures: 2,
      openedAt: 1_700_000,
      revision: 3,
    })
  })

  it("reports a stale observation", async () => {
    const { subject } = coordinator([1, 3, 111, 0, 0, 7, 0, 0, 0])
    await expect(subject.observe(identity, observeParams)).resolves.toEqual({
      type: "stale",
      currentGeneration: 7,
    })
  })

  it("sends the revision it observed on the next call", async () => {
    const reply = [1, 3, 111, 1, 0, 0, 4, 1, 0]
    const { subject, calls } = coordinator(reply)
    await subject.observe(identity, observeParams)
    await subject.observe(identity, observeParams)
    expect(calls[1]?.[11]).toBe(3)
  })

  it("accepts a reply that carries the record body", async () => {
    const reply = [1, 3, 111, 1, 0, 0, 4, 1, 0, record(3)]
    const { subject, calls } = coordinator(reply)
    await subject.observe(identity, observeParams)
    await subject.observe(identity, observeParams)
    expect(calls[1]?.[11]).toBe(3)
  })
})

describe("breaker readState protocol", () => {
  it("returns null but still remembers a gone record revision", async () => {
    const { subject, calls } = coordinator([1, 4, 222, 0, 0, 0, 0])
    await expect(subject.readState(identity)).resolves.toBeNull()
    await subject.readState(identity)
    expect(calls[1]?.[2]).toBe(4)
  })

  it("reports state, generation, openedAt and revision", async () => {
    const { subject } = coordinator([1, 4, 222, 1, 1, 2, 1_699_999_999_999])
    await expect(subject.readState(identity)).resolves.toEqual({
      state: "open",
      generation: 2,
      openedAt: 1_699_999_999_999,
      revision: 4,
      changedAt: 222,
    })
  })

  it("rejects a body stored for another policy", async () => {
    const rate = { policy: "rateLimit" as const, rate: 3, burst: 1 }
    const body = encodeConfigurationRecord({
      schemaVersion: CONFIGURATION_SCHEMA_VERSION,
      revision: 1,
      changedAt: 1,
      settings: rate,
      effective: resolveConfiguration(rate),
    })
    const { subject } = coordinator([1, 1, 1, 1, 0, 0, 0, body])
    await expect(subject.readState(identity)).rejects.toBeInstanceOf(
      ConfigurationError,
    )
  })
})

const admitParams = {
  probeToken: "probe-1",
  openMs: 10_000,
  halfOpenProbes: 2,
  probeLeaseTtlMs: 30_000,
}
const settleParams = {
  probeToken: "probe-1",
  outcome: "success" as const,
  generation: 0,
  halfOpenSuccesses: 1,
  openMs: 10_000,
  closedCleanupMs: 60_000,
  windowTtlMs: 60_000,
}

describe("breaker probe protocol", () => {
  it("decodes an admitted probe", async () => {
    const { subject, calls } = coordinator([1, 0, 0, 1, 2, 0, 1, 1])
    await expect(subject.admitProbe(identity, admitParams)).resolves.toEqual({
      type: "admitted",
      generation: 0,
      probeCount: 1,
      stateChanged: true,
    })
    expect(calls[0]?.[7]).toBe(0)
  })

  it("decodes a probe refused while open", async () => {
    const { subject } = coordinator([1, 0, 0, 0, 1, 0, 0, 0])
    await expect(subject.admitProbe(identity, admitParams)).resolves.toEqual({
      type: "rejected",
      reason: "open",
      generation: 0,
    })
  })

  it("decodes a settlement that closes the breaker", async () => {
    const { subject, calls } = coordinator([1, 0, 0, 2, 0, 1, 0])
    await expect(subject.settleProbe(identity, settleParams)).resolves.toEqual({
      type: "transitioned",
      newState: "closed",
      newGeneration: 1,
      previousState: "half-open",
      revision: 0,
    })
    expect(calls[0]?.[9]).toBe(0)
  })

  it("decodes a settlement that re-opens the breaker with its open time", async () => {
    const { subject } = coordinator([1, 4, 111, 2, 1, 3, 1_700_000])
    await expect(subject.settleProbe(identity, settleParams)).resolves.toEqual({
      type: "transitioned",
      newState: "open",
      newGeneration: 3,
      previousState: "half-open",
      openedAt: 1_700_000,
      revision: 4,
    })
  })
})

describe("breaker configuration errors", () => {
  it("maps the unsupported-record reply to a configuration error", async () => {
    const client = {
      async eval() {
        throw new Error("unsupported configuration record")
      },
      async hmget() {
        return []
      },
    }
    const subject = redisCircuitBreakerCoordinator(client, {
      namespace: "test",
    })
    await expect(
      subject.observe(identity, observeParams),
    ).rejects.toBeInstanceOf(ConfigurationError)
  })

  it("wraps other failures as coordinator unavailable", async () => {
    const client = {
      async eval() {
        throw new Error("connection lost")
      },
      async hmget() {
        return []
      },
    }
    const subject = redisCircuitBreakerCoordinator(client, {
      namespace: "test",
    })
    await expect(
      subject.observe(identity, observeParams),
    ).rejects.toBeInstanceOf(CoordinatorUnavailableError)
  })

  it("rejects an envelope it does not understand", async () => {
    const { subject } = coordinator([2, 0, 0, 1, 0, 0, 0, 0])
    await expect(
      subject.observe(identity, observeParams),
    ).rejects.toBeInstanceOf(CoordinatorUnavailableError)
  })
})
