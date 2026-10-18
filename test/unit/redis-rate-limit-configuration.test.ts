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
  redisRateLimitCoordinator,
} from "../../src/redis.js"

const identity = { name: "api", operation: "call", scope: "tenant-a" }
const params = { emissionIntervalMs: 1, burstDelayMs: 0 }
const settings: ConfigurationSettings = {
  policy: "rateLimit",
  rate: 1,
  burst: 1,
}

function record(revision: number): string {
  return encodeConfigurationRecord({
    schemaVersion: CONFIGURATION_SCHEMA_VERSION,
    revision,
    changedAt: 1_700_000_000_000,
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
    subject: redisRateLimitCoordinator(client, { namespace: "test" }),
    calls,
    keyCounts,
  }
}

describe("rate limit configuration protocol", () => {
  it("declares the record key and sends the baseline and revision", async () => {
    const { subject, calls, keyCounts } = coordinator([1, 1, 0, 0, 0])
    await expect(subject.command(identity, params)).resolves.toEqual({
      allowed: true,
      retryAfterMs: 0,
    })
    expect(keyCounts[0]).toBe(2)
    const args = calls[0] ?? []
    expect(args[0]).toBe(
      coordinationKey("test", "ratelimit:api", "call", "tenant-a", "rate"),
    )
    expect(args[1]).toBe(
      coordinationKey(
        "test",
        "ratelimit:api",
        "call",
        "tenant-a",
        "configuration",
      ),
    )
    expect(args.slice(2)).toEqual([1, 0, 0])
  })

  it("sends the revision it observed on the next command", async () => {
    const { subject, calls } = coordinator([1, 0, 250, 4, 123])
    await expect(subject.command(identity, params)).resolves.toEqual({
      allowed: false,
      retryAfterMs: 250,
    })
    await subject.command(identity, params)
    expect(calls[1]?.[4]).toBe(4)
  })

  it("accepts a reply that carries the record body", async () => {
    const { subject, calls } = coordinator([1, 1, 0, 2, 123, record(2)])
    await subject.command(identity, params)
    await subject.command(identity, params)
    expect(calls[1]?.[4]).toBe(2)
  })

  it("rejects a record stored for another policy", async () => {
    const bulk = { policy: "bulkhead" as const, limit: 1 }
    const body = encodeConfigurationRecord({
      schemaVersion: CONFIGURATION_SCHEMA_VERSION,
      revision: 1,
      changedAt: 1,
      settings: bulk,
      effective: resolveConfiguration(bulk),
    })
    const { subject } = coordinator([1, 1, 0, 1, 1, body])
    await expect(subject.command(identity, params)).rejects.toBeInstanceOf(
      ConfigurationError,
    )
  })

  it("maps the unsupported-record reply to a configuration error", async () => {
    const client = {
      async eval() {
        throw new Error("unsupported configuration record")
      },
      async hmget() {
        return []
      },
    }
    const subject = redisRateLimitCoordinator(client, { namespace: "test" })
    await expect(subject.command(identity, params)).rejects.toBeInstanceOf(
      ConfigurationError,
    )
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
    const subject = redisRateLimitCoordinator(client, { namespace: "test" })
    await expect(subject.command(identity, params)).rejects.toBeInstanceOf(
      CoordinatorUnavailableError,
    )
  })

  it("rejects an envelope it does not understand", async () => {
    const { subject } = coordinator([2, 1, 0, 0, 0])
    await expect(subject.command(identity, params)).rejects.toBeInstanceOf(
      CoordinatorUnavailableError,
    )
  })

  it("rejects invalid parameters before calling the client", async () => {
    const { subject, calls } = coordinator([1, 1, 0, 0, 0])
    await expect(
      subject.command(identity, { emissionIntervalMs: 0, burstDelayMs: 0 }),
    ).rejects.toBeInstanceOf(RangeError)
    expect(calls).toEqual([])
  })
})
