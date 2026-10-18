import { describe, expect, it } from "vitest"
import {
  CONFIGURATION_SCHEMA_VERSION,
  ConfigurationError,
  encodeConfigurationRecord,
  resolveConfiguration,
} from "../../src/core/configuration.js"
import type { ConfigurationSettings } from "../../src/core/configuration.js"
import { coordinationKey } from "../../src/coordination/redis/keys.js"
import { createRevisionCache } from "../../src/coordination/redis/revisions.js"
import {
  CoordinatorUnavailableError,
  redisCoordinator,
} from "../../src/redis.js"

const identity = { name: "orders", operation: "submit", scope: "tenant-a" }
const settings: ConfigurationSettings = { policy: "bulkhead", limit: 40 }

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
    subject: redisCoordinator(client, { namespace: "test" }),
    calls,
    keyCounts,
  }
}

describe("bulkhead configuration protocol", () => {
  it("declares the record key and sends the baseline revision", async () => {
    const { subject, calls, keyCounts } = coordinator([1, 1, 0, 0, 0])
    await expect(
      subject.command(identity, "acquire", "token", 30_000, 40),
    ).resolves.toEqual({ allowed: true, occupancy: 0 })
    expect(keyCounts[0]).toBe(2)
    const args = calls[0] ?? []
    expect(args[0]).toBe(
      coordinationKey("test", "bulkhead:orders", "submit", "tenant-a"),
    )
    expect(args[1]).toBe(
      coordinationKey(
        "test",
        "bulkhead:orders",
        "submit",
        "tenant-a",
        "configuration",
      ),
    )
    expect(args.slice(2)).toEqual(["acquire", "token", 30_000, 40, 0])
  })

  it("sends the revision it observed on the next command", async () => {
    const { subject, calls } = coordinator([1, 1, 1, 4, 123])
    await subject.command(identity, "acquire", "token", 30_000, 40)
    await subject.command(identity, "acquire", "token", 30_000, 40)
    expect(calls[1]?.[6]).toBe(4)
  })

  it("accepts a reply that carries the record body", async () => {
    const { subject, calls } = coordinator([1, 1, 1, 2, 123, record(2)])
    await subject.command(identity, "acquire", "token", 30_000, 40)
    await subject.command(identity, "acquire", "token", 30_000, 40)
    expect(calls[1]?.[6]).toBe(2)
  })

  it("rejects a record stored for another policy", async () => {
    const rate = { policy: "rateLimit" as const, rate: 3, burst: 1 }
    const body = encodeConfigurationRecord({
      schemaVersion: CONFIGURATION_SCHEMA_VERSION,
      revision: 1,
      changedAt: 1,
      settings: rate,
      effective: resolveConfiguration(rate),
    })
    const { subject } = coordinator([1, 1, 1, 1, 1, body])
    await expect(
      subject.command(identity, "acquire", "token", 30_000, 40),
    ).rejects.toBeInstanceOf(ConfigurationError)
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
    const subject = redisCoordinator(client, { namespace: "test" })
    await expect(
      subject.command(identity, "acquire", "token", 30_000, 40),
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
    const subject = redisCoordinator(client, { namespace: "test" })
    await expect(
      subject.command(identity, "acquire", "token", 30_000, 40),
    ).rejects.toBeInstanceOf(CoordinatorUnavailableError)
  })

  it("rejects an envelope it does not understand", async () => {
    const { subject } = coordinator([2, 1, 0, 0, 0])
    await expect(
      subject.command(identity, "acquire", "token", 30_000, 40),
    ).rejects.toBeInstanceOf(CoordinatorUnavailableError)
  })
})

describe("revision cache", () => {
  it("starts unknown and ignores non-positive revisions", () => {
    const cache = createRevisionCache(2)
    expect(cache.get("a")).toBe(0)
    cache.set("a", 3)
    expect(cache.get("a")).toBe(3)
    cache.set("a", 0)
    expect(cache.get("a")).toBe(3)
  })

  it("evicts the oldest entry when full", () => {
    const cache = createRevisionCache(2)
    cache.set("a", 1)
    cache.set("b", 2)
    cache.set("c", 3)
    expect(cache.size()).toBe(2)
    expect(cache.get("a")).toBe(0)
    expect(cache.get("c")).toBe(3)
  })
})
