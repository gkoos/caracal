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
  redisConfigurationManager,
} from "../../src/redis.js"

const identity = {
  policy: "bulkhead" as const,
  name: "orders",
  operation: "submit",
  scope: "tenant-a",
}

const settings = { policy: "bulkhead" as const, limit: 40 }

function record(revision: number): string {
  return encodeConfigurationRecord({
    schemaVersion: CONFIGURATION_SCHEMA_VERSION,
    revision,
    changedAt: 1_700_000_000_000,
    settings,
    effective: resolveConfiguration(settings),
  })
}

function manager(reply: unknown) {
  const calls: (string | number)[][] = []
  const client = {
    async eval(_script: string, _keys: number, ...args: (string | number)[]) {
      calls.push(args)
      return reply
    },
  }
  return {
    manager: redisConfigurationManager(client, { namespace: "test" }),
    calls,
  }
}

describe("configuration manager read", () => {
  it("returns null when no record exists", async () => {
    const { manager: subject } = manager(null)
    await expect(subject.read(identity)).resolves.toBeNull()
  })

  it("decodes a stored record", async () => {
    const { manager: subject } = manager(record(3))
    await expect(subject.read(identity)).resolves.toMatchObject({
      revision: 3,
      settings,
    })
  })

  it("reads the record key its coordinator uses", async () => {
    const { manager: subject, calls } = manager(null)
    await subject.read(identity)
    expect(calls[0]?.[0]).toBe(
      coordinationKey(
        "test",
        "bulkhead:orders",
        "submit",
        "tenant-a",
        "configuration",
      ),
    )
  })

  it("rejects a corrupt record", async () => {
    const { manager: subject } = manager("{")
    await expect(subject.read(identity)).rejects.toBeInstanceOf(
      ConfigurationError,
    )
  })

  it("wraps a transport failure", async () => {
    const client = {
      async eval() {
        throw new Error("connection lost")
      },
    }
    const subject = redisConfigurationManager(client, { namespace: "test" })
    await expect(subject.read(identity)).rejects.toBeInstanceOf(
      CoordinatorUnavailableError,
    )
  })
})

describe("configuration manager update", () => {
  it("sends a revision-zero template and the fingerprint", async () => {
    const { manager: subject, calls } = manager([2, 1, 1, record(1)])
    await subject.update(identity, 0, settings)
    const args = calls[0] ?? []
    expect(args[1]).toBe(0)
    expect(JSON.parse(String(args[2]))).toMatchObject({
      revision: 0,
      changedAt: 0,
      fingerprint: "bulkhead|limit=40",
    })
    expect(args[3]).toBe("bulkhead|limit=40")
  })

  it("reports applied, unchanged and conflict with the snapshot", async () => {
    for (const [status, expected] of [
      [2, "applied"],
      [0, "unchanged"],
      [1, "conflict"],
    ] as const) {
      const { manager: subject } = manager([status, 2, 1, record(2)])
      await expect(
        subject.update(identity, 1, settings),
      ).resolves.toMatchObject({
        status: expected,
        snapshot: { revision: 2 },
      })
    }
  })

  it("reports a conflict without a snapshot when the record is gone", async () => {
    const { manager: subject } = manager([1, 0, 0, ""])
    await expect(subject.update(identity, 4, settings)).resolves.toEqual({
      status: "conflict",
      snapshot: null,
    })
  })

  it("rejects a revision mismatch between the reply and the body", async () => {
    const { manager: subject } = manager([2, 9, 1, record(2)])
    await expect(subject.update(identity, 1, settings)).rejects.toBeInstanceOf(
      ConfigurationError,
    )
  })

  it("rejects a malformed reply", async () => {
    const { manager: subject } = manager([7])
    await expect(subject.update(identity, 0, settings)).rejects.toBeInstanceOf(
      ConfigurationError,
    )
  })

  it("rejects a policy mismatch before calling the client", async () => {
    const { manager: subject, calls } = manager([2, 1, 1, record(1)])
    await expect(
      subject.update(
        { ...identity, policy: "rateLimit" as const },
        0,
        settings,
      ),
    ).rejects.toBeInstanceOf(TypeError)
    expect(calls).toEqual([])
  })

  it("rejects a negative expected revision before calling the client", async () => {
    const { manager: subject, calls } = manager([2, 1, 1, record(1)])
    await expect(subject.update(identity, -1, settings)).rejects.toBeInstanceOf(
      RangeError,
    )
    expect(calls).toEqual([])
  })

  it("rejects settings no constructor would accept", async () => {
    const { manager: subject, calls } = manager([2, 1, 1, record(1)])
    await expect(
      subject.update(identity, 0, { policy: "bulkhead", limit: 0 }),
    ).rejects.toBeInstanceOf(RangeError)
    expect(calls).toEqual([])
  })
})
