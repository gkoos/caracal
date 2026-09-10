import { describe, expect, it } from "vitest"
import { redisCoordinator } from "../../src/coordination/redis/bulkhead.js"
import { redisCircuitBreakerCoordinator } from "../../src/coordination/redis/circuit-breaker.js"
import {
  createCoordinationClient,
  createCoordinationClusterClient,
} from "../../src/coordination/redis/client.js"

describe("createCoordinationClusterClient", () => {
  it("rejects an empty nodes array", () => {
    expect(() => createCoordinationClusterClient([])).toThrow(RangeError)
  })

  it("rejects an invalid commandTimeout", () => {
    expect(() =>
      createCoordinationClusterClient([{ host: "localhost", port: 7000 }], 0),
    ).toThrow(RangeError)
    expect(() =>
      createCoordinationClusterClient([{ host: "localhost", port: 7000 }], -1),
    ).toThrow(RangeError)
  })

  it("returns a Cluster instance with disconnect method", () => {
    const client = createCoordinationClusterClient([
      { host: "127.0.0.1", port: 7000 },
    ])
    expect(typeof client.disconnect).toBe("function")
    expect(typeof client.eval).toBe("function")
    expect(typeof client.hmget).toBe("function")
    client.disconnect()
  })

  it("satisfies RedisScriptClient — both coordinators accept it without error", () => {
    const client = createCoordinationClusterClient([
      { host: "127.0.0.1", port: 7000 },
    ])
    expect(() =>
      redisCoordinator(client, { namespace: "test:cluster" }),
    ).not.toThrow()
    expect(() =>
      redisCircuitBreakerCoordinator(client, { namespace: "test:cluster" }),
    ).not.toThrow()
    client.disconnect()
  })

  it("accepts multiple nodes", () => {
    const client = createCoordinationClusterClient([
      { host: "127.0.0.1", port: 7000 },
      { host: "127.0.0.1", port: 7001 },
      { host: "127.0.0.1", port: 7002 },
    ])
    expect(typeof client.disconnect).toBe("function")
    client.disconnect()
  })
})

describe("createCoordinationClient", () => {
  it("rejects an invalid commandTimeout", () => {
    expect(() => createCoordinationClient("redis://localhost", 0)).toThrow(
      RangeError,
    )
  })
})
