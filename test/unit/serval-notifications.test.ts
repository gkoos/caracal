/**
 * Unit tests for the Serval invalidation protocol and its transports.
 *
 * The invalidation is a versioned hint, so the two things that matter are that a
 * malformed payload is reported rather than guessed at, and that the transport
 * layer removes every listener it registered - a long-lived session reconnects
 * many times and must not leak handlers.
 */

import type { Redis } from "ioredis"
import { describe, expect, it } from "vitest"
import {
  SERVAL_INVALIDATION_FORMAT,
  createRedisNotificationTransport,
  createServalNotificationChannel,
  decodeServalInvalidation,
  encodeServalInvalidation,
  type ServalInvalidation,
  type ServalSubscriptionClient,
  type ServalSubscriptionListener,
} from "../../src/coordination/serval/notifications.js"
import { createFakeTransport } from "../support/serval/notification-transport.js"
import { TEST_IDENTITY } from "../support/serval/session-harness.js"

function invalidation(
  overrides: Partial<ServalInvalidation> = {},
): ServalInvalidation {
  return {
    identity: TEST_IDENTITY,
    version: {
      authorityIncarnation: "authority-1",
      epoch: 2,
      version: 3,
      configRevision: 4,
    },
    kind: "authority",
    ...overrides,
  }
}

/** A subscription client that records listeners, so leaks are observable. */
function fakeSubscriptionClient() {
  const listeners = new Map<string, Set<ServalSubscriptionListener>>()
  const subscribed: string[] = []
  let failSubscribe = false

  const client: ServalSubscriptionClient = {
    async subscribe(channel) {
      if (failSubscribe) throw new Error("subscribe failed")
      subscribed.push(channel)
      return 1
    },
    async unsubscribe(channel) {
      const index = subscribed.indexOf(channel)
      if (index >= 0) subscribed.splice(index, 1)
      return 1
    },
    on(event, listener) {
      const set = listeners.get(event) ?? new Set()
      set.add(listener)
      listeners.set(event, set)
      return client
    },
    off(event, listener) {
      listeners.get(event)?.delete(listener)
      return client
    },
  }

  return {
    client,
    emit(event: string, ...args: readonly unknown[]) {
      for (const listener of listeners.get(event) ?? []) {
        ;(listener as (...args: readonly unknown[]) => void)(...args)
      }
    },
    listenerCount(event: string) {
      return listeners.get(event)?.size ?? 0
    },
    subscribedChannels() {
      return [...subscribed]
    },
    failNextSubscribe() {
      failSubscribe = true
    },
  }
}

describe("serval invalidation codec", () => {
  it("round-trips an invalidation", () => {
    const original = invalidation()
    const decoded = decodeServalInvalidation(encodeServalInvalidation(original))

    expect(decoded).toEqual(original)
  })

  it("encodes a compact, versioned payload", () => {
    const wire = JSON.parse(encodeServalInvalidation(invalidation())) as Record<
      string,
      unknown
    >

    expect(wire.v).toBe(SERVAL_INVALIDATION_FORMAT)
    // The payload names the identity and the version and carries no record body.
    expect(Object.keys(wire).sort()).toEqual([
      "c",
      "e",
      "i",
      "k",
      "n",
      "o",
      "r",
      "s",
      "t",
      "v",
    ])
    expect(wire.t).toBe("authority")
  })

  it("reports a malformed payload instead of guessing", () => {
    const payloads: unknown[] = [
      undefined,
      null,
      42,
      "",
      "{",
      "[]",
      "null",
      JSON.stringify({ v: 99 }),
      JSON.stringify({
        ...JSON.parse(encodeServalInvalidation(invalidation())),
        t: "other",
      }),
      JSON.stringify({
        ...JSON.parse(encodeServalInvalidation(invalidation())),
        k: "unknown-policy",
      }),
      JSON.stringify({
        ...JSON.parse(encodeServalInvalidation(invalidation())),
        e: -1,
      }),
      JSON.stringify({
        ...JSON.parse(encodeServalInvalidation(invalidation())),
        r: 1.5,
      }),
      JSON.stringify({
        ...JSON.parse(encodeServalInvalidation(invalidation())),
        s: "",
      }),
      JSON.stringify({
        ...JSON.parse(encodeServalInvalidation(invalidation())),
        i: "x".repeat(257),
      }),
    ]

    for (const payload of payloads) {
      expect(decodeServalInvalidation(payload), String(payload)).toBeUndefined()
    }
  })

  it("accepts both invalidation kinds", () => {
    for (const kind of ["authority", "configuration"] as const) {
      expect(
        decodeServalInvalidation(
          encodeServalInvalidation(invalidation({ kind })),
        )?.kind,
      ).toBe(kind)
    }
  })
})

/** Compile-time guard: the ioredis client satisfies the subscription contract. */
type RedisSatisfiesSubscriptionClient = Redis extends ServalSubscriptionClient
  ? true
  : never

describe("serval redis notification transport", () => {
  it("accepts an ioredis client as a subscription client", () => {
    // If ioredis stopped satisfying the contract this assignment would not
    // typecheck, because the alias resolves to `never`.
    const guard: RedisSatisfiesSubscriptionClient = true
    expect(guard).toBe(true)
  })

  it("subscribes to its channel and forwards only that channel's messages", async () => {
    const fake = fakeSubscriptionClient()
    const transport = createRedisNotificationTransport(
      fake.client,
      "caracal:channel",
    )
    const received: string[] = []
    const statuses: string[] = []

    await transport.open({
      onMessage: (payload) => received.push(payload),
      onStatus: (status) => statuses.push(status),
    })

    expect(fake.subscribedChannels()).toEqual(["caracal:channel"])
    expect(statuses).toEqual(["connected"])

    fake.emit("message", "caracal:channel", "payload-1")
    fake.emit("message", "another:channel", "payload-2")

    expect(received).toEqual(["payload-1"])
  })

  it("reports the client's connection changes", async () => {
    const fake = fakeSubscriptionClient()
    const transport = createRedisNotificationTransport(fake.client, "ch")
    const statuses: string[] = []
    await transport.open({
      onMessage: () => {},
      onStatus: (status) => statuses.push(status),
    })

    fake.emit("close")
    fake.emit("ready")
    fake.emit("end")
    fake.emit("error", new Error("boom"))

    expect(statuses).toEqual([
      "connected",
      "disconnected",
      "connected",
      "disconnected",
      "disconnected",
    ])
  })

  it("removes every listener and unsubscribes on close", async () => {
    const fake = fakeSubscriptionClient()
    const transport = createRedisNotificationTransport(fake.client, "ch")
    await transport.open({ onMessage: () => {}, onStatus: () => {} })
    expect(fake.listenerCount("message")).toBe(1)

    await transport.close()

    for (const event of ["message", "error", "close", "end", "ready"]) {
      expect(fake.listenerCount(event), event).toBe(0)
    }
    expect(fake.subscribedChannels()).toEqual([])
  })

  it("reports disconnected and rethrows when the subscription fails", async () => {
    const fake = fakeSubscriptionClient()
    fake.failNextSubscribe()
    const transport = createRedisNotificationTransport(fake.client, "ch")
    const statuses: string[] = []

    await expect(
      transport.open({
        onMessage: () => {},
        onStatus: (status) => statuses.push(status),
      }),
    ).rejects.toThrow("subscribe failed")
    expect(statuses).toEqual(["disconnected"])
  })
})

function channelHarness() {
  const fake = createFakeTransport()
  const received: ServalInvalidation[] = []
  const malformed: string[] = []
  let reconnects = 0

  const channel = createServalNotificationChannel({
    transport: fake.transport,
    onInvalidation: (value) => received.push(value),
    onMalformed: (payload) => malformed.push(payload),
    onReconnect: () => {
      reconnects += 1
    },
  })

  return {
    fake,
    channel,
    received,
    malformed,
    reconnects: () => reconnects,
  }
}

describe("serval notification channel", () => {
  it("dispatches a decoded invalidation and reports a malformed payload", async () => {
    const harness = channelHarness()

    expect(harness.channel.status).toBe("idle")
    await harness.channel.open()
    expect(harness.channel.status).toBe("connected")
    expect(harness.channel.subscribed).toBe(true)

    harness.fake.deliver(encodeServalInvalidation(invalidation()))
    harness.fake.deliver("{")

    expect(harness.received).toEqual([invalidation()])
    expect(harness.malformed).toEqual(["{"])
  })

  it("treats only a connection after a disconnect as a reconnect", async () => {
    const harness = channelHarness()

    await harness.channel.open()
    expect(harness.reconnects()).toBe(0)

    harness.fake.disconnect()
    expect(harness.channel.status).toBe("disconnected")

    harness.fake.reconnect()
    expect(harness.reconnects()).toBe(1)
    expect(harness.channel.status).toBe("connected")
  })

  it("is idempotent on open and closes once", async () => {
    const harness = channelHarness()

    await harness.channel.open()
    await harness.channel.open()
    expect(harness.fake.opens).toBe(1)

    await harness.channel.close()
    await harness.channel.close()
    expect(harness.fake.closes).toBe(1)
    expect(harness.channel.status).toBe("idle")
    expect(harness.channel.subscribed).toBe(false)
  })

  it("retries an open that failed", async () => {
    const harness = channelHarness()
    harness.fake.failNextOpen()

    await expect(harness.channel.open()).rejects.toThrow()
    expect(harness.channel.subscribed).toBe(false)
    expect(harness.channel.status).toBe("disconnected")

    await harness.channel.open()
    expect(harness.channel.subscribed).toBe(true)
    expect(harness.channel.status).toBe("connected")
    expect(harness.fake.opens).toBe(2)
  })

  it("drops a message that arrives after close", async () => {
    const harness = channelHarness()
    await harness.channel.open()
    await harness.channel.close()

    harness.fake.deliver(encodeServalInvalidation(invalidation()))

    expect(harness.received).toEqual([])
  })
})
