import {
  isValidServalIdentity,
  isValidServalVersion,
  type ServalAuthorityVersion,
  type ServalScopeIdentity,
} from "./authority.js"

/**
 * The versioned invalidation and the transport that carries it.
 *
 * An invalidation is a compact **hint**: it names an identity and the version it
 * moved to, and never carries a payload. A subscriber applies a hint only when it
 * advances the version it already holds, so reordering and duplication are
 * harmless, and it drops a hint for an identity it does not hold, so fan-out can
 * be broad without per-scope subscription management.
 *
 * The wire format is versioned: an unknown format is malformed rather than
 * guessed at, and a malformed payload is reported, not silently ignored.
 */

export type ServalInvalidationKind = "authority" | "configuration"

export interface ServalInvalidation {
  readonly identity: ServalScopeIdentity
  readonly version: ServalAuthorityVersion
  readonly kind: ServalInvalidationKind
}

/** The wire format this build publishes and accepts. */
export const SERVAL_INVALIDATION_FORMAT = 1

export function encodeServalInvalidation(
  invalidation: ServalInvalidation,
): string {
  return JSON.stringify({
    v: SERVAL_INVALIDATION_FORMAT,
    k: invalidation.identity.policyKind,
    n: invalidation.identity.policyName,
    o: invalidation.identity.operation,
    s: invalidation.identity.scope,
    i: invalidation.version.authorityIncarnation,
    e: invalidation.version.epoch,
    r: invalidation.version.version,
    c: invalidation.version.configRevision,
    t: invalidation.kind,
  })
}

/** Decodes a payload, or returns `undefined` when it is malformed. */
export function decodeServalInvalidation(
  payload: unknown,
): ServalInvalidation | undefined {
  if (typeof payload !== "string") return undefined

  let parsed: unknown
  try {
    parsed = JSON.parse(payload)
  } catch {
    return undefined
  }
  if (typeof parsed !== "object" || parsed === null) return undefined

  const wire = parsed as Record<string, unknown>
  if (wire.v !== SERVAL_INVALIDATION_FORMAT) return undefined
  if (wire.t !== "authority" && wire.t !== "configuration") return undefined

  const identity = {
    policyKind: wire.k,
    policyName: wire.n,
    operation: wire.o,
    scope: wire.s,
  }
  const version = {
    authorityIncarnation: wire.i,
    epoch: wire.e,
    version: wire.r,
    configRevision: wire.c,
  }
  if (!isValidServalIdentity(identity)) return undefined
  if (!isValidServalVersion(version)) return undefined

  return { identity, version, kind: wire.t }
}

export type ServalTransportStatus = "connected" | "disconnected"

export interface ServalNotificationHandlers {
  readonly onMessage: (payload: string) => void
  readonly onStatus: (status: ServalTransportStatus) => void
}

export interface ServalNotificationTransport {
  /** Subscribes and starts reporting messages and connection changes. */
  open(handlers: ServalNotificationHandlers): Promise<void>
  close(): Promise<void>
}

/**
 * A Redis client event listener.
 *
 * Declared as `(...args: unknown[]) => void` because that is the shape an
 * EventEmitter satisfies structurally. A `never[]` parameter list reads better at
 * a call site but is rejected for the generic `off` overload ioredis inherits, so
 * the transport casts once at its single registration point instead.
 */
export type ServalSubscriptionListener = (...args: unknown[]) => void

/**
 * The subscription capability the Redis notification transport needs.
 *
 * ioredis satisfies it structurally; a test double implements it directly, so
 * the transport is verifiable without a Redis server.
 */
export interface ServalSubscriptionClient {
  subscribe(channel: string): Promise<unknown>
  unsubscribe(channel: string): Promise<unknown>
  on(event: string, listener: ServalSubscriptionListener): unknown
  off(event: string, listener: ServalSubscriptionListener): unknown
}

/**
 * The first transport: one Redis pub/sub channel per namespace.
 *
 * A disconnect is reported as `disconnected` and a reconnect as `connected`, and
 * the channel layer turns the latter into a reconciliation. Delivery is
 * best-effort by construction; correctness comes from expiry and reconciliation,
 * never from the message.
 */
export function createRedisNotificationTransport(
  client: ServalSubscriptionClient,
  channel: string,
): ServalNotificationTransport {
  const listeners: Array<{
    readonly event: string
    readonly listener: ServalSubscriptionListener
  }> = []

  function listen(event: string, listener: (...args: never[]) => void): void {
    // The one place the two views of a listener meet: call sites pass the
    // arguments they care about, the client is registered with the shape an
    // EventEmitter declares.
    const registered = listener as ServalSubscriptionListener
    client.on(event, registered)
    listeners.push({ event, listener: registered })
  }

  return {
    async open(handlers) {
      const onMessage = (received: string, payload: string): void => {
        if (received === channel) {
          handlers.onMessage(payload)
        }
      }
      const installed = listeners.length
      listen("message", onMessage)
      listen("error", () => handlers.onStatus("disconnected"))
      listen("close", () => handlers.onStatus("disconnected"))
      listen("end", () => handlers.onStatus("disconnected"))
      listen("ready", () => handlers.onStatus("connected"))

      try {
        await client.subscribe(channel)
      } catch (error) {
        // This attempt's listeners go with the attempt: the channel records a failed
        // open as not-opened, so its close() would skip the transport cleanup that
        // removes them, and every retry would install another set.
        for (const { event, listener } of listeners.splice(installed)) {
          client.off(event, listener)
        }
        handlers.onStatus("disconnected")
        throw error
      }
      handlers.onStatus("connected")
    },

    async close() {
      // Every listener this transport registered is removed, so a long-lived
      // session that reconnects repeatedly cannot leak handlers.
      for (const { event, listener } of listeners.splice(0)) {
        client.off(event, listener)
      }
      try {
        await client.unsubscribe(channel)
      } catch {
        // Best effort: shutdown must not depend on a clean unsubscribe.
      }
    },
  }
}

/**
 * The publish capability the Redis invalidation publisher needs.
 *
 * ioredis satisfies it structurally; a test double implements it directly, so the
 * publisher is verifiable without a Redis server.
 */
export interface ServalPublisherClient {
  publish(channel: string, payload: string): Promise<unknown>
}

/**
 * Publishes a versioned invalidation.
 *
 * Publication is best-effort by contract: a hint that is lost is a hint that
 * arrives by the next reconciliation, so a failure must never fail the operation
 * that produced the transition.
 */
export interface ServalInvalidationPublisher {
  publish(payload: string): Promise<void>
}

export function createRedisInvalidationPublisher(
  client: ServalPublisherClient,
  channel: string,
): ServalInvalidationPublisher {
  return {
    async publish(payload) {
      await client.publish(channel, payload)
    },
  }
}

export type ServalChannelStatus = "idle" | ServalTransportStatus

export interface ServalNotificationChannelOptions {
  readonly transport: ServalNotificationTransport
  /** A well-formed invalidation. Version filtering is the caller's decision. */
  readonly onInvalidation: (invalidation: ServalInvalidation) => void
  /** A payload that is not a valid invalidation of a known format. */
  readonly onMalformed: (payload: string) => void
  /** The transport reported a connection after having been disconnected. */
  readonly onReconnect: () => void
}

export interface ServalNotificationChannel {
  readonly status: ServalChannelStatus
  /** True once the transport accepted the subscription. */
  readonly subscribed: boolean
  open(): Promise<void>
  close(): Promise<void>
}

/**
 * Binds a transport to the invalidation protocol: decode, report, dispatch.
 *
 * A reconnect is only reported as such when the channel had previously seen a
 * disconnect. The first connection is not a reconnect, so the initial
 * reconciliation stays the cold start's job rather than running twice.
 */
export function createServalNotificationChannel(
  options: ServalNotificationChannelOptions,
): ServalNotificationChannel {
  let status: ServalChannelStatus = "idle"
  let opened = false
  let subscribed = false

  return {
    get status() {
      return status
    },

    get subscribed() {
      return subscribed
    },

    async open() {
      if (opened || subscribed) return
      opened = true

      try {
        await options.transport.open({
          onMessage(payload) {
            const invalidation = decodeServalInvalidation(payload)
            if (invalidation === undefined) {
              options.onMalformed(payload)
              return
            }
            options.onInvalidation(invalidation)
          },
          onStatus(next) {
            const reconnected =
              status === "disconnected" && next === "connected"
            status = next
            if (reconnected) options.onReconnect()
          },
        })
        subscribed = true
      } catch (error) {
        // A failed open is retryable: the transport may connect later.
        opened = false
        status = "disconnected"
        throw error
      }
    },

    async close() {
      if (!opened && !subscribed) return
      opened = false
      subscribed = false
      status = "idle"
      await options.transport.close()
    },
  }
}
