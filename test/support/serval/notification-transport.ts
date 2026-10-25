import {
  encodeServalInvalidation,
  type ServalInvalidation,
  type ServalNotificationHandlers,
  type ServalNotificationTransport,
} from "../../../src/coordination/serval/notifications.js"

/**
 * A controllable notification transport.
 *
 * Delivery is explicit, so a test can duplicate a message, reorder two, drop one
 * or withhold a reconnect - the cases the session must survive without trusting
 * the hint.
 */

export interface FakeTransport {
  readonly transport: ServalNotificationTransport
  /** Delivers one raw payload. Silently dropped when not subscribed. */
  deliver(payload: string): void
  /** Delivers one encoded invalidation. */
  deliverInvalidation(invalidation: ServalInvalidation): void
  /** Delivers the same payload twice, as a duplicated publish does. */
  deliverTwice(payload: string): void
  /** Reports a lost connection. */
  disconnect(): void
  /** Reports a connection after a disconnect, which the session treats as one. */
  reconnect(): void
  /** Rejects the next `open`, as an unreachable Redis does. */
  failNextOpen(): void
  readonly opens: number
  readonly closes: number
  readonly delivered: number
  readonly subscribed: boolean
}

export function createFakeTransport(): FakeTransport {
  let handlers: ServalNotificationHandlers | undefined
  let opens = 0
  let closes = 0
  let delivered = 0
  let failOpen = false

  return {
    transport: {
      async open(next) {
        opens += 1
        if (failOpen) {
          failOpen = false
          next.onStatus("disconnected")
          throw new Error("fake transport cannot subscribe")
        }
        handlers = next
        next.onStatus("connected")
      },

      async close() {
        closes += 1
        handlers = undefined
      },
    },

    deliver(payload) {
      if (handlers === undefined) return
      delivered += 1
      handlers.onMessage(payload)
    },

    deliverInvalidation(invalidation) {
      this.deliver(encodeServalInvalidation(invalidation))
    },

    deliverTwice(payload) {
      this.deliver(payload)
      this.deliver(payload)
    },

    disconnect() {
      handlers?.onStatus("disconnected")
    },

    reconnect() {
      handlers?.onStatus("connected")
    },

    failNextOpen() {
      failOpen = true
    },

    get opens() {
      return opens
    },

    get closes() {
      return closes
    },

    get delivered() {
      return delivered
    },

    get subscribed() {
      return handlers !== undefined
    },
  }
}
