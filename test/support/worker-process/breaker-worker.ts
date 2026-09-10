import { createCoordinationClient } from "../../../src/coordination/redis/client.js"
import { circuitBreaker, operation } from "../../../src/index.js"
import { redisCircuitBreakerCoordinator } from "../../../src/redis.js"

const client = createCoordinationClient(process.env.CARACAL_REDIS_URL ?? "")
await client.connect()
process.send?.({ id: 0, value: "ready" })

type Message = {
  id: number
  action: "execute" | "freeze"
  outcome: "success" | "failure"
  operationName: string
  namespace: string
  scope: string
  minimumThroughput?: number
  failureThreshold?: number
  windowSize?: number
  openMs?: number
  halfOpenProbes?: number
  halfOpenSuccesses?: number
  probeLeaseTtlMs?: number
  /** For freeze: how many ms to block */
  freezeMs?: number
}

process.on("message", async (message: Message) => {
  try {
    if (message.action === "freeze") {
      process.send?.({ id: message.id, value: "frozen" }, () => {
        // Block the event loop to simulate a dead worker; probe tokens expire
        // naturally while this worker is frozen.
        Atomics.wait(
          new Int32Array(new SharedArrayBuffer(4)),
          0,
          0,
          message.freezeMs ?? 30_000,
        )
      })
      return
    }

    // "execute" — run one attempt through the distributed circuit breaker.
    const coordinator = redisCircuitBreakerCoordinator(client, {
      namespace: message.namespace,
    })

    const policy = circuitBreaker.distributed({
      name: "breaker",
      coordinator,
      scope: () => message.scope ?? "shared",
      minimumThroughput: message.minimumThroughput,
      failureThreshold: message.failureThreshold,
      windowSize: message.windowSize,
      openMs: message.openMs,
      halfOpenProbes: message.halfOpenProbes,
      halfOpenSuccesses: message.halfOpenSuccesses,
      probeLeaseTtlMs: message.probeLeaseTtlMs,
    })

    const op = operation({
      name: message.operationName ?? "work",
      adapter: {
        capabilities: () => ({ abort: "unsupported", replay: "safe" }),
        execute: async () => {
          if (message.outcome === "failure") throw new Error("injected-failure")
          return "ok"
        },
      },
      policies: [policy],
      events: {
        emit: (event) => {
          if (event.type.startsWith("breaker.")) {
            process.send?.({
              id: -1,
              event: {
                type: event.type,
                at: event.at,
                // Forward fields useful for test assertions
                ...("state" in event ? { state: event.state } : {}),
                ...("previousState" in event
                  ? { previousState: event.previousState }
                  : {}),
                ...("generation" in event
                  ? { generation: event.generation }
                  : {}),
                ...("outcome" in event ? { outcome: event.outcome } : {}),
                ...("behavior" in event ? { behavior: event.behavior } : {}),
                ...("reason" in event ? { reason: event.reason } : {}),
              },
            })
          }
        },
      },
    })

    const value = await op.execute(undefined)
    process.send?.({ id: message.id, value })
  } catch (error) {
    process.send?.({ id: message.id, error: String(error) })
  }
})

process.on("disconnect", () => {
  client.disconnect()
  process.exit(0)
})
