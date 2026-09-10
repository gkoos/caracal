import { createCoordinationClient } from "../../../src/coordination/redis/client.js"
import { leaseCommand } from "../../../src/coordination/redis/leases.js"

const client = createCoordinationClient(process.env.CARACAL_REDIS_URL ?? "")
await client.connect()
process.send?.({ id: 0, value: "ready" })
process.on(
  "message",
  async (message: {
    id: number
    action:
      | "acquire"
      | "renew"
      | "release"
      | "freeze"
      | "execute"
      | "execute-timeout"
    key: string
    token: string
    ttl: number
    limit: number
  }) => {
    try {
      if (
        message.action === "execute" ||
        message.action === "execute-timeout"
      ) {
        const policy = bulkhead.distributed({
          name: "pool",
          limit: message.limit,
          leaseMs: message.ttl,
          coordinator: redisCoordinator(client, { namespace: message.token }),
          scope: () => "shared",
        })
        const op = operation({
          name: "work",
          adapter: {
            capabilities: () => ({ abort: "unsupported", replay: "safe" }),
            execute: async () => {
              const response = await fetch(message.key)
              return response.text()
            },
          },
          policies:
            message.action === "execute-timeout"
              ? [policy, timeout({ ms: 100 })]
              : [policy],
          events: {
            emit: (event) => {
              if (event.type.startsWith("bulkhead."))
                process.send?.({
                  id: -1,
                  event: { type: event.type, at: event.at },
                })
            },
          },
        })
        const value = await op.execute(undefined)
        process.send?.({ id: message.id, value })
        return
      }
      if (message.action === "freeze") {
        process.send?.({ id: message.id, value: "frozen" }, () => {
          Atomics.wait(
            new Int32Array(new SharedArrayBuffer(4)),
            0,
            0,
            message.ttl,
          )
        })
        return
      }
      const value = await leaseCommand(
        client,
        message.key,
        message.action,
        message.token,
        message.ttl,
        message.limit,
      )
      process.send?.({ id: message.id, value })
    } catch (error) {
      process.send?.({ id: message.id, error: String(error) })
    }
  },
)
process.on("disconnect", () => {
  client.disconnect()
  process.exit(0)
})

import { bulkhead, operation, timeout } from "../../../src/index.js"
import { redisCoordinator } from "../../../src/redis.js"
