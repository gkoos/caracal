/**
 * Soak worker for test/integration/soak.integration.test.ts.
 *
 * It drives the *shipped* coordinators - `redisCoordinator` or
 * `redisCircuitBreakerCoordinator` - rather than a test-only lease helper, and
 * reports every interval during which its adapter was running, timed on the
 * Redis server's clock so the parent can reconstruct concurrency across worker
 * processes. Configuration arrives through the environment, so the parent owns
 * the schedule and a worker killed mid-permit takes nothing with it.
 */
import { createHash } from "node:crypto"
import {
  bulkhead,
  circuitBreaker,
  operation,
  type Policy,
} from "../../../src/index.js"
import { redisCoordinator } from "../../../src/coordination/redis/bulkhead.js"
import { redisCircuitBreakerCoordinator } from "../../../src/coordination/redis/circuit-breaker.js"
import { createCoordinationClient } from "../../../src/coordination/redis/client.js"

if (!process.send) {
  throw new Error("soak worker must be forked with an IPC channel")
}
const send = (message: unknown): void => {
  process.send?.(message)
}

const url = process.env.CARACAL_REDIS_URL ?? ""
const namespace = process.env.CARACAL_SOAK_NAMESPACE ?? ""
const mode =
  process.env.CARACAL_SOAK_MODE === "breaker" ? "breaker" : "bulkhead"
const limit = Number(process.env.CARACAL_SOAK_LIMIT ?? "3")
const leaseMs = Number(process.env.CARACAL_SOAK_LEASE_MS ?? "400")
const runMs = Number(process.env.CARACAL_SOAK_RUN_MS ?? "15000")
const seed = Number(process.env.CARACAL_SOAK_SEED ?? "1")
const failureRate = Number(process.env.CARACAL_SOAK_FAILURE_RATE ?? "0.4")

/** Seeded, so a failing schedule can be replayed exactly by the parent. */
function mulberry32(seedValue: number) {
  let state = seedValue
  return () => {
    state = (state + 0x6d2b79f5) | 0
    let t = Math.imul(state ^ (state >>> 15), 1 | state)
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t
    return ((t ^ (t >>> 14)) >>> 0) / 4_294_967_296
  }
}
const random = mulberry32(seed)

const sleep = (ms: number) =>
  new Promise<void>((resolve) => setTimeout(resolve, ms))

const client = createCoordinationClient(url)
const sha1 = (body: string) => createHash("sha1").update(body).digest("hex")

/**
 * The coordinators need `eval`, `evalsha` and `hmget` only. Recording what they
 * send lets the test assert the shipped Lua bodies are the ones in play, rather
 * than a near-copy that only the tests agree on.
 */
const sentScripts = new Set<string>()
const recording = {
  eval(script: string, numberOfKeys: number, ...args: (string | number)[]) {
    sentScripts.add(sha1(script))
    return client.eval(script, numberOfKeys, ...args)
  },
  evalsha(sha: string, numberOfKeys: number, ...args: (string | number)[]) {
    sentScripts.add(sha)
    return client.evalsha(sha, numberOfKeys, ...args)
  },
  hmget(key: string, ...fields: string[]) {
    return client.hmget(key, ...fields)
  },
}

/** Server clock, so intervals from different worker processes are comparable. */
async function serverNow(): Promise<number> {
  const [seconds, microseconds] = (await client.time()) as number[]
  return Number(seconds) * 1000 + Math.floor(Number(microseconds) / 1000)
}

function soakPolicy(): Policy {
  if (mode === "breaker") {
    return circuitBreaker.distributed({
      name: "soak",
      coordinator: redisCircuitBreakerCoordinator(recording, { namespace }),
      scope: () => "shared",
      minimumThroughput: 5,
      failureThreshold: 0.5,
      openMs: 300,
      halfOpenProbes: 2,
      halfOpenSuccesses: 1,
      windowSize: 20,
    })
  }
  return bulkhead.distributed({
    name: "soak",
    limit,
    leaseMs,
    coordinator: redisCoordinator(recording, { namespace }),
    scope: () => "shared",
  })
}

const subject = operation({
  name: "soak-op",
  adapter: {
    capabilities: () => ({
      abort: "supported" as const,
      replay: "safe" as const,
    }),
    execute: async () => {
      // The adapter runs only once a permit - or a breaker probe - was granted,
      // so this interval is a conservative lower bound on the admitted window.
      const start = await serverNow()
      try {
        await sleep(10 + Math.floor(random() * 60))
        if (mode === "breaker" && random() < failureRate) {
          throw new Error("soak dependency failure")
        }
        return "ok"
      } finally {
        send({ kind: "interval", start, end: await serverNow() })
      }
    },
  },
  policies: [soakPolicy()],
})

process.on("message", (message: { action?: string; freezeMs?: number }) => {
  if (message?.action === "freeze") {
    const until = Date.now() + (message.freezeMs ?? 1_000)
    // Blocking the event loop is what a long GC pause or a suspended VM looks
    // like to Redis: no renewals, no releases, no replies, until it ends.
    while (Date.now() < until) {
      /* spin */
    }
    return
  }
  if (message?.action === "stop") {
    send({ kind: "scripts", scripts: [...sentScripts] })
    client.disconnect()
    process.exit(0)
  }
})

send({ kind: "ready" })

const deadline = Date.now() + runMs
let attempts = 0
let succeeded = 0
while (Date.now() < deadline) {
  attempts += 1
  let outcome = "success"
  try {
    await subject.execute(undefined)
    succeeded += 1
  } catch (error) {
    outcome = error instanceof Error ? error.name : "Error"
  }
  send({ kind: "attempt", outcome, succeeded, attempts, localAt: Date.now() })
  await sleep(Math.floor(random() * 20))
}

send({ kind: "done", attempts, succeeded })
client.disconnect()
process.exit(0)
