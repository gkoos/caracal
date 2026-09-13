import { fork, type ChildProcess } from "node:child_process"
import { randomUUID } from "node:crypto"
import { fileURLToPath } from "node:url"
import { describe, expect, it } from "vitest"
import { bulkhead, operation } from "../../src/index.js"
import { createCoordinationClient } from "../../src/coordination/redis/client.js"
import { scriptSha } from "../../src/coordination/redis/eval-script.js"
import { coordinationKey } from "../../src/coordination/redis/keys.js"
import { redisCoordinator } from "../../src/coordination/redis/bulkhead.js"
import {
  breakerAdmitProbeV1,
  breakerObserveV1,
  breakerSettleProbeV1,
  bulkheadLeaseV1,
} from "../../src/coordination/redis/scripts.js"
import { redisProxy } from "../support/redis-proxy.js"

/**
 * Soak and chaos test for the distributed claims.
 *
 * The invariant is the one the library exists to provide: never more than `limit`
 * concurrent admitted, no double admission, no leaked permit - while worker
 * processes are killed mid-permit, have their event loops frozen past their
 * lease, and lose the network.
 *
 * Two independent witnesses:
 *
 * 1. A sampler that reads the server clock and the live lease count in one atomic
 *    script, so every sample is a coherent (time, count) pair on the server's
 *    clock. "Live" means deadline in the future: expired members stay in the
 *    sorted set until a script prunes them, so a raw ZCARD would over-count and
 *    invent violations.
 * 2. Every worker reports the interval during which its adapter ran, on that same
 *    clock. The adapter only runs after admission, so reconstruction is a
 *    conservative lower bound: it cannot invent a violation, and it cross-checks
 *    the sampler in the direction that matters - the live count must never be
 *    *lower* than the work the workers believe they hold.
 *
 * The workers drive the shipped coordinators, and the scripts they send are
 * asserted against the production constants, so a test-only near-copy cannot
 * pass this suite.
 */

const redisUrl = process.env.CARACAL_REDIS_URL
const WORKERS = Number(process.env.CARACAL_SOAK_WORKERS ?? "4")
const LIMIT = Number(process.env.CARACAL_SOAK_LIMIT ?? "3")
const LEASE_MS = Number(process.env.CARACAL_SOAK_LEASE_MS ?? "400")
const SEED = Number(process.env.CARACAL_SOAK_SEED ?? "1")
const RUN_MS = Math.round(
  Number(process.env.CARACAL_SOAK_MINUTES ?? "0.25") * 60_000,
)
const SAMPLE_MS = 5

/**
 * `{now, liveLeases}` for the bulkhead; `{now, liveProbes, generationMembers}`
 * for the breaker. Written here rather than in src because it is a measurement,
 * not part of the shipped protocol - and it reads the same server clock the
 * coordinators do, so no offset estimation is needed.
 */
const SAMPLER = `
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
if #KEYS == 1 then
  return {now, redis.call('ZCOUNT', KEYS[1], now + 1, '+inf')}
end
local state = redis.call('HMGET', KEYS[1], 'state', 'generation')
local generation = state[2]
local members = 0
if generation then
  local prefix = generation .. ':'
  for _, member in ipairs(redis.call('ZRANGE', KEYS[2], 0, -1)) do
    if string.sub(member, 1, string.len(prefix)) == prefix then
      members = members + 1
    end
  end
end
return {now, redis.call('ZCOUNT', KEYS[3], now + 1, '+inf'), members}
`

type Interval = { worker: number; start: number; end: number }
type Sample = { at: number; live: number; generationMembers: number }
type Action =
  | { kind: "kill"; worker: number; atFraction: number }
  | { kind: "freeze"; worker: number; ms: number; atFraction: number }
  | { kind: "partition"; ms: number; atFraction: number }
  | { kind: "delete-state"; atFraction: number }

type SoakReport = {
  intervals: Interval[]
  samples: Sample[]
  actions: Array<{ label: string; at: number; localAt: number }>
  scripts: string[]
  attempts: number
  lastAttemptAt: number
  diagnostics: string
  maxLive: number
  maxGenerationMembers: number
  reconstructedMax: number
  finalLive: number
}
const sleep = (ms: number) =>
  new Promise<void>((resolve) => setTimeout(resolve, ms))

/** Live leases at `key`, on the same clock the coordinators use. */
async function liveLeases(
  client: ReturnType<typeof createCoordinationClient>,
  key: string,
): Promise<number> {
  const result = (await client.eval(SAMPLER, 1, key)) as number[]
  return Number(result[1])
}

function spawnWorker(options: {
  url: string
  namespace: string
  mode: "bulkhead" | "breaker"
  seed: number
}): ChildProcess {
  return fork(
    fileURLToPath(
      new URL("../support/worker-process/soak-worker.ts", import.meta.url),
    ),
    [],
    {
      execArgv: ["--import", "tsx"],
      stdio: ["ignore", "inherit", "inherit", "ipc"],
      env: {
        ...process.env,
        CARACAL_REDIS_URL: options.url,
        CARACAL_SOAK_NAMESPACE: options.namespace,
        CARACAL_SOAK_MODE: options.mode,
        CARACAL_SOAK_LIMIT: String(LIMIT),
        CARACAL_SOAK_LEASE_MS: String(LEASE_MS),
        // Long enough that a worker never terminates itself: the parent owns the
        // schedule and stops the survivors when it is done with them.
        CARACAL_SOAK_RUN_MS: String(RUN_MS + 120_000),
        CARACAL_SOAK_SEED: String(options.seed),
      },
    },
  )
}

/** Highest number of workers simultaneously inside their adapter. */
function reconstructedMax(intervals: Interval[]): number {
  const events = intervals.flatMap((interval) => [
    { at: interval.start, delta: 1 },
    { at: interval.end, delta: -1 },
  ])
  events.sort((a, b) => a.at - b.at || a.delta - b.delta)
  let current = 0
  let max = 0
  for (const event of events) {
    current += event.delta
    max = Math.max(max, current)
  }
  return max
}

async function runSoak(options: {
  mode: "bulkhead" | "breaker"
  actions?: Action[]
}): Promise<SoakReport> {
  const namespace = `soak-${randomUUID()}`
  const workerCount = Math.max(2, WORKERS)
  const sampler = createCoordinationClient(redisUrl as string)
  await sampler.connect()

  const leaseKey = coordinationKey(
    namespace,
    "bulkhead:soak",
    "soak-op",
    "shared",
  )
  const stateKey = coordinationKey(
    namespace,
    "breaker:soak",
    "soak-op",
    "shared",
    "breaker",
  )
  const observationsKey = coordinationKey(
    namespace,
    "breaker:soak",
    "soak-op",
    "shared",
    "observations",
  )
  const probesKey = coordinationKey(
    namespace,
    "breaker:soak",
    "soak-op",
    "shared",
    "probes",
  )
  const keys =
    options.mode === "breaker"
      ? [stateKey, observationsKey, probesKey]
      : [leaseKey]

  const intervals: Interval[] = []
  const samples: Sample[] = []
  const fired: Array<{ label: string; at: number; localAt: number }> = []
  const scripts = new Set<string>()
  const attemptsByWorker = new Map<number, number>()
  const lastIntervalByWorker = new Map<number, number>()
  const exitCodes = new Map<number, number | null>()
  let lastAttemptAt = 0
  let stopping = false

  const proxy = options.actions?.some((action) => action.kind === "partition")
    ? await redisProxy(new URL(redisUrl as string))
    : undefined

  const children = Array.from({ length: workerCount }, (_value, index) => {
    const child = spawnWorker({
      url: proxy ? proxy.url : (redisUrl as string),
      namespace,
      mode: options.mode,
      seed: SEED + index,
    })
    child.on(
      "message",
      (message: {
        kind?: string
        start?: number
        end?: number
        scripts?: string[]
        attempts?: number
        localAt?: number
      }) => {
        if (message.kind === "interval") {
          intervals.push({
            worker: index,
            start: Number(message.start),
            end: Number(message.end),
          })
          lastIntervalByWorker.set(index, Number(message.start))
          return
        }
        if (message.kind === "attempt") {
          attemptsByWorker.set(index, Number(message.attempts ?? 0))
          lastAttemptAt = Math.max(lastAttemptAt, Number(message.localAt ?? 0))
          return
        }
        if (message.kind === "scripts") {
          for (const script of message.scripts ?? []) scripts.add(script)
        }
      },
    )
    return child
  })

  children.forEach((child, index) => {
    child.on("exit", (code) => exitCodes.set(index, code))
  })

  const diagnostics = (): string => {
    const perWorker = children.map(
      (_child, index) =>
        `#${index} attempts=${attemptsByWorker.get(index) ?? 0}` +
        ` lastIntervalStart=${lastIntervalByWorker.get(index) ?? 0}` +
        ` exit=${exitCodes.get(index) ?? "running"}`,
    )
    return (
      `actions=${JSON.stringify(fired)} attempts=${[...attemptsByWorker.values()].reduce((a, b) => a + b, 0)}` +
      ` lastAttemptAt=${lastAttemptAt} intervals=${intervals.length} samples=${samples.length}` +
      ` [${perWorker.join(" | ")}]`
    )
  }

  const readClock = async (): Promise<number> => {
    const [seconds, microseconds] = (await sampler.time()) as number[]
    return Number(seconds) * 1000 + Math.floor(Number(microseconds) / 1000)
  }

  const sample = async (): Promise<void> => {
    const result = (await sampler.eval(
      SAMPLER,
      keys.length,
      ...keys,
    )) as number[]
    samples.push({
      at: Number(result[0]),
      live: Number(result[1]),
      generationMembers: Number(result[2] ?? 0),
    })
  }

  const sampling = setInterval(() => {
    if (stopping) return
    void sample().catch(() => {})
  }, SAMPLE_MS)

  const startedAt = Date.now()
  for (const action of options.actions ?? []) {
    // Absolute schedule: each action fires at its fraction of the run, so a slow
    // action (a freeze waits for the lease to lapse) cannot push the rest of the
    // schedule past the workers' own deadline.
    const targetAt = startedAt + Math.round(RUN_MS * action.atFraction)
    await sleep(Math.max(0, targetAt - Date.now()))
    if (action.kind === "kill") {
      children[action.worker]?.kill("SIGKILL")
      fired.push({
        label: `kill worker ${action.worker}`,
        at: await readClock(),
        localAt: Date.now(),
      })
      continue
    }
    if (action.kind === "freeze") {
      children[action.worker]?.send({ action: "freeze", freezeMs: action.ms })
      fired.push({
        label: `freeze worker ${action.worker} for ${action.ms}ms`,
        at: await readClock(),
        localAt: Date.now(),
      })
      continue
    }
    if (action.kind === "delete-state") {
      // Losing the state hash while observations live is the epoch-mint path:
      // the window must keep counting only its own generation afterwards.
      await sampler.del(stateKey)
      fired.push({
        label: "delete breaker state hash",
        at: await readClock(),
        localAt: Date.now(),
      })
      continue
    }
    proxy?.disconnect()
    fired.push({
      label: `partition for ${action.ms}ms`,
      at: await readClock(),
      localAt: Date.now(),
    })
    await sleep(action.ms)
    proxy?.restore()
  }

  // Settle window: after the last action the survivors must be seen admitting
  // again, so the schedule does not end the instant the chaos does.
  await sleep(Math.max(2_000, LEASE_MS * 2 + 500))

  // Let the survivors finish, collecting the scripts they actually sent.
  const survivors = children.filter(
    (child) => child.exitCode === null && child.signalCode === null,
  )
  await Promise.all(
    survivors.map(
      (child) =>
        new Promise<void>((resolve) => {
          const timer = setTimeout(() => {
            child.kill("SIGKILL")
            resolve()
          }, 5_000)
          child.once("exit", () => {
            clearTimeout(timer)
            resolve()
          })
          child.send({ action: "stop" })
        }),
    ),
  )
  stopping = true
  clearInterval(sampling)
  // Give a killed worker's permit time to lapse before reading the final count:
  // nothing released it, so expiry is what has to reclaim it.
  await sleep(LEASE_MS + 300)
  await sample().catch(() => {})

  const final = (await sampler.eval(SAMPLER, keys.length, ...keys)) as number[]
  await proxy?.close()
  await sampler.disconnect()

  return {
    intervals,
    samples,
    actions: fired,
    scripts: [...scripts],
    attempts: [...attemptsByWorker.values()].reduce(
      (sum, value) => sum + value,
      0,
    ),
    lastAttemptAt,
    diagnostics: diagnostics(),
    maxLive: Math.max(...samples.map((entry) => entry.live), 0),
    maxGenerationMembers: Math.max(
      ...samples.map((entry) => entry.generationMembers),
      0,
    ),
    reconstructedMax: reconstructedMax(intervals),
    finalLive: Number(final[1]),
  }
}

describe.skipIf(!redisUrl)("distributed soak and chaos", () => {
  it("reclaims a released permit immediately, not at expiry", {
    timeout: 20_000,
  }, async () => {
    // The soak's leak check cannot see this on its own: a permit that is never
    // released still expires within one lease period, so the invariant holds
    // either way. Here the lease is five seconds long, so a live count of zero
    // straight after the call can only come from the release.
    const namespace = `soak-release-${randomUUID()}`
    const client = createCoordinationClient(redisUrl as string)
    await client.connect()
    const policy = bulkhead.distributed({
      name: "release",
      limit: 1,
      leaseMs: 5_000,
      coordinator: redisCoordinator(client, { namespace }),
      scope: () => "shared",
    })
    const subject = operation({
      name: "soak-op",
      adapter: {
        capabilities: () => ({
          abort: "supported" as const,
          replay: "safe" as const,
        }),
        execute: async () => "ok",
      },
      policies: [policy],
    })

    await expect(subject.execute(undefined)).resolves.toBe("ok")
    expect(
      await liveLeases(
        client,
        coordinationKey(namespace, "bulkhead:release", "soak-op", "shared"),
      ),
    ).toBe(0)
    client.disconnect()
  })

  it("never exceeds the limit across worker death, stalls and partitions", {
    timeout: RUN_MS * 6 + 10_000,
  }, async () => {
    const report = await runSoak({
      mode: "bulkhead",
      actions: [
        { kind: "kill", worker: 0, atFraction: 0.25 },
        { kind: "freeze", worker: 1, ms: LEASE_MS * 2, atFraction: 0.45 },
        { kind: "partition", ms: 750, atFraction: 0.7 },
      ],
    })

    // The run has to be busy enough to mean anything.
    expect(report.samples.length).toBeGreaterThan(20)
    expect(report.attempts).toBeGreaterThan(50)
    expect(report.intervals.length).toBeGreaterThan(50)

    // 1. The claim, on every coherent (time, count) sample: never over the limit.
    expect(report.maxLive).toBeLessThanOrEqual(LIMIT)

    // 2. The same claim, from the workers' own view of what they held. This is
    //    the conservative direction, so it cannot invent a violation.
    expect(report.reconstructedMax).toBeLessThanOrEqual(LIMIT)

    // 3. Chaos did not wedge the policy: work resumes after the last action.
    const lastActionAt = Math.max(...report.actions.map((action) => action.at))
    const lastIntervalStart = Math.max(
      ...report.intervals.map((interval) => interval.start),
      0,
    )
    expect(
      lastIntervalStart,
      `no admitted interval started after the last action: ` +
        `lastIntervalStart=${lastIntervalStart} ${report.diagnostics}`,
    ).toBeGreaterThan(lastActionAt)
    expect(report.actions).toHaveLength(3)

    // 4. Nothing leaked: every permit is released or expired once the workers
    //    are gone, including the killed worker's.
    expect(report.finalLive).toBe(0)

    // 5. The Lua in play is the shipped script, and only the shipped script.
    expect(report.scripts).toEqual([scriptSha(bulkheadLeaseV1)])
  })

  it("keeps the recovery window bounded and keeps counting after a state loss", {
    timeout: RUN_MS * 6 + 10_000,
  }, async () => {
    const report = await runSoak({
      mode: "breaker",
      actions: [
        { kind: "freeze", worker: 1, ms: 600, atFraction: 0.3 },
        { kind: "delete-state", atFraction: 0.55 },
      ],
    })

    expect(report.samples.length).toBeGreaterThan(20)
    expect(report.attempts).toBeGreaterThan(50)

    // At most `halfOpenProbes` recovery probes run at once, and the window
    // never accumulates more members than `windowSize` for the generation that
    // owns it - the property the epoch comparison exists to protect.
    expect(report.maxLive).toBeLessThanOrEqual(2)
    expect(report.maxGenerationMembers).toBeLessThanOrEqual(20)

    // The work continued across the state loss and the frozen worker.
    const lastActionAt = Math.max(...report.actions.map((action) => action.at))
    const lastIntervalStart = Math.max(
      ...report.intervals.map((interval) => interval.start),
      0,
    )
    expect(
      lastIntervalStart,
      `no admitted interval started after the last action: ` +
        `lastIntervalStart=${lastIntervalStart} ${report.diagnostics}`,
    ).toBeGreaterThan(lastActionAt)

    // Every script sent is a production constant, so nothing test-only is
    // standing in for the breaker's own Lua.
    expect(report.scripts.length).toBeGreaterThan(0)
    for (const script of report.scripts) {
      expect([
        scriptSha(breakerObserveV1),
        scriptSha(breakerAdmitProbeV1),
        scriptSha(breakerSettleProbeV1),
      ]).toContain(script)
    }
  })
})
