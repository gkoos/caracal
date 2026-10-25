import { execFileSync, fork } from "node:child_process"
import { mkdirSync, writeFileSync } from "node:fs"
import { fileURLToPath } from "node:url"
import { startDownstream, type DownstreamMode } from "./downstream.js"
import {
  percentile,
  type Metrics,
  type Profile,
  type RunCell,
} from "./worker.js"

/**
 * The Serval fault lab.
 *
 * It runs the same offered workload through real worker processes, a real Redis and a
 * controlled downstream, and compares four profiles: unprotected, the existing strict
 * Redis coordinators, Serval, and Serval with a local guardrail. Nothing here is part
 * of `npm test`: the point is to measure trade-offs, not to gate a commit.
 *
 * What it is careful about, deliberately:
 *
 * - **Paired, seeded runs.** Every profile sees the same cells, the same offered load
 *   and the same seeds, so a difference is attributable.
 * - **Physical work is measured at the dependency**, not inferred from any
 *   coordinator's accounting: a client that walks away does not stop the server, and
 *   the overshoot area is integrated from the server's own concurrency.
 * - **The critical interaction is its own cell**: the dependency slows *while* Redis
 *   is unavailable, which is where redistributed capacity could amplify a stressed
 *   dependency.
 *
 *   npm run bench:serval
 */

interface CellSpec {
  readonly name: string
  readonly mode: DownstreamMode
  /** True when every replica loses Redis; `partition` isolates a subset instead. */
  readonly redisOutage: boolean
  /** How many replicas are partitioned from Redis while the rest stay healthy. */
  readonly partition: number
}

const PROFILES: readonly Profile[] = [
  "unprotected",
  "strict",
  "serval",
  "serval-guarded",
]

const CELLS: readonly CellSpec[] = [
  { name: "healthy", mode: "healthy", redisOutage: false, partition: 0 },
  { name: "downstream-slow", mode: "slow", redisOutage: false, partition: 0 },
  { name: "redis-outage", mode: "healthy", redisOutage: true, partition: 0 },
  // The critical interaction: a stressed dependency while coordination is gone.
  { name: "slow+outage", mode: "slow", redisOutage: true, partition: 0 },
  { name: "slow+partition", mode: "slow", redisOutage: false, partition: 1 },
]

interface RunResult {
  readonly profile: Profile
  readonly cell: string
  readonly replicas: number
  readonly seed: number
  readonly metrics: Metrics
  readonly physical: {
    readonly peak: number
    readonly overshootArea: number
    readonly aboveTargetMs: number
    readonly failed: number
  }
  readonly error?: string
}

interface WorkerHandle {
  readonly child: ReturnType<typeof fork>
  send(cell: RunCell): Promise<Metrics>
  stop(): void
}

function spawnWorker(redisUrl: string): WorkerHandle {
  const child = fork(
    fileURLToPath(new URL("./worker.ts", import.meta.url)),
    [],
    {
      execArgv: ["--import", "tsx"],
      env: { ...process.env, CARACAL_REDIS_URL: redisUrl },
      stdio: ["ignore", "inherit", "inherit", "ipc"],
    },
  )
  let pending:
    | { resolve(metrics: Metrics): void; reject(error: Error): void }
    | undefined
  child.on("message", (message: { metrics?: Metrics; error?: string }) => {
    // The worker announces itself before any run; only a run result settles one.
    if (message.metrics === undefined && message.error === undefined) return
    const current = pending
    pending = undefined
    if (current === undefined) return
    if (message.error !== undefined) current.reject(new Error(message.error))
    else if (message.metrics !== undefined) current.resolve(message.metrics)
  })
  child.on("exit", (code) => {
    const current = pending
    pending = undefined
    current?.reject(new Error(`a worker exited with code ${code}`))
  })
  return {
    child,
    send(cell) {
      return new Promise<Metrics>((resolve, reject) => {
        pending = { resolve, reject }
        child.send({ action: "run", cell })
      })
    },
    stop() {
      if (child.exitCode === null) {
        try {
          child.send({ action: "stop" })
        } catch {
          // The worker is already gone.
        }
        // A worker that cannot shut down cleanly must not keep the run alive.
        setTimeout(() => {
          if (child.exitCode === null) child.kill("SIGKILL")
        }, 500).unref()
      }
    },
  }
}

function numbers(environment: string, fallback: readonly number[]): number[] {
  const configured = process.env[environment]
  if (configured === undefined || configured === "") return [...fallback]
  return configured
    .split(",")
    .map((value) => Number(value.trim()))
    .filter((value) => Number.isFinite(value) && value > 0)
}

function revision(): string {
  try {
    return execFileSync("git", ["rev-parse", "HEAD"], {
      encoding: "utf8",
    }).trim()
  } catch {
    return "unknown"
  }
}

/** Sums the counters and merges the samples of one cell's replicas. */
function aggregate(runs: readonly Metrics[]): Metrics {
  const endToEnd = runs.flatMap((run) => [...run.endToEnd])
  const admissionLatency = runs.flatMap((run) => [...run.admissionLatency])
  const total = (pick: (run: Metrics) => number): number =>
    runs.reduce((sum, run) => sum + pick(run), 0)
  return {
    offered: total((run) => run.offered),
    ok: total((run) => run.ok),
    rejectedRate: total((run) => run.rejectedRate),
    rejectedBulkhead: total((run) => run.rejectedBulkhead),
    failed: total((run) => run.failed),
    other: total((run) => run.other),
    redisOps: total((run) => run.redisOps),
    // Summed across replicas: the safety claim compares observed tracked occupancy against the
    // sum of the local ceilings, so a per-replica maximum would understate it.
    peakTracked: total((run) => run.peakTracked),
    admissionLatency,
    endToEnd,
  }
}

function report(
  results: readonly RunResult[],
  config: {
    readonly redisUrl: string
    readonly replicaCounts: readonly number[]
    readonly seeds: readonly number[]
    readonly offers: number
    readonly intervalMs: number
    readonly target: number
  },
): void {
  const groups = new Map<string, RunResult[]>()
  for (const result of results) {
    const key = `${result.cell}|${result.profile}|${result.replicas}`
    const bucket = groups.get(key) ?? []
    bucket.push(result)
    groups.set(key, bucket)
  }

  console.log(
    `\nServal fault lab - ${config.offers} offers per replica at ${config.intervalMs}ms, target ${config.target}, revision ${revision()}`,
  )
  console.log(
    `profiles ${PROFILES.join(", ")} | replicas ${config.replicaCounts.join(", ")} | seeds ${config.seeds.join(", ")}\n`,
  )

  for (const cell of CELLS) {
    console.log(
      `cell ${cell.name}${cell.redisOutage ? " - Redis unavailable" : ""}${cell.partition > 0 ? ` - ${cell.partition} replica partitioned` : ""}`,
    )
    console.log(
      "  profile         rep  offered   ok  rej-rate  rej-bulk  failed  redis/off  p95-ms  peak-tracked  peak-physical  overshoot-area",
    )
    for (const profile of PROFILES) {
      for (const replicas of config.replicaCounts) {
        const bucket = groups.get(`${cell.name}|${profile}|${replicas}`) ?? []
        const metrics = aggregate(bucket.map((result) => result.metrics))
        const peakPhysical = Math.max(
          ...bucket.map((result) => result.physical.peak),
          0,
        )
        const overshootArea = bucket.reduce(
          (sum, result) => sum + result.physical.overshootArea,
          0,
        )
        const redisPerOffer =
          metrics.offered === 0 ? 0 : metrics.redisOps / metrics.offered
        console.log(
          `  ${profile.padEnd(15)}${String(replicas).padStart(3)}  ${String(metrics.offered).padStart(7)}  ${String(metrics.ok).padStart(4)}  ${String(metrics.rejectedRate).padStart(8)}  ${String(metrics.rejectedBulkhead).padStart(8)}  ${String(metrics.failed).padStart(6)}  ${redisPerOffer.toFixed(2).padStart(9)}  ${String(percentile(metrics.endToEnd, 0.95)).padStart(6)}  ${String(metrics.peakTracked).padStart(12)}  ${String(peakPhysical).padStart(13)}  ${String(overshootArea).padStart(14)}`,
        )
      }
    }
    console.log("")
    console.log(
      "tracked is what the delegated bulkhead accounts for locally; the strict bulkhead keeps its leases in Redis, so its column reads 0",
    )
  }

  console.log("paired differences against the unprotected reference")
  for (const cell of CELLS) {
    const reference = aggregate(
      (groups.get(`${cell.name}|unprotected|1`) ?? []).map((r) => r.metrics),
    )
    const serval = aggregate(
      (groups.get(`${cell.name}|serval|1`) ?? []).map((r) => r.metrics),
    )
    const strict = aggregate(
      (groups.get(`${cell.name}|strict|1`) ?? []).map((r) => r.metrics),
    )
    const share = (value: number, base: number): string =>
      base === 0 ? "n/a" : `${Math.round((value / base) * 100)}%`
    const perOffer = (metrics: Metrics): string =>
      (metrics.offered === 0 ? 0 : metrics.redisOps / metrics.offered).toFixed(
        2,
      )
    console.log(
      `  ${cell.name.padEnd(18)} admitted ${serval.ok}/${reference.ok} (${share(serval.ok, reference.ok)}) vs strict ${strict.ok} | redis/offer ${perOffer(serval)} vs strict ${perOffer(strict)}`,
    )
  }

  const directory = fileURLToPath(new URL("./results/", import.meta.url))
  mkdirSync(directory, { recursive: true })
  const file = `${directory}${new Date().toISOString().replace(/[:.]/g, "-")}.json`
  writeFileSync(
    file,
    `${JSON.stringify(
      { revision: revision(), config, node: process.version, results },
      null,
      2,
    )}\n`,
  )
  console.log(`\nraw per-seed results written to ${file}\n`)
}

async function main(): Promise<void> {
  const redisUrl = process.env.CARACAL_REDIS_URL ?? "redis://127.0.0.1:6379"
  const replicaCounts = numbers("CARACAL_LAB_REPLICAS", [1, 4])
  const seeds = numbers("CARACAL_LAB_SEEDS", [1])
  const offers = Number(process.env.CARACAL_LAB_OFFERS ?? 24)
  const intervalMs = Number(process.env.CARACAL_LAB_INTERVAL ?? 10)
  const target = Number(process.env.CARACAL_LAB_TARGET ?? 4)
  const identity = { name: "lab-payments", operation: "charge" }

  const downstream = await startDownstream({ target, latencyMs: 40 })
  const results: RunResult[] = []
  const runId = Date.now()

  try {
    for (const profile of PROFILES) {
      for (const cell of CELLS) {
        for (const replicas of replicaCounts) {
          for (const seed of seeds) {
            const namespace = `serval-lab-${profile}-${cell.name}-${replicas}-${seed}-${runId}`
            console.log(
              `running ${profile} / ${cell.name} / ${replicas} replica(s) / seed ${seed}`,
            )
            downstream.setMode(cell.mode, {
              latencyMs: cell.mode === "slow" ? 40 : 0,
            })
            downstream.reset()

            const workers: { handle: WorkerHandle; redisUrl: string }[] = []
            try {
              for (let index = 0; index < replicas; index += 1) {
                // A full outage partitions every replica; the partition cell isolates a
                // subset and leaves the rest healthy.
                const isolated =
                  cell.redisOutage ||
                  (!cell.redisOutage && index < cell.partition)
                const workerUrl = isolated ? "redis://127.0.0.1:1" : redisUrl
                workers.push({
                  handle: spawnWorker(workerUrl),
                  redisUrl: workerUrl,
                })
              }

              const settled = await Promise.all(
                workers.map((worker) =>
                  worker.handle
                    .send({
                      namespace,
                      profile,
                      offers,
                      intervalMs,
                      target,
                      guardrailLimit: target,
                      // The dependency ignores cancellation, so the lab measures the
                      // physical work that outlives its admission.
                      abort: "unsupported",
                      timeoutMs: 60,
                      redisUrl: worker.redisUrl,
                      downstreamUrl: downstream.url,
                      identity,
                    })
                    .then(
                      (metrics) => ({ metrics }),
                      (error: Error) => ({ error: error.message }),
                    ),
                ),
              )

              const usable = settled
                .filter(
                  (entry): entry is { metrics: Metrics } =>
                    "metrics" in entry && entry.metrics !== undefined,
                )
                .map((entry) => entry.metrics)
              const stats = downstream.stats()
              const lost = workers.length - usable.length
              results.push({
                profile,
                cell: cell.name,
                replicas,
                seed,
                metrics: aggregate(usable),
                physical: {
                  peak: stats.peakPhysical,
                  overshootArea: stats.overshootArea,
                  aboveTargetMs: stats.aboveTargetMs,
                  failed: stats.failed,
                },
                ...(lost === 0
                  ? {}
                  : { error: `${lost} replica(s) reported no metrics` }),
              })
            } finally {
              for (const worker of workers) worker.handle.stop()
            }
          }
        }
      }
    }
  } finally {
    await downstream.close()
  }

  report(results, {
    redisUrl,
    replicaCounts,
    seeds,
    offers,
    intervalMs,
    target,
  })
}

await main()
