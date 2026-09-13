#!/usr/bin/env node
/**
 * Benchmark for local policy overhead and Redis script transport.
 *
 * Part 1 measures the per-operation cost of local timeout, retry, bulkhead, and
 * circuit breaker policies over a no-op adapter. These numbers provide a floor
 * for performance regression detection; they are not load benchmarks.
 *
 * Part 2 compares EVAL (the whole Lua source on every call) with EVALSHA
 * (40-byte SHA) for the scripts Caracal actually ships. It measures bytes on
 * the socket, sequential and concurrent throughput, and server-side
 * `usec_per_call` from INFO commandstats. The Lua bodies are captured from the
 * wire through a counting proxy, so the benchmark always measures what the
 * coordinators really send. It needs a local Valkey; without one it prints a
 * skip notice and exits. See docs/redis.md for the trade-off this quantifies.
 *
 * Usage:
 *   npm run build
 *   node scripts/bench.mjs
 *   npm run redis:up && node scripts/bench.mjs
 *   CARACAL_REDIS_URL=redis://host:6379 node scripts/bench.mjs
 *
 * Output: median and p99 latency in microseconds (µs) per operation, and
 *         throughput in operations per second.
 */

import { createHash, randomUUID } from "node:crypto"
import net from "node:net"
import {
  bulkhead,
  circuitBreaker,
  operation,
  retry,
  timeout,
} from "../dist/index.js"
import {
  createCoordinationClient,
  redisCircuitBreakerCoordinator,
  redisCoordinator,
} from "../dist/redis.js"

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function median(sorted) {
  return sorted[Math.floor(sorted.length / 2)]
}

function p99(sorted) {
  return sorted[Math.floor(sorted.length * 0.99)]
}

const jsonPath = process.argv
  .find((arg) => arg.startsWith("--json="))
  ?.slice("--json=".length)

/** Latency rows, for the JSON report the gate reads. */
const results = []
/** Allocation rows, for the JSON report the gate reads. */
const allocations = []

async function bench(label, op, iterations = 50_000) {
  // Warm up
  for (let i = 0; i < 1_000; i++) await op.execute(undefined).catch(() => {})

  const times = []
  for (let i = 0; i < iterations; i++) {
    const t0 = performance.now()
    await op.execute(undefined).catch(() => {})
    times.push(performance.now() - t0)
  }

  times.sort((a, b) => a - b)
  const medUs = (median(times) * 1_000).toFixed(1)
  const p99Us = (p99(times) * 1_000).toFixed(1)
  const opsPerSec = Math.round(
    1_000 / (times.reduce((a, b) => a + b, 0) / times.length),
  )

  results.push({
    label,
    iterations,
    medianUs: Number(medUs),
    p99Us: Number(p99Us),
    opsPerSec,
  })
  console.log(
    `${label.padEnd(40)} median=${medUs.padStart(7)}µs  p99=${p99Us.padStart(7)}µs  ops/s=${String(opsPerSec).padStart(8)}`,
  )
}

/**
 * Bytes allocated per operation, and bytes retained per operation.
 *
 * Both numbers come from `process.memoryUsage().heapUsed` around a forced
 * collection, which measures what V8 reports rather than an allocation profiler:
 *
 * - `bytesPerAttempt` samples the heap every `batch` iterations *without*
 *   collecting in between, so it approximates allocation volume. The batch is
 *   sized well below the young generation, so a batch's allocations are still
 *   live when it is sampled. It is an approximation, and the gate that reads it
 *   uses a generous ceiling.
 * - `retainedBytesPerAttempt` collects at both ends, so it measures per-attempt
 *   growth. That one is exact enough to be a leak check: the runtime must not
 *   accumulate per-attempt state.
 *
 * `--expose-gc` is required, so the section skips rather than reporting a
 * meaningless number without it.
 */
async function benchAllocations(label, op, iterations = 20_000) {
  if (typeof globalThis.gc !== "function") {
    console.log(`  ${label.padEnd(38)} skipped - run with: node --expose-gc`)
    return
  }

  const batch = 2_000
  for (let i = 0; i < 1_000; i++) await op.execute(undefined).catch(() => {})

  globalThis.gc()
  const settled = process.memoryUsage().heapUsed
  let allocated = 0
  let previous = settled
  for (let done = 0; done < iterations; done += batch) {
    const count = Math.min(batch, iterations - done)
    for (let i = 0; i < count; i++) await op.execute(undefined).catch(() => {})
    const current = process.memoryUsage().heapUsed
    allocated += Math.max(0, current - previous)
    previous = current
  }

  globalThis.gc()
  const retained = Math.max(0, process.memoryUsage().heapUsed - settled)
  const bytesPerAttempt = allocated / iterations
  const retainedBytesPerAttempt = retained / iterations

  allocations.push({
    label,
    iterations,
    bytesPerAttempt,
    retainedBytesPerAttempt,
  })
  console.log(
    `  ${label.padEnd(38)} ${bytesPerAttempt.toFixed(0).padStart(6)} B/attempt allocated` +
      `  ${retainedBytesPerAttempt.toFixed(2).padStart(6)} B/attempt retained`,
  )
}

// ---------------------------------------------------------------------------
// Adapters
// ---------------------------------------------------------------------------

const noopAdapter = {
  capabilities: () => ({ abort: "unsupported", replay: "safe" }),
  execute: async () => "ok",
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

console.log("\n=== Caracal local policy baseline benchmarks ===\n")
console.log(
  "Each row: 50 000 operations through the named policy combination.\n",
)

await bench("bare adapter call (no framework)", {
  execute: async () => noopAdapter.execute(),
})

await bench(
  "no policy (baseline)",
  operation({ name: "b", adapter: noopAdapter }),
)

await bench(
  "timeout only",
  operation({
    name: "b",
    adapter: noopAdapter,
    policies: [timeout({ ms: 5_000 })],
  }),
)

await bench(
  "retry only (1 attempt, success)",
  operation({
    name: "b",
    adapter: noopAdapter,
    policies: [retry({ maxAttempts: 3 })],
  }),
)

await bench(
  "timeout + retry (success path)",
  operation({
    name: "b",
    adapter: noopAdapter,
    policies: [timeout({ ms: 5_000 }), retry({ maxAttempts: 3 })],
  }),
)

const localBh = bulkhead.local({ name: "b", limit: 1_000 })
await bench(
  "local bulkhead (uncontested)",
  operation({ name: "b", adapter: noopAdapter, policies: [localBh] }),
)

const localBreaker = circuitBreaker.local({
  name: "b",
  minimumThroughput: 1_000_000, // never opens
  failureThreshold: 0.5,
})
await bench(
  "local circuit breaker (always closed)",
  operation({ name: "b", adapter: noopAdapter, policies: [localBreaker] }),
)

const fullLocal = circuitBreaker.local({
  name: "b",
  minimumThroughput: 1_000_000,
  failureThreshold: 0.5,
})
const fullBh = bulkhead.local({ name: "b", limit: 1_000 })
await bench(
  "timeout + retry + breaker + bulkhead",
  operation({
    name: "b",
    adapter: noopAdapter,
    policies: [
      fullLocal,
      timeout({ ms: 5_000 }),
      retry({ maxAttempts: 3 }),
      fullBh,
    ],
  }),
)

// ---------------------------------------------------------------------------
// Part 2: Redis script transport (EVAL vs EVALSHA)
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Allocations per operation
// ---------------------------------------------------------------------------

console.log("\n=== Allocation per operation (requires --expose-gc) ===\n")
await benchAllocations("bare adapter call (no framework)", {
  execute: async () => noopAdapter.execute(),
})
await benchAllocations(
  "no policy",
  operation({ name: "alloc", adapter: noopAdapter }),
)
await benchAllocations(
  "full local set",
  operation({
    name: "alloc",
    adapter: noopAdapter,
    policies: [
      timeout({ ms: 5_000 }),
      retry({ maxAttempts: 3 }),
      bulkhead.local({ name: "alloc", limit: 1_000 }),
      circuitBreaker.local({ name: "alloc" }),
    ],
  }),
)

const REDIS_URL =
  process.argv
    .find((arg) => arg.startsWith("--redis-url="))
    ?.slice("--redis-url=".length) ??
  process.env.CARACAL_REDIS_URL ??
  "redis://127.0.0.1:6379"

const WARMUP_ITERATIONS = 250
const SEQUENTIAL_ITERATIONS = 3_000
const BURST_ITERATIONS = 20_000
const BURST_CONCURRENCY = 50

/** Cheap reachability probe so the section can skip instead of hanging. */
async function isReachable(url) {
  const { hostname, port } = new URL(url)
  return await new Promise((resolve) => {
    const socket = net.connect(Number(port), hostname)
    const finish = (reachable) => {
      socket.destroy()
      resolve(reachable)
    }
    socket.setTimeout(300)
    socket.once("connect", () => finish(true))
    socket.once("timeout", () => finish(false))
    socket.once("error", () => finish(false))
  })
}

/**
 * Parses one client request (a RESP array of bulk strings) out of `buffer`.
 * Returns the arguments and how many bytes were consumed, or null when the
 * buffer does not yet hold a complete request.
 */
function parseRequest(buffer) {
  let offset = 0
  const readLine = () => {
    const end = buffer.indexOf("\r\n", offset)
    if (end === -1) return null
    const line = buffer.toString("utf8", offset, end)
    offset = end + 2
    return line
  }

  const header = readLine()
  if (header === null || !header.startsWith("*")) return null
  const count = Number(header.slice(1))
  const args = []
  for (let index = 0; index < count; index++) {
    const elementHeader = readLine()
    if (elementHeader === null) return null
    if (elementHeader.startsWith("$")) {
      const length = Number(elementHeader.slice(1))
      if (offset + length + 2 > buffer.length) return null
      args.push(buffer.toString("utf8", offset, offset + length))
      offset += length + 2
    } else {
      args.push(elementHeader.slice(1))
    }
  }
  return { args, consumed: offset }
}

/**
 * Minimal TCP passthrough that counts bytes in both directions and captures the
 * arguments of the first EVAL it sees. Taking the Lua body from the wire means
 * the benchmark cannot drift from what the coordinators actually send.
 */
async function startCaptureProxy(upstream) {
  const { hostname, port } = new URL(upstream)
  const capture = { bytesUp: 0, bytesDown: 0, evalArgs: null }

  const server = net.createServer((downstream) => {
    const up = net.connect(Number(port), hostname)
    let pending = Buffer.alloc(0)

    downstream.on("data", (chunk) => {
      capture.bytesUp += chunk.length
      up.write(chunk)
      if (capture.evalArgs !== null) return

      pending = Buffer.concat([pending, chunk])
      while (true) {
        const request = parseRequest(pending)
        if (request === null) break
        pending = pending.subarray(request.consumed)
        if (request.args[0] === "eval") {
          capture.evalArgs = request.args.slice(1)
          break
        }
      }
    })
    up.on("data", (chunk) => {
      capture.bytesDown += chunk.length
      downstream.write(chunk)
    })
    downstream.on("error", () => up.destroy())
    up.on("error", () => downstream.destroy())
    up.on("close", () => downstream.destroy())
    downstream.on("close", () => up.destroy())
  })

  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve))
  const address = server.address()
  return {
    url: `redis://127.0.0.1:${address.port}`,
    capture,
    close: () => new Promise((resolve) => server.close(resolve)),
  }
}

/** `INFO commandstats` → command name → { calls, usec }. */
function parseCommandStats(info) {
  const stats = new Map()
  for (const rawLine of info.split("\n")) {
    const line = rawLine.trim()
    if (!line.startsWith("cmdstat_")) continue
    const separator = line.indexOf(":")
    const name = line.slice("cmdstat_".length, separator)
    const fields = {}
    for (const pair of line.slice(separator + 1).split(",")) {
      const [key, value] = pair.split("=")
      fields[key] = value
    }
    stats.set(name, {
      calls: Number(fields.calls ?? 0),
      usec: Number(fields.usec ?? 0),
    })
  }
  return stats
}

function statDelta(before, after, command) {
  const from = before.get(command) ?? { calls: 0, usec: 0 }
  const to = after.get(command) ?? { calls: 0, usec: 0 }
  const calls = to.calls - from.calls
  const usec = to.usec - from.usec
  return { calls, usec, usecPerCall: calls > 0 ? usec / calls : 0 }
}

function percentile(sorted, fraction) {
  const index = Math.min(
    sorted.length - 1,
    Math.floor(sorted.length * fraction),
  )
  return sorted[index]
}

/**
 * Warms up, then times `iterations` calls at the given concurrency. Request
 * bytes come from the socket itself, server cost from commandstats; the INFO
 * probes are issued outside both measurement windows.
 */
async function timeCalls(client, call, iterations, concurrency) {
  for (let index = 0; index < WARMUP_ITERATIONS; index++) await call()

  const statsBefore = parseCommandStats(await client.info("commandstats"))
  const bytesBefore = client.stream.bytesWritten
  const latencies = []

  const started = performance.now()
  if (concurrency === 1) {
    for (let index = 0; index < iterations; index++) {
      const t0 = performance.now()
      await call()
      latencies.push(performance.now() - t0)
    }
  } else {
    let issued = 0
    await Promise.all(
      Array.from({ length: concurrency }, async () => {
        while (issued < iterations) {
          issued += 1
          await call()
        }
      }),
    )
  }
  const elapsedMs = performance.now() - started

  const bytesUp = client.stream.bytesWritten - bytesBefore
  const statsAfter = parseCommandStats(await client.info("commandstats"))

  return {
    latencies,
    elapsedMs,
    bytesPerOp: bytesUp / iterations,
    opsPerSecond: iterations / (elapsedMs / 1_000),
    statsBefore,
    statsAfter,
  }
}

/**
 * Runs `drive` through a real coordinator behind the counting proxy and returns
 * the captured `[luaBody, numberOfKeys, ...args]` tuple of the first EVAL.
 */
/**
 * Runs `drive` through a real coordinator behind the counting proxy and returns
 * the captured `[luaBody, numberOfKeys, ...args]` tuple of the first EVAL.
 *
 * `evalScript` prefers `EVALSHA`, and the server keeps its script cache across
 * runs - so on a warm cache no EVAL is ever sent and there would be nothing to
 * capture. The client handed to `drive` therefore omits `evalsha`, which makes
 * the coordinator send the body with `EVAL`. That is what this section measures;
 * it deliberately does not flush the server's cache.
 */
async function captureCoordinatorEvalArgs(drive) {
  const proxy = await startCaptureProxy(REDIS_URL)
  const client = createCoordinationClient(proxy.url)
  const bodyOnly = {
    eval: (script, numberOfKeys, ...args) =>
      client.eval(script, numberOfKeys, ...args),
    hmget: (key, ...fields) => client.hmget(key, ...fields),
  }
  try {
    await client.connect()
    await drive(bodyOnly)
    for (let attempt = 0; attempt < 50; attempt++) {
      if (proxy.capture.evalArgs !== null) break
      await new Promise((resolve) => setTimeout(resolve, 20))
    }
    if (proxy.capture.evalArgs === null) {
      throw new Error("No EVAL reached the capture proxy")
    }
    return proxy.capture.evalArgs
  } finally {
    client.disconnect()
    await proxy.close()
  }
}

async function benchRedisScriptTransport() {
  console.log("\n=== Redis script transport benchmark: EVAL vs EVALSHA ===\n")

  if (!(await isReachable(REDIS_URL))) {
    console.log(
      `Skipped: no Redis at ${REDIS_URL}. Start one with: npm run redis:up\n`,
    )
    return
  }

  const namespace = `bench-${randomUUID()}`
  const identity = { name: "bench", operation: "bench", scope: "shared" }

  // Capture the exact payloads the shipped coordinators send.
  const observeArgs = await captureCoordinatorEvalArgs((client) =>
    redisCircuitBreakerCoordinator(client, { namespace }).observe(identity, {
      generation: 0,
      outcome: "success",
      uuid: randomUUID(),
      windowTtlMs: 60_000,
      // Never opens, so every call performs the full window scan.
      minimumThroughput: 1_000_000,
      failureThresholdNumerator: 500,
      windowSize: 10,
      openMs: 30_000,
    }),
  )

  const bulkheadArgs = await captureCoordinatorEvalArgs((client) =>
    redisCoordinator(client, { namespace }).command(
      identity,
      "acquire",
      "bench-token",
      30_000,
      1_000,
    ),
  )

  const client = createCoordinationClient(REDIS_URL)
  await client.connect()
  console.log(
    `Redis: ${REDIS_URL}`,
    `\nSequential: ${SEQUENTIAL_ITERATIONS} calls with one in flight.`,
    `\nBurst: ${BURST_ITERATIONS} calls at concurrency ${BURST_CONCURRENCY}.\n`,
  )

  const keysToDelete = new Set()
  const measured = []
  const summary = []

  try {
    for (const captured of [
      { script: "breakerObserveV1", args: observeArgs },
      { script: "bulkheadLeaseV1", args: bulkheadArgs },
    ]) {
      const [body, numberOfKeys, ...args] = captured.args
      const bodyBytes = Buffer.byteLength(body)
      const sha = createHash("sha1").update(body).digest("hex")

      // Loading first makes the EVALSHA row the steady state, and the returned
      // SHA is the self-check that our hash matches the server's.
      const serverSha = await client.script("LOAD", body)
      if (serverSha !== sha) {
        throw new Error(
          `${captured.script}: client SHA1 ${sha} != server SHA1 ${serverSha}`,
        )
      }
      for (const key of args.slice(0, Number(numberOfKeys))) {
        keysToDelete.add(key)
      }

      console.log(`${captured.script} — ${bodyBytes} B Lua, SHA1 ${sha}`)

      const variants = [
        {
          label: "EVAL",
          command: "eval",
          call: () => client.eval(body, numberOfKeys, ...args),
        },
        {
          label: "EVALSHA",
          command: "evalsha",
          call: () => client.evalsha(sha, numberOfKeys, ...args),
        },
      ]

      for (const variant of variants) {
        const sequential = await timeCalls(
          client,
          variant.call,
          SEQUENTIAL_ITERATIONS,
          1,
        )
        const stats = statDelta(
          sequential.statsBefore,
          sequential.statsAfter,
          variant.command,
        )
        const latencies = sequential.latencies.sort((a, b) => a - b)
        const burst = await timeCalls(
          client,
          variant.call,
          BURST_ITERATIONS,
          BURST_CONCURRENCY,
        )

        console.log(
          `  ${variant.label.padEnd(7)}` +
            ` bytes/op=${sequential.bytesPerOp.toFixed(0).padStart(5)}` +
            `  median=${(percentile(latencies, 0.5) * 1_000).toFixed(0).padStart(4)}µs` +
            `  p99=${(percentile(latencies, 0.99) * 1_000).toFixed(0).padStart(4)}µs` +
            `  server=${stats.usecPerCall.toFixed(2)}µs/call`,
        )
        console.log(
          `          burst ${String(Math.round(burst.opsPerSecond)).padStart(7)} ops/s` +
            `  ${((burst.bytesPerOp * burst.opsPerSecond) / 1e6).toFixed(1).padStart(5)} MB/s client→server`,
        )

        measured.push({
          script: captured.script,
          label: variant.label,
          bytesPerOp: sequential.bytesPerOp,
          usecPerCall: stats.usecPerCall,
        })
      }

      const [evalRow, evalshaRow] = measured.filter(
        (row) => row.script === captured.script,
      )
      summary.push({
        script: captured.script,
        bodyBytes,
        evalBytes: evalRow.bytesPerOp,
        evalshaBytes: evalshaRow.bytesPerOp,
        evalUsec: evalRow.usecPerCall,
        evalshaUsec: evalshaRow.usecPerCall,
        bytesSaved: 100 * (1 - evalshaRow.bytesPerOp / evalRow.bytesPerOp),
        serverSaved: 100 * (1 - evalshaRow.usecPerCall / evalRow.usecPerCall),
      })
      console.log("")
    }
  } finally {
    if (keysToDelete.size > 0) await client.del(...keysToDelete)
    client.disconnect()
  }

  console.log("EVALSHA vs EVAL, per script:")
  for (const row of summary) {
    console.log(
      `  ${row.script.padEnd(18)} ${String(row.bodyBytes).padStart(4)} B Lua` +
        `  bytes/op ${row.evalBytes.toFixed(0)} → ${row.evalshaBytes.toFixed(0)} (-${row.bytesSaved.toFixed(0)}%)` +
        `  server ${row.evalUsec.toFixed(2)} → ${row.evalshaUsec.toFixed(2)}µs/call (-${row.serverSaved.toFixed(0)}%)`,
    )
  }
  console.log(
    "\nNotes: bytes/op is measured with net.Socket.bytesWritten and includes RESP",
    "\nframing, keys and args; server cost is the cmdstat_eval / cmdstat_evalsha",
    "\ndelta from INFO commandstats. Client latency here is dominated by the",
    "\nDocker-mapped loopback RTT and is noisy - bytes and server µs/call are the",
    "\nsignals that transfer to a real deployment.",
    "\nEVALSHA assumes the script is already cached: a NOSCRIPT reply costs one",
    "\nextra round trip and is the recovery path any EVALSHA design has to implement.",
    "\nSizes are measured on the wire, so composed scripts differ from their source",
    "\nlength: bulkheadLeaseV1 is 122 B of source that evaluates to the 868 B above",
    "\nbecause it interpolates leaseV1. The other two breaker scripts (admit ~1.7 kB,",
    "\nsettle ~1.8 kB) fall between the two scripts measured here.\n",
  )
}

await benchRedisScriptTransport()

if (jsonPath) {
  const { writeFileSync } = await import("node:fs")
  writeFileSync(
    jsonPath,
    `${JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        node: process.version,
        platform: `${process.platform}-${process.arch}`,
        results,
        allocations,
      },
      null,
      2,
    )}\n`,
  )
  console.log(`\nJSON report written to ${jsonPath}\n`)
}
