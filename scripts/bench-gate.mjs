#!/usr/bin/env node
/**
 * Bench gate.
 *
 * Runs scripts/bench.mjs, reads its JSON report, and fails on regressions that
 * do not depend on how fast the machine is:
 *
 * - per-policy latency relative to the `no policy` row measured in the same run,
 *   so a shared runner's clock cancels out;
 * - allocations per attempt relative to bench/baseline.json, which counts what
 *   the code allocates rather than how quickly it runs;
 * - retained bytes per attempt, absolutely - the runtime must not accumulate
 *   per-attempt state, so this one is a leak check and not a performance one.
 *
 * Absolute microseconds are printed for humans but never asserted: a CI runner
 * is not a stable clock, and a gate that flaps gets switched off.
 *
 *   npm run bench:gate            assert
 *   npm run bench:baseline        rewrite bench/baseline.json
 */
import { spawn } from "node:child_process"
import { mkdirSync, readFileSync, writeFileSync } from "node:fs"
import { tmpdir } from "node:os"
import { dirname, join } from "node:path"

const BASELINE_PATH = "bench/baseline.json"
const NO_POLICY = "no policy (baseline)"

/**
 * Ceilings in multiples of the same-run `no policy` row. The generous headroom
 * on the rows containing `timeout` is deliberate: a timer plus a combined
 * AbortSignal costs ~14x an empty pipeline on this machine, so those rows gate
 * order-of-magnitude regressions rather than noise.
 */
const RATIO_LIMITS = {
  "retry only (1 attempt, success)": 5,
  "local bulkhead (uncontested)": 5,
  "local circuit breaker (always closed)": 5,
  "timeout only": 30,
  "timeout + retry (success path)": 30,
  "timeout + retry + breaker + bulkhead": 45,
}

const ALLOCATION_MULTIPLIER = 2
const RETAINED_BYTES_PER_ATTEMPT = 16

const updateBaseline = process.argv.includes("--update-baseline")
const reportPath = join(tmpdir(), `caracal-bench-${process.pid}.json`)

const benchExit = await new Promise((resolve) => {
  const child = spawn(
    process.execPath,
    ["--expose-gc", "scripts/bench.mjs", `--json=${reportPath}`],
    { stdio: "inherit" },
  )
  child.on("exit", (code) => resolve(code ?? 1))
})
if (benchExit !== 0) {
  console.error(`\nbench.mjs exited ${benchExit}; the gate cannot report.\n`)
  process.exit(benchExit)
}

const report = JSON.parse(readFileSync(reportPath, "utf8"))
const latency = new Map(report.results.map((row) => [row.label, row]))
const allocation = new Map(report.allocations.map((row) => [row.label, row]))

if (updateBaseline) {
  mkdirSync(dirname(BASELINE_PATH), { recursive: true })
  writeFileSync(BASELINE_PATH, `${JSON.stringify(report, null, 2)}\n`)
  console.log(`\nBaseline written to ${BASELINE_PATH}\n`)
  process.exit(0)
}

const baseline = (() => {
  try {
    return JSON.parse(readFileSync(BASELINE_PATH, "utf8"))
  } catch {
    return null
  }
})()
const baselineAllocations = new Map(
  (baseline?.allocations ?? []).map((row) => [row.label, row]),
)

const failures = []
const notes = []

function check(ok, message) {
  if (!ok) failures.push(message)
  console.log(`  ${ok ? "ok  " : "FAIL"}  ${message}`)
}

console.log("\n=== Bench gate ===\n")

const reference = latency.get(NO_POLICY)
if (!reference) {
  failures.push(`no ${NO_POLICY} row in the report`)
} else {
  for (const [label, limit] of Object.entries(RATIO_LIMITS)) {
    const row = latency.get(label)
    if (!row) {
      failures.push(`no ${label} row in the report`)
      continue
    }
    const ratio = row.medianUs / reference.medianUs
    check(
      ratio <= limit,
      `${label}: ${row.medianUs.toFixed(1)}µs = ${ratio.toFixed(1)}x no-policy (limit ${limit}x)`,
    )
  }

  const bare = latency.get("bare adapter call (no framework)")
  if (bare) {
    console.log(
      `\n  framework overhead: ${(reference.medianUs - bare.medianUs).toFixed(1)}µs per operation ` +
        `over a bare adapter call (${bare.medianUs.toFixed(2)}µs)\n`,
    )
  }
}

console.log("")
for (const row of report.allocations) {
  const limit = baselineAllocations.get(row.label)
  if (!limit) {
    notes.push(`${row.label}: no baseline allocation figure to compare against`)
    continue
  }
  const ceiling = limit.bytesPerAttempt * ALLOCATION_MULTIPLIER
  check(
    row.bytesPerAttempt <= ceiling,
    `${row.label}: ${row.bytesPerAttempt.toFixed(0)} B/attempt allocated ` +
      `(baseline ${limit.bytesPerAttempt.toFixed(0)} B, ceiling ${ceiling.toFixed(0)} B)`,
  )
}

console.log("")
for (const row of report.allocations) {
  check(
    row.retainedBytesPerAttempt <= RETAINED_BYTES_PER_ATTEMPT,
    `${row.label}: ${row.retainedBytesPerAttempt.toFixed(2)} B/attempt retained ` +
      `(limit ${RETAINED_BYTES_PER_ATTEMPT} B - per-attempt state must not accumulate)`,
  )
}

if (notes.length > 0) {
  console.log("")
  for (const note of notes) console.log(`  note  ${note}`)
}

console.log(
  `\nRun on ${report.platform} / ${report.node}.\n` +
    `${failures.length === 0 ? "Gate passed." : `${failures.length} gate failure(s).`}\n`,
)
process.exit(failures.length === 0 ? 0 : 1)
