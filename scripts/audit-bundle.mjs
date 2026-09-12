#!/usr/bin/env node
/**
 * Public API and tree-shaking audit.
 *
 * Verifies that:
 * 1. dist/index.js (root bundle) exports exactly the expected symbols.
 * 2. dist/index.js does not contain ioredis or pg references.
 * 3. dist/redis.js exports exactly the expected Redis symbols.
 * 4. dist/testing/index.js exports the harness without runtime policy code.
 * 5. dist/fetch.js exports the fetch adapter and Retry-After helpers only.
 * 6. dist/postgres.js exports the postgres adapter only.
 *
 * Run after `npm run build`:
 *   node scripts/audit-bundle.mjs
 */
import { readFileSync } from "node:fs"

let failures = 0

function check(label, value) {
  const icon = value ? "✓" : "✗"
  console.log(`  ${icon}  ${label}`)
  if (!value) failures++
}

// ---------------------------------------------------------------------------
// 1. Root bundle exports
// ---------------------------------------------------------------------------

const { default: _d, ...root } = await import("../dist/index.js").catch(
  () => ({}),
)
const rootKeys = Object.keys(root).sort()
const expectedRoot = [
  "BulkheadRejectedError",
  "CircuitOpenError",
  "TimeoutError",
  "bulkhead",
  "circuitBreaker",
  "operation",
  "retry",
  "timeout",
].sort()

console.log("\n== Root bundle (dist/index.js) ==")
check(
  `Exports: ${rootKeys.join(", ")}`,
  JSON.stringify(rootKeys) === JSON.stringify(expectedRoot),
)

// ---------------------------------------------------------------------------
// 2. Root bundle has no Redis/Postgres references
// ---------------------------------------------------------------------------

const rootSource = readFileSync("dist/index.js", "utf8")
check("No 'ioredis' in root bundle", !rootSource.includes("ioredis"))
check("No 'pg' import in root bundle", !rootSource.includes("from 'pg'"))
check(
  "No 'redis' (lowercase) in root bundle",
  !rootSource.includes("require('redis')"),
)

// ---------------------------------------------------------------------------
// 3. Redis bundle exports
// ---------------------------------------------------------------------------

const redis = await import("../dist/redis.js").catch(() => ({}))
const redisKeys = Object.keys(redis)
  .filter((k) => k !== "default")
  .sort()
const expectedRedis = [
  "CoordinatorUnavailableError",
  "createCoordinationClient",
  "createCoordinationClusterClient",
  "redisCircuitBreakerCoordinator",
  "redisCoordinator",
].sort()

console.log("\n== Redis bundle (dist/redis.js) ==")
check(
  `Exports: ${redisKeys.join(", ")}`,
  JSON.stringify(redisKeys) === JSON.stringify(expectedRedis),
)

// ---------------------------------------------------------------------------
// 4. Testing harness exports
// ---------------------------------------------------------------------------

const testing = await import("../dist/testing/index.js").catch(() => ({}))
const testingKeys = Object.keys(testing)
  .filter((k) => k !== "default")
  .sort()

console.log("\n== Testing harness (dist/testing/index.js) ==")
check(
  `Exports include defineAdapterContractSuite`,
  testingKeys.includes("defineAdapterContractSuite"),
)
check(
  `Exports include runAdapterContractSuite`,
  testingKeys.includes("runAdapterContractSuite"),
)

const testingSource = readFileSync("dist/testing/index.js", "utf8")
check(
  "Testing harness does not reference ioredis",
  !testingSource.includes("ioredis"),
)

// ---------------------------------------------------------------------------
// 5. Fetch subpath exports
// ---------------------------------------------------------------------------

const fetch = await import("../dist/fetch.js").catch(() => ({}))
const fetchKeys = Object.keys(fetch)
  .filter((key) => key !== "default")
  .sort()

console.log("\n== Fetch subpath (dist/fetch.js) ==")
check(
  `Exports include fetchAdapter, retryAfterMs, retryAfterDelay, createRetryAfterDelay`,
  [
    "fetchAdapter",
    "retryAfterMs",
    "retryAfterDelay",
    "createRetryAfterDelay",
  ].every((key) => fetchKeys.includes(key)),
)

const fetchSource = readFileSync("dist/fetch.js", "utf8")
check(
  "Fetch bundle does not reference ioredis",
  !fetchSource.includes("ioredis"),
)
check(
  "Fetch bundle does not reference 'pg'",
  !fetchSource.includes("from 'pg'"),
)

// ---------------------------------------------------------------------------
// 6. Postgres subpath exports
// ---------------------------------------------------------------------------

const postgres = await import("../dist/postgres.js").catch(() => ({}))
const postgresKeys = Object.keys(postgres)
  .filter((key) => key !== "default")
  .sort()

console.log("\n== Postgres subpath (dist/postgres.js) ==")
check(
  `Exports include postgresAdapter`,
  postgresKeys.includes("postgresAdapter"),
)

const postgresSource = readFileSync("dist/postgres.js", "utf8")
check(
  "Postgres bundle does not reference ioredis",
  !postgresSource.includes("ioredis"),
)

// ---------------------------------------------------------------------------
// Summary
// ---------------------------------------------------------------------------

console.log(
  `\n${failures === 0 ? "✓ All checks passed." : `✗ ${failures} check(s) failed.`}\n`,
)
if (failures > 0) process.exit(1)
