#!/usr/bin/env node
/**
 * Public API and tree-shaking audit.
 *
 * Verifies that:
 * 1. dist/index.js (root bundle) exports exactly the expected symbols.
 * 2. dist/index.js does not contain ioredis or pg references.
 * 3. dist/redis.js exports exactly the expected Redis symbols.
 * 4. dist/testing/index.js exports the harness without runtime policy code.
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
// Summary
// ---------------------------------------------------------------------------

console.log(
  `\n${failures === 0 ? "✓ All checks passed." : `✗ ${failures} check(s) failed.`}\n`,
)
if (failures > 0) process.exit(1)
