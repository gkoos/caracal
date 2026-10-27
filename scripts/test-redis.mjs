import { spawn } from "node:child_process"

async function run(command, args, env = process.env) {
  await new Promise((resolve, reject) => {
    const child = spawn(command, args, { stdio: "inherit", env })
    child.on("error", reject)
    child.on("exit", (code) =>
      code === 0 ? resolve() : reject(new Error(`${command} exited ${code}`)),
    )
  })
}
if (!process.env.CARACAL_REDIS_URL)
  await run("docker", ["compose", "up", "-d", "--wait", "valkey"])
await run(
  process.execPath,
  [
    "node_modules/vitest/vitest.mjs",
    "run",
    // One file at a time: the ACL suite flushes the script cache on purpose, and a
    // parallel round-trip test would count that fallback as an extra coordinator call.
    "--no-file-parallelism",
    "test/integration/redis.integration.test.ts",
    "test/integration/bulkhead.redis.integration.test.ts",
    "test/integration/circuit-breaker.redis.integration.test.ts",
    "test/integration/rate-limit.redis.integration.test.ts",
    // The Serval suites exercise the same scripts through the delegated authorities, so
    // they belong in the same serialized run: the ACL suite's flush below would otherwise
    // be invisible to them.
    "test/serval/integration/breaker.redis.integration.test.ts",
    "test/serval/integration/bulkhead.redis.integration.test.ts",
    "test/serval/integration/rate-limit.redis.integration.test.ts",
    "test/serval/integration/composition.integration.test.ts",
    "test/serval/integration/public-api.integration.test.ts",
    "test/integration/coordinator-conformance.integration.test.ts",
    "test/integration/redis-acl.integration.test.ts",
    "test/integration/coordinator-round-trips.integration.test.ts",
  ],
  {
    ...process.env,
    CARACAL_REDIS_URL:
      process.env.CARACAL_REDIS_URL ?? "redis://127.0.0.1:6379",
  },
)
