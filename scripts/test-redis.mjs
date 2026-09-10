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
    "test/integration/redis.integration.test.ts",
    "test/integration/bulkhead.redis.integration.test.ts",
    "test/integration/circuit-breaker.redis.integration.test.ts",
    "test/integration/coordinator-conformance.integration.test.ts",
    "test/integration/redis-acl.integration.test.ts",
  ],
  {
    ...process.env,
    CARACAL_REDIS_URL:
      process.env.CARACAL_REDIS_URL ?? "redis://127.0.0.1:6379",
  },
)
