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
    "test/integration/soak.integration.test.ts",
  ],
  {
    ...process.env,
    CARACAL_REDIS_URL:
      process.env.CARACAL_REDIS_URL ?? "redis://127.0.0.1:6379",
    // CI sets this to 1; locally the default keeps the suite quick.
    CARACAL_SOAK_MINUTES: process.env.CARACAL_SOAK_MINUTES ?? "0.25",
  },
)
