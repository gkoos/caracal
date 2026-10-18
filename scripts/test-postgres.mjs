import { spawn } from "node:child_process"

function run(command, args, environment) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, { stdio: "inherit", env: environment })
    child.on("error", reject)
    child.on("exit", (code) =>
      code === 0 ? resolve() : reject(new Error(`${command} exited ${code}`)),
    )
  })
}

const environment = {
  ...process.env,
  CARACAL_POSTGRES_URL:
    process.env.CARACAL_POSTGRES_URL ??
    "postgresql://caracal:caracal@127.0.0.1:5432/caracal",
}

// CI provides a database service and sets CARACAL_POSTGRES_URL; only start the
// local stack when the caller has not.
if (!process.env.CARACAL_POSTGRES_URL) {
  const npm =
    process.platform === "win32" ? (process.env.ComSpec ?? "cmd.exe") : "npm"
  const args =
    process.platform === "win32"
      ? ["/d", "/s", "/c", "npm run postgres:up"]
      : ["run", "postgres:up"]
  await run(npm, args, environment)
}

// Only the suites this job is named for: the Redis files have their own job.
await run(
  process.execPath,
  [
    "node_modules/vitest/vitest.mjs",
    "run",
    "test/integration/postgres.integration.test.ts",
    "test/integration/fetch.integration.test.ts",
  ],
  environment,
)
