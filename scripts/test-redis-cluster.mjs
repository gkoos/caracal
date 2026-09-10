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

const defaultUrls = "127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002"

if (!process.env.CARACAL_REDIS_CLUSTER_URLS)
  await run("docker", [
    "compose",
    "-f",
    "compose.cluster.yaml",
    "up",
    "-d",
    "--wait",
  ])

await run(
  process.execPath,
  [
    "node_modules/vitest/vitest.mjs",
    "run",
    "test/integration/redis-cluster.integration.test.ts",
  ],
  {
    ...process.env,
    CARACAL_REDIS_CLUSTER_URLS:
      process.env.CARACAL_REDIS_CLUSTER_URLS ?? defaultUrls,
  },
)
