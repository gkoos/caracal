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
  CARACAL_AWS_ENDPOINT:
    process.env.CARACAL_AWS_ENDPOINT ?? "http://127.0.0.1:4566",
}

// CI provides an endpoint through a service container and sets
// CARACAL_AWS_ENDPOINT; only start the local stack when the caller has not.
if (!process.env.CARACAL_AWS_ENDPOINT) {
  const npm =
    process.platform === "win32" ? (process.env.ComSpec ?? "cmd.exe") : "npm"
  const args =
    process.platform === "win32"
      ? ["/d", "/s", "/c", "npm run floci:up"]
      : ["run", "floci:up"]
  await run(npm, args, environment)
}

await run(
  process.execPath,
  [
    "node_modules/vitest/vitest.mjs",
    "run",
    "test/integration/aws-sdk.integration.test.ts",
  ],
  environment,
)
