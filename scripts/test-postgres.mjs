import { spawn } from "node:child_process"

function runNpm(script, environment) {
  const command =
    process.platform === "win32" ? (process.env.ComSpec ?? "cmd.exe") : "npm"
  const args =
    process.platform === "win32"
      ? ["/d", "/s", "/c", `npm run ${script}`]
      : ["run", script]

  return new Promise((resolve, reject) => {
    const child = spawn(command, args, { stdio: "inherit", env: environment })
    child.on("error", reject)
    child.on("exit", (code) => {
      code === 0
        ? resolve()
        : reject(
            new Error(`npm run ${script} exited with ${code ?? "no status"}`),
          )
    })
  })
}

const environment = {
  ...process.env,
  CARACAL_POSTGRES_URL:
    process.env.CARACAL_POSTGRES_URL ??
    "postgresql://caracal:caracal@127.0.0.1:5432/caracal",
}

await runNpm("postgres:up", environment)
await runNpm("test:integration", environment)
