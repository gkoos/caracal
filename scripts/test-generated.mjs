import { spawn } from "node:child_process"

/**
 * Runs every generated suite (test/property and test/fuzz) at a deeper case
 * count than a default run. The depth is CARACAL_TEST_RUNS: 10 here, unless the
 * environment already asks for more (the nightly workflow uses 25).
 *
 * `npm run test:generated` runs the same suites at depth 1, which is what CI
 * does on every pull request. This script is the local and nightly counterpart.
 */
const depth = process.env.CARACAL_TEST_RUNS ?? "10"

await new Promise((resolve, reject) => {
  const child = spawn(
    process.execPath,
    ["node_modules/vitest/vitest.mjs", "run", "test/property", "test/fuzz"],
    {
      stdio: "inherit",
      env: { ...process.env, CARACAL_TEST_RUNS: depth },
    },
  )
  child.on("error", reject)
  child.on("exit", (code) =>
    code === 0 ? resolve() : reject(new Error(`vitest exited ${code}`)),
  )
})
