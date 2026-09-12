import { readFileSync } from "node:fs"
import { describe, expect, it } from "vitest"

/**
 * Package contract checks.
 *
 * Both failures these guard against were silent: `engines` claimed a Node range
 * the runtime does not support, and a published entry point was built from
 * source that the `files` field does not ship.
 */
const pkg = JSON.parse(
  readFileSync(new URL("../../package.json", import.meta.url), "utf8"),
) as {
  engines: { node: string }
  files: string[]
}

function readSource(relative: string): string {
  return readFileSync(new URL(`../../${relative}`, import.meta.url), "utf8")
}

describe("package contract", () => {
  it("requires the Node version its runtime features need", () => {
    // AbortSignal.any landed in Node 20.3.0; a major-only floor would accept
    // 20.0-20.2, which install cleanly and then throw at runtime.
    const users = [
      "src/core/runtime.ts",
      "src/core/timeout.ts",
      "src/core/bulkhead.ts",
      "src/adapters/fetch/adapter.ts",
    ].filter((file) => readSource(file).includes("AbortSignal.any"))

    expect(users.length).toBeGreaterThan(0)
    expect(pkg.engines.node).toBe(">=20.3.0")
  })

  it("publishes the source of every entry point it ships", () => {
    const tsup = readSource("tsup.config.ts")
    const entries = [
      ...tsup.matchAll(/(?:"[^"]+"|[\w$]+): "([^"]+\.ts)"/g),
    ].map((match) => match[1] as string)

    expect(entries.length).toBeGreaterThan(0)
    for (const entry of entries) {
      const directory = entry.split("/").slice(0, -1).join("/")
      expect(
        pkg.files.includes(directory),
        `${entry} is built but its source directory "${directory}" is not in files`,
      ).toBe(true)
    }
  })
})
