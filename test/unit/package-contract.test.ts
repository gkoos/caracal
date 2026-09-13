import { readdirSync, readFileSync } from "node:fs"
import { describe, expect, it } from "vitest"

/**
 * Package contract checks.
 *
 * The failures these guard against were all silent: `engines` claimed a Node
 * range the runtime does not support, a published entry point was built from
 * source that the `files` field does not ship, and the documented Node floor
 * drifted between files on release.
 */
const pkg = JSON.parse(
  readFileSync(new URL("../../package.json", import.meta.url), "utf8"),
) as {
  engines: { node: string }
  files: string[]
  version: string
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

  it("states the same Node floor in every document that mentions one", () => {
    const floor = pkg.engines.node
      .replace(/^[^\d]*/, "")
      .split(".")
      .slice(0, 2)
      .join(".")
    const documents = [
      "README.md",
      "SECURITY.md",
      ...readdirSync(new URL("../../docs", import.meta.url))
        .filter((name) => name.endsWith(".md"))
        .map((name) => `docs/${name}`),
    ]
    // Two phrasings count as stating a floor: "Node 20.3" followed by a "+"/"or
    // newer"-style qualifier, and a bare parenthesised range such as "(>= 20)".
    // The second form is included because it is what `npm run node:check` reads
    // like in prose, and it is exactly the phrasing the first version of this
    // guard missed.
    const mentions = documents.flatMap((file) =>
      [
        ...readSource(file).matchAll(
          /Node(?:\.js)?\s*(\d+(?:\.\d+)?)\s*(?:\+|or (?:newer|later|higher))|\(>=\s*(\d+(?:\.\d+)?)\)/g,
        ),
      ].map((match) => ({
        file,
        stated: (match[1] ?? match[2]) as string,
      })),
    )

    expect(mentions.length).toBeGreaterThan(0)
    for (const mention of mentions) {
      expect(
        mention.stated,
        `${mention.file} must state the same Node floor as engines (${pkg.engines.node})`,
      ).toBe(floor)
    }
  })

  it("keeps the supported-version table in step with the package version", () => {
    const [major, minor] = pkg.version.split(".")
    expect(readSource("SECURITY.md")).toContain(`| ${major}.${minor}.x`)
  })
})
