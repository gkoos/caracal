import { describe, expect, it } from "vitest"
import { readSource, testFiles } from "../support/package-surface.js"

/**
 * Reason strings are a closed set, so they are checked like one.
 *
 * They were prose in two places that disagreed with each other and with the
 * code: the core reference listed `coordinator-unavailable` as a
 * `BulkheadRejectedError` reason when that failure rethrows the coordinator's
 * own error, the events reference called one set a superset of the other when
 * neither is, and four documented event reasons had no test asserting them. The
 * unions in `types.ts` are the source of truth: the docs must agree with them,
 * and every member must be asserted by a test.
 */
function unionMembers(name: string): string[] {
  const declaration = new RegExp(
    `export type ${name} =([\\s\\S]*?)\\n\\n`,
  ).exec(readSource("src/core/types.ts"))?.[1]
  expect(
    declaration,
    `${name} must be declared in src/core/types.ts`,
  ).toBeDefined()
  return [...(declaration as string).matchAll(/"([^"]+)"/g)].map(
    (match) => match[1] as string,
  )
}

describe("reason contract", () => {
  const rejected = unionMembers("BulkheadRejectedReason")
  const emitted = unionMembers("BulkheadEventReason")

  it("declares both closed sets", () => {
    expect(rejected.length).toBeGreaterThan(0)
    expect(emitted.length).toBeGreaterThan(0)
  })

  it("matches the bulkhead reason table in the events reference", () => {
    const table =
      readSource("docs/events-and-observability.md").split(
        "`reason` values by event:",
      )[1] ?? ""
    const documented = [
      ...new Set(
        [
          ...table.matchAll(
            /^\| `bulkhead\.[a-z.-]+`(?: \([a-z]+\))? \| `([^`]+)` \|/gm,
          ),
        ].map((match) => match[1] as string),
      ),
    ].sort()
    expect(documented.length).toBeGreaterThan(0)
    expect(documented).toEqual([...emitted].sort())
  })

  it("matches the error reasons documented in the core reference", () => {
    const listed =
      readSource("docs/core-api.md").match(
        /BulkheadRejectedError\.reason`\*\* is one of ([^\n.]+)/,
      )?.[1] ?? ""
    const documented = [...listed.matchAll(/`([^`]+)`/g)]
      .map((match) => match[1] as string)
      .sort()
    expect(documented.length).toBeGreaterThan(0)
    expect(documented).toEqual([...rejected].sort())
  })

  it("asserts every reason value in a test", () => {
    const sources = testFiles()
      .map((file) => readSource(file))
      .join("\n")
    for (const reason of [...new Set([...rejected, ...emitted])]) {
      expect(sources, `no test asserts reason: "${reason}"`).toContain(
        `reason: "${reason}"`,
      )
    }
  })
})
