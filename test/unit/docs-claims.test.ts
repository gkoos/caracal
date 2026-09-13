import { describe, expect, it, vi } from "vitest"
import { createRetryAfterDelay } from "../../src/fetch.js"
import { bulkhead, circuitBreaker } from "../../src/index.js"
import type {
  BreakerCoordinator,
  BulkheadCoordinator,
  RetryContext,
} from "../../src/index.js"
import {
  buildEntries,
  declaredValues,
  docsPages,
  entryExports,
  readSource,
  sourceFiles,
} from "../support/package-surface.js"

/**
 * Documentation claims, checked against the code they describe.
 *
 * Every guard here exists because the claim it checks was, at some point, false:
 * a cap that jitter exceeded, a Node floor that no longer matched the check
 * script, an exported helper no page named, a state-machine trigger describing a
 * timer the implementation does not have, and an error reference that did not
 * exist. Prose is only a contract while something fails when it stops being true.
 */

/** Entries -> the pages that must name what they export. */
const ENTRY_DOCS: Record<string, readonly string[]> = {
  "src/index.ts": ["docs/core-api.md", "README.md"],
  "src/redis.ts": ["docs/redis.md"],
  "src/fetch.ts": ["docs/fetch.md"],
  "src/postgres.ts": ["docs/postgres.md"],
  "test/harness/index.ts": ["docs/adapter-contracts.md"],
}

describe("documentation claims", () => {
  it("links every documentation page from docs/README.md", () => {
    const index = readSource("docs/README.md")
    const pages = docsPages().filter((page) => page !== "docs/README.md")
    expect(pages.length).toBeGreaterThan(0)
    for (const page of pages) {
      expect(index, `${page} is not linked from docs/README.md`).toContain(
        page.replace("docs/", ""),
      )
    }
  })

  it("states the largest wait retryAfterDelay can return", () => {
    const random = vi.spyOn(Math, "random").mockReturnValue(1)
    let longest: number
    try {
      const delay = createRetryAfterDelay()
      const context = {
        result: new Response(null, { headers: { "retry-after": "600" } }),
      } as unknown as RetryContext
      longest = Math.max(
        ...[1, 2, 3, 4, 5].map((attempt) => delay(attempt, context)),
      )
    } finally {
      random.mockRestore()
    }

    // The 30 s cap applies to the deterministic wait; default additive jitter
    // lengthens it. This is the number a caller composing an outer deadline needs.
    expect(longest).toBe(33_000)

    const section = readSource("docs/fetch.md").split("## Retry-After")[1] ?? ""
    const spellings = [
      `${longest / 1000} s`,
      `${longest / 1000}s`,
      String(longest),
      `${longest / 1000} 000`,
    ]
    expect(
      spellings.some((form) => section.includes(form)),
      `docs/fetch.md must state the ${longest} ms (${longest / 1000} s) worst case`,
    ).toBe(true)
  })

  it("keeps node:check on the same floor as engines", () => {
    const pkg = JSON.parse(readSource("package.json")) as {
      engines: { node: string }
    }
    const floor = readSource("scripts/check-node-version.mjs").match(
      /minimumNodeVersion = "([^"]+)"/,
    )?.[1]
    expect(floor).toBeDefined()
    expect(pkg.engines.node).toBe(`>=${floor}`)
  })

  it("names every value export of every public entry in the docs", () => {
    const values = new Set(
      sourceFiles().flatMap((file) => declaredValues(file)),
    )
    const docs = ["README.md", ...docsPages()].map((page) => ({
      page,
      source: readSource(page),
    }))

    for (const entry of buildEntries()) {
      for (const name of entryExports(entry)) {
        if (!values.has(name)) continue
        const named = docs.filter(({ source }) => source.includes(name))
        expect(
          named.map(({ page }) => page),
          `${entry} exports ${name}, which no page names`,
        ).not.toEqual([])
      }
    }
  })

  it("records a documentation page for every build entry", () => {
    expect(Object.keys(ENTRY_DOCS), "every entry needs a page").toEqual(
      buildEntries(),
    )
    const pages = ["README.md", ...docsPages()]
    for (const page of Object.values(ENTRY_DOCS).flat()) {
      expect(pages, `${page} is not a documentation page`).toContain(page)
    }
  })

  it("pins the shape of every policy object", () => {
    const coordinator: BulkheadCoordinator = {
      async command() {
        return { allowed: true, occupancy: 0 }
      },
    }
    const breakerCoordinator = {} as unknown as BreakerCoordinator

    expect(
      Object.keys(bulkhead.local({ name: "shape", limit: 1 })).sort(),
    ).toEqual(["coordination", "execute", "name", "phase", "snapshot"])
    expect(
      Object.keys(
        bulkhead.distributed({
          name: "shape",
          limit: 1,
          coordinator,
          scope: () => "scope",
        }),
      ).sort(),
    ).toEqual(["coordination", "execute", "name", "phase"])
    expect(Object.keys(circuitBreaker.local({ name: "shape" })).sort()).toEqual(
      ["coordination", "execute", "name", "snapshot"],
    )
    expect(
      Object.keys(
        circuitBreaker.distributed({
          name: "shape",
          coordinator: breakerCoordinator,
          scope: () => "scope",
        }),
      ).sort(),
    ).toEqual(["coordination", "execute", "name"])
  })

  it("documents the introspection every policy carries", () => {
    for (const page of ["docs/bulkhead.md", "docs/circuit-breaker.md"]) {
      const source = readSource(page)
      expect(
        source,
        `${page} must document the coordination property`,
      ).toContain("coordination")
      expect(source, `${page} must document snapshot()`).toContain("snapshot")
    }
  })

  it("describes the local half-open trigger as admission-driven", () => {
    const row = readSource("docs/circuit-breaker.md")
      .split("\n")
      .find((line) => line.startsWith("| `open` | `half-open` |"))
    expect(row, "the state machine must table open -> half-open").toBeDefined()
    expect(row).toMatch(/admission|attempt/i)
  })

  it("names every exported error class in the core reference", () => {
    const values = new Set(
      sourceFiles().flatMap((file) => declaredValues(file)),
    )
    const errorsSection =
      readSource("docs/core-api.md").split("## Errors")[1]?.split("\n## ")[0] ??
      ""
    expect(errorsSection).not.toBe("")
    const errors = [
      ...new Set(
        buildEntries()
          .flatMap((entry) => [...entryExports(entry)])
          .filter((name) => name.endsWith("Error") && values.has(name)),
      ),
    ]
    expect(errors.length).toBeGreaterThan(0)
    for (const name of errors) {
      // The Errors table specifically: a passing mention elsewhere on the page
      // must not satisfy this, which is what a mutation showed the first version
      // of this guard allowed.
      expect(
        errorsSection,
        `${name} is exported but has no row in the core-api.md Errors table`,
      ).toContain(`| \`${name}\` |`)
    }
  })
})
