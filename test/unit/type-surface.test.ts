import { describe, expect, it } from "vitest"
import {
  buildEntries,
  declaredTypes,
  docsPages,
  entryExports,
  readSource,
  sourceFiles,
} from "../support/package-surface.js"

/**
 * Documented types must be importable.
 *
 * `scripts/audit-bundle.mjs` pins runtime export keys only, so a type can be
 * documented in prose, implemented in src, and still be unnamed at the package
 * boundary - which is what happened to `EventOutcome`: `src/core/index.ts`
 * exported it, the root entry did not, and no check noticed.
 *
 * The three sets are derived here: what the sources declare, what the build
 * entries export (following re-export chains), and what the documentation names.
 */
describe("documented type surface", () => {
  it("finds the build entries the guards assume", () => {
    expect(buildEntries()).toContain("src/index.ts")
    expect(buildEntries()).toContain("test/harness/index.ts")
  })

  it("exports every documented type from a public entry", () => {
    const exported = new Set(
      buildEntries().flatMap((entry) => [...entryExports(entry)]),
    )
    const declared = new Set(
      sourceFiles().flatMap((file) => declaredTypes(file)),
    )
    expect(declared.size).toBeGreaterThan(0)

    const documented = new Map<string, string[]>()
    for (const page of ["README.md", "SECURITY.md", ...docsPages()]) {
      for (const match of readSource(page).matchAll(
        /`([A-Za-z][A-Za-z0-9_]*)`/g,
      )) {
        const name = match[1] as string
        if (!declared.has(name)) continue
        documented.set(name, [...(documented.get(name) ?? []), page])
      }
    }
    expect(documented.size).toBeGreaterThan(0)

    const missing = [...documented]
      .filter(([name]) => !exported.has(name))
      .map(
        ([name, pages]) =>
          `${name} (named in ${[...new Set(pages)].join(", ")})`,
      )
    expect(
      missing,
      "a documented type must be importable from a public entry",
    ).toEqual([])
  })
})
