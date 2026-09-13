import { readFileSync, readdirSync } from "node:fs"
import { posix } from "node:path"

/**
 * Source and documentation reading for the public-surface guards.
 *
 * These helpers exist so a guard derives its expectations from the build
 * configuration and the sources rather than from a hand-kept list that can drift
 * the moment a file is renamed or an entry is added.
 */

export function readSource(relative: string): string {
  return readFileSync(new URL(`../../${relative}`, import.meta.url), "utf8")
}

export function docsPages(): string[] {
  return readdirSync(new URL("../../docs", import.meta.url))
    .filter((name) => name.endsWith(".md"))
    .map((name) => `docs/${name}`)
}

/** Every TypeScript file the package builds or ships, as repo-relative paths. */
export function sourceFiles(): string[] {
  const roots = ["src", "test/harness"]
  return roots.flatMap((root) =>
    readdirSync(new URL(`../../${root}`, import.meta.url), { recursive: true })
      .map((name) => `${root}/${String(name).replaceAll("\\", "/")}`)
      .filter((file) => file.endsWith(".ts")),
  )
}

/** Every test file, so a guard can assert that a claim is asserted somewhere. */
export function testFiles(): string[] {
  return readdirSync(new URL("../../test", import.meta.url), {
    recursive: true,
  })
    .map((name) => `test/${String(name).replaceAll("\\", "/")}`)
    .filter((file) => file.endsWith(".ts"))
}

/** Build entries, read from the tsup configuration the build actually uses. */
export function buildEntries(): string[] {
  return [
    ...readSource("tsup.config.ts").matchAll(
      /(?:"[^"]+"|[\w$]+):\s*"([^"]+\.ts)"/g,
    ),
  ].map((match) => match[1] as string)
}

const REEXPORT = /export (?:type\s*)?\{([^}]*)\}\s*from\s*"([^"]+)"/g
const DECLARATION = /export (?:type|interface|class|const|function)\s+([\w$]+)/g
const TYPE_DECLARATION = /export (?:type|interface|class)\s+([\w$]+)/g
const VALUE_DECLARATION =
  /export (?:async\s+|abstract\s+)?(?:const|function|class)\s+([\w$]+)/g

function collect(file: string, pattern: RegExp): string[] {
  return [...readSource(file).matchAll(pattern)].map(
    (match) => match[1] as string,
  )
}

export function declaredTypes(file: string): string[] {
  return collect(file, TYPE_DECLARATION)
}

export function declaredValues(file: string): string[] {
  return collect(file, VALUE_DECLARATION)
}

/**
 * Every name an entry actually exposes.
 *
 * Resolution is name-scoped: following a re-export only expands the names that
 * were asked for, so a module reachable through a chain cannot contribute
 * exports the entry never re-exported of its own accord. That distinction is the
 * whole point of the guard - `src/core/index.ts` exports `EventOutcome`, and the
 * root entry does not, which a naive transitive walk hides.
 */
export function entryExports(entry: string): Set<string> {
  const names = new Set<string>()
  const seen = new Set<string>()
  const queue: { file: string; only?: Set<string> }[] = [{ file: entry }]

  while (queue.length > 0) {
    const { file, only } = queue.pop() as { file: string; only?: Set<string> }
    const key = `${file}|${only ? [...only].sort().join(",") : "*"}`
    if (seen.has(key)) continue
    seen.add(key)

    const source = readSource(file)
    for (const match of source.matchAll(REEXPORT)) {
      const listed = (match[1] as string)
        .split(",")
        .map((name) => name.trim().replace(/^type\s+/, ""))
        .map((name) =>
          name
            .split(/\s+as\s+/)
            .pop()
            ?.trim(),
        )
        .filter((name): name is string => Boolean(name))
      const selected = only ? listed.filter((name) => only.has(name)) : listed
      for (const name of selected) names.add(name)

      const target = posix
        .normalize(posix.join(posix.dirname(file), match[2] as string))
        .replace(/\.js$/, ".ts")
      if (selected.length > 0 && target.endsWith(".ts")) {
        queue.push({ file: target, only: new Set(selected) })
      }
    }

    for (const name of collect(file, DECLARATION)) {
      if (!only || only.has(name)) names.add(name)
    }
  }

  return names
}
