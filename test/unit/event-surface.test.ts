import { readFileSync, readdirSync } from "node:fs"
import { describe, expect, it } from "vitest"
import type { OperationEvent } from "../../src/core/types.js"

/**
 * The event surface, declared once and checked against the code and the docs.
 *
 * `satisfies Record<OperationEvent["type"], EventCoverage>` makes the table
 * exhaustive at typecheck time: adding a member to the union without a row here
 * (or leaving a row behind) fails `npm run typecheck`. The assertions below then
 * check the table against the union text, the emitting sources and the
 * documentation, so an event cannot exist without an emitter, a reference row
 * and a place in every page that claims to list it.
 */
interface EventCoverage {
  /** Sources that emit it. At least one listed source must contain the literal. */
  readonly emitters: readonly string[]
  /** Pages whose "Relevant events" list must mention it. */
  readonly relevantPages: readonly string[]
}

const EVENT_COVERAGE = {
  "execution.started": {
    emitters: ["src/core/operation.ts"],
    relevantPages: [],
  },
  "execution.settled": {
    emitters: ["src/core/operation.ts"],
    relevantPages: [],
  },
  "attempt.started": { emitters: ["src/core/operation.ts"], relevantPages: [] },
  "attempt.settled": { emitters: ["src/core/operation.ts"], relevantPages: [] },
  "timeout.triggered": {
    emitters: ["src/core/timeout.ts"],
    relevantPages: ["docs/timeout-and-retry.md"],
  },
  "retry.scheduled": {
    emitters: ["src/core/retry.ts"],
    relevantPages: ["docs/timeout-and-retry.md"],
  },
  "retry.exhausted": {
    emitters: ["src/core/retry.ts"],
    relevantPages: ["docs/timeout-and-retry.md"],
  },
  "retry.declined": {
    emitters: ["src/core/retry.ts"],
    relevantPages: ["docs/timeout-and-retry.md"],
  },
  "breaker.state-changed": {
    emitters: ["src/core/circuit-breaker.ts"],
    relevantPages: ["docs/circuit-breaker.md"],
  },
  "breaker.rejected": {
    emitters: ["src/core/circuit-breaker.ts"],
    relevantPages: ["docs/circuit-breaker.md"],
  },
  "breaker.observation": {
    emitters: ["src/core/circuit-breaker.ts"],
    relevantPages: ["docs/circuit-breaker.md"],
  },
  "breaker.probe-started": {
    emitters: ["src/core/circuit-breaker.ts"],
    relevantPages: ["docs/circuit-breaker.md"],
  },
  "breaker.observation-stale": {
    emitters: ["src/core/circuit-breaker.ts"],
    relevantPages: ["docs/circuit-breaker.md"],
  },
  "breaker.coordinator-error": {
    emitters: ["src/core/circuit-breaker.ts"],
    relevantPages: ["docs/circuit-breaker.md"],
  },
  "breaker.degraded": {
    emitters: ["src/core/circuit-breaker.ts"],
    relevantPages: ["docs/circuit-breaker.md"],
  },
  "bulkhead.admitted": {
    emitters: ["src/core/bulkhead.ts"],
    relevantPages: ["docs/bulkhead.md"],
  },
  "bulkhead.rejected": {
    emitters: ["src/core/bulkhead.ts"],
    relevantPages: ["docs/bulkhead.md"],
  },
  "bulkhead.waited": {
    emitters: ["src/core/bulkhead.ts"],
    relevantPages: ["docs/bulkhead.md"],
  },
  "bulkhead.released": {
    emitters: ["src/core/bulkhead.ts"],
    relevantPages: ["docs/bulkhead.md"],
  },
  "bulkhead.lease-lost": {
    emitters: ["src/core/bulkhead.ts"],
    relevantPages: ["docs/bulkhead.md"],
  },
  "bulkhead.degraded": {
    emitters: ["src/core/bulkhead.ts"],
    relevantPages: ["docs/bulkhead.md"],
  },
} satisfies Record<OperationEvent["type"], EventCoverage>

function readSource(relative: string): string {
  return readFileSync(new URL(`../../${relative}`, import.meta.url), "utf8")
}

function docPages(): string[] {
  return readdirSync(new URL("../../docs", import.meta.url))
    .filter((name) => name.endsWith(".md"))
    .map((name) => `docs/${name}`)
}

/**
 * The events the union declares, read out of the declaration's module.
 *
 * The literal is not always inline: `OperationEvent` references named members
 * such as `BreakerRejectedEvent`, whose literals sit in their own declarations.
 * In `types.ts` every quoted dotted string is an event name, so the whole module
 * is the safe source of truth.
 */
function unionMembers(): string[] {
  const source = readSource("src/core/types.ts")
  expect(source).toContain("export type OperationEvent =")
  return [...source.matchAll(/"([a-z][a-z-]*\.[a-z-]+)"/g)].map(
    (match) => match[1] as string,
  )
}

/** True when `file` names the event, inline or via a `<namespace>.${...}` template. */
function emits(file: string, event: string): boolean {
  const source = readSource(file)
  const [namespace] = event.split(".")
  return (
    source.includes(`"${event}"`) ||
    source.includes(`'${event}'`) ||
    source.includes(`\`${namespace}.\${`)
  )
}

/** The "Relevant events" list a page publishes, if it publishes one. */
function relevantEvents(page: string): string[] {
  const line = readSource(page).match(/Relevant events: (.+)\.\s*$/m)?.[1] ?? ""
  return [...line.matchAll(/`([^`]+)`/g)].map((match) => match[1] as string)
}

const manifest: Record<string, EventCoverage> = EVENT_COVERAGE
const events = Object.keys(manifest).sort()

describe("event surface", () => {
  it("covers exactly the events the OperationEvent union declares", () => {
    expect(events).toEqual(unionMembers().sort())
  })

  it("names an emitting source that contains the event literal", () => {
    for (const [event, coverage] of Object.entries(manifest)) {
      const emitters = coverage.emitters.filter((file) => emits(file, event))
      expect(
        emitters.length,
        `${event} is not emitted by any listed source`,
      ).toBeGreaterThan(0)
    }
  })

  it("gives every event a row in the reference table", () => {
    const reference = readSource("docs/events-and-observability.md")
    for (const event of events) {
      expect(
        reference,
        `${event} has no row in the events reference`,
      ).toContain(`| \`${event}\` |`)
    }
  })

  it("documents no event the union does not declare", () => {
    const documented = [
      ...readSource("docs/events-and-observability.md").matchAll(
        /^\| `([a-z][a-z-]*\.[a-z-]+)` \|/gm,
      ),
    ].map((match) => match[1] as string)
    expect(documented.length).toBeGreaterThan(0)
    for (const event of documented) {
      expect(events, `${event} is documented but not declared`).toContain(event)
    }
  })

  it("keeps every page's Relevant events list in step with the table", () => {
    const pages = docPages().filter((page) =>
      /Relevant events:/.test(readSource(page)),
    )
    expect(pages.length).toBeGreaterThan(0)
    for (const page of pages) {
      const expected = Object.entries(manifest)
        .filter(([, coverage]) => coverage.relevantPages.includes(page))
        .map(([event]) => event)
        .sort()
      expect(relevantEvents(page).sort(), `${page} Relevant events`).toEqual(
        expected,
      )
    }
  })

  it("points every Relevant events entry at a page that exists", () => {
    const pages = docPages()
    for (const coverage of Object.values(manifest)) {
      for (const page of coverage.relevantPages) {
        expect(pages, `${page} is not a documentation page`).toContain(page)
      }
    }
  })
})
