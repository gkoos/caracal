import { describe, expect, it, vi } from "vitest"
import { operation } from "../../src/index.js"

/**
 * What constructing and running an operation must not touch.
 *
 * Both of these were silent: `events` was frozen in place on the caller's own
 * array, and `classify` was called purely to fill in an event payload, even when
 * no sink would receive it - which matters because the classifier contract has
 * to be pure for that call to be free.
 */
const traits = () => ({
  abort: "unsupported" as const,
  replay: "safe" as const,
})

describe("operation inputs", () => {
  it("does not freeze or retain the caller's events array", () => {
    const sinks = [{ emit: () => {} }]
    operation({
      name: "inputs",
      adapter: { capabilities: traits, execute: async () => 1 },
      events: sinks,
    })

    expect(Object.isFrozen(sinks)).toBe(false)
    // Would throw in strict mode if the runtime had frozen the caller's array.
    sinks.push({ emit: () => {} })
    expect(sinks).toHaveLength(2)
  })

  it("does not classify when no sink will receive the verdict", async () => {
    const classify = vi.fn(() => "success" as const)
    const subject = operation({
      name: "inputs",
      adapter: {
        capabilities: traits,
        execute: async () => 1,
        classify,
      },
    })

    for (let i = 0; i < 25; i += 1) await subject.execute(undefined)
    expect(classify).not.toHaveBeenCalled()
  })

  it("still classifies once a sink is configured", async () => {
    const classify = vi.fn(() => "success" as const)
    const delivered: string[] = []
    const subject = operation({
      name: "inputs",
      adapter: {
        capabilities: traits,
        execute: async () => 1,
        classify,
      },
      events: { emit: (event) => delivered.push(event.type) },
    })

    await subject.execute(undefined)
    expect(classify).toHaveBeenCalled()
    expect(delivered).toContain("attempt.settled")
  })
})
