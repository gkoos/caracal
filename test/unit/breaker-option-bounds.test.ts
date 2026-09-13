import { describe, expect, it } from "vitest"
import { circuitBreaker } from "../../src/index.js"

/**
 * `windowSize` is both a memory bound and a per-observation cost: the script
 * scans every retained member to count the current epoch's share of the window,
 * inside a single-threaded server. Only a lower bound used to be enforced, so
 * `windowSize: 100_000` was accepted and turned each observation into a
 * hundred-thousand-iteration Lua loop.
 */
const traits = () => ({
  abort: "unsupported" as const,
  replay: "safe" as const,
})

describe("windowSize bounds", () => {
  it("accepts the documented maximum and rejects anything above it", () => {
    expect(() =>
      circuitBreaker.local({ name: "bounds", windowSize: 10_000 }),
    ).not.toThrow()
    expect(() =>
      circuitBreaker.local({ name: "bounds", windowSize: 10_001 }),
    ).toThrow(RangeError)
  })

  it("bounds the distributed window the same way", () => {
    const coordinator = {} as never
    expect(() =>
      circuitBreaker.distributed({
        name: "bounds",
        coordinator,
        scope: () => "scope",
        windowSize: 10_001,
      }),
    ).toThrow(RangeError)
  })

  it("still rejects non-integers and non-positive values", () => {
    for (const windowSize of [0, -1, 1.5, Number.NaN]) {
      expect(() =>
        circuitBreaker.local({ name: "bounds", windowSize }),
      ).toThrow(RangeError)
    }
  })

  it("runs an operation with a window at the maximum", async () => {
    const subject = {
      adapter: { capabilities: traits, execute: async () => "ok" },
      policies: [circuitBreaker.local({ name: "bounds", windowSize: 10_000 })],
    }
    const { operation } = await import("../../src/index.js")
    await expect(
      operation({ name: "bounds", ...subject }).execute(undefined),
    ).resolves.toBe("ok")
  })
})
