import { describe, expect, it } from "vitest"

import { defineAdapterContractSuite } from "../harness/index.js"

describe("adapter contract harness", () => {
  it("does not require a particular test runner", async () => {
    const suite = defineAdapterContractSuite({
      name: "custom",
      adapter: {
        capabilities: () => ({ abort: "unsupported", replay: "unknown" }),
        execute: async () => "ok",
      },
      success: {
        args: undefined,
        assertResult: (result) => expect(result).toBe("ok"),
      },
      capabilities: [
        {
          args: undefined,
          expected: { abort: "unsupported", replay: "unknown" },
        },
      ],
    })

    expect(suite.checks.map((check) => check.name)).toEqual([
      "custom: capabilities 1",
      "custom: successful operation lifecycle",
    ])
    await expect(
      Promise.all(suite.checks.map((check) => check.run())),
    ).resolves.toHaveLength(2)
  })
})
