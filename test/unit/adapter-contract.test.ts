import { describe, expect, it } from "vitest"

import {
  createScriptedAdapter,
  defineAdapterContractSuite,
  runAdapterContractSuite,
} from "../harness/index.js"

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

  it("checks that an abandoned result is disposed", async () => {
    const suite = defineAdapterContractSuite({
      name: "disposing",
      adapter: {
        capabilities: () => ({ abort: "unsupported", replay: "safe" }),
        execute: async () => "ok",
        dispose: () => undefined,
      },
      success: { args: undefined },
      capabilities: [
        { args: undefined, expected: { abort: "unsupported", replay: "safe" } },
      ],
    })

    expect(suite.checks.map((check) => check.name)).toContain(
      "disposing: abandoned result is disposed",
    )
    await expect(
      suite.checks
        .find(
          (check) => check.name === "disposing: abandoned result is disposed",
        )
        ?.run(),
    ).resolves.toBeUndefined()
  })

  it("passes an abort check when a scripted adapter honours cancellation", async () => {
    // The lifecycle check consumes the first outcome; the abort check consumes
    // the second. The harness runs its checks in that order on one adapter.
    const adapter = createScriptedAdapter<undefined, string>({
      capabilities: { abort: "supported", replay: "safe" },
      outcomes: [{ status: "success", value: "ok" }, { status: "hang" }],
    })
    const suite = defineAdapterContractSuite({
      name: "scripted-honest",
      adapter,
      success: {
        args: undefined,
        assertResult: (result) => expect(result).toBe("ok"),
      },
      capabilities: [
        {
          args: undefined,
          expected: { abort: "supported", replay: "safe" },
        },
      ],
      abort: {
        args: undefined,
        verify: async ({ controller, execute }) => {
          const pending = execute()
          controller.abort()
          await expect(pending).rejects.toBe(controller.signal.reason)
          // Prove the adapter started before the abort; otherwise the runtime's
          // pre-admission check would make this pass without testing the adapter.
          expect(adapter.calls).toHaveLength(2)
        },
      },
    })

    await runAdapterContractSuite(suite)
  })

  it("fails the abort check when a scripted adapter ignores cancellation", async () => {
    const adapter = createScriptedAdapter<undefined, string>({
      capabilities: { abort: "supported", replay: "safe" },
      outcomes: [
        { status: "success", value: "ok" },
        { status: "hang", abort: "ignore" },
      ],
    })
    const suite = defineAdapterContractSuite({
      name: "scripted-dishonest",
      adapter,
      success: { args: undefined },
      capabilities: [
        {
          args: undefined,
          expected: { abort: "supported", replay: "safe" },
        },
      ],
      abort: {
        args: undefined,
        verify: async ({ controller, execute }) => {
          const pending = execute()
          const settlement = pending.then(
            () => "resolved" as const,
            () => "rejected" as const,
          )
          controller.abort()
          expect(adapter.calls).toHaveLength(2)

          // A hang that ignores the abort never settles. Bound the check so a
          // dishonest adapter fails the assertion instead of stalling the suite.
          let timer: ReturnType<typeof setTimeout> | undefined
          const timedOut = new Promise<"pending">((resolve) => {
            timer = setTimeout(() => resolve("pending"), 25)
          })
          try {
            await expect(
              Promise.race([settlement, timedOut]),
              "scripted adapter ignored the abort signal",
            ).resolves.toBe("rejected")
          } finally {
            if (timer !== undefined) {
              clearTimeout(timer)
            }
          }
        },
      },
    })

    await expect(runAdapterContractSuite(suite)).rejects.toThrow(
      /scripted adapter ignored the abort signal/,
    )
  })
})
