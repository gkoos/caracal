import { describe, expect, it, vi } from "vitest"
import type { Adapter, ExecutionContext, Outcome } from "../../src/index.js"
import { operation, retry } from "../../src/index.js"
import { fetchAdapter } from "../../src/fetch.js"

function retryOnceAdapter(
  dispose: (outcome: Outcome<string>) => void | Promise<void>,
): Adapter<undefined, string> {
  let calls = 0
  return {
    capabilities: () => ({
      abort: "unsupported" as const,
      replay: "safe" as const,
    }),
    execute: async () => `result-${++calls}`,
    classify: () => (calls === 1 ? "retryable" : "success"),
    dispose,
  }
}

describe("adapter dispose hook", () => {
  it("disposes a settled success result that retry abandons", async () => {
    const disposed: Array<Outcome<unknown>> = []
    const subject = operation({
      name: "dispose-success",
      adapter: retryOnceAdapter((outcome) => {
        disposed.push(outcome)
      }),
      policies: [retry({ maxAttempts: 2 })],
    })

    await expect(subject.execute(undefined)).resolves.toBe("result-2")
    expect(disposed).toEqual([{ status: "success", value: "result-1" }])
  })

  it("disposes a settled error result that retry abandons", async () => {
    const disposed: Array<Outcome<unknown>> = []
    let calls = 0
    const subject = operation({
      name: "dispose-error",
      adapter: {
        capabilities: () => ({
          abort: "unsupported" as const,
          replay: "safe" as const,
        }),
        execute: async () => {
          calls += 1
          if (calls === 1) throw new Error("transient")
          return "ok"
        },
        classify: (outcome) =>
          outcome.status === "failure" ? "retryable" : "success",
        dispose: (outcome) => {
          disposed.push(outcome)
        },
      },
      policies: [retry({ maxAttempts: 2 })],
    })

    await expect(subject.execute(undefined)).resolves.toBe("ok")
    expect(disposed).toHaveLength(1)
    expect(disposed[0].status).toBe("failure")
  })

  it("does not dispose a result that is returned to the caller", async () => {
    const disposed: Array<Outcome<unknown>> = []
    const subject = operation({
      name: "dispose-returned",
      adapter: {
        capabilities: () => ({
          abort: "unsupported" as const,
          replay: "safe" as const,
        }),
        execute: async () => "final",
        classify: () => "success",
        dispose: (outcome) => {
          disposed.push(outcome)
        },
      },
      policies: [retry({ maxAttempts: 3 })],
    })

    await expect(subject.execute(undefined)).resolves.toBe("final")
    expect(disposed).toHaveLength(0)
  })

  it("swallows a throwing or rejecting dispose without changing the caller", async () => {
    await expect(
      operation({
        name: "dispose-throw",
        adapter: retryOnceAdapter(() => {
          throw new Error("dispose boom")
        }),
        policies: [retry({ maxAttempts: 2 })],
      }).execute(undefined),
    ).resolves.toBe("result-2")

    await expect(
      operation({
        name: "dispose-reject",
        adapter: retryOnceAdapter(() => Promise.reject(new Error("rejected"))),
        policies: [retry({ maxAttempts: 2 })],
      }).execute(undefined),
    ).resolves.toBe("result-2")
  })
})

describe("fetch adapter dispose", () => {
  it("cancels the response body for a success outcome", async () => {
    const cancel = vi.fn()
    const adapter = fetchAdapter({ fetch: async () => new Response("x") })
    await adapter.dispose?.(
      { status: "success", value: { body: { cancel } } as unknown as Response },
      {} as ExecutionContext,
    )
    expect(cancel).toHaveBeenCalledTimes(1)
  })

  it("ignores an error outcome", async () => {
    const adapter = fetchAdapter({ fetch: async () => new Response("x") })
    await expect(
      adapter.dispose?.(
        { status: "failure", error: new Error("boom") },
        {} as ExecutionContext,
      ),
    ).toBeUndefined()
  })
})
