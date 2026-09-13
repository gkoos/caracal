import { describe, expect, it } from "vitest"
import type { ExecutionContext } from "../../src/index.js"
import { operation, timeout } from "../../src/index.js"
import { admissionSignal } from "../../src/core/runtime.js"

/**
 * The composite admission signal is derived state, not a fresh object per read.
 *
 * `admissionSignal` used to build `AbortSignal.any([...])` on every call, and it
 * is read five to eight times per attempt by the operation, retry and the
 * bulkheads. Two reads of one context therefore returned two different objects,
 * which is both an allocation and a trap for any future `removeEventListener`
 * call against the value.
 */
describe("admission signal", () => {
  it("returns the same composite for repeated reads of one context", async () => {
    const controller = new AbortController()
    let captured: ExecutionContext | undefined
    const subject = operation({
      name: "admission",
      adapter: {
        capabilities: () => ({
          abort: "supported" as const,
          replay: "safe" as const,
        }),
        execute: async (_args: undefined, context) => {
          captured = context
          return 1
        },
      },
      policies: [timeout({ ms: 5_000 })],
    })

    await subject.execute(undefined, { signal: controller.signal })
    expect(captured).toBeDefined()
    const context = captured as ExecutionContext

    const first = admissionSignal(context)
    const second = admissionSignal(context)
    expect(first).toBeDefined()
    expect(first).toBe(second)

    // The composite still honours both inputs it was built from.
    controller.abort()
    expect(first?.aborted).toBe(true)
    expect(admissionSignal(context)?.aborted).toBe(true)
  })

  it("returns the plain signal when there is nothing to combine", async () => {
    const controller = new AbortController()
    let captured: ExecutionContext | undefined
    const subject = operation({
      name: "admission",
      adapter: {
        capabilities: () => ({
          abort: "supported" as const,
          replay: "safe" as const,
        }),
        execute: async (_args: undefined, context) => {
          captured = context
          return 1
        },
      },
    })

    await subject.execute(undefined, { signal: controller.signal })
    const context = captured as ExecutionContext
    expect(admissionSignal(context)).toBe(controller.signal)
  })
})
