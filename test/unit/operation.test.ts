import { describe, expect, it } from "vitest"
import type {
  Adapter,
  EventSink,
  OperationEvent,
  Policy,
} from "../../src/index.js"
import { operation } from "../../src/index.js"

describe("operation", () => {
  it("creates an immutable invocation context from adapter traits", async () => {
    const controller = new AbortController()
    let observedContext: unknown
    const adapter: Adapter<{ readonly id: string }, string> = {
      capabilities: () => ({ abort: "supported", replay: "safe" }),
      execute: async (_args, context) => {
        observedContext = context
        return "ok"
      },
    }
    const orders = operation({ name: "orders", adapter })

    await expect(
      orders.execute(
        { id: "42" },
        {
          executionId: "execution-42",
          signal: controller.signal,
          metadata: { region: "eu-west-2" },
        },
      ),
    ).resolves.toBe("ok")

    expect(observedContext).toMatchObject({
      operationName: "orders",
      executionId: "execution-42",
      attempt: 1,
      signal: controller.signal,
      metadata: { region: "eu-west-2" },
      capabilities: { abort: "supported", replay: "safe" },
    })
    expect(Object.isFrozen(observedContext)).toBe(true)
  })

  it("applies policies in declared outer-to-inner order", async () => {
    const calls: string[] = []
    const policy = (name: string): Policy => ({
      name,
      async execute(context, next) {
        calls.push(`${name}:before`)
        try {
          return await next(context)
        } finally {
          calls.push(`${name}:after`)
        }
      },
    })
    const adapter: Adapter<void, string> = {
      capabilities: () => ({ abort: "unsupported", replay: "unknown" }),
      execute: async () => {
        calls.push("adapter")
        return "done"
      },
    }
    const subject = operation({
      name: "ordered",
      adapter,
      policies: [policy("outer"), policy("inner")],
    })

    await expect(subject.execute()).resolves.toBe("done")
    expect(calls).toEqual([
      "outer:before",
      "inner:before",
      "adapter",
      "inner:after",
      "outer:after",
    ])
  })

  it("emits classified outcomes without allowing sink failures to alter execution", async () => {
    const events: OperationEvent[] = []
    const recorder: EventSink = { emit: (event) => events.push(event) }
    const brokenSink: EventSink = {
      emit: () => {
        throw new Error("observer failure")
      },
    }
    const adapter: Adapter<void, number> = {
      capabilities: () => ({ abort: "supported", replay: "unsafe" }),
      execute: async () => 204,
      classify: () => "ignored",
    }
    const subject = operation({
      name: "events",
      adapter,
      events: [recorder, brokenSink],
    })

    await expect(
      subject.execute(undefined, { executionId: "events-1" }),
    ).resolves.toBe(204)
    expect(events.map((event) => event.type)).toEqual([
      "execution.started",
      "attempt.started",
      "attempt.settled",
      "execution.settled",
    ])
    expect(events[2]).toMatchObject({
      type: "attempt.settled",
      classification: "ignored",
    })
  })

  it("preserves the original adapter error", async () => {
    const error = new Error("downstream unavailable")
    const subject = operation({
      name: "errors",
      adapter: {
        capabilities: () => ({ abort: "unsupported", replay: "unknown" }),
        execute: async () => Promise.reject(error),
      },
    })

    await expect(subject.execute(undefined)).rejects.toBe(error)
  })
})
