import { getEventListeners } from "node:events"
import { describe, expect, it, vi } from "vitest"
import { MAX_TIMER_MS } from "../../src/core/runtime.js"
import type {
  ExecutionContext,
  OperationEvent,
  Outcome,
} from "../../src/index.js"
import { operation, retry } from "../../src/index.js"
import { createScriptedAdapter, type ScriptedStep } from "../harness/index.js"

const capabilities = { abort: "supported", replay: "safe" } as const

function context(overrides: Partial<ExecutionContext> = {}): ExecutionContext {
  return Object.freeze({
    operationName: "scripted-test",
    executionId: "execution-1",
    attempt: 1,
    signal: undefined,
    metadata: {},
    capabilities,
    classify: (outcome: Outcome<unknown>) =>
      outcome.status === "success" ? "success" : "failure",
    ...overrides,
  })
}

describe("createScriptedAdapter", () => {
  it("consumes outcomes in order and preserves value and error identity", async () => {
    const value = { id: 1 }
    const error = new Error("boom")
    const adapter = createScriptedAdapter<undefined, { id: number }>({
      capabilities,
      outcomes: [
        { status: "success", value },
        { status: "failure", error },
        { status: "success", value: { id: 3 } },
      ],
    })

    await expect(adapter.execute(undefined, context())).resolves.toBe(value)
    await expect(adapter.execute(undefined, context())).rejects.toBe(error)
    await expect(adapter.execute(undefined, context())).resolves.toEqual({
      id: 3,
    })
    await expect(adapter.execute(undefined, context())).rejects.toThrow(
      /exhausted/,
    )
    expect(adapter.calls).toHaveLength(3)
  })

  it("records the arguments and attempt context of each call", async () => {
    const adapter = createScriptedAdapter<string, string>({
      capabilities,
      outcomes: [
        { status: "success", value: "first" },
        { status: "success", value: "second" },
      ],
    })
    const first = context({ attempt: 1, executionId: "a" })
    const second = context({ attempt: 2, executionId: "b" })

    await adapter.execute("one", first)
    await adapter.execute("two", second)

    expect(adapter.calls.map((call) => call.index)).toEqual([0, 1])
    expect(adapter.calls.map((call) => call.args)).toEqual(["one", "two"])
    expect(adapter.calls.map((call) => call.context)).toEqual([first, second])
  })

  it("reserves entries in invocation order when calls run concurrently", async () => {
    vi.useFakeTimers()
    try {
      const adapter = createScriptedAdapter<undefined, string>({
        capabilities,
        outcomes: [
          { status: "success", value: "slow", delayMs: 20 },
          { status: "success", value: "fast" },
        ],
      })
      const first = adapter.execute(
        undefined,
        context({ executionId: "first" }),
      )
      const second = adapter.execute(
        undefined,
        context({ executionId: "second" }),
      )

      await expect(second).resolves.toBe("fast")
      await vi.advanceTimersByTimeAsync(20)
      await expect(first).resolves.toBe("slow")
      expect(adapter.calls.map((call) => call.context.executionId)).toEqual([
        "first",
        "second",
      ])
    } finally {
      vi.useRealTimers()
    }
  })

  it("rejects with a descriptive error on exhaustion and never repeats the final entry", async () => {
    const adapter = createScriptedAdapter<undefined, string>({
      capabilities,
      outcomes: [{ status: "success", value: "only" }],
    })

    await expect(adapter.execute(undefined, context())).resolves.toBe("only")
    await expect(adapter.execute(undefined, context())).rejects.toThrow(
      /call 2.*1 configured/,
    )
    // Exhausted attempts are not reserved, so they do not advance the index:
    // every later call still reports the same missing entry.
    await expect(adapter.execute(undefined, context())).rejects.toThrow(
      /call 2.*1 configured/,
    )
    expect(adapter.calls).toHaveLength(1)
  })

  it("copies the script and each entry at creation", async () => {
    const outcome: { status: "success"; value: string } = {
      status: "success",
      value: "first",
    }
    const outcomes: ScriptedStep<string>[] = [outcome]
    const adapter = createScriptedAdapter<undefined, string>({
      capabilities,
      outcomes,
    })

    outcomes.push({ status: "success", value: "second" })
    outcomes[0] = { status: "success", value: "replaced" }
    outcome.value = "mutated"

    await expect(adapter.execute(undefined, context())).resolves.toBe("first")
    await expect(adapter.execute(undefined, context())).rejects.toThrow(
      /exhausted/,
    )
  })

  it("rejects an empty script on the first execution without recording a call", async () => {
    const adapter = createScriptedAdapter<undefined, string>({
      capabilities,
      outcomes: [],
    })

    await expect(adapter.execute(undefined, context())).rejects.toThrow(
      /exhausted/,
    )
    expect(adapter.calls).toHaveLength(0)
  })

  it("supports object and function capabilities without consuming entries", () => {
    const objectAdapter = createScriptedAdapter<{ mode: string }, string>({
      capabilities: { abort: "unsupported", replay: "unknown" },
      outcomes: [{ status: "success", value: "ok" }],
    })
    expect(objectAdapter.capabilities({ mode: "anything" })).toEqual({
      abort: "unsupported",
      replay: "unknown",
    })
    expect(objectAdapter.calls).toHaveLength(0)

    const functionAdapter = createScriptedAdapter<
      { mode: "safe" | "unsafe" },
      string
    >({
      capabilities: (args) => ({
        abort: "supported",
        replay: args.mode === "safe" ? "safe" : "unsafe",
      }),
      outcomes: [{ status: "success", value: "ok" }],
    })
    expect(functionAdapter.capabilities({ mode: "safe" })).toEqual({
      abort: "supported",
      replay: "safe",
    })
    expect(functionAdapter.capabilities({ mode: "unsafe" })).toEqual({
      abort: "supported",
      replay: "unsafe",
    })
    expect(functionAdapter.calls).toHaveLength(0)
  })

  it("classifies repeatedly without consuming entries", () => {
    const classify = vi.fn((outcome: Outcome<string>) =>
      outcome.status === "failure" ? "retryable" : "success",
    )
    const adapter = createScriptedAdapter<undefined, string>({
      capabilities,
      classify,
      outcomes: [{ status: "success", value: "ok" }],
    })
    const outcome = { status: "failure" as const, error: new Error("x") }

    expect(adapter.classify?.(outcome)).toBe("retryable")
    expect(adapter.classify?.(outcome)).toBe("retryable")
    expect(classify).toHaveBeenCalledTimes(2)
    expect(adapter.calls).toHaveLength(0)
  })

  it("omits classify and dispose when they are not supplied", () => {
    const adapter = createScriptedAdapter<undefined, string>({
      capabilities,
      outcomes: [{ status: "success", value: "ok" }],
    })

    expect(adapter.classify).toBeUndefined()
    expect(adapter.dispose).toBeUndefined()
  })

  it("classifies each retry attempt more than once without consuming extra entries", async () => {
    const classify = vi.fn((outcome: Outcome<string>) =>
      outcome.status === "failure" ? "retryable" : "success",
    )
    const events: OperationEvent[] = []
    const adapter = createScriptedAdapter<undefined, string>({
      capabilities: { abort: "unsupported", replay: "safe" },
      classify,
      outcomes: [
        { status: "failure", error: new Error("first") },
        { status: "success", value: "ok" },
      ],
    })
    const subject = operation({
      name: "classification-does-not-consume",
      adapter,
      policies: [retry({ maxAttempts: 2 })],
      events: { emit: (event) => events.push(event) },
    })

    await expect(subject.execute(undefined)).resolves.toBe("ok")
    expect(adapter.calls).toHaveLength(2)
    expect(classify.mock.calls.length).toBeGreaterThan(2)
    expect(
      events.filter((event) => event.type === "attempt.settled"),
    ).toHaveLength(2)
  })

  it("consumes one entry and rejects with the caller's reason when the signal is already aborted", async () => {
    const controller = new AbortController()
    const reason = new Error("caller cancelled")
    controller.abort(reason)
    const adapter = createScriptedAdapter<undefined, string>({
      capabilities,
      outcomes: [{ status: "success", value: "never" }],
    })

    await expect(
      adapter.execute(undefined, context({ signal: controller.signal })),
    ).rejects.toBe(reason)
    expect(adapter.calls).toHaveLength(1)
    await expect(adapter.execute(undefined, context())).rejects.toThrow(
      /exhausted/,
    )
  })

  it("does not invoke the adapter when the operation is cancelled before the attempt", async () => {
    const controller = new AbortController()
    const reason = new Error("caller cancelled")
    controller.abort(reason)
    const adapter = createScriptedAdapter<undefined, string>({
      capabilities,
      outcomes: [{ status: "success", value: "unexpected" }],
    })
    const subject = operation({
      name: "cancelled-before-adapter",
      adapter,
      policies: [retry({ maxAttempts: 2 })],
    })

    await expect(
      subject.execute(undefined, { signal: controller.signal }),
    ).rejects.toBe(reason)
    expect(adapter.calls).toHaveLength(0)
  })

  it("cancels a delayed outcome, clears its timer, and removes its listener", async () => {
    vi.useFakeTimers()
    try {
      const controller = new AbortController()
      const reason = new Error("stop")
      const adapter = createScriptedAdapter<undefined, string>({
        capabilities,
        outcomes: [{ status: "success", value: "late", delayMs: 20 }],
      })

      const pending = adapter.execute(
        undefined,
        context({ signal: controller.signal }),
      )
      const rejected = expect(pending).rejects.toBe(reason)
      expect(getEventListeners(controller.signal, "abort")).toHaveLength(1)
      expect(vi.getTimerCount()).toBe(1)

      controller.abort(reason)
      await rejected

      expect(getEventListeners(controller.signal, "abort")).toHaveLength(0)
      expect(vi.getTimerCount()).toBe(0)
    } finally {
      vi.useRealTimers()
    }
  })

  it("cancels a hang and removes its listener without scheduling a timer", async () => {
    vi.useFakeTimers()
    try {
      const controller = new AbortController()
      const reason = new Error("stop")
      const adapter = createScriptedAdapter<undefined, string>({
        capabilities,
        outcomes: [{ status: "hang" }],
      })

      const pending = adapter.execute(
        undefined,
        context({ signal: controller.signal }),
      )
      const rejected = expect(pending).rejects.toBe(reason)
      expect(getEventListeners(controller.signal, "abort")).toHaveLength(1)
      expect(vi.getTimerCount()).toBe(0)

      controller.abort(reason)
      await rejected

      expect(getEventListeners(controller.signal, "abort")).toHaveLength(0)
      expect(vi.getTimerCount()).toBe(0)
    } finally {
      vi.useRealTimers()
    }
  })

  it("honours the caller's signal even when capabilities declare abort: unsupported", async () => {
    const controller = new AbortController()
    const reason = new Error("caller")
    const adapter = createScriptedAdapter<undefined, string>({
      capabilities: { abort: "unsupported", replay: "safe" },
      outcomes: [{ status: "hang" }],
    })

    const pending = adapter.execute(
      undefined,
      context({
        signal: controller.signal,
        capabilities: { abort: "unsupported", replay: "safe" },
      }),
    )
    const rejected = expect(pending).rejects.toBe(reason)
    controller.abort(reason)
    await rejected
  })

  it("keeps a cancelled entry consumed", async () => {
    const controller = new AbortController()
    const reason = new Error("stop")
    const adapter = createScriptedAdapter<undefined, string>({
      capabilities,
      outcomes: [
        { status: "success", value: "first", delayMs: 10 },
        { status: "success", value: "second" },
      ],
    })

    const pending = adapter.execute(
      undefined,
      context({ signal: controller.signal }),
    )
    const rejected = expect(pending).rejects.toBe(reason)
    controller.abort(reason)
    await rejected

    await expect(adapter.execute(undefined, context())).resolves.toBe("second")
    expect(adapter.calls.map((call) => call.index)).toEqual([0, 1])
  })

  it("settles a delayed success and failure on their timers", async () => {
    vi.useFakeTimers()
    try {
      const error = new Error("late failure")
      const adapter = createScriptedAdapter<undefined, string>({
        capabilities,
        outcomes: [
          { status: "success", value: "ok", delayMs: 10 },
          { status: "failure", error, delayMs: 20 },
        ],
      })

      const success = adapter.execute(undefined, context())
      const failure = adapter.execute(undefined, context())
      const successExpectation = expect(success).resolves.toBe("ok")
      const failureExpectation = expect(failure).rejects.toBe(error)

      await vi.advanceTimersByTimeAsync(10)
      await successExpectation
      await vi.advanceTimersByTimeAsync(10)
      await failureExpectation
      expect(vi.getTimerCount()).toBe(0)
    } finally {
      vi.useRealTimers()
    }
  })

  it("ignores the signal for an entry marked abort: ignore and settles later", async () => {
    vi.useFakeTimers()
    try {
      const controller = new AbortController()
      const adapter = createScriptedAdapter<undefined, string>({
        capabilities,
        outcomes: [
          { status: "success", value: "late", delayMs: 20, abort: "ignore" },
        ],
      })

      const pending = adapter.execute(
        undefined,
        context({ signal: controller.signal }),
      )
      expect(getEventListeners(controller.signal, "abort")).toHaveLength(0)
      controller.abort(new Error("ignored"))

      await vi.advanceTimersByTimeAsync(20)
      await expect(pending).resolves.toBe("late")
      expect(getEventListeners(controller.signal, "abort")).toHaveLength(0)
      expect(vi.getTimerCount()).toBe(0)
    } finally {
      vi.useRealTimers()
    }
  })

  it("leaves an abort-ignoring hang pending with no timer or listener", async () => {
    vi.useFakeTimers()
    try {
      const controller = new AbortController()
      const adapter = createScriptedAdapter<undefined, string>({
        capabilities,
        outcomes: [{ status: "hang", abort: "ignore" }],
      })

      const pending = adapter.execute(
        undefined,
        context({ signal: controller.signal }),
      )
      pending.catch(() => {})
      controller.abort(new Error("ignored"))
      const state = Promise.race([
        pending.then(
          () => "settled" as const,
          () => "settled" as const,
        ),
        new Promise<"pending">((resolve) => {
          setTimeout(() => resolve("pending"), 25)
        }),
      ])

      await vi.advanceTimersByTimeAsync(25)
      await expect(state).resolves.toBe("pending")
      expect(getEventListeners(controller.signal, "abort")).toHaveLength(0)
      expect(vi.getTimerCount()).toBe(0)
    } finally {
      vi.useRealTimers()
    }
  })

  it.each([
    ["negative", -1],
    ["NaN", Number.NaN],
    ["infinite", Number.POSITIVE_INFINITY],
    ["fractional", 0.5],
    ["above the timer limit", 2_147_483_648],
    ["not a number", "10"],
  ])("rejects an invalid delay: %s", (_label, delayMs) => {
    expect(() =>
      createScriptedAdapter<undefined, string>({
        capabilities,
        outcomes: [
          {
            status: "success",
            value: "ok",
            delayMs: delayMs as number,
          },
        ],
      }),
    ).toThrow(RangeError)
  })

  it("accepts zero and the largest schedulable delay", () => {
    expect(MAX_TIMER_MS).toBe(2_147_483_647)
    for (const delayMs of [0, MAX_TIMER_MS]) {
      expect(() =>
        createScriptedAdapter<undefined, string>({
          capabilities,
          outcomes: [{ status: "success", value: "ok", delayMs }],
        }),
      ).not.toThrow()
    }
  })

  it("forwards the disposal hook for an abandoned retry outcome", async () => {
    const error = new Error("transient")
    const disposed: Array<{
      outcome: Outcome<string>
      context: ExecutionContext
    }> = []
    const adapter = createScriptedAdapter<undefined, string>({
      capabilities: { abort: "unsupported", replay: "safe" },
      classify: (outcome) =>
        outcome.status === "failure" ? "retryable" : "success",
      outcomes: [
        { status: "failure", error },
        { status: "success", value: "recovered" },
      ],
      dispose: (outcome, context) => {
        disposed.push({ outcome, context })
      },
    })
    const subject = operation({
      name: "scripted-dispose",
      adapter,
      policies: [retry({ maxAttempts: 2 })],
    })

    await expect(subject.execute(undefined)).resolves.toBe("recovered")
    expect(disposed).toHaveLength(1)
    const first = disposed[0]
    if (first === undefined) {
      throw new Error("expected one disposed outcome")
    }
    expect(first.outcome.status).toBe("failure")
    if (first.outcome.status === "failure") {
      expect(first.outcome.error).toBe(error)
    }
    expect(first.context.attempt).toBe(1)
  })
})
