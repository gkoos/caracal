import { describe, expect, it } from "vitest"
import type { Adapter, EventSink, OperationEvent } from "../../src/index.js"
import { operation, retry } from "../../src/index.js"

describe("retry", () => {
  it("retries thrown, adapter-classified failures up to the configured bound", async () => {
    const attempts: number[] = []
    const events: OperationEvent[] = []
    const adapter: Adapter<void, string> = {
      capabilities: () => ({ abort: "unsupported", replay: "safe" }),
      execute: async (_args, context) => {
        attempts.push(context.attempt)
        if (context.attempt < 3) {
          throw new Error(`failure-${context.attempt}`)
        }
        return "recovered"
      },
      classify: () => "retryable",
    }
    const subject = operation({
      name: "retry-errors",
      adapter,
      policies: [retry({ maxAttempts: 3 })],
      events: { emit: (event) => events.push(event) },
    })

    await expect(subject.execute(undefined)).resolves.toBe("recovered")
    expect(attempts).toEqual([1, 2, 3])
    expect(
      events.filter((event) => event.type === "retry.scheduled"),
    ).toHaveLength(2)
  })

  it("retries returned results that the adapter classifies as failures", async () => {
    const attempts: number[] = []
    const adapter: Adapter<void, number> = {
      capabilities: () => ({ abort: "unsupported", replay: "safe" }),
      execute: async (_args, context) => {
        attempts.push(context.attempt)
        return context.attempt === 1 ? 503 : 200
      },
      classify: (outcome) =>
        outcome.status === "success" && outcome.value === 503
          ? "retryable"
          : "success",
    }
    const subject = operation({
      name: "retry-results",
      adapter,
      policies: [retry({ maxAttempts: 2 })],
    })

    await expect(subject.execute(undefined)).resolves.toBe(200)
    expect(attempts).toEqual([1, 2])
  })

  it("does not start an attempt after caller cancellation", async () => {
    const controller = new AbortController()
    const cancellation = new Error("caller cancelled")
    controller.abort(cancellation)
    let calls = 0
    const subject = operation({
      name: "cancelled-retry",
      adapter: {
        capabilities: () => ({ abort: "unsupported", replay: "safe" }),
        execute: async () => {
          calls += 1
          return "unexpected"
        },
      },
      policies: [retry({ maxAttempts: 2 })],
    })

    await expect(
      subject.execute(undefined, { signal: controller.signal }),
    ).rejects.toBe(cancellation)
    expect(calls).toBe(0)
  })

  it("returns the final classified result after the attempt bound is exhausted", async () => {
    const events: OperationEvent[] = []
    const adapter: Adapter<void, number> = {
      capabilities: () => ({ abort: "unsupported", replay: "safe" }),
      execute: async () => 503,
      classify: () => "retryable",
    }
    const subject = operation({
      name: "exhausted-retry",
      adapter,
      policies: [retry({ maxAttempts: 2 })],
      events: { emit: (event) => events.push(event) } satisfies EventSink,
    })

    await expect(subject.execute(undefined)).resolves.toBe(503)
    expect(
      events.filter((event) => event.type === "retry.exhausted"),
    ).toHaveLength(1)
  })
})
