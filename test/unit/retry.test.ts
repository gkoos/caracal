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

// ---------------------------------------------------------------------------
// Delay bounds
// ---------------------------------------------------------------------------

describe("retry delay bounds", () => {
  it("rejects a static delay the platform cannot schedule", () => {
    // setTimeout clamps anything above 2147483647 ms to 1 ms, so this would
    // silently retry immediately instead of waiting.
    expect(() => retry({ maxAttempts: 2, delay: 2_147_483_648 })).toThrow(
      RangeError,
    )
  })

  it("rejects a computed delay the platform cannot schedule", async () => {
    const subject = operation({
      name: "huge-delay",
      adapter: {
        capabilities: () => ({
          abort: "unsupported" as const,
          replay: "safe" as const,
        }),
        execute: async () => {
          throw new Error("boom")
        },
        classify: () => "retryable" as const,
      },
      policies: [retry({ maxAttempts: 2, delay: () => 2_147_483_648 })],
    })

    await expect(subject.execute(undefined)).rejects.toThrow(RangeError)
  })

  it("accepts a delay at the bound", () => {
    expect(() => retry({ maxAttempts: 2, delay: 2_147_483_647 })).not.toThrow()
  })
})

// ---------------------------------------------------------------------------
// Declined retries
// ---------------------------------------------------------------------------

describe("retry.declined", () => {
  it("reports a retryable failure that cannot be replayed", async () => {
    const events: OperationEvent[] = []
    const subject = operation({
      name: "no-replay",
      adapter: {
        capabilities: () => ({
          abort: "unsupported" as const,
          replay: "unsafe" as const,
        }),
        execute: async () => {
          throw new Error("boom")
        },
        classify: () => "retryable" as const,
      },
      policies: [retry({ maxAttempts: 3 })],
      events: { emit: (event) => events.push(event) },
    })

    await subject.execute(undefined).catch(() => {})

    expect(events.filter((e) => e.type === "retry.declined")).toMatchObject([
      { reason: "replay-unsafe" },
    ])
  })

  it("reports an outcome that is not retryable", async () => {
    const events: OperationEvent[] = []
    const subject = operation({
      name: "not-retryable",
      adapter: {
        capabilities: () => ({
          abort: "unsupported" as const,
          replay: "safe" as const,
        }),
        execute: async () => {
          throw new Error("nope")
        },
        classify: () => "failure" as const,
      },
      policies: [retry({ maxAttempts: 3 })],
      events: { emit: (event) => events.push(event) },
    })

    await subject.execute(undefined).catch(() => {})

    expect(events.filter((e) => e.type === "retry.declined")).toMatchObject([
      { reason: "not-retryable" },
    ])
  })

  it("does not report a decline when the first attempt succeeded", async () => {
    const events: OperationEvent[] = []
    const subject = operation({
      name: "succeeded",
      adapter: {
        capabilities: () => ({
          abort: "unsupported" as const,
          replay: "safe" as const,
        }),
        execute: async () => "ok",
      },
      policies: [retry({ maxAttempts: 3 })],
      events: { emit: (event) => events.push(event) },
    })

    await expect(subject.execute(undefined)).resolves.toBe("ok")
    // A success emits the lifecycle and nothing else: there was no retry to
    // decline, so a counter over `retry.declined` must not track successes.
    expect(events.map((event) => event.type)).toEqual([
      "execution.started",
      "attempt.started",
      "attempt.settled",
      "execution.settled",
    ])
  })

  it("does not report a decline when the caller cancelled", async () => {
    const events: OperationEvent[] = []
    const controller = new AbortController()
    controller.abort()
    const subject = operation({
      name: "cancelled",
      adapter: {
        capabilities: () => ({
          abort: "unsupported" as const,
          replay: "safe" as const,
        }),
        execute: async () => {
          throw new Error("boom")
        },
        classify: () => "retryable" as const,
      },
      policies: [retry({ maxAttempts: 3 })],
      events: { emit: (event) => events.push(event) },
    })

    await subject
      .execute(undefined, { signal: controller.signal })
      .catch(() => {})

    expect(events.filter((e) => e.type === "retry.declined")).toHaveLength(0)
  })
})
