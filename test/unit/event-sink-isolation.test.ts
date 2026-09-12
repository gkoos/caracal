import { describe, expect, it } from "vitest"
import type { OperationEvent } from "../../src/index.js"
import { circuitBreaker, operation } from "../../src/index.js"

/**
 * Sink isolation.
 *
 * The docs promise that a failing sink cannot affect execution.  Synchronous
 * throws were already contained; a sink whose `emit` returns a rejected promise
 * was not, and surfaced as an unhandled rejection (which can terminate the
 * process).  These tests fail the run if a rejection escapes again.
 */
const capabilities = () => ({ abort: "unsupported", replay: "safe" }) as const

describe("event sink isolation", () => {
  it("does not surface rejections from an asynchronous sink", async () => {
    const subject = operation({
      name: "async-sink",
      adapter: { capabilities, execute: async () => "ok" },
      events: {
        emit: async () => {
          throw new Error("async sink boom")
        },
      },
    })

    await expect(subject.execute(undefined)).resolves.toBe("ok")
    // Give a rejected sink promise a turn to surface.
    await new Promise((resolve) => setTimeout(resolve, 0))
  })

  it("keeps delivering to other sinks when one rejects", async () => {
    const seen: string[] = []
    const subject = operation({
      name: "mixed-sinks",
      adapter: { capabilities, execute: async () => "ok" },
      events: [
        {
          emit: async () => {
            throw new Error("async sink boom")
          },
        },
        {
          emit: (event) => {
            seen.push(event.type)
          },
        },
      ],
    })

    await subject.execute(undefined)
    await new Promise((resolve) => setTimeout(resolve, 0))

    expect(seen).toContain("execution.started")
    expect(seen).toContain("execution.settled")
  })

  it("keeps isolating a synchronously throwing sink", async () => {
    const subject = operation({
      name: "sync-sink",
      adapter: { capabilities, execute: async () => "ok" },
      events: {
        emit: () => {
          throw new Error("sync sink boom")
        },
      },
    })

    await expect(subject.execute(undefined)).resolves.toBe("ok")
  })

  it("never exposes the error or the result to a sink", async () => {
    const secret = "top-secret-token"
    const events: OperationEvent[] = []

    const failing = operation({
      name: "failing",
      adapter: {
        capabilities,
        execute: async () => {
          throw new Error(secret)
        },
      },
      events: { emit: (event) => events.push(event) },
    })
    await failing.execute(undefined).catch(() => {})

    const succeeding = operation({
      name: "succeeding",
      adapter: { capabilities, execute: async () => ({ secret }) },
      events: { emit: (event) => events.push(event) },
    })
    await succeeding.execute(undefined)

    // Neither the error message nor the result value may be reachable.
    expect(JSON.stringify(events)).not.toContain(secret)
    for (const event of events) {
      if ("outcome" in event) {
        expect(Object.keys(event.outcome)).toEqual(["status"])
      }
    }
  })

  it("cannot change what a policy decides by mutating what it receives", async () => {
    // The breaker classifies the error *after* the attempt event is emitted, so
    // a sink that could reach the error could rewrite the breaker's verdict.
    const breaker = circuitBreaker.local({
      name: "mutating-sink",
      minimumThroughput: 1,
      classify: (error) =>
        (error as { classification?: string } | undefined)?.classification ===
        "failure"
          ? "failure"
          : "success",
    })
    const subject = operation({
      name: "mutating-sink",
      adapter: {
        capabilities,
        execute: async () => {
          throw Object.assign(new Error("boom"), { classification: "success" })
        },
      },
      policies: [breaker],
      events: {
        emit: (event) => {
          const payload = event as {
            outcome?: { status?: string; error?: { classification?: string } }
          }
          if (payload.outcome?.error) {
            payload.outcome.error.classification = "failure"
          }
          if (payload.outcome) payload.outcome.status = "success"
        },
      },
    })

    await subject.execute(undefined).catch(() => {})

    expect(breaker.snapshot().failures).toBe(0)
    expect(breaker.snapshot().state).toBe("closed")
  })
})
