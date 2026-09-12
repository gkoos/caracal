import { describe, expect, it } from "vitest"
import { operation } from "../../src/index.js"

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
})
