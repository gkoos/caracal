import { describe, expect, it } from "vitest"
import type {
  Adapter,
  Classification,
  OperationCapabilities,
  Outcome,
} from "../../src/index.js"
import { operation, retry } from "../../src/index.js"

/**
 * Every adapter method is called on the adapter.
 *
 * `capabilities` and `execute` always were, but `classify` and `dispose` were
 * taken off the adapter and invoked detached. An adapter holding state on its
 * instance - a class, or an object literal reading a sibling method through
 * `this` - therefore threw a `TypeError` out of the classification path: the
 * attempt's value or error was lost, `retry` classified the classifier's own
 * failure instead of the outcome, and the breaker recorded no observation at
 * all. `dispose` threw too, into the isolation that exists to swallow a *user's*
 * dispose error, so an abandoned body or handle was never released and nothing
 * reported that it had not been.
 *
 * The adapters below keep their state in private fields on purpose: a lost
 * receiver is then a hard `TypeError` rather than a silently wrong answer.
 */
const traits = (): OperationCapabilities => ({
  abort: "unsupported",
  replay: "safe",
})

/** Classifies its first attempt `retryable`, so `retry` runs a second one. */
class RetryOnceAdapter implements Adapter<undefined, string> {
  #calls = 0

  capabilities(): OperationCapabilities {
    return traits()
  }

  async execute(): Promise<string> {
    return `result-${++this.#calls}`
  }

  classify(): Classification {
    return this.#calls === 1 ? "retryable" : "success"
  }
}

describe("adapter method receiver", () => {
  it("classifies through a method that reads instance state", async () => {
    const subject = operation({
      name: "receiver-classify",
      adapter: new RetryOnceAdapter(),
      policies: [retry({ maxAttempts: 2 })],
    })

    // Without the receiver the classifier throws, and a successful first
    // attempt is lost to that `TypeError` instead of being retried.
    await expect(subject.execute(undefined)).resolves.toBe("result-2")
  })

  it("classifies for the attempt.settled event through the adapter", async () => {
    const classifications: Classification[] = []
    const subject = operation({
      name: "receiver-event",
      adapter: new RetryOnceAdapter(),
      policies: [retry({ maxAttempts: 2 })],
      events: {
        emit: (event) => {
          if (event.type === "attempt.settled") {
            classifications.push(event.classification)
          }
        },
      },
    })

    await expect(subject.execute(undefined)).resolves.toBe("result-2")

    // Both the value and the verdict come from the adapter's private state.
    expect(classifications).toEqual(["retryable", "success"])
  })

  it("keeps the caller's error when classification reads instance state", async () => {
    const boom = new Error("dependency refused")

    class FailingAdapter implements Adapter<undefined, string> {
      #classifications = 0

      capabilities(): OperationCapabilities {
        return traits()
      }

      async execute(): Promise<string> {
        throw boom
      }

      classify(): Classification {
        this.#classifications += 1
        return "failure"
      }

      get classifications(): number {
        return this.#classifications
      }
    }

    const adapter = new FailingAdapter()
    const subject = operation({
      name: "receiver-classify-error",
      adapter,
      policies: [retry({ maxAttempts: 2 })],
    })

    await expect(subject.execute(undefined)).rejects.toBe(boom)
    // The outcome was classified by the adapter, not by a failed call to it.
    expect(adapter.classifications).toBe(1)
  })

  it("disposes an abandoned result through a method that reads instance state", async () => {
    class DisposingAdapter implements Adapter<undefined, string> {
      #calls = 0
      readonly released: string[] = []

      capabilities(): OperationCapabilities {
        return traits()
      }

      async execute(): Promise<string> {
        return `result-${++this.#calls}`
      }

      // An arrow property, so this test fails on the receiver `dispose` lost
      // rather than on the classification `retry` runs first.
      classify = (): Classification =>
        this.#calls === 1 ? "retryable" : "success"

      dispose(outcome: Outcome<string>): void {
        if (outcome.status === "success") {
          this.released.push(outcome.value)
        }
      }
    }

    const adapter = new DisposingAdapter()
    const subject = operation({
      name: "receiver-dispose",
      adapter,
      policies: [retry({ maxAttempts: 2 })],
    })

    await expect(subject.execute(undefined)).resolves.toBe("result-2")
    // Without the receiver this stays empty: the release threw, and dispose
    // isolation swallowed it exactly as it swallows a user's dispose error.
    expect(adapter.released).toEqual(["result-1"])
  })

  it("calls every adapter method with the adapter as receiver", async () => {
    const receivers: unknown[] = []

    class RecordingAdapter implements Adapter<undefined, string> {
      #calls = 0

      capabilities(): OperationCapabilities {
        receivers.push(this)
        return traits()
      }

      async execute(): Promise<string> {
        receivers.push(this)
        return `result-${++this.#calls}`
      }

      classify(): Classification {
        receivers.push(this)
        return this.#calls === 1 ? "retryable" : "success"
      }

      dispose(): void {
        receivers.push(this)
      }
    }

    const adapter = new RecordingAdapter()
    const subject = operation({
      name: "receiver-record",
      adapter,
      policies: [retry({ maxAttempts: 2 })],
    })

    await expect(subject.execute(undefined)).resolves.toBe("result-2")

    // capabilities once, execute twice, classify twice (retry's two calls) and
    // dispose once for the abandoned first attempt.
    expect(receivers).toHaveLength(6)
    for (const receiver of receivers) {
      expect(receiver).toBe(adapter)
    }
  })

  it("keeps the receiver for an object-literal adapter", async () => {
    const classifications: Classification[] = []
    const literal: Adapter<undefined, string> = {
      capabilities: traits,
      execute: async () => "final",
      // Reads a sibling method through `this`, the way an object-literal
      // adapter factoring shared logic between its methods does.
      classify(this: Adapter<undefined, string>): Classification {
        return typeof this.execute === "function" ? "success" : "failure"
      },
    }

    const subject = operation({
      name: "receiver-literal",
      adapter: literal,
      events: {
        emit: (event) => {
          if (event.type === "attempt.settled") {
            classifications.push(event.classification)
          }
        },
      },
    })

    await expect(subject.execute(undefined)).resolves.toBe("final")
    expect(classifications).toEqual(["success"])
  })
})
