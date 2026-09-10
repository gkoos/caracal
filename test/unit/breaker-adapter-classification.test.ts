import { afterEach, describe, expect, it, vi } from "vitest"
import { fetchAdapter } from "../../src/fetch.js"
import type { BreakerClassifier, OperationEvent } from "../../src/index.js"
import {
  CircuitOpenError,
  circuitBreaker,
  operation,
  retry,
} from "../../src/index.js"
import { memoryBreakerCoordinator } from "../support/memory-coordinator/memory-circuit-breaker.js"

const capabilities = () => ({
  abort: "unsupported" as const,
  replay: "safe" as const,
})

describe.each(["local", "distributed"] as const)(
  "%s breaker adapter classification",
  (coordination) => {
    function breaker(classify?: BreakerClassifier) {
      const options = {
        name: "test",
        minimumThroughput: 1,
        openMs: 10,
        halfOpenSuccesses: 1,
        classify,
      }
      return coordination === "local"
        ? circuitBreaker.local(options)
        : circuitBreaker.distributed({
            ...options,
            coordinator: memoryBreakerCoordinator(),
            scope: () => "shared",
          })
    }

    afterEach(() => vi.useRealTimers())

    it("opens on the final HTTP 503 after exhausting retries", async () => {
      const response = new Response(null, { status: 503 })
      const fetch = vi.fn<typeof globalThis.fetch>().mockResolvedValue(response)
      const events: OperationEvent[] = []
      const op = operation({
        name: "http",
        adapter: fetchAdapter({ fetch }),
        policies: [breaker(), retry({ maxAttempts: 2 })],
        events: { emit: (e) => events.push(e) },
      })

      await expect(op.execute({ url: "https://example.test" })).resolves.toBe(
        response,
      )
      expect(fetch).toHaveBeenCalledTimes(2)
      expect(events.filter((e) => e.type === "breaker.observation")).toEqual([
        expect.objectContaining({ outcome: "failure" }),
      ])
      await expect(
        op.execute({ url: "https://example.test" }),
      ).rejects.toBeInstanceOf(CircuitOpenError)
      expect(fetch).toHaveBeenCalledTimes(2)
    })

    it("records success when retry recovers from an HTTP 503", async () => {
      const response = new Response(null, { status: 200 })
      const fetch = vi
        .fn<typeof globalThis.fetch>()
        .mockResolvedValueOnce(new Response(null, { status: 503 }))
        .mockResolvedValue(response)
      const events: OperationEvent[] = []
      const op = operation({
        name: "http",
        adapter: fetchAdapter({ fetch }),
        policies: [breaker(), retry({ maxAttempts: 2 })],
        events: { emit: (e) => events.push(e) },
      })

      await expect(op.execute({ url: "https://example.test" })).resolves.toBe(
        response,
      )
      expect(events.filter((e) => e.type === "breaker.observation")).toEqual([
        expect.objectContaining({ outcome: "success" }),
      ])
      await expect(op.execute({ url: "https://example.test" })).resolves.toBe(
        response,
      )
      expect(fetch).toHaveBeenCalledTimes(3)
    })

    it("opens on a returned failure without retrying or changing the result", async () => {
      const result = { failed: true }
      const execute = vi.fn(async () => result)
      const op = operation({
        name: "work",
        adapter: { capabilities, execute, classify: () => "failure" },
        policies: [breaker(), retry({ maxAttempts: 2 })],
      })

      await expect(op.execute(undefined)).resolves.toBe(result)
      await expect(op.execute(undefined)).rejects.toBeInstanceOf(
        CircuitOpenError,
      )
      expect(execute).toHaveBeenCalledTimes(1)
    })

    it.each([false, true])(
      "does not record ignored outcomes (throws=%s)",
      async (throws) => {
        const result = new Error("ignored")
        const execute = vi.fn(async () => {
          if (throws) throw result
          return result
        })
        const events: OperationEvent[] = []
        const op = operation({
          name: "work",
          adapter: { capabilities, execute, classify: () => "ignored" },
          policies: [breaker(), retry({ maxAttempts: 2 })],
          events: { emit: (e) => events.push(e) },
        })

        for (let i = 0; i < 2; i++) {
          if (throws) await expect(op.execute(undefined)).rejects.toBe(result)
          else await expect(op.execute(undefined)).resolves.toBe(result)
        }
        expect(execute).toHaveBeenCalledTimes(2)
        expect(
          events.filter((e) => e.type === "breaker.observation"),
        ).toHaveLength(0)
      },
    )

    it("honors success classification of a thrown outcome and preserves the error", async () => {
      const error = new Error("application outcome")
      const events: OperationEvent[] = []
      const op = operation({
        name: "work",
        adapter: {
          capabilities,
          execute: async () => {
            throw error
          },
          classify: () => "success",
        },
        policies: [breaker()],
        events: { emit: (e) => events.push(e) },
      })

      await expect(op.execute(undefined)).rejects.toBe(error)
      await expect(op.execute(undefined)).rejects.toBe(error)
      expect(events.filter((e) => e.type === "breaker.observation")).toEqual([
        expect.objectContaining({ outcome: "success" }),
        expect.objectContaining({ outcome: "success" }),
      ])
    })

    it.each([false, true])(
      "preserves explicit breaker classifier precedence and arguments (throws=%s)",
      async (throws) => {
        const error = new Error("failure")
        const classify = vi.fn<BreakerClassifier>(() => "ignored")
        const events: OperationEvent[] = []
        const op = operation({
          name: "work",
          adapter: {
            capabilities,
            execute: async () => {
              if (throws) throw error
              return "failed result"
            },
            classify: () => "failure",
          },
          policies: [breaker(classify)],
          events: { emit: (e) => events.push(e) },
        })

        for (let i = 0; i < 2; i++) {
          if (throws) await expect(op.execute(undefined)).rejects.toBe(error)
          else
            await expect(op.execute(undefined)).resolves.toBe("failed result")
        }
        expect(classify).toHaveBeenCalledTimes(2)
        expect(classify).toHaveBeenCalledWith(
          throws ? error : undefined,
          !throws,
        )
        expect(
          events.filter((e) => e.type === "breaker.observation"),
        ).toHaveLength(0)
      },
    )

    it("reopens on an HTTP 503 probe and closes on a successful probe", async () => {
      vi.useFakeTimers()
      const fetch = vi
        .fn<typeof globalThis.fetch>()
        .mockResolvedValue(new Response(null, { status: 503 }))
      const events: OperationEvent[] = []
      const op = operation({
        name: "http",
        adapter: fetchAdapter({ fetch }),
        policies: [breaker()],
        events: { emit: (e) => events.push(e) },
      })
      const args = { url: "https://example.test" }

      await op.execute(args)
      await expect(op.execute(args)).rejects.toBeInstanceOf(CircuitOpenError)
      await vi.advanceTimersByTimeAsync(10)
      await op.execute(args)
      await expect(op.execute(args)).rejects.toBeInstanceOf(CircuitOpenError)
      await vi.advanceTimersByTimeAsync(10)
      fetch.mockResolvedValue(new Response(null, { status: 200 }))
      await op.execute(args)
      await op.execute(args)
      expect(
        events
          .filter((e) => e.type === "breaker.state-changed")
          .map((e) => e.state),
      ).toEqual(["open", "half-open", "open", "half-open", "closed"])
    })
  },
)
