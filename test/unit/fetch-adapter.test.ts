import { describe, expect, it } from "vitest"
import { fetchAdapter } from "../../src/fetch.js"
import { operation, retry } from "../../src/index.js"
import {
  defineAdapterContractSuite,
  runAdapterContractSuite,
} from "../harness/index.js"

describe("fetchAdapter", () => {
  it("satisfies the runner-agnostic adapter contract", async () => {
    const adapter = fetchAdapter({
      fetch: async () => new Response("ok", { status: 200 }),
    })
    const suite = defineAdapterContractSuite({
      name: "fetch",
      adapter,
      success: {
        args: { url: "https://example.test/orders" },
        assertResult: async (response) =>
          expect(await response.text()).toBe("ok"),
      },
      capabilities: [
        {
          args: { url: "https://example.test/orders" },
          expected: { abort: "supported", replay: "safe" },
        },
        {
          args: {
            url: "https://example.test/orders",
            options: { method: "POST" },
          },
          expected: { abort: "supported", replay: "unsafe" },
        },
      ],
      classifications: [
        {
          outcome: {
            status: "success",
            value: new Response(null, { status: 503 }),
          },
          expected: "retryable",
        },
        {
          outcome: {
            status: "success",
            value: new Response(null, { status: 200 }),
          },
          expected: "success",
        },
      ],
    })

    await expect(runAdapterContractSuite(suite)).resolves.toBeUndefined()
  })

  it("uses response classification to retry a returned 5xx response", async () => {
    let calls = 0
    const adapter = fetchAdapter({
      fetch: async () => {
        calls += 1
        return new Response(null, { status: calls === 1 ? 503 : 200 })
      },
    })
    const subject = operation({
      name: "fetch-retry",
      adapter,
      policies: [retry({ maxAttempts: 2 })],
    })

    const response = await subject.execute({
      url: "https://example.test/orders",
    })
    expect(response.status).toBe(200)
    expect(calls).toBe(2)
  })

  it("combines the operation cancellation signal with request initialization", async () => {
    const requestController = new AbortController()
    const operationController = new AbortController()
    let receivedSignal: AbortSignal | undefined
    const adapter = fetchAdapter({
      fetch: async (_input, init) => {
        receivedSignal = init?.signal ?? undefined
        return new Response(null, { status: 200 })
      },
    })
    const subject = operation({ name: "fetch-signals", adapter })

    await subject.execute(
      {
        url: "https://example.test/orders",
        options: { signal: requestController.signal },
      },
      { signal: operationController.signal },
    )
    operationController.abort()

    expect(receivedSignal?.aborted).toBe(true)
  })
})
