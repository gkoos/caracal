/**
 * Third-party adapter API review.
 *
 * This test demonstrates the full `@gkoos/caracal/testing` contract suite being used
 * as a third-party adapter author would use it: implementing an adapter from
 * scratch and running it through the harness without any knowledge of Caracal
 * internals.
 *
 * The custom adapter wraps a mock RPC client with realistic abort, replay, and
 * classification semantics. The harness verifies that the adapter satisfies
 * the public adapter contract before the test runner sees it.
 */
import { describe, expect, it } from "vitest"
import type { Adapter, Classification, Outcome } from "../../src/index.js"
import { operation, retry, timeout } from "../../src/index.js"
import {
  defineAdapterContractSuite,
  runAdapterContractSuite,
} from "../harness/index.js"

// ---------------------------------------------------------------------------
// Example third-party adapter — a simple mock RPC client
// ---------------------------------------------------------------------------

interface RpcRequest {
  readonly method: string
  readonly path: string
  readonly idempotent: boolean
}

interface RpcResponse {
  readonly status: number
  readonly body: string
}

class RpcError extends Error {
  constructor(
    readonly code: "network-error" | "server-error" | "client-error",
    readonly status?: number,
  ) {
    super(`RPC ${code}`)
    this.name = "RpcError"
  }
}

/** Mock RPC transport; real implementation would call a network endpoint. */
async function rpcTransport(
  req: RpcRequest,
  signal?: AbortSignal,
): Promise<RpcResponse> {
  // Honour cancellation before doing any work
  signal?.throwIfAborted()

  // Simulate a response based on path for testing purposes
  if (req.path === "/fail-network") throw new RpcError("network-error")
  if (req.path === "/fail-server") throw new RpcError("server-error", 503)
  if (req.path === "/fail-client") throw new RpcError("client-error", 400)
  if (req.path === "/slow") {
    await new Promise<void>((_, reject) => {
      const t = setTimeout(() => reject(new RpcError("network-error")), 10_000)
      signal?.addEventListener("abort", () => {
        clearTimeout(t)
        reject(signal.reason)
      })
    })
  }
  return { status: 200, body: "ok" }
}

/** Custom RPC adapter implementing the Caracal adapter contract. */
const rpcAdapter: Adapter<RpcRequest, RpcResponse> = {
  /**
   * Capabilities vary per invocation:
   * - `abort` is always "supported" because we pass AbortSignal to the transport.
   * - `replay` is "safe" for idempotent methods and "unsafe" otherwise.
   */
  capabilities(req) {
    return {
      abort: "supported",
      replay: req.idempotent ? "safe" : "unsafe",
    }
  },

  async execute(req, context) {
    return rpcTransport(req, context.signal)
  },

  /**
   * Classify outcomes for retry and circuit-breaker purposes.
   * - Network errors and 503s are transient and retryable.
   * - 4xx client errors are permanent failures.
   * - Success is success.
   */
  classify(outcome: Outcome<RpcResponse>): Classification {
    if (outcome.status === "success") return "success"
    const { error } = outcome
    if (error instanceof RpcError) {
      if (error.code === "network-error") return "retryable"
      if (error.status === 503) return "retryable"
      return "failure"
    }
    return "failure"
  },
}

// ---------------------------------------------------------------------------
// Harness-based contract review
// ---------------------------------------------------------------------------

describe("third-party RPC adapter — contract review via caracal/testing", () => {
  const suite = defineAdapterContractSuite<RpcRequest, RpcResponse>({
    name: "rpc-adapter",

    adapter: rpcAdapter,

    success: {
      args: { method: "GET", path: "/", idempotent: true },
      assertResult: (r) => expect(r.status).toBe(200),
    },

    capabilities: [
      // Idempotent GET: abort=supported, replay=safe
      {
        args: { method: "GET", path: "/items/1", idempotent: true },
        expected: { abort: "supported", replay: "safe" },
      },
      // Non-idempotent POST: abort=supported, replay=unsafe
      {
        args: { method: "POST", path: "/items", idempotent: false },
        expected: { abort: "supported", replay: "unsafe" },
      },
    ],

    classifications: [
      {
        outcome: { status: "success", value: { status: 200, body: "ok" } },
        expected: "success",
      },
      {
        outcome: { status: "failure", error: new RpcError("network-error") },
        expected: "retryable",
      },
      {
        outcome: {
          status: "failure",
          error: new RpcError("server-error", 503),
        },
        expected: "retryable",
      },
      {
        outcome: {
          status: "failure",
          error: new RpcError("client-error", 400),
        },
        expected: "failure",
      },
    ],

    abort: {
      args: { method: "GET", path: "/slow", idempotent: true },
      verify: async ({ controller, execute }) => {
        const pending = execute()
        // Abort after a short delay
        setTimeout(() => controller.abort(), 10)
        await expect(pending).rejects.toThrow()
      },
    },
  })

  it("runs the full contract suite without failures", async () => {
    await runAdapterContractSuite(suite)
    // If runAdapterContractSuite resolves without throwing, all checks passed.
  })

  it("exposes the expected check names to third-party test runners", () => {
    expect(suite.checks.map((c) => c.name)).toEqual([
      "rpc-adapter: capabilities 1",
      "rpc-adapter: capabilities 2",
      "rpc-adapter: successful operation lifecycle",
      "rpc-adapter: classification 1",
      "rpc-adapter: classification 2",
      "rpc-adapter: classification 3",
      "rpc-adapter: classification 4",
      "rpc-adapter: abort behavior",
    ])
  })

  // Individual check registration pattern — as a third-party would wire it
  for (const check of suite.checks) {
    it(check.name, () => check.run())
  }
})

// ---------------------------------------------------------------------------
// Integration with policies — adapter + retry + timeout
// ---------------------------------------------------------------------------

describe("third-party RPC adapter — policy integration", () => {
  it("retries network errors on idempotent requests", async () => {
    let calls = 0
    const countingAdapter: Adapter<RpcRequest, RpcResponse> = {
      capabilities: (req) => rpcAdapter.capabilities(req),
      execute: async (_req, _context) => {
        calls++
        if (calls < 3) throw new RpcError("network-error")
        return { status: 200, body: "ok" }
      },
      classify: rpcAdapter.classify,
    }

    const op = operation({
      name: "rpc",
      adapter: countingAdapter,
      policies: [timeout({ ms: 5_000 }), retry({ maxAttempts: 3 })],
    })

    const result = await op.execute({
      method: "GET",
      path: "/",
      idempotent: true,
    })
    expect(result.status).toBe(200)
    expect(calls).toBe(3)
  })

  it("does not retry non-idempotent requests by default (replay=unsafe)", async () => {
    let calls = 0
    const failingAdapter: Adapter<RpcRequest, RpcResponse> = {
      capabilities: (req) => ({
        abort: "supported",
        replay: req.idempotent ? "safe" : "unsafe",
      }),
      execute: async () => {
        calls++
        throw new RpcError("network-error")
      },
      classify: rpcAdapter.classify,
    }

    const op = operation({
      name: "rpc",
      adapter: failingAdapter,
      policies: [retry({ maxAttempts: 3 })],
    })

    await expect(
      op.execute({ method: "POST", path: "/items", idempotent: false }),
    ).rejects.toBeInstanceOf(RpcError)

    // Non-idempotent: only 1 attempt made even though maxAttempts=3
    expect(calls).toBe(1)
  })

  it("honours timeout and delivers abort signal to adapter", async () => {
    let aborted = false
    const slowAdapter: Adapter<RpcRequest, RpcResponse> = {
      capabilities: () => ({ abort: "supported", replay: "safe" }),
      execute: async (_req, context) => {
        await new Promise<void>((_, reject) => {
          const t = setTimeout(() => reject(new Error("timeout")), 10_000)
          context.signal?.addEventListener("abort", () => {
            aborted = true
            clearTimeout(t)
            reject(context.signal?.reason)
          })
        })
        return { status: 200, body: "never" }
      },
    }

    const op = operation({
      name: "rpc",
      adapter: slowAdapter,
      policies: [timeout({ ms: 50 })],
    })

    await expect(
      op.execute({ method: "GET", path: "/slow", idempotent: true }),
    ).rejects.toThrow()

    expect(aborted).toBe(true)
  })
})
