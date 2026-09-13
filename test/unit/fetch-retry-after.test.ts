import { describe, expect, it, vi } from "vitest"
import {
  createRetryAfterDelay,
  fetchAdapter,
  retryAfterDelay,
  retryAfterMs,
} from "../../src/fetch.js"
import type { RetryContext } from "../../src/index.js"
import { operation, retry } from "../../src/index.js"

function context(
  value: unknown,
  status: "success" | "failure" = "success",
): RetryContext {
  return {
    outcome:
      status === "success"
        ? { status: "success", value }
        : { status: "failure", error: value },
    result: status === "success" ? value : undefined,
    error: status === "success" ? undefined : value,
    capabilities: { abort: "supported", replay: "safe" },
    metadata: { tenant: "acme" },
  }
}

function response(status: number, retryAfter?: string): Response {
  return new Response(null, {
    status,
    headers:
      retryAfter === undefined ? undefined : { "retry-after": retryAfter },
  })
}

describe("retryAfterMs", () => {
  it("parses delta-seconds", () => {
    expect(retryAfterMs(context(response(429, "2")))).toBe(2_000)
  })

  it("parses zero", () => {
    expect(retryAfterMs(context(response(503, "0")))).toBe(0)
  })

  it("parses an HTTP-date relative to now", () => {
    const now = Date.UTC(2026, 0, 1, 12, 0, 0)
    const when = new Date(now + 5_000).toUTCString()
    expect(retryAfterMs(context(response(429, when)), now)).toBe(5_000)
  })

  it("clamps a past HTTP-date to zero", () => {
    const now = Date.UTC(2026, 0, 1, 12, 0, 0)
    const when = new Date(now - 5_000).toUTCString()
    expect(retryAfterMs(context(response(429, when)), now)).toBe(0)
  })

  it("ignores malformed and missing headers", () => {
    expect(retryAfterMs(context(response(429, "soon")))).toBeUndefined()
    expect(retryAfterMs(context(response(429)))).toBeUndefined()
  })

  it("returns undefined for non-response results", () => {
    expect(retryAfterMs(context({ ok: true }))).toBeUndefined()
    expect(retryAfterMs(context("plain"))).toBeUndefined()
  })

  it("reads headers from a thrown response-bearing value", () => {
    const thrown = { headers: new Headers({ "retry-after": "1" }) }
    expect(retryAfterMs(context(thrown, "failure"))).toBe(1_000)
  })
})

describe("retryAfterDelay", () => {
  it("uses exponential backoff when there is no header", () => {
    const first = retryAfterDelay(1, context(response(503)))
    expect(first).toBeGreaterThanOrEqual(100)
    expect(first).toBeLessThanOrEqual(110)

    const third = retryAfterDelay(3, context(response(503)))
    expect(third).toBeGreaterThanOrEqual(400)
    expect(third).toBeLessThanOrEqual(440)
  })

  it("waits at least as long as the server asked", () => {
    const delay = retryAfterDelay(1, context(response(429, "5")))
    expect(delay).toBeGreaterThanOrEqual(5_000)
    expect(delay).toBeLessThanOrEqual(5_500)
  })

  it("caps the server-provided delay", () => {
    const delay = retryAfterDelay(1, context(response(429, "600")))
    expect(delay).toBeGreaterThanOrEqual(30_000)
    expect(delay).toBeLessThanOrEqual(33_000)
  })

  it("caps exponential backoff", () => {
    const delay = retryAfterDelay(20, context(response(503)))
    expect(delay).toBeGreaterThanOrEqual(30_000)
    expect(delay).toBeLessThanOrEqual(33_000)
  })
})

describe("createRetryAfterDelay", () => {
  it("matches the default pacing with no options", () => {
    const delay = createRetryAfterDelay()
    const value = delay(1, context(response(503)))
    expect(value).toBeGreaterThanOrEqual(100)
    expect(value).toBeLessThanOrEqual(110)
  })

  it("applies custom backoff parameters", () => {
    const delay = createRetryAfterDelay({
      baseMs: 50,
      factor: 3,
      jitterRatio: 0,
    })
    expect(delay(1, context(response(503)))).toBe(50)
    expect(delay(2, context(response(503)))).toBe(150)
  })

  it("still prefers a longer Retry-After over custom backoff", () => {
    const delay = createRetryAfterDelay({ jitterRatio: 0 })
    expect(delay(2, context(response(429, "3")))).toBe(3_000)
  })

  it("applies a custom cap to backoff and header alike", () => {
    const delay = createRetryAfterDelay({ maxDelayMs: 1_000, jitterRatio: 0 })
    expect(delay(10, context(response(503)))).toBe(1_000)
    expect(delay(1, context(response(429, "5")))).toBe(1_000)
  })

  it("rejects invalid options", () => {
    expect(() => createRetryAfterDelay({ baseMs: -1 })).toThrow(RangeError)
    expect(() => createRetryAfterDelay({ factor: 0.5 })).toThrow(RangeError)
    expect(() => createRetryAfterDelay({ maxDelayMs: -1 })).toThrow(RangeError)
    expect(() => createRetryAfterDelay({ jitterRatio: 2 })).toThrow(RangeError)
  })

  it("never returns a delay the retry policy would reject", () => {
    // `maxDelayMs` bounds the deterministic part only and jitter is additive, so
    // without a clamp the composed value can exceed what `retry` accepts - and
    // the RangeError that follows replaces whatever the attempt actually threw.
    const random = vi.spyOn(Math, "random").mockReturnValue(1)
    try {
      const atTheBound = createRetryAfterDelay({
        maxDelayMs: 2_147_483_647,
        jitterRatio: 1,
      })
      expect(atTheBound(1, context(response(503, "2147483647")))).toBe(
        2_147_483_647,
      )

      // The documented default keeps its documented worst case, so the clamp is
      // not quietly reshaping ordinary configurations.
      expect(createRetryAfterDelay()(1, context(response(503, "600")))).toBe(
        33_000,
      )
    } finally {
      random.mockRestore()
    }
  })
})

describe("retry delay context", () => {
  it("passes the settled response and execution context to a custom delay", async () => {
    const seen: RetryContext[] = []
    let calls = 0
    const adapter = fetchAdapter({
      fetch: async () => {
        calls += 1
        return calls === 1
          ? new Response(null, { status: 429 })
          : new Response("ok", { status: 200 })
      },
    })
    const subject = operation({
      name: "retry-context",
      adapter,
      policies: [
        retry({
          maxAttempts: 2,
          delay: (_attempt, context) => {
            seen.push(context)
            return 0
          },
        }),
      ],
    })

    const result = await subject.execute(
      { url: "https://example.test/orders" },
      { metadata: { tenant: "acme" } },
    )

    expect(result).toBeInstanceOf(Response)
    expect(calls).toBe(2)
    expect(seen).toHaveLength(1)
    const first = seen[0]
    if (first === undefined) {
      throw new Error("expected a retry context")
    }
    expect(first.result).toBeInstanceOf(Response)
    expect((first.result as Response).status).toBe(429)
    expect(first.capabilities.replay).toBe("safe")
    expect(first.metadata).toEqual({ tenant: "acme" })
  })

  it("drives retries from Retry-After end to end", async () => {
    let calls = 0
    const adapter = fetchAdapter({
      fetch: async () => {
        calls += 1
        return calls === 1
          ? new Response(null, {
              status: 429,
              headers: { "retry-after": "0" },
            })
          : new Response("ok", { status: 200 })
      },
    })
    const subject = operation({
      name: "retry-after",
      adapter,
      policies: [retry({ maxAttempts: 2, delay: retryAfterDelay })],
    })

    const result = await subject.execute({
      url: "https://example.test/orders",
    })

    expect(result.status).toBe(200)
    expect(calls).toBe(2)
  })

  it("rejects a custom delay that returns an invalid value", async () => {
    const subject = operation({
      name: "invalid-delay",
      adapter: {
        capabilities: () => ({ abort: "unsupported", replay: "safe" }),
        execute: async () => 503,
        classify: () => "retryable",
      },
      policies: [retry({ maxAttempts: 2, delay: () => -1 })],
    })

    await expect(subject.execute(undefined)).rejects.toThrow(RangeError)
  })
})

describe("retryAfterMs bounds", () => {
  it("clamps a delta-seconds header to the largest schedulable delay", () => {
    // setTimeout cannot schedule more than 2147483647 ms; it fires almost
    // immediately, so the parser never hands out more than that.
    expect(retryAfterMs(context(response(429, "99999999")))).toBe(2_147_483_647)
  })

  it("leaves a large but schedulable delay untouched", () => {
    expect(retryAfterMs(context(response(429, "2147483")))).toBe(2_147_483_000)
  })

  it("clamps a far-future HTTP-date", () => {
    const now = Date.UTC(2026, 0, 1)
    const when = new Date(now + 100 * 24 * 60 * 60 * 1000).toUTCString()
    expect(retryAfterMs(context(response(429, when)), now)).toBe(2_147_483_647)
  })

  it("still honours a normal header", () => {
    expect(retryAfterMs(context(response(429, "2")))).toBe(2_000)
  })
})
