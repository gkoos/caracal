import { describe, expect, it } from "vitest"
import type {
  OperationEvent,
  Policy,
  RateLimitCoordinator,
} from "../../src/index.js"
import {
  operation,
  RateLimitExceededError,
  rateLimit,
} from "../../src/index.js"
import { memoryRateLimitCoordinator } from "../support/memory-coordinator/memory-rate-limit.js"

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const traits = () => ({
  abort: "unsupported" as const,
  replay: "safe" as const,
})

function makeOp(
  policy: Policy,
  work: () => Promise<unknown>,
  events: OperationEvent[] = [],
) {
  return operation({
    name: "work",
    adapter: { capabilities: traits, execute: work },
    policies: [policy],
    events: { emit: (e) => events.push(e) },
  })
}

const ok = () => Promise.resolve("ok")

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

describe("rateLimit.distributed — validation", () => {
  const coordinator = memoryRateLimitCoordinator()

  it("rejects an empty name", () => {
    expect(() =>
      rateLimit.distributed({
        name: "",
        rate: 10,
        coordinator,
        scope: () => "x",
      }),
    ).toThrow()
  })

  it("requires coordinator", () => {
    expect(() =>
      rateLimit.distributed({
        name: "x",
        rate: 10,
        coordinator: null as never,
        scope: () => "x",
      }),
    ).toThrow()
  })

  it("requires scope to be a function", () => {
    expect(() =>
      rateLimit.distributed({
        name: "x",
        rate: 10,
        coordinator,
        scope: "static" as never,
      }),
    ).toThrow()
  })

  it("rejects a non-positive rate", () => {
    expect(() =>
      rateLimit.distributed({
        name: "x",
        rate: 0,
        coordinator,
        scope: () => "x",
      }),
    ).toThrow()
  })
})

// ---------------------------------------------------------------------------
// Admission
// ---------------------------------------------------------------------------

describe("rateLimit.distributed — admission", () => {
  it("delegates to the coordinator and surfaces retry-after", async () => {
    const coordinator = memoryRateLimitCoordinator()
    coordinator.setNow(0)
    const policy = rateLimit.distributed({
      name: "x",
      rate: 1000,
      coordinator,
      scope: () => "shared",
    })
    const op = makeOp(policy, ok)

    await expect(op.execute(undefined)).resolves.toBe("ok")
    await expect(op.execute(undefined)).rejects.toMatchObject({
      name: "RateLimitExceededError",
      coordination: "distributed",
      policyName: "x",
      scope: "shared",
      retryAfterMs: 1,
    })

    coordinator.setNow(1)
    await expect(op.execute(undefined)).resolves.toBe("ok")
  })

  it("isolates budgets by scope", async () => {
    const coordinator = memoryRateLimitCoordinator()
    coordinator.setNow(0)
    const policy = rateLimit.distributed({
      name: "x",
      rate: 1000,
      coordinator,
      scope: (ctx) => String(ctx.metadata.scope),
    })
    const make = () =>
      operation({
        name: "work",
        adapter: { capabilities: traits, execute: ok },
        policies: [policy],
      })

    await make().execute(undefined, { metadata: { scope: "a" } })
    await make().execute(undefined, { metadata: { scope: "b" } })
    await expect(
      make().execute(undefined, { metadata: { scope: "a" } }),
    ).rejects.toBeInstanceOf(RateLimitExceededError)
  })

  it("fails closed on coordinator error and emits the closed reason set", async () => {
    const coordinator: RateLimitCoordinator = {
      async command() {
        throw new Error("boom")
      },
    }
    const events: OperationEvent[] = []
    const policy = rateLimit.distributed({
      name: "x",
      rate: 1000,
      coordinator,
      scope: () => "shared",
    })
    const op = makeOp(policy, ok, events)

    await expect(op.execute(undefined)).rejects.toThrow("boom")
    expect(events.find((e) => e.type === "ratelimit.degraded")).toMatchObject({
      reason: "admission-unknown",
    })
    expect(events.find((e) => e.type === "ratelimit.rejected")).toMatchObject({
      reason: "coordinator-unavailable",
    })
  })
})

// ---------------------------------------------------------------------------
// Scope validation
// ---------------------------------------------------------------------------

describe("rateLimit.distributed — scope validation at runtime", () => {
  it("throws TypeError when scope returns an empty string", async () => {
    const policy = rateLimit.distributed({
      name: "x",
      rate: 1000,
      coordinator: memoryRateLimitCoordinator(),
      scope: () => "",
    })
    await expect(makeOp(policy, ok).execute(undefined)).rejects.toBeInstanceOf(
      TypeError,
    )
  })
})
