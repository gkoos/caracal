import { describe, expect, it, vi } from "vitest"
import type {
  Adapter,
  BulkheadCoordinator,
  OperationEvent,
  Policy,
} from "../../src/index.js"
import {
  BulkheadRejectedError,
  bulkhead,
  operation,
  retry,
  TimeoutError,
  timeout,
} from "../../src/index.js"
import { Deferred } from "../support/deferred.js"

const traits = () => ({
  abort: "unsupported" as const,
  replay: "safe" as const,
})
function subject(
  policy: Policy,
  work: () => Promise<unknown>,
  policies: Policy[] = [],
  events: OperationEvent[] = [],
) {
  return operation({
    name: "work",
    adapter: { capabilities: traits, execute: work },
    policies: [policy, ...policies],
    events: { emit: (e) => events.push(e) },
  })
}
describe("local bulkhead", () => {
  it("enforces instance-local capacity and holds through caller timeout in either order", async () => {
    vi.useFakeTimers()
    try {
      for (const abort of ["supported", "unsupported"] as const) {
        for (const reversed of [false, true]) {
          const gate = new Deferred<void>()
          const policy = bulkhead.local({ name: "pool", limit: 1 })
          const policies = [policy, timeout({ ms: 10 })]
          const op = operation({
            name: "work",
            adapter: {
              capabilities: () => ({ abort, replay: "safe" }),
              execute: () => gate.promise,
            },
            policies: reversed ? policies.reverse() : policies,
          })
          const call = expect(op.execute(undefined)).rejects.toBeInstanceOf(
            TimeoutError,
          )
          await vi.advanceTimersByTimeAsync(10)
          await call
          expect(policy.snapshot().occupancy).toBe(1)
          await expect(op.execute(undefined)).rejects.toBeInstanceOf(
            BulkheadRejectedError,
          )
          const independent = subject(
            bulkhead.local({ name: "pool", limit: 1 }),
            async () => 1,
          )
          await expect(independent.execute(undefined)).resolves.toBe(1)
          gate.resolve()
          await vi.advanceTimersByTimeAsync(0)
          expect(policy.snapshot().occupancy).toBe(0)
        }
      }
    } finally {
      vi.useRealTimers()
    }
  })
  it("bounds its FIFO queue, removes cancelled waiters, and hands off without barging", async () => {
    const policy = bulkhead.local({
      name: "pool",
      limit: 1,
      queue: { limit: 2, timeoutMs: 1000 },
    })
    const gates = [
      new Deferred<void>(),
      new Deferred<void>(),
      new Deferred<void>(),
    ]
    const starts: number[] = []
    const op = operation({
      name: "work",
      adapter: {
        capabilities: traits,
        execute: async (i: number) => {
          starts.push(i)
          await gates[i]?.promise
        },
      },
      policies: [policy],
    })
    const first = op.execute(0)
    const controller = new AbortController()
    const cancelled = expect(
      op.execute(1, { signal: controller.signal }),
    ).rejects.toBe("cancelled")
    const last = op.execute(2)
    await expect(op.execute(3)).rejects.toMatchObject({ reason: "capacity" })
    controller.abort("cancelled")
    await cancelled
    expect(policy.snapshot().waiting).toBe(1)
    gates[0]?.resolve()
    await first
    expect(starts).toEqual([0, 2])
    gates[2]?.resolve()
    await last
    expect(policy.snapshot()).toEqual({
      coordination: "local",
      occupancy: 0,
      waiting: 0,
    })
  })
  it("times out waiting without starting unsupported work later", async () => {
    vi.useFakeTimers()
    try {
      const gate = new Deferred<void>()
      const policy = bulkhead.local({
        name: "pool",
        limit: 1,
        queue: { limit: 2, timeoutMs: 100 },
      })
      const work = vi.fn(() => gate.promise)
      const first = subject(policy, work).execute(undefined)
      const call = expect(
        subject(policy, work, [timeout({ ms: 10 })]).execute(undefined),
      ).rejects.toBeInstanceOf(TimeoutError)
      await vi.advanceTimersByTimeAsync(10)
      await call
      expect(policy.snapshot().waiting).toBe(0)
      const queued = expect(
        subject(policy, work).execute(undefined),
      ).rejects.toMatchObject({ reason: "wait-timeout" })
      await vi.advanceTimersByTimeAsync(100)
      await queued
      gate.resolve()
      await first
      expect(work).toHaveBeenCalledTimes(1)
    } finally {
      vi.useRealTimers()
    }
  })
  it("admits every retry and releases on adapter failure", async () => {
    const policy = bulkhead.local({ name: "pool", limit: 1 })
    const events: OperationEvent[] = []
    let attempts = 0
    const adapter: Adapter<void, number> = {
      capabilities: traits,
      classify: (o) => (o.status === "failure" ? "retryable" : "success"),
      execute: async () => {
        if (++attempts < 3) throw new Error("retry")
        return attempts
      },
    }
    const op = operation({
      name: "work",
      adapter,
      policies: [policy, retry({ maxAttempts: 3 })],
      events: { emit: (e) => events.push(e) },
    })
    expect(await op.execute(undefined)).toBe(3)
    expect(
      events
        .filter((e) => e.type === "bulkhead.admitted")
        .map((e) => e.context.attempt),
    ).toEqual([1, 2, 3])
    expect(policy.snapshot().occupancy).toBe(0)
  })
  it("validates queue and capacity configuration", () => {
    for (const limit of [0, -1, 1.5, Infinity])
      expect(() => bulkhead.local({ name: "pool", limit })).toThrow()
    expect(() =>
      bulkhead.local({
        name: "pool",
        limit: 1,
        queue: { limit: 0, timeoutMs: 10 },
      }),
    ).toThrow()
  })
})
describe("distributed lifecycle", () => {
  it.each(["denied", "stalled"])(
    "loses a %s renewal and cancels nested admission even for unsupported work",
    async (mode) => {
      vi.useFakeTimers()
      try {
        const local = bulkhead.local({
          name: "local",
          limit: 1,
          queue: { limit: 1, timeoutMs: 2000 },
        })
        const held = new Deferred<void>()
        const owner = subject(local, () => held.promise).execute(undefined)
        const renewal = new Deferred<{ allowed: boolean; occupancy: number }>()
        const command = vi.fn<BulkheadCoordinator["command"]>(
          async (_identity, action) => {
            if (action === "renew")
              return mode === "denied"
                ? { allowed: false, occupancy: 0 }
                : renewal.promise
            return { allowed: true, occupancy: action === "acquire" ? 1 : 0 }
          },
        )
        const remote = bulkhead.distributed({
          name: "remote",
          limit: 1,
          leaseMs: 300,
          scope: () => "shared",
          coordinator: { command },
        })
        const work = vi.fn(async () => 1)
        const call = expect(
          subject(remote, work, [local]).execute(undefined),
        ).rejects.toMatchObject({ reason: "lease-lost" })
        await vi.advanceTimersByTimeAsync(300)
        await call
        expect(work).not.toHaveBeenCalled()
        expect(local.snapshot().waiting).toBe(0)
        renewal.resolve({ allowed: true, occupancy: 1 })
        await vi.advanceTimersByTimeAsync(1000)
        expect(command.mock.calls.filter((c) => c[1] === "renew")).toHaveLength(
          1,
        )
        held.resolve()
        await owner
      } finally {
        vi.useRealTimers()
      }
    },
  )
  it("rejects an acquisition acknowledged after its lease deadline", async () => {
    vi.useFakeTimers()
    try {
      const acquisition = new Deferred<{
        allowed: boolean
        occupancy: number
      }>()
      const command = vi.fn<BulkheadCoordinator["command"]>(
        async (_identity, action) =>
          action === "acquire"
            ? acquisition.promise
            : { allowed: true, occupancy: 0 },
      )
      const policy = bulkhead.distributed({
        name: "pool",
        limit: 1,
        leaseMs: 300,
        scope: () => "a",
        coordinator: { command },
      })
      const work = vi.fn(async () => 1)
      const call = expect(
        subject(policy, work).execute(undefined),
      ).rejects.toMatchObject({ reason: "admission-expired" })
      await vi.advanceTimersByTimeAsync(400)
      acquisition.resolve({ allowed: true, occupancy: 1 })
      await call
      expect(work).not.toHaveBeenCalled()
      expect(command.mock.calls.map((c) => c[1])).toEqual([
        "acquire",
        "release",
      ])
    } finally {
      vi.useRealTimers()
    }
  })
  it("never starts an attempt after delayed admission outlives its timeout", async () => {
    vi.useFakeTimers()
    try {
      const admitted = new Deferred<{ allowed: boolean; occupancy: number }>()
      const command = vi.fn<BulkheadCoordinator["command"]>(
        async (_identity, action) =>
          action === "acquire"
            ? admitted.promise
            : { allowed: true, occupancy: 0 },
      )
      const policy = bulkhead.distributed({
        name: "pool",
        limit: 1,
        scope: () => "a",
        coordinator: { command },
      })
      const work = vi.fn(async () => 1)
      const call = expect(
        subject(policy, work, [timeout({ ms: 10 })]).execute(undefined),
      ).rejects.toBeInstanceOf(TimeoutError)
      await vi.advanceTimersByTimeAsync(10)
      await call
      admitted.resolve({ allowed: true, occupancy: 1 })
      await vi.advanceTimersByTimeAsync(0)
      expect(work).not.toHaveBeenCalled()
      expect(command.mock.calls.map((c) => c[1])).toEqual([
        "acquire",
        "release",
      ])
    } finally {
      vi.useRealTimers()
    }
  })
  it("reports renewal failure, requests abort, and preserves the underlying result on failed release", async () => {
    vi.useFakeTimers()
    try {
      const command = vi.fn<BulkheadCoordinator["command"]>(
        async (_identity, action) => {
          if (action !== "acquire") throw new Error("offline")
          return { allowed: true, occupancy: 1 }
        },
      )
      const policy = bulkhead.distributed({
        name: "pool",
        limit: 1,
        scope: () => "a",
        leaseMs: 300,
        coordinator: { command },
      })
      const gate = new Deferred<number>()
      let signal: AbortSignal | undefined
      const events: OperationEvent[] = []
      const op = operation({
        name: "work",
        adapter: {
          capabilities: () => ({ abort: "supported", replay: "safe" }),
          execute: async (_args: undefined, ctx) => {
            signal = ctx.signal
            return gate.promise
          },
        },
        policies: [policy],
        events: { emit: (e) => events.push(e) },
      })
      const call = op.execute(undefined)
      await vi.advanceTimersByTimeAsync(100)
      expect(signal?.aborted).toBe(true)
      expect(
        events.filter((e) => e.type === "bulkhead.lease-lost"),
      ).toHaveLength(1)
      await vi.advanceTimersByTimeAsync(1000)
      expect(command.mock.calls.filter((c) => c[1] === "renew")).toHaveLength(1)
      gate.resolve(42)
      expect(await call).toBe(42)
      expect(events).toContainEqual(
        expect.objectContaining({
          type: "bulkhead.degraded",
          reason: "release-unknown",
          coordination: "distributed",
        }),
      )
    } finally {
      vi.useRealTimers()
    }
  })
})

// ---------------------------------------------------------------------------
// Event accounting
// ---------------------------------------------------------------------------

describe("local bulkhead events", () => {
  it("reports the released occupancy before handing the permit to the successor", async () => {
    const capacity = bulkhead.local({
      name: "queue-events",
      limit: 1,
      queue: { limit: 1, timeoutMs: 1_000 },
    })
    const events: OperationEvent[] = []
    let releaseGate: (() => void) | undefined
    const gate = new Promise<void>((resolve) => {
      releaseGate = resolve
    })

    const first = subject(
      capacity,
      async () => {
        await gate
        return "first"
      },
      [],
      events,
    )
    const second = subject(
      capacity,
      () => Promise.resolve("second"),
      [],
      events,
    )

    const running = first.execute(undefined)
    const queued = second.execute(undefined)
    releaseGate?.()
    await Promise.all([running, queued])

    expect(events.some((e) => e.type === "bulkhead.waited")).toBe(true)
    const released = events.filter((e) => e.type === "bulkhead.released")
    const admitted = events.filter((e) => e.type === "bulkhead.admitted")
    // Both permits are released in the end, and each release reports the
    // occupancy left behind.  Granting the queued successor first would make the
    // first release report 1, as if a permit were still in flight.
    expect(released.map((e) => (e as { occupancy: number }).occupancy)).toEqual(
      [0, 0],
    )
    expect(admitted.map((e) => (e as { occupancy: number }).occupancy)).toEqual(
      [1, 1],
    )
    // The released permit is reported before the successor's admission, so the
    // pair reads monotonically instead of the release appearing to include it.
    const types = events.map((e) => e.type)
    expect(types.indexOf("bulkhead.released")).toBeLessThan(
      types.lastIndexOf("bulkhead.admitted"),
    )
  })
})

// ---------------------------------------------------------------------------
// Distributed admission deadline
// ---------------------------------------------------------------------------

describe("distributed bulkhead admission deadline", () => {
  it("reports a permit whose lease expired before the call could start", async () => {
    const events: OperationEvent[] = []
    // Acquiring takes longer than the lease, so the permit is granted but its
    // deadline has already passed by the time the call would start.
    const coordinator: BulkheadCoordinator = {
      async command(_identity, action) {
        if (action === "acquire") {
          await new Promise((resolve) => setTimeout(resolve, 150))
        }
        return { allowed: true, occupancy: 1 }
      },
    }

    const capacity = bulkhead.distributed({
      name: "deadline",
      coordinator,
      scope: () => "shared",
      limit: 1,
      leaseMs: 100,
    })
    const subject = operation({
      name: "deadline",
      adapter: { capabilities: traits, execute: async () => "ok" },
      policies: [capacity],
      events: { emit: (event) => events.push(event) },
    })

    const error = await subject.execute(undefined).catch((thrown) => thrown)

    expect(error).toBeInstanceOf(BulkheadRejectedError)
    expect((error as BulkheadRejectedError).reason).toBe("admission-expired")
    // The rejection must reach sinks, not only the caller.
    expect(events.filter((e) => e.type === "bulkhead.rejected")).toMatchObject([
      { reason: "admission-expired" },
    ])
  })
})
