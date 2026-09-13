import { describe, expect, it, vi } from "vitest"
import type { Policy } from "../../src/index.js"
import {
  bulkhead,
  circuitBreaker,
  operation,
  retry,
  timeout,
} from "../../src/index.js"

/**
 * A sink-free operation must not do per-event work.
 *
 * `emitRuntimeEvent` used to build the full event object - including a
 * `Date.now()` call - before checking whether any sink would receive it, so
 * every lifecycle event allocated on the hot path even with observability
 * turned off. This is asserted through a proxy for that work rather than
 * through allocations, so it cannot flake: with no sink configured, the number
 * of clock reads must not scale with the number of events emitted.
 */
const traits = () => ({
  abort: "unsupported" as const,
  replay: "safe" as const,
})

const executions = 50
/** Four lifecycle events per execution: started/settled × execution/attempt. */
const lifecycleEvents = 4

async function withPolicies(policies: Policy[]): Promise<number> {
  const subject = operation({
    name: "sink-free",
    adapter: { capabilities: traits, execute: async () => 1 },
    policies,
  })
  const now = vi.spyOn(Date, "now")
  try {
    for (let i = 0; i < executions; i += 1) await subject.execute(undefined)
    return now.mock.calls.length
  } finally {
    now.mockRestore()
  }
}

describe("sink-free execution", () => {
  it("does not build events for an operation with no policies", async () => {
    const clockReads = await withPolicies([])
    expect(clockReads).toBeLessThan(lifecycleEvents)
  })

  it("does not build events per attempt under the local policies", async () => {
    const clockReads = await withPolicies([
      timeout({ ms: 5_000 }),
      retry({ maxAttempts: 2 }),
      bulkhead.local({ name: "sink-free", limit: 4 }),
      circuitBreaker.local({ name: "sink-free" }),
    ])
    expect(clockReads).toBeLessThan(lifecycleEvents)
  })

  it("still delivers every event once a sink is configured", async () => {
    const events: string[] = []
    const subject = operation({
      name: "sink-free",
      adapter: { capabilities: traits, execute: async () => 1 },
      events: { emit: (event) => events.push(event.type) },
    })
    await subject.execute(undefined)
    expect(events).toEqual([
      "execution.started",
      "attempt.started",
      "attempt.settled",
      "execution.settled",
    ])
  })
})
