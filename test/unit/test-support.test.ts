import { describe, expect, it } from "vitest"

import { Barrier } from "../support/barrier.js"
import { Deferred } from "../support/deferred.js"
import { MemoryBulkhead } from "../support/memory-coordinator/memory-bulkhead.js"

describe("test support", () => {
  it("coordinates deterministic in-process test actors", async () => {
    const barrier = new Barrier(2)
    const order: string[] = []
    const first = barrier.wait().then(() => order.push("first"))
    const second = barrier.wait().then(() => order.push("second"))

    await Promise.all([first, second])
    expect(order).toHaveLength(2)
  })

  it("controls deferred work and test-only bulkhead occupancy", async () => {
    const deferred = new Deferred<string>()
    const bulkhead = new MemoryBulkhead(1)

    expect(bulkhead.tryAcquire()).toBe(true)
    expect(bulkhead.tryAcquire()).toBe(false)
    deferred.resolve("done")
    await expect(deferred.promise).resolves.toBe("done")
    bulkhead.release()
    expect(bulkhead.occupancy()).toBe(0)
  })
})
