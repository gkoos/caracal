import { describe, expect, it } from "vitest"

import { ManualClock } from "../support/manual-clock.js"

describe("ManualClock", () => {
  it("settles due sleepers in chronological order", async () => {
    const clock = new ManualClock()
    const events: string[] = []
    const later = clock.sleep(20).then(() => events.push("later"))
    const sooner = clock.sleep(10).then(() => events.push("sooner"))

    clock.advanceBy(10)
    await sooner
    expect(events).toEqual(["sooner"])

    clock.advanceBy(10)
    await later
    expect(events).toEqual(["sooner", "later"])
    expect(clock.now()).toBe(20)
  })
})
