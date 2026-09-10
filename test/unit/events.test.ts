import { describe, expect, it } from "vitest"

import { EventRecorder } from "../support/events.js"

describe("EventRecorder", () => {
  it("preserves event order and timestamps", () => {
    const recorder = new EventRecorder<string>()

    recorder.record("started", 10)
    recorder.record("settled", 20)

    expect(recorder.all()).toEqual([
      { at: 10, value: "started" },
      { at: 20, value: "settled" },
    ])
    expect(recorder.values()).toEqual(["started", "settled"])
  })
})
