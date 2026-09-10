type ScheduledTask = {
  readonly at: number
  readonly run: () => void
}

/**
 * A small deterministic clock for tests that need to advance time without
 * sleeping. It is test support, not a production scheduler abstraction.
 */
export class ManualClock {
  #now: number
  #tasks: ScheduledTask[] = []

  constructor(initialNow = 0) {
    this.#now = initialNow
  }

  now(): number {
    return this.#now
  }

  sleep(delayMs: number): Promise<void> {
    if (delayMs < 0) {
      throw new RangeError("delayMs must be non-negative")
    }

    return new Promise((resolve) => {
      this.#tasks.push({ at: this.#now + delayMs, run: resolve })
      this.#tasks.sort((left, right) => left.at - right.at)
    })
  }

  advanceBy(durationMs: number): void {
    if (durationMs < 0) {
      throw new RangeError("durationMs must be non-negative")
    }

    this.advanceTo(this.#now + durationMs)
  }

  advanceTo(target: number): void {
    if (target < this.#now) {
      throw new RangeError("ManualClock cannot move backwards")
    }

    while (this.#tasks[0]?.at !== undefined && this.#tasks[0].at <= target) {
      const next = this.#tasks.shift()
      if (next === undefined) {
        break
      }
      this.#now = next.at
      next.run()
    }

    this.#now = target
  }
}
