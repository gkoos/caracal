/** A reusable deterministic barrier for coordinating in-process test actors. */
export class Barrier {
  #arrived = 0
  #release: (() => void) | undefined
  readonly #ready: Promise<void>

  constructor(readonly participants: number) {
    if (!Number.isInteger(participants) || participants < 1) {
      throw new RangeError("Barrier participants must be a positive integer")
    }

    this.#ready = new Promise((resolve) => {
      this.#release = resolve
    })
  }

  async wait(): Promise<void> {
    this.#arrived += 1
    if (this.#arrived > this.participants) {
      throw new Error("Barrier received more participants than configured")
    }
    if (this.#arrived === this.participants) {
      this.#release?.()
    }
    await this.#ready
  }
}
