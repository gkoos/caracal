export type RecordedEvent<T> = {
  readonly at: number
  readonly value: T
}

/** A deterministic, test-only event sink. */
export class EventRecorder<T> {
  readonly #events: RecordedEvent<T>[] = []

  record(value: T, at = Date.now()): void {
    this.#events.push({ at, value })
  }

  all(): readonly RecordedEvent<T>[] {
    return this.#events
  }

  values(): readonly T[] {
    return this.#events.map((event) => event.value)
  }

  clear(): void {
    this.#events.length = 0
  }
}
