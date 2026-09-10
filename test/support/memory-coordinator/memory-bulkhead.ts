/**
 * Deterministic test support for future bulkhead policy tests. It is not a
 * production coordinator and is deliberately not reachable from package exports.
 */
export class MemoryBulkhead {
  #inUse = 0

  constructor(readonly limit: number) {
    if (!Number.isInteger(limit) || limit < 1) {
      throw new RangeError("MemoryBulkhead limit must be a positive integer")
    }
  }

  tryAcquire(): boolean {
    if (this.#inUse >= this.limit) {
      return false
    }
    this.#inUse += 1
    return true
  }

  release(): void {
    if (this.#inUse === 0) {
      throw new Error("MemoryBulkhead released without an acquired permit")
    }
    this.#inUse -= 1
  }

  occupancy(): number {
    return this.#inUse
  }
}
