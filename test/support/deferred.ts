/** A deterministic promise controller for tests. */
export class Deferred<Value> {
  readonly promise: Promise<Value>
  #resolve!: (value: Value | PromiseLike<Value>) => void
  #reject!: (reason?: unknown) => void

  constructor() {
    this.promise = new Promise<Value>((resolve, reject) => {
      this.#resolve = resolve
      this.#reject = reject
    })
  }

  resolve(value: Value): void {
    this.#resolve(value)
  }

  reject(reason?: unknown): void {
    this.#reject(reason)
  }
}
