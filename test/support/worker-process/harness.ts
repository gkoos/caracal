import { fork } from "node:child_process"
import { fileURLToPath } from "node:url"

// ---------------------------------------------------------------------------
// BreakerWorker — wraps breaker-worker.ts for integration tests
// ---------------------------------------------------------------------------

export type BreakerExecuteOptions = {
  outcome: "success" | "failure"
  operationName?: string
  namespace: string
  scope?: string
  minimumThroughput?: number
  failureThreshold?: number
  windowSize?: number
  openMs?: number
  halfOpenProbes?: number
  halfOpenSuccesses?: number
  probeLeaseTtlMs?: number
}

export class BreakerWorker {
  readonly child
  readonly events: Array<Record<string, unknown>> = []
  #id = 0
  #ready = false
  #pending = new Map<
    number,
    {
      resolve(value: unknown): void
      reject(error: Error): void
      timer: ReturnType<typeof setTimeout>
    }
  >()

  constructor(url: string) {
    this.child = fork(
      fileURLToPath(new URL("./breaker-worker.ts", import.meta.url)),
      [],
      {
        execArgv: ["--import", "tsx"],
        env: { ...process.env, CARACAL_REDIS_URL: url },
        stdio: ["ignore", "inherit", "inherit", "ipc"],
      },
    )
    this.child.on(
      "message",
      (message: {
        id: number
        value?: unknown
        error?: string
        event?: Record<string, unknown>
      }) => {
        if (message.id === -1 && message.event) {
          this.events.push(message.event)
          return
        }
        if (message.id === 0) this.#ready = true
        const pending = this.#pending.get(message.id)
        if (!pending) return
        clearTimeout(pending.timer)
        this.#pending.delete(message.id)
        if (message.error) pending.reject(new Error(message.error))
        else pending.resolve(message.value)
      },
    )
    this.child.on("exit", () => {
      for (const pending of this.#pending.values()) {
        clearTimeout(pending.timer)
        pending.reject(new Error("Worker exited"))
      }
      this.#pending.clear()
    })
  }

  #wait(id: number, timeoutMs = 10_000): Promise<unknown> {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.#pending.delete(id)
        reject(new Error("Worker response timed out"))
      }, timeoutMs)
      this.#pending.set(id, { resolve, reject, timer })
    })
  }

  ready() {
    if (this.#ready) return Promise.resolve("ready")
    return this.#wait(0)
  }

  execute(opts: BreakerExecuteOptions) {
    const id = ++this.#id
    const result = this.#wait(id)
    this.child.send({ id, action: "execute", ...opts })
    return result
  }

  freeze(freezeMs = 30_000) {
    const id = ++this.#id
    const result = this.#wait(id)
    this.child.send({ id, action: "freeze", freezeMs })
    return result
  }

  async stop() {
    if (this.child.exitCode !== null || this.child.signalCode !== null) return
    await new Promise<void>((resolve) => {
      this.child.once("exit", () => resolve())
      this.child.kill("SIGKILL")
    })
  }
}

// ---------------------------------------------------------------------------
// LeaseWorker — wraps lease-worker.ts for bulkhead integration tests
// ---------------------------------------------------------------------------

export class LeaseWorker {
  readonly child
  readonly events: unknown[] = []
  #id = 0
  #ready = false
  #pending = new Map<
    number,
    {
      resolve(value: unknown): void
      reject(error: Error): void
      timer: ReturnType<typeof setTimeout>
    }
  >()
  constructor(url: string) {
    this.child = fork(
      fileURLToPath(new URL("./lease-worker.ts", import.meta.url)),
      [],
      {
        execArgv: ["--import", "tsx"],
        env: { ...process.env, CARACAL_REDIS_URL: url },
        stdio: ["ignore", "inherit", "inherit", "ipc"],
      },
    )
    this.child.on(
      "message",
      (message: { id: number; value?: unknown; error?: string }) => {
        this.events.push(message)
        if (message.id === 0) this.#ready = true
        const pending = this.#pending.get(message.id)
        if (!pending) return
        clearTimeout(pending.timer)
        this.#pending.delete(message.id)
        if (message.error) pending.reject(new Error(message.error))
        else pending.resolve(message.value)
      },
    )
    this.child.on("exit", () => {
      for (const pending of this.#pending.values()) {
        clearTimeout(pending.timer)
        pending.reject(new Error("Worker exited"))
      }
      this.#pending.clear()
    })
  }
  #wait(id: number): Promise<unknown> {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.#pending.delete(id)
        reject(new Error("Worker response timed out"))
      }, 10000)
      this.#pending.set(id, { resolve, reject, timer })
    })
  }
  ready() {
    if (this.#ready) return Promise.resolve("ready")
    return this.#wait(0)
  }
  command(action: string, key: string, token: string, ttl = 1000, limit = 1) {
    const id = ++this.#id
    const result = this.#wait(id)
    this.child.send({ id, action, key, token, ttl, limit })
    return result
  }
  async stop() {
    if (this.child.exitCode !== null || this.child.signalCode !== null) return
    await new Promise<void>((resolve) => {
      this.child.once("exit", () => resolve())
      this.child.kill("SIGKILL")
    })
  }
}
