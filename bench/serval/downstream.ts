import { createServer, type ServerResponse } from "node:http"

/**
 * The controlled downstream dependency for the Serval fault lab.
 *
 * It answers on a real socket and accounts for the work it actually does, not for the
 * work its clients believe they are doing: a response is counted until it is finished
 * here, and a client that disconnects does not stop it. That is what makes the lab's
 * physical-concurrency numbers independent of any coordinator's accounting - the
 * distinction the RFC insists on, because an abandoned admission is not evidence that
 * the dependency stopped working.
 */

export type DownstreamMode = "healthy" | "slow" | "failing" | "intermittent"

export interface DownstreamStats {
  readonly started: number
  readonly finished: number
  readonly failed: number
  /** The highest number of responses this server was working on at once. */
  readonly peakPhysical: number
  /** Integral of `max(0, physical - target)` over time, in millisecond-responses. */
  readonly overshootArea: number
  readonly aboveTargetMs: number
}

export interface Downstream {
  readonly url: string
  setMode(mode: DownstreamMode, options?: { latencyMs?: number }): void
  stats(): DownstreamStats
  reset(): void
  close(): Promise<void>
}

export async function startDownstream(
  options: { readonly target?: number; readonly latencyMs?: number } = {},
): Promise<Downstream> {
  const target = options.target ?? 1
  const pending = new Set<ServerResponse>()
  let mode: DownstreamMode = "healthy"
  let latencyMs = options.latencyMs ?? 0
  let started = 0
  let finished = 0
  let failed = 0
  let peakPhysical = 0
  let overshootArea = 0
  let aboveTargetMs = 0
  let lastSampleAt = Date.now()
  let lastPhysical = 0

  /** Integrates the overshoot between samples, so the area is measured, not guessed. */
  function sample(): void {
    const now = Date.now()
    const elapsed = now - lastSampleAt
    lastSampleAt = now
    if (elapsed <= 0) return
    if (lastPhysical > target) {
      overshootArea += (lastPhysical - target) * elapsed
      aboveTargetMs += elapsed
    }
    lastPhysical = pending.size
    peakPhysical = Math.max(peakPhysical, pending.size)
  }

  const server = createServer(async (request, response) => {
    if (request.url === "/health") {
      response.end("ok")
      return
    }

    sample()
    started += 1
    pending.add(response)
    peakPhysical = Math.max(peakPhysical, pending.size)
    const decision = { mode, latencyMs }

    // Deliberately no `close` listener: work the dependency started is finished here
    // whether or not the client is still waiting for it.
    const settle = (status: number): void => {
      if (!pending.delete(response)) return
      finished += 1
      if (status >= 500) failed += 1
      response.statusCode = status
      response.end(status >= 500 ? "unavailable" : "done")
    }

    if (decision.latencyMs > 0) {
      await new Promise((resolve) => setTimeout(resolve, decision.latencyMs))
    }
    if (decision.mode === "failing") {
      settle(503)
      return
    }
    if (decision.mode === "intermittent") {
      // Every third response fails: a dependency that is neither up nor down.
      settle(started % 3 === 0 ? 503 : 200)
      return
    }
    settle(200)
  })

  await new Promise<void>((resolve) =>
    server.listen(0, "127.0.0.1", () => resolve()),
  )
  const address = server.address()
  if (address === null || typeof address === "string") {
    throw new Error("the downstream server has no address")
  }

  const ticker = setInterval(sample, 5)
  ticker.unref()

  return {
    url: `http://127.0.0.1:${address.port}`,
    setMode(next, override = {}) {
      mode = next
      latencyMs = override.latencyMs ?? (next === "slow" ? 40 : 0)
    },
    stats() {
      sample()
      return {
        started,
        finished,
        failed,
        peakPhysical,
        overshootArea: Math.round(overshootArea),
        aboveTargetMs,
      }
    },
    reset() {
      started = 0
      finished = 0
      failed = 0
      peakPhysical = pending.size
      overshootArea = 0
      aboveTargetMs = 0
      lastSampleAt = Date.now()
      lastPhysical = pending.size
    },
    async close() {
      clearInterval(ticker)
      for (const response of pending) response.end("done")
      pending.clear()
      server.closeAllConnections()
      await new Promise<void>((resolve, reject) =>
        server.close((error) => (error ? reject(error) : resolve())),
      )
    },
  }
}
