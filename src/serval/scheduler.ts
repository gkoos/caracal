/**
 * The clock and timer capability the Serval session needs.
 *
 * `now()` is **monotonic** elapsed time, because every local deadline is anchored
 * to it: a wall-clock jump must not lengthen or shorten authority. Server
 * timestamps never enter a client-side admission decision.
 *
 * Tests inject a deterministic implementation; the default one uses the
 * monotonic clock and unref'd timers, so an idle session never keeps the process
 * alive.
 */

export interface ServalScheduler {
  /** Monotonic milliseconds. */
  now(): number
  /** Runs `task` after `delayMs`; returns a cancellation function. */
  schedule(delayMs: number, task: () => void): () => void
}

export interface ServalSchedulerOptions {
  readonly now?: () => number
}

export function createServalScheduler(
  options: ServalSchedulerOptions = {},
): ServalScheduler {
  const now = options.now ?? (() => performance.now())
  return {
    now,
    schedule(delayMs, task) {
      const handle = setTimeout(task, Math.max(0, delayMs))
      handle.unref()
      return () => clearTimeout(handle)
    },
  }
}
