import type { ServalScheduler } from "../../../src/serval/scheduler.js"

/**
 * A deterministic clock and task queue for Serval tests.
 *
 * Time only moves when a test says so, and a task runs only when its due time is
 * reached, so a refresh loop, a coalesced invalidation and a reconnect are all
 * reproducible. `pending()` is what a bounded-cleanup assertion reads: after a
 * session closes, nothing may be left scheduled.
 */

export interface DeterministicScheduler extends ServalScheduler {
  /** Runs every task due within `ms`, in due order, then flushes microtasks. */
  advanceBy(ms: number): Promise<void>
  /** Tasks that are scheduled and not yet run. */
  pending(): number
}

interface ScheduledTask {
  readonly at: number
  readonly sequence: number
  readonly task: () => void
  cancelled: boolean
}

export function createDeterministicScheduler(
  initialNow = 0,
): DeterministicScheduler {
  let current = initialNow
  let sequence = 0
  const tasks: ScheduledTask[] = []

  /** Lets a task's async continuation settle before the next one runs. */
  async function flush(): Promise<void> {
    for (let turn = 0; turn < 8; turn += 1) {
      await Promise.resolve()
    }
  }

  function takeDue(target: number): ScheduledTask | undefined {
    let best = -1
    for (let index = 0; index < tasks.length; index += 1) {
      const candidate = tasks[index] as ScheduledTask
      if (candidate.cancelled || candidate.at > target) continue
      const currentBest =
        best === -1 ? undefined : (tasks[best] as ScheduledTask)
      if (
        currentBest === undefined ||
        candidate.at < currentBest.at ||
        (candidate.at === currentBest.at &&
          candidate.sequence < currentBest.sequence)
      ) {
        best = index
      }
    }
    if (best === -1) return undefined
    return tasks.splice(best, 1)[0]
  }

  return {
    now() {
      return current
    },

    schedule(delayMs, task) {
      sequence += 1
      const scheduled: ScheduledTask = {
        at: current + Math.max(0, delayMs),
        sequence,
        task,
        cancelled: false,
      }
      tasks.push(scheduled)
      return () => {
        scheduled.cancelled = true
      }
    },

    async advanceBy(ms) {
      if (!Number.isFinite(ms) || ms < 0) {
        throw new RangeError("advanceBy needs a non-negative finite duration")
      }
      const target = current + ms
      for (;;) {
        const next = takeDue(target)
        if (next === undefined) break
        current = Math.max(current, next.at)
        next.task()
        await flush()
      }
      current = target
      await flush()
    },

    pending() {
      return tasks.filter((task) => !task.cancelled).length
    },
  }
}
