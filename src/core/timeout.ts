import { emitRuntimeEvent, withAdmissionSignal, withSignal } from "./runtime.js"
import type { ExecutionContext, Next, Policy } from "./types.js"

export class TimeoutError extends Error {
  readonly timeoutMs: number

  constructor(timeoutMs: number) {
    super(`Operation timed out after ${timeoutMs}ms`)
    this.name = "TimeoutError"
    this.timeoutMs = timeoutMs
  }
}

export interface TimeoutOptions {
  readonly ms: number
}

function combineSignals(
  external: AbortSignal | undefined,
  timeoutController: AbortController,
): AbortSignal {
  if (external === undefined) {
    return timeoutController.signal
  }

  return AbortSignal.any([external, timeoutController.signal])
}

/** Bounds caller wait time and requests cancellation only when the adapter supports it. */
export function timeout(options: TimeoutOptions): Policy {
  if (!Number.isFinite(options.ms) || options.ms <= 0) {
    throw new RangeError("timeout ms must be a finite positive number")
  }

  return Object.freeze({
    name: "timeout",
    async execute<Result>(
      context: ExecutionContext,
      next: Next<Result>,
    ): Promise<Result> {
      const timeoutError = new TimeoutError(options.ms)
      const supportsAbort = context.capabilities.abort === "supported"
      const controller = new AbortController()
      const attemptContext = withAdmissionSignal(
        supportsAbort
          ? withSignal(context, combineSignals(context.signal, controller))
          : context,
        controller.signal,
      )
      let timer: ReturnType<typeof setTimeout> | undefined

      const timeoutPromise = new Promise<never>((_resolve, reject) => {
        timer = setTimeout(() => {
          controller.abort(timeoutError)
          emitRuntimeEvent(context, {
            type: "timeout.triggered",
            timeoutMs: options.ms,
            abortRequested: supportsAbort,
          })
          reject(timeoutError)
        }, options.ms)
      })

      try {
        return await Promise.race([next(attemptContext), timeoutPromise])
      } finally {
        if (timer !== undefined) {
          clearTimeout(timer)
        }
      }
    },
  })
}
