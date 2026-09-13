import {
  MAX_TIMER_MS,
  emitRuntimeEvent,
  withAdmissionSignal,
  withSignal,
} from "./runtime.js"
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
  if (options.ms > MAX_TIMER_MS) {
    throw new RangeError(
      `timeout ms must not exceed ${MAX_TIMER_MS} ms, the largest delay setTimeout honours`,
    )
  }

  return Object.freeze({
    name: "timeout",
    async execute<Result>(
      context: ExecutionContext,
      next: Next<Result>,
    ): Promise<Result> {
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
          // Built here rather than above, and that is a trade-off, not a free
          // win. Constructing an Error captures a stack, which measures ~6us
          // against ~0.6us with `Error.stackTraceLimit = 0` and grows with
          // pipeline depth - more than the rest of this policy put together,
          // paid on every attempt to benefit only the ones that time out.
          // Building it in the timer moves that cost to the timeout path, at
          // the price of the caller's frame: the stack now shows the timer
          // internals. `timeoutMs`, and the operation name and execution id on
          // the `timeout.triggered` event, are what identify the call instead.
          const timeoutError = new TimeoutError(options.ms)
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
