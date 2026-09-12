import {
  MAX_TIMER_MS,
  admissionSignal,
  emitRuntimeEvent,
  nextAttempt,
} from "./runtime.js"
import type {
  Classification,
  ExecutionContext,
  ExecutionMetadata,
  Next,
  OperationCapabilities,
  Outcome,
  Policy,
} from "./types.js"

/**
 * Settled-attempt information passed to a custom `delay` function.
 *
 * `result` and `error` are conveniences over `outcome`; the core never
 * interprets them, so an adapter-specific helper can pace retries from
 * protocol feedback (for example an HTTP `Retry-After` header) without
 * leaking protocol types into the core.
 */
export interface RetryContext {
  readonly outcome: Outcome<unknown>
  readonly result: unknown
  readonly error: unknown
  readonly capabilities: OperationCapabilities
  readonly metadata: ExecutionMetadata
}

export type RetryDelay =
  | number
  | ((attempt: number, context: RetryContext) => number)

export interface RetryOptions {
  readonly maxAttempts: number
  readonly delay?: RetryDelay
}

function throwIfAborted(signal: AbortSignal | undefined): void {
  if (!signal?.aborted) {
    return
  }

  throw (
    signal.reason ?? new DOMException("The operation was aborted", "AbortError")
  )
}

function retryContext(
  context: ExecutionContext,
  outcome: Outcome<unknown>,
): RetryContext {
  return {
    outcome,
    result: outcome.status === "success" ? outcome.value : undefined,
    error: outcome.status === "failure" ? outcome.error : undefined,
    capabilities: context.capabilities,
    metadata: context.metadata,
  }
}

function delayFor(
  options: RetryOptions,
  context: ExecutionContext,
  attempt: number,
  outcome: Outcome<unknown>,
): number {
  const configured = options.delay
  const delay =
    typeof configured === "function"
      ? configured(attempt, retryContext(context, outcome))
      : (configured ?? 0)
  if (!Number.isFinite(delay) || delay < 0) {
    throw new RangeError("retry delay must be a finite non-negative number")
  }
  if (delay > MAX_TIMER_MS) {
    throw new RangeError(
      `retry delay must not exceed ${MAX_TIMER_MS} ms, the largest delay setTimeout honours`,
    )
  }

  return delay
}

function wait(delayMs: number, signal: AbortSignal | undefined): Promise<void> {
  if (delayMs === 0) {
    throwIfAborted(signal)
    return Promise.resolve()
  }

  return new Promise((resolve, reject) => {
    const timer = setTimeout(done, delayMs)

    function done(): void {
      signal?.removeEventListener("abort", aborted)
      resolve()
    }

    function aborted(): void {
      clearTimeout(timer)
      reject(
        signal?.reason ??
          new DOMException("The operation was aborted", "AbortError"),
      )
    }

    if (signal?.aborted) {
      aborted()
      return
    }

    signal?.addEventListener("abort", aborted, { once: true })
  })
}

function summarized<Result>(outcome: Outcome<Result>): Outcome<undefined> {
  return outcome.status === "success"
    ? { status: "success", value: undefined }
    : { status: "failure", error: outcome.error }
}

function classification<Result>(
  context: ExecutionContext,
  outcome: Outcome<Result>,
): Classification {
  return context.classify(outcome)
}

/** Retries adapter-classified failures within a single logical invocation. */
export function retry(options: RetryOptions): Policy {
  if (!Number.isInteger(options.maxAttempts) || options.maxAttempts < 1) {
    throw new RangeError("maxAttempts must be a positive integer")
  }
  if (
    typeof options.delay === "number" &&
    (!Number.isFinite(options.delay) ||
      options.delay < 0 ||
      options.delay > MAX_TIMER_MS)
  ) {
    throw new RangeError(
      `retry delay must be a finite number within 0..${MAX_TIMER_MS} ms`,
    )
  }

  return Object.freeze({
    name: "retry",
    async execute<Result>(
      initialContext: ExecutionContext,
      next: Next<Result>,
    ): Promise<Result> {
      let context = initialContext

      for (;;) {
        throwIfAborted(admissionSignal(context))

        try {
          const value = await next(context)
          const outcome: Outcome<Result> = { status: "success", value }
          const outcomeClassification = classification(context, outcome)
          if (
            outcomeClassification !== "retryable" ||
            context.capabilities.replay !== "safe"
          ) {
            return value
          }

          if (context.attempt >= options.maxAttempts) {
            emitRuntimeEvent(context, {
              type: "retry.exhausted",
              outcome: summarized(outcome),
              classification: outcomeClassification,
            })
            return value
          }

          const delayMs = delayFor(options, context, context.attempt, outcome)
          emitRuntimeEvent(context, {
            type: "retry.scheduled",
            nextAttempt: context.attempt + 1,
            delayMs,
            outcome: summarized(outcome),
            classification: outcomeClassification,
          })
          await wait(delayMs, admissionSignal(context))
          context = nextAttempt(context)
        } catch (error) {
          const outcome: Outcome<Result> = { status: "failure", error }
          const outcomeClassification = classification(context, outcome)

          if (
            admissionSignal(context)?.aborted ||
            context.capabilities.replay !== "safe" ||
            outcomeClassification !== "retryable"
          ) {
            throw error
          }

          if (context.attempt >= options.maxAttempts) {
            emitRuntimeEvent(context, {
              type: "retry.exhausted",
              outcome: summarized(outcome),
              classification: outcomeClassification,
            })
            throw error
          }

          const delayMs = delayFor(options, context, context.attempt, outcome)
          emitRuntimeEvent(context, {
            type: "retry.scheduled",
            nextAttempt: context.attempt + 1,
            delayMs,
            outcome: summarized(outcome),
            classification: outcomeClassification,
          })
          await wait(delayMs, admissionSignal(context))
          context = nextAttempt(context)
        }
      }
    },
  })
}
