import type { RetryContext } from "../../core/retry.js"

/**
 * Opt-in retry pacing for `@gkoos/caracal/fetch` that honours the HTTP
 * `Retry-After` header. It is protocol-specific, so it lives with the
 * adapter rather than in the protocol-agnostic core.
 */

const DEFAULTS = {
  baseMs: 100,
  factor: 2,
  maxDelayMs: 30_000,
  jitterRatio: 0.1,
} as const

const deltaSecondsPattern = /^\d+$/

/**
 * Structurally extracts a `Headers`-like object. `instanceof Response` is
 * unreliable across realms and custom fetch implementations.
 */
function headersOf(value: unknown): Headers | undefined {
  if (typeof value !== "object" || value === null) return undefined
  const headers = (value as { headers?: unknown }).headers
  if (typeof headers !== "object" || headers === null) return undefined
  const get = (headers as { get?: unknown }).get
  return typeof get === "function" ? (headers as Headers) : undefined
}

/**
 * Parses `Retry-After` (delta-seconds or HTTP-date) from a settled fetch
 * outcome. Reads the response from `context.result`, or from
 * `context.error` when a custom fetch implementation throws a
 * response-bearing error.
 *
 * Returns `undefined` when no usable header is present. Malformed values
 * are ignored rather than thrown.
 */
export function retryAfterMs(
  context: RetryContext,
  now: number = Date.now(),
): number | undefined {
  const headers = headersOf(context.result) ?? headersOf(context.error)
  if (headers === undefined) return undefined

  const value = headers.get("retry-after")?.trim()
  if (!value) return undefined

  if (deltaSecondsPattern.test(value)) {
    const seconds = Number(value)
    return Number.isSafeInteger(seconds) ? seconds * 1000 : undefined
  }

  const timestamp = Date.parse(value)
  return Number.isNaN(timestamp) ? undefined : Math.max(0, timestamp - now)
}

/** Options for `createRetryAfterDelay`. */
export interface RetryAfterDelayOptions {
  /** Base of the exponential backoff in ms. Default: 100. */
  readonly baseMs?: number
  /** Exponential growth factor (>= 1). Default: 2. */
  readonly factor?: number
  /** Ceiling applied to the deterministic wait in ms. Default: 30 000. */
  readonly maxDelayMs?: number
  /** Additive jitter as a fraction of the wait, within [0, 1]. Default: 0.1. */
  readonly jitterRatio?: number
}

/** A `RetryDelay` that reads `Retry-After` from the settled outcome. */
export type RetryAfterDelay = (attempt: number, context: RetryContext) => number

/**
 * Builds a `RetryDelay` that waits for the longer of exponential backoff
 * and the `Retry-After` the server sent, then adds additive jitter.
 *
 * Jitter only ever lengthens the wait, so a server-provided minimum is
 * never retried early.
 */
export function createRetryAfterDelay(
  options: RetryAfterDelayOptions = {},
): RetryAfterDelay {
  const baseMs = options.baseMs ?? DEFAULTS.baseMs
  const factor = options.factor ?? DEFAULTS.factor
  const maxDelayMs = options.maxDelayMs ?? DEFAULTS.maxDelayMs
  const jitterRatio = options.jitterRatio ?? DEFAULTS.jitterRatio

  if (!Number.isFinite(baseMs) || baseMs < 0)
    throw new RangeError("retryAfterDelay baseMs must be finite and >= 0")
  if (!Number.isFinite(factor) || factor < 1)
    throw new RangeError("retryAfterDelay factor must be finite and >= 1")
  if (!Number.isFinite(maxDelayMs) || maxDelayMs < 0)
    throw new RangeError("retryAfterDelay maxDelayMs must be finite and >= 0")
  if (!Number.isFinite(jitterRatio) || jitterRatio < 0 || jitterRatio > 1)
    throw new RangeError("retryAfterDelay jitterRatio must be within [0, 1]")

  return (attempt, context) => {
    const backoff = Math.min(baseMs * factor ** (attempt - 1), maxDelayMs)
    const base = Math.min(
      Math.max(backoff, retryAfterMs(context) ?? 0),
      maxDelayMs,
    )
    return base + Math.random() * base * jitterRatio
  }
}

/**
 * Ready-to-use default: `retry({ maxAttempts: 3, delay: retryAfterDelay })`.
 * Use `createRetryAfterDelay()` to change the pacing.
 */
export const retryAfterDelay: RetryAfterDelay = createRetryAfterDelay()
