import { MAX_TIMER_MS } from "../../src/core/runtime.js"
import type {
  Adapter,
  Classification,
  ExecutionContext,
  OperationCapabilities,
  Outcome,
} from "../../src/core/types.js"

/**
 * One scripted adapter behavior: a settlement, a settlement after a delay, or a
 * hang. `abort: "ignore"` makes this entry ignore `context.signal`, independently
 * of the capabilities the adapter declares. That is deliberate: it is how a test
 * can model an adapter that claims `abort: "supported"` and then does not honor
 * it, without teaching the runtime a special case.
 */
export type ScriptedStep<Result> =
  | Readonly<{
      status: "success"
      value: Result
      delayMs?: number
      abort?: "ignore"
    }>
  | Readonly<{
      status: "failure"
      error: unknown
      delayMs?: number
      abort?: "ignore"
    }>
  | Readonly<{
      status: "hang"
      abort?: "ignore"
    }>

/** One `execute()` invocation, recorded when it reserves a script entry. */
export interface ScriptedCall<Args> {
  /** The zero-based position in the supplied script. */
  readonly index: number
  readonly args: Args
  readonly context: ExecutionContext
}

export interface ScriptedAdapterOptions<Args, Result> {
  /**
   * Capabilities for every call, or a function evaluated per `capabilities()`
   * read. Reading capabilities never consumes a script entry.
   */
  readonly capabilities:
    | OperationCapabilities
    | ((args: Args) => OperationCapabilities)
  /** Optional classifier, invoked unchanged. Calling it never consumes an entry. */
  readonly classify?: (outcome: Outcome<Result>) => Classification
  /** The queue of behaviors, consumed in invocation order. */
  readonly outcomes: readonly ScriptedStep<Result>[]
  /** Optional cleanup hook for an outcome the runtime abandons. */
  readonly dispose?: (
    outcome: Outcome<Result>,
    context: ExecutionContext,
  ) => void | Promise<void>
}

export interface ScriptedAdapter<Args, Result> extends Adapter<Args, Result> {
  /**
   * A frozen snapshot of the invocations that reserved an entry, in reservation
   * order. Read it after the execution you are asserting on; a held reference
   * does not update. Exhausted calls are not recorded.
   */
  readonly calls: readonly ScriptedCall<Args>[]
}

function assertDelay(delayMs: unknown, index: number): void {
  if (delayMs === undefined) {
    return
  }
  if (
    typeof delayMs !== "number" ||
    !Number.isFinite(delayMs) ||
    !Number.isInteger(delayMs) ||
    delayMs < 0 ||
    delayMs > MAX_TIMER_MS
  ) {
    throw new RangeError(
      `scripted outcome ${index + 1}: delayMs must be an integer between 0 and ${MAX_TIMER_MS}, received ${String(delayMs)}`,
    )
  }
}

function assertStep<Result>(step: ScriptedStep<Result>, index: number): void {
  if (step === null || typeof step !== "object") {
    throw new TypeError(`scripted outcome ${index + 1} must be an object`)
  }
  if (
    step.status !== "success" &&
    step.status !== "failure" &&
    step.status !== "hang"
  ) {
    throw new TypeError(`scripted outcome ${index + 1} has an unknown status`)
  }
  if (step.abort !== undefined && step.abort !== "ignore") {
    throw new TypeError(
      `scripted outcome ${index + 1}: abort must be "ignore" when present`,
    )
  }
  if (step.status === "hang") {
    if ("delayMs" in step && step.delayMs !== undefined) {
      throw new TypeError(
        `scripted outcome ${index + 1}: a hang cannot have delayMs`,
      )
    }
    return
  }
  assertDelay(step.delayMs, index)
}

function copyCapabilities(
  capabilities: OperationCapabilities,
): OperationCapabilities {
  if (
    capabilities === null ||
    typeof capabilities !== "object" ||
    (capabilities.abort !== "supported" &&
      capabilities.abort !== "unsupported") ||
    (capabilities.replay !== "safe" &&
      capabilities.replay !== "unsafe" &&
      capabilities.replay !== "unknown")
  ) {
    throw new TypeError(
      "scripted adapter capabilities must declare a supported abort and a known replay",
    )
  }

  return Object.freeze({
    abort: capabilities.abort,
    replay: capabilities.replay,
  })
}

function abortReason(signal: AbortSignal | undefined): unknown {
  // Preserve the caller's reason exactly, including an explicit `null`.
  // `AbortSignal.throwIfAborted()` does the same; only a signal that leaves
  // `reason` undefined falls back to the platform default.
  if (signal === undefined || signal.reason === undefined) {
    return new DOMException("The operation was aborted", "AbortError")
  }
  return signal.reason
}

function runHang<Result>(
  signal: AbortSignal | undefined,
  ignoreAbort: boolean,
): Promise<Result> {
  if (ignoreAbort || signal === undefined) {
    return new Promise<Result>(() => {})
  }
  if (signal.aborted) {
    return Promise.reject(abortReason(signal))
  }

  return new Promise<Result>((_resolve, reject) => {
    let settled = false

    function onAbort(): void {
      if (settled) {
        return
      }
      settled = true
      signal?.removeEventListener("abort", onAbort)
      reject(abortReason(signal))
    }

    signal.addEventListener("abort", onAbort, { once: true })
    if (signal.aborted) {
      onAbort()
    }
  })
}

function runDelayed<Result>(
  step: Extract<ScriptedStep<Result>, { status: "success" | "failure" }>,
  delayMs: number,
  signal: AbortSignal | undefined,
  ignoreAbort: boolean,
): Promise<Result> {
  return new Promise<Result>((resolve, reject) => {
    let settled = false
    let timer: ReturnType<typeof setTimeout> | undefined

    function cleanup(): void {
      if (timer !== undefined) {
        clearTimeout(timer)
      }
      signal?.removeEventListener("abort", onAbort)
    }

    function settle(): void {
      if (settled) {
        return
      }
      settled = true
      cleanup()
      if (step.status === "success") {
        resolve(step.value)
      } else {
        reject(step.error)
      }
    }

    function onAbort(): void {
      if (settled) {
        return
      }
      settled = true
      cleanup()
      reject(abortReason(signal))
    }

    if (!ignoreAbort && signal?.aborted) {
      onAbort()
      return
    }

    timer = setTimeout(settle, delayMs)

    if (!ignoreAbort) {
      signal?.addEventListener("abort", onAbort, { once: true })
      // Re-check after registering: a signal that aborted between the check
      // above and the registration must still clear the timer.
      if (signal?.aborted) {
        onAbort()
      }
    }
  })
}

function runStep<Result>(
  step: ScriptedStep<Result>,
  signal: AbortSignal | undefined,
): Promise<Result> {
  const ignoreAbort = step.abort === "ignore"

  if (step.status === "hang") {
    return runHang(signal, ignoreAbort)
  }

  if (step.delayMs === undefined) {
    if (!ignoreAbort && signal?.aborted) {
      return Promise.reject(abortReason(signal))
    }
    return step.status === "success"
      ? Promise.resolve(step.value)
      : Promise.reject(step.error)
  }

  return runDelayed(step, step.delayMs, signal, ignoreAbort)
}

/**
 * Creates an adapter whose calls settle from a fixed script.
 *
 * Each `execute()` reserves one entry synchronously, before awaiting anything,
 * so concurrent calls consume entries in invocation order. The queue belongs to
 * this adapter instance and continues across retries and later operations;
 * create a fresh adapter for an independent scenario. Capability reads,
 * classification and disposal never consume entries, and an exhausted script
 * rejects rather than repeating its final entry.
 */
export function createScriptedAdapter<Args, Result>(
  options: ScriptedAdapterOptions<Args, Result>,
): ScriptedAdapter<Args, Result> {
  if (!Array.isArray(options.outcomes)) {
    throw new TypeError("scripted outcomes must be an array")
  }

  const script = Object.freeze(
    options.outcomes.map((step, index) => {
      assertStep(step, index)
      return Object.freeze({ ...step }) as ScriptedStep<Result>
    }),
  )

  const capabilitiesOption = options.capabilities
  const fixedCapabilities =
    typeof capabilitiesOption === "function"
      ? undefined
      : copyCapabilities(capabilitiesOption)
  const classifyOption = options.classify
  const disposeOption = options.dispose
  const history: ScriptedCall<Args>[] = []
  let nextIndex = 0

  const adapter: ScriptedAdapter<Args, Result> = {
    capabilities(args: Args): OperationCapabilities {
      if (typeof capabilitiesOption === "function") {
        return copyCapabilities(capabilitiesOption(args))
      }
      if (fixedCapabilities === undefined) {
        throw new TypeError("scripted adapter capabilities are missing")
      }
      return fixedCapabilities
    },

    execute(args: Args, context: ExecutionContext): Promise<Result> {
      const index = nextIndex
      if (index >= script.length) {
        return Promise.reject(
          new Error(
            `scripted adapter exhausted for operation "${context.operationName}": ` +
              `execute() call ${index + 1} has no scripted outcome ` +
              `(${script.length} configured)`,
          ),
        )
      }

      nextIndex += 1
      const step = script[index] as ScriptedStep<Result>
      history.push(Object.freeze({ index, args, context }))
      return runStep(step, context.signal)
    },

    get calls(): readonly ScriptedCall<Args>[] {
      return Object.freeze(history.map((call) => Object.freeze({ ...call })))
    },

    ...(classifyOption === undefined
      ? {}
      : {
          classify: (outcome: Outcome<Result>): Classification =>
            classifyOption(outcome),
        }),

    ...(disposeOption === undefined
      ? {}
      : {
          dispose: (
            outcome: Outcome<Result>,
            context: ExecutionContext,
          ): void | Promise<void> => disposeOption(outcome, context),
        }),
  }

  return Object.freeze(adapter)
}
