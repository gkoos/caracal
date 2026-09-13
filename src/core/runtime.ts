import type {
  Classification,
  EventSink,
  ExecutionContext,
  OperationEvent,
  Outcome,
  OutcomeClassifier,
} from "./types.js"

const eventSinks = Symbol("caracal.eventSinks")
const admissionSignals = new WeakMap<ExecutionContext, AbortSignal>()
/**
 * Composite admission signals, derived once per context.
 *
 * The inputs are immutable - the context is frozen, its `signal` is fixed, and
 * its admission signal is set when the context is created - and this is read five
 * to eight times per attempt by the operation, retry and the bulkheads. Building
 * it per read also meant `admissionSignal(ctx) !== admissionSignal(ctx)`, which
 * would leak any future `removeEventListener` against the value.
 */
const compositeSignals = new WeakMap<ExecutionContext, AbortSignal>()

export function admissionSignal(
  context: ExecutionContext,
): AbortSignal | undefined {
  const admission = admissionSignals.get(context)
  if (!admission) return context.signal
  if (!context.signal) return admission

  const cached = compositeSignals.get(context)
  if (cached) return cached

  const composite = AbortSignal.any([admission, context.signal])
  compositeSignals.set(context, composite)
  return composite
}
export function withAdmissionSignal(
  context: ExecutionContext,
  signal: AbortSignal,
): ExecutionContext {
  const derived = attachRuntime(
    { ...context },
    runtimeContext(context)[eventSinks] ?? [],
  )
  const previous = admissionSignal(context)
  admissionSignals.set(
    derived,
    previous ? AbortSignal.any([previous, signal]) : signal,
  )
  return derived
}
function inheritAdmission(
  source: ExecutionContext,
  target: ExecutionContext,
): ExecutionContext {
  const signal = admissionSignals.get(source)
  if (signal) admissionSignals.set(target, signal)
  return target
}

export type EventWithoutRuntimeFields = OperationEvent extends infer Event
  ? Event extends OperationEvent
    ? Omit<Event, "at" | "context">
    : never
  : never

type RuntimeExecutionContext = ExecutionContext & {
  readonly [eventSinks]: readonly EventSink[]
}

function runtimeContext(context: ExecutionContext): RuntimeExecutionContext {
  return context as RuntimeExecutionContext
}

function attachRuntime(
  values: ExecutionContext,
  sinks: readonly EventSink[],
): ExecutionContext {
  const context = values as RuntimeExecutionContext
  Object.defineProperty(context, eventSinks, { value: sinks })
  return Object.freeze(context)
}

export function createExecutionContext(
  values: Omit<ExecutionContext, "attempt">,
  sinks: readonly EventSink[],
): ExecutionContext {
  return attachRuntime({ attempt: 1, ...values }, sinks)
}

export function nextAttempt(context: ExecutionContext): ExecutionContext {
  return inheritAdmission(
    context,
    attachRuntime(
      { ...context, attempt: context.attempt + 1 },
      runtimeContext(context)[eventSinks] ?? [],
    ),
  )
}

export function withSignal(
  context: ExecutionContext,
  signal: AbortSignal | undefined,
): ExecutionContext {
  return inheritAdmission(
    context,
    attachRuntime(
      { ...context, signal },
      runtimeContext(context)[eventSinks] ?? [],
    ),
  )
}

/**
 * Largest delay `setTimeout` honours.  Anything above it is silently clamped to
 * 1 ms by the platform, so accepting larger values would turn a long wait into
 * an immediate one.
 */
export const MAX_TIMER_MS = 2_147_483_647

/**
 * Delivers one event to one sink, isolating execution from it.  Sinks are
 * fire-and-forget: a synchronous throw and a rejected promise are both dropped,
 * so a failing sink can neither modify resilience execution nor surface as an
 * unhandled rejection.
 */
export function emitToSink(sink: EventSink, event: OperationEvent): void {
  try {
    const pending = sink.emit(event) as unknown
    const thenable = pending as { catch?: unknown } | null | undefined
    if (typeof thenable?.catch === "function") {
      void (pending as Promise<unknown>).catch(() => {})
    }
  } catch {
    // See above: a failing sink must never modify execution.
  }
}

export function emitRuntimeEvent(
  context: ExecutionContext,
  event: EventWithoutRuntimeFields,
): void {
  const sinks = runtimeContext(context)[eventSinks] ?? []
  // No sinks configured: skip building the event at all. This is the common case
  // for an operation with observability turned off, and building it would
  // allocate per event and call `Date.now()` on the hot path for nobody. The
  // gate in scripts/bench-gate.mjs measures allocations per attempt.
  if (sinks.length === 0) return

  const fullEvent = { ...event, at: Date.now(), context } as OperationEvent

  for (const sink of sinks) {
    emitToSink(sink, fullEvent)
  }
}

export function createClassifier<Result>(
  classify: ((outcome: Outcome<Result>) => Classification) | undefined,
): OutcomeClassifier {
  return (outcome) => {
    if (classify === undefined) {
      return outcome.status === "success" ? "success" : "failure"
    }

    return classify(outcome as Outcome<Result>)
  }
}
