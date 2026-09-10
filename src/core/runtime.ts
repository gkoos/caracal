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
export function admissionSignal(
  context: ExecutionContext,
): AbortSignal | undefined {
  const admission = admissionSignals.get(context)
  return admission && context.signal
    ? AbortSignal.any([admission, context.signal])
    : (admission ?? context.signal)
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

type EventWithoutRuntimeFields = OperationEvent extends infer Event
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

export function emitRuntimeEvent(
  context: ExecutionContext,
  event: EventWithoutRuntimeFields,
): void {
  const sinks = runtimeContext(context)[eventSinks] ?? []
  const fullEvent = { ...event, at: Date.now(), context } as OperationEvent

  for (const sink of sinks) {
    try {
      sink.emit(fullEvent)
    } catch {
      // Observability must not modify resilience execution.
    }
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
