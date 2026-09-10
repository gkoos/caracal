/** Traits that may vary for each invocation of an adapter. */
export type OperationCapabilities = Readonly<{
  abort: "supported" | "unsupported"
  replay: "safe" | "unsafe" | "unknown"
}>

export type Outcome<Result> =
  | Readonly<{ status: "success"; value: Result }>
  | Readonly<{ status: "failure"; error: unknown }>

/** An adapter's interpretation of an outcome for resilience policies. */
/** `retryable` is a failure eligible for the local retry policy. */
export type Classification = "success" | "failure" | "retryable" | "ignored"

export type OutcomeClassifier = (outcome: Outcome<unknown>) => Classification

export interface Adapter<Args, Result> {
  execute(args: Args, context: ExecutionContext): Promise<Result>
  capabilities(args: Args): OperationCapabilities
  classify?(outcome: Outcome<Result>): Classification
}

export type ExecutionMetadata = Readonly<Record<string, unknown>>

/**
 * Immutable state for one attempt. Retry derives later attempt contexts from
 * this value; an operation starts at attempt 1.
 */
export interface ExecutionContext {
  readonly operationName: string
  readonly executionId: string
  readonly attempt: number
  readonly signal: AbortSignal | undefined
  readonly metadata: ExecutionMetadata
  readonly capabilities: OperationCapabilities
  readonly classify: OutcomeClassifier
}

// ---------------------------------------------------------------------------
// Circuit-breaker event types
// ---------------------------------------------------------------------------

export type BreakerStateChangedEvent = Readonly<{
  type: "breaker.state-changed"
  at: number
  context: ExecutionContext
  coordination: "local" | "distributed"
  policyName: string
  scope: string
  state: "open" | "half-open" | "closed"
  previousState: "closed" | "open" | "half-open"
  /** Present on distributed events; absent on local events. */
  generation?: number
}>

export type BreakerRejectedEvent = Readonly<{
  type: "breaker.rejected"
  at: number
  context: ExecutionContext
  coordination: "local" | "distributed"
  policyName: string
  scope: string
  state: "open" | "half-open"
  generation?: number
}>

export type BreakerObservationEvent = Readonly<{
  type: "breaker.observation"
  at: number
  context: ExecutionContext
  coordination: "local" | "distributed"
  policyName: string
  scope: string
  outcome: "success" | "failure"
  generation?: number
}>

export type BreakerProbeStartedEvent = Readonly<{
  type: "breaker.probe-started"
  at: number
  context: ExecutionContext
  coordination: "local" | "distributed"
  policyName: string
  scope: string
  generation?: number
}>

export type BreakerObservationStaleEvent = Readonly<{
  type: "breaker.observation-stale"
  at: number
  context: ExecutionContext
  coordination: "distributed"
  policyName: string
  scope: string
  /** Generation of the attempt that was dropped. */
  attemptGeneration: number
  /** Current generation in the coordinator at the time the stale result arrived. */
  currentGeneration: number
}>

export type BreakerCoordinatorErrorEvent = Readonly<{
  type: "breaker.coordinator-error"
  at: number
  context: ExecutionContext
  coordination: "distributed"
  policyName: string
  scope: string
  operation: "admit" | "observe" | "settle-probe"
  error: unknown
}>

export type BreakerDegradedEvent = Readonly<{
  type: "breaker.degraded"
  at: number
  context: ExecutionContext
  coordination: "distributed"
  policyName: string
  scope: string
  reason: "coordinator-unavailable"
  behavior: "fail-open" | "fail-closed"
}>

// ---------------------------------------------------------------------------
// Unified operation event union
// ---------------------------------------------------------------------------

export type OperationEvent =
  | BreakerStateChangedEvent
  | BreakerRejectedEvent
  | BreakerObservationEvent
  | BreakerProbeStartedEvent
  | BreakerObservationStaleEvent
  | BreakerCoordinatorErrorEvent
  | BreakerDegradedEvent
  | Readonly<{
      type:
        | "bulkhead.admitted"
        | "bulkhead.rejected"
        | "bulkhead.waited"
        | "bulkhead.released"
        | "bulkhead.lease-lost"
        | "bulkhead.degraded"
      at: number
      context: ExecutionContext
      coordination: "local" | "distributed"
      policyName: string
      scope: string
      occupancy?: number
      reason?: string
    }>
  | Readonly<{
      type: "execution.started"
      at: number
      context: ExecutionContext
    }>
  | Readonly<{ type: "attempt.started"; at: number; context: ExecutionContext }>
  | Readonly<{
      type: "attempt.settled"
      at: number
      context: ExecutionContext
      outcome: Outcome<undefined>
      classification: Classification
    }>
  | Readonly<{
      type: "execution.settled"
      at: number
      context: ExecutionContext
      outcome: Outcome<undefined>
    }>
  | Readonly<{
      type: "timeout.triggered"
      at: number
      context: ExecutionContext
      timeoutMs: number
      abortRequested: boolean
    }>
  | Readonly<{
      type: "retry.scheduled"
      at: number
      context: ExecutionContext
      nextAttempt: number
      delayMs: number
      outcome: Outcome<undefined>
      classification: Classification
    }>
  | Readonly<{
      type: "retry.exhausted"
      at: number
      context: ExecutionContext
      outcome: Outcome<undefined>
      classification: Classification
    }>

/** Output-only observability contract. Sinks cannot alter policy execution. */
export interface EventSink {
  emit(event: OperationEvent): void
}

export type EventSinks = EventSink | readonly EventSink[]

export type OperationExecuteOptions = Readonly<{
  signal?: AbortSignal
  metadata?: Readonly<Record<string, unknown>>
  executionId?: string
}>

export type Next<Result> = (context: ExecutionContext) => Promise<Result>

/** A policy wraps execution; it is not a generic lifecycle hook system. */
export interface Policy {
  /** Attempt-phase policies wrap each adapter call and must await underlying settlement. */
  readonly phase?: "attempt"
  readonly name: string
  execute<Result>(
    context: ExecutionContext,
    next: Next<Result>,
  ): Promise<Result>
}

export interface Operation<Args, Result> {
  readonly name: string
  execute(args: Args, options?: OperationExecuteOptions): Promise<Result>
}

export interface OperationOptions<Args, Result> {
  readonly name: string
  readonly adapter: Adapter<Args, Result>
  readonly policies?: readonly Policy[]
  readonly events?: EventSinks
}
