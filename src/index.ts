/** Caracal's protocol-agnostic runtime entry point. */

export type {
  BulkheadCoordinator,
  DistributedBulkheadOptions,
  LocalBulkheadOptions,
} from "./core/bulkhead.js"
export { BulkheadRejectedError, bulkhead } from "./core/bulkhead.js"
export type {
  AdmitProbeResult,
  BreakerClassifier,
  BreakerCoordinator,
  BreakerIdentity,
  BreakerOutcome,
  BreakerSnapshot,
  BreakerState,
  DistributedBreakerOptions,
  LocalBreakerOptions,
  ObserveResult,
  SettleProbeResult,
} from "./core/circuit-breaker.js"
export { CircuitOpenError, circuitBreaker } from "./core/circuit-breaker.js"
export type {
  Adapter,
  Classification,
  EventOutcome,
  EventSink,
  EventSinks,
  ExecutionContext,
  ExecutionMetadata,
  Next,
  Operation,
  OperationCapabilities,
  OperationEvent,
  OperationExecuteOptions,
  OperationOptions,
  Outcome,
  OutcomeClassifier,
  Policy,
  RetryContext,
  RetryDelay,
  RetryOptions,
  TimeoutOptions,
} from "./core/index.js"
export {
  operation,
  retry,
  TimeoutError,
  timeout,
} from "./core/index.js"
