import { randomUUID } from "node:crypto"

import {
  admissionSignal,
  createClassifier,
  createExecutionContext,
  emitToSink,
  type EventWithoutRuntimeFields,
} from "./runtime.js"
import type {
  Adapter,
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
  Policy,
} from "./types.js"

/** Frozen and shared: the summary is a constant, not a fresh object per event. */
const SUCCESS_OUTCOME: EventOutcome = Object.freeze({ status: "success" })
const FAILURE_OUTCOME: EventOutcome = Object.freeze({ status: "failure" })

const summarizeSuccess = (): EventOutcome => SUCCESS_OUTCOME
const summarizeFailure = (): EventOutcome => FAILURE_OUTCOME

function immutableCapabilities(
  capabilities: OperationCapabilities,
): OperationCapabilities {
  return Object.freeze({ ...capabilities })
}

function immutableMetadata(
  metadata: Readonly<Record<string, unknown>> | undefined,
): ExecutionMetadata {
  return Object.freeze({ ...(metadata ?? {}) })
}

function normalizeSinks(events: EventSinks | undefined): readonly EventSink[] {
  if (events === undefined) {
    return []
  }

  // Copied, never adopted: the array is frozen below, and freezing the caller's
  // own array as a side effect of constructing an operation is not ours to do.
  return "emit" in events ? [events] : [...events]
}

/**
 * Delivers one event, building it only when a sink will actually receive it.
 *
 * An operation with no sinks therefore does no per-event work at all: no event
 * object, no clock read, no iteration. `test/unit/no-sink-fast-path.test.ts`
 * pins that, and `scripts/bench-gate.mjs` measures the allocations it avoids.
 */
function emit(
  sinks: readonly EventSink[],
  context: ExecutionContext,
  event: EventWithoutRuntimeFields,
): void {
  if (sinks.length === 0) return

  const fullEvent = { ...event, at: Date.now(), context } as OperationEvent
  for (const sink of sinks) {
    emitToSink(sink, fullEvent)
  }
}

function validateName(name: string, kind: "operation" | "policy"): void {
  if (name.trim().length === 0) {
    throw new Error(`${kind} name must not be empty`)
  }
}

function createPipeline<Result>(
  policies: readonly Policy[],
  adapter: Next<Result>,
): Next<Result> {
  return policies.reduceRight<Next<Result>>(
    (next, policy) => async (context) => policy.execute(context, next),
    adapter,
  )
}

function invokeAdapter<Args, Result>(
  adapter: Adapter<Args, Result>,
  args: Args,
  sinks: readonly EventSink[],
): Next<Result> {
  return async (context) => {
    admissionSignal(context)?.throwIfAborted()
    emit(sinks, context, { type: "attempt.started" })

    try {
      const value = await adapter.execute(args, context)
      if (sinks.length > 0) {
        // Only classified to fill the event: with no sink the verdict would be
        // computed for nobody, and the classifier contract has to stay pure for
        // that call to be free.
        const outcome: Outcome<Result> = { status: "success", value }
        emit(sinks, context, {
          type: "attempt.settled",
          outcome: summarizeSuccess(),
          classification: context.classify(outcome),
        })
      }
      return value
    } catch (error) {
      if (sinks.length > 0) {
        const outcome: Outcome<Result> = { status: "failure", error }
        emit(sinks, context, {
          type: "attempt.settled",
          outcome: summarizeFailure(),
          classification: context.classify(outcome),
        })
      }
      throw error
    }
  }
}

/** Creates a named, protocol-agnostic operation. */
export function operation<Args, Result>(
  options: OperationOptions<Args, Result>,
): Operation<Args, Result> {
  validateName(options.name, "operation")
  for (const policy of options.policies ?? []) {
    validateName(policy.name, "policy")
  }

  const policies = Object.freeze([...(options.policies ?? [])])
  const sinks = Object.freeze(normalizeSinks(options.events))
  // The partition is constant: `policies` is frozen here, so the two filters run
  // once per operation rather than once per execution. The chains themselves
  // still have to be built per call, because the terminal adapter step closes
  // over the invocation's own `args`.
  const attemptPolicies = policies.filter(
    (policy) => policy.phase === "attempt",
  )
  const outerPolicies = policies.filter((policy) => policy.phase !== "attempt")

  return Object.freeze({
    name: options.name,
    async execute(
      args: Args,
      executeOptions: OperationExecuteOptions = {},
    ): Promise<Result> {
      const capabilities = options.adapter.capabilities(args)
      const context = createExecutionContext(
        {
          operationName: options.name,
          executionId: executeOptions.executionId ?? randomUUID(),
          signal: executeOptions.signal,
          metadata: immutableMetadata(executeOptions.metadata),
          capabilities: immutableCapabilities(capabilities),
          classify: createClassifier(options.adapter.classify),
        },
        sinks,
      )
      const adapter = createPipeline(
        attemptPolicies,
        invokeAdapter(options.adapter, args, sinks),
      )
      const pipeline = createPipeline(outerPolicies, adapter)

      emit(sinks, context, { type: "execution.started" })
      try {
        const value = await pipeline(context)
        emit(sinks, context, {
          type: "execution.settled",
          outcome: summarizeSuccess(),
        })
        return value
      } catch (error) {
        emit(sinks, context, {
          type: "execution.settled",
          outcome: summarizeFailure(),
        })
        throw error
      }
    },
  })
}
