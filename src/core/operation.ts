import { randomUUID } from "node:crypto"

import {
  admissionSignal,
  createClassifier,
  createExecutionContext,
  emitToSink,
} from "./runtime.js"
import type {
  Adapter,
  EventSink,
  EventSinks,
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

const summarizeSuccess = (): Outcome<undefined> => ({
  status: "success",
  value: undefined,
})
const summarizeFailure = (error: unknown): Outcome<undefined> => ({
  status: "failure",
  error,
})

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

  return "emit" in events ? [events] : events
}

function emit(sinks: readonly EventSink[], event: OperationEvent): void {
  for (const sink of sinks) {
    emitToSink(sink, event)
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
    emit(sinks, { type: "attempt.started", at: Date.now(), context })

    try {
      const value = await adapter.execute(args, context)
      const outcome: Outcome<Result> = { status: "success", value }
      const classification = context.classify(outcome)
      emit(sinks, {
        type: "attempt.settled",
        at: Date.now(),
        context,
        outcome: summarizeSuccess(),
        classification,
      })
      return value
    } catch (error) {
      const outcome: Outcome<Result> = { status: "failure", error }
      const classification = context.classify(outcome)
      emit(sinks, {
        type: "attempt.settled",
        at: Date.now(),
        context,
        outcome: summarizeFailure(error),
        classification,
      })
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
        policies.filter((policy) => policy.phase === "attempt"),
        invokeAdapter(options.adapter, args, sinks),
      )
      const pipeline = createPipeline(
        policies.filter((policy) => policy.phase !== "attempt"),
        adapter,
      )

      emit(sinks, { type: "execution.started", at: Date.now(), context })
      try {
        const value = await pipeline(context)
        emit(sinks, {
          type: "execution.settled",
          at: Date.now(),
          context,
          outcome: summarizeSuccess(),
        })
        return value
      } catch (error) {
        emit(sinks, {
          type: "execution.settled",
          at: Date.now(),
          context,
          outcome: summarizeFailure(error),
        })
        throw error
      }
    },
  })
}
