import type { QueryResult } from "pg"

import type {
  Adapter,
  Classification,
  OperationCapabilities,
  Outcome,
} from "../../core/types.js"

export type PostgresReplay =
  | OperationCapabilities["replay"]
  | ((args: PostgresQueryArgs) => OperationCapabilities["replay"])

export type PostgresQueryArgs = Readonly<{
  sql: string
  values?: readonly unknown[]
  /** Application-declared per-query replay trait; never inferred from SQL. */
  replay?: OperationCapabilities["replay"]
}>

export interface PostgresQueryable {
  query(query: {
    text: string
    values?: readonly unknown[]
  }): Promise<QueryResult>
}

export interface PostgresAdapterOptions {
  readonly replay?: PostgresReplay
  readonly classifyError?: (error: unknown) => Classification
}

const retryableSqlStates = new Set([
  "40001", // serialization_failure
  "40P01", // deadlock_detected
  "55P03", // lock_not_available
  "53300", // too_many_connections
  "57P01", // admin_shutdown
  "57P02", // crash_shutdown
  "57P03", // cannot_connect_now
])

function sqlState(error: unknown): string | undefined {
  if (typeof error !== "object" || error === null || !("code" in error)) {
    return undefined
  }

  return typeof error.code === "string" ? error.code : undefined
}

function defaultErrorClassification(error: unknown): Classification {
  const state = sqlState(error)
  return state?.startsWith("08") === true ||
    (state !== undefined && retryableSqlStates.has(state))
    ? "retryable"
    : "failure"
}

function replayFor(
  args: PostgresQueryArgs,
  configured: PostgresReplay | undefined,
): OperationCapabilities["replay"] {
  if (args.replay !== undefined) {
    return args.replay
  }

  return typeof configured === "function"
    ? configured(args)
    : (configured ?? "unknown")
}

/**
 * Adapter for node-postgres (`pg`) 8.x query clients/pools. The standard
 * `query()` contract does not provide a portable AbortSignal cancellation path,
 * so this adapter accurately declares abort as unsupported.
 */
export function postgresAdapter(
  client: PostgresQueryable,
  options: PostgresAdapterOptions = {},
): Adapter<PostgresQueryArgs, QueryResult> {
  return Object.freeze({
    capabilities(args: PostgresQueryArgs): OperationCapabilities {
      return { abort: "unsupported", replay: replayFor(args, options.replay) }
    },
    execute(args: PostgresQueryArgs): Promise<QueryResult> {
      return client.query({
        text: args.sql,
        values: args.values === undefined ? undefined : [...args.values],
      })
    },
    classify(outcome: Outcome<QueryResult>): Classification {
      if (outcome.status === "success") {
        return "success"
      }

      return (
        options.classifyError?.(outcome.error) ??
        defaultErrorClassification(outcome.error)
      )
    },
  })
}
