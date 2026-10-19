import type {
  Adapter,
  Classification,
  ExecutionContext,
  OperationCapabilities,
  Outcome,
} from "../../core/types.js"

export type AwsSdkReplay<Args> =
  | OperationCapabilities["replay"]
  | ((args: Args) => OperationCapabilities["replay"])

/**
 * Keeps `Command` inferred from the command factory rather than from the
 * client's broader command union. Local instead of the built-in `NoInfer` so the
 * published types do not require consumers on TypeScript 5.4+.
 */
type NoInferValue<T> = [T][T extends unknown ? 0 : never]

/**
 * The command's output type, read structurally from the Smithy command class
 * without importing the SDK. A test fake that does not expose the middleware
 * shape falls back to `unknown`.
 */
type AwsSdkCommandOutput<Command> = Command extends {
  resolveMiddleware(
    ...args: never[]
  ): (...args: never[]) => Promise<{ output: infer Output }>
}
  ? Output
  : unknown

/**
 * The shape the adapter needs from an SDK v3 client. Only `send` is used, so any
 * service client satisfies it without this package importing the SDK.
 */
export interface AwsSdkClient<Command, Result> {
  send(
    command: Command,
    options?: { readonly abortSignal?: AbortSignal },
  ): Promise<Result>
}

export interface AwsSdkAdapterOptions<Args, Command> {
  readonly client: AwsSdkClient<
    NoInferValue<Command>,
    AwsSdkCommandOutput<Command>
  >
  /**
   * Builds a command. Called during capability resolution when `replay` is not
   * fixed, and again for each attempt in `execute()`; keep it pure and cheap.
   */
  readonly command: (args: Args) => Command
  /** Overrides the per-command default. */
  readonly replay?: AwsSdkReplay<Args>
  /** Adds service-specific classification to the shared default. */
  readonly classifyError?: (error: unknown) => Classification
  /** Extra cleanup after the default stream disposal. */
  readonly dispose?: (
    outcome: Outcome<AwsSdkCommandOutput<Command>>,
    context: ExecutionContext,
  ) => void | Promise<void>
}

const REPLAY_BY_COMMAND: Readonly<
  Record<string, OperationCapabilities["replay"]>
> = Object.freeze({
  BatchGetItem: "safe",
  DescribeTable: "safe",
  GetItem: "safe",
  GetObject: "safe",
  GetQueueAttributes: "safe",
  GetQueueUrl: "safe",
  HeadBucket: "safe",
  HeadObject: "safe",
  ListBuckets: "safe",
  ListObjectsV2: "safe",
  ListTables: "safe",
  Query: "safe",
  Scan: "safe",

  Invoke: "unsafe",
  InvokeAsync: "unsafe",
  Publish: "unsafe",
  PublishBatch: "unsafe",
  PutEvents: "unsafe",
  PutRecord: "unsafe",
  PutRecords: "unsafe",
  SendEmail: "unsafe",
  SendMessage: "unsafe",
  SendMessageBatch: "unsafe",
  SendRawEmail: "unsafe",
  SendTemplatedEmail: "unsafe",
  StartExecution: "unsafe",
  StartSyncExecution: "unsafe",
  StartWorkflowExecution: "unsafe",
})

const RETRYABLE_NAMES = new Set([
  "InternalFailure",
  "InternalServerError",
  "ProvisionedThroughputExceededException",
  "RequestLimitExceeded",
  "RequestThrottled",
  "RequestThrottledException",
  "RequestTimeout",
  "RequestTimeoutException",
  "ServiceUnavailable",
  "ServiceUnavailableException",
  "SlowDown",
  "ThrottledException",
  "Throttling",
  "ThrottlingException",
  "TimeoutError",
  "TooManyRequestsException",
])

const PERMANENT_NAMES = new Set([
  "AccessDeniedException",
  "ConditionalCheckFailedException",
  "EntityAlreadyExistsException",
  "InvalidParameterValue",
  "MissingParameter",
  "NoSuchBucket",
  "NoSuchKey",
  "ResourceNotFoundException",
  "SerializationException",
  "UnauthorizedOperation",
  "ValidationException",
])

const RETRYABLE_CODES = new Set([
  "EAI_AGAIN",
  "ECONNRESET",
  "EHOSTUNREACH",
  "ENETUNREACH",
  "EPIPE",
  "ETIMEDOUT",
])

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null
}

function commandName(command: unknown): string | undefined {
  const ctor = (command as { constructor?: { name?: unknown } } | null)
    ?.constructor
  return typeof ctor?.name === "string" && ctor.name.length > 0
    ? ctor.name
    : undefined
}

function errorName(error: unknown): string | undefined {
  if (!isRecord(error)) return undefined
  if (typeof error.name === "string" && error.name.length > 0) return error.name
  if (typeof error.Code === "string" && error.Code.length > 0) return error.Code
  return undefined
}

function hasRetryableMarker(error: unknown): boolean {
  return isRecord(error) && isRecord(error.$retryable)
}

function httpStatusCode(error: unknown): number | undefined {
  if (!isRecord(error)) return undefined
  const metadata = error.$metadata
  if (isRecord(metadata) && typeof metadata.httpStatusCode === "number") {
    return metadata.httpStatusCode
  }
  const response = error.$response
  if (isRecord(response) && typeof response.statusCode === "number") {
    return response.statusCode
  }
  return undefined
}

function fault(error: unknown): string | undefined {
  return isRecord(error) && typeof error.$fault === "string"
    ? error.$fault
    : undefined
}

/**
 * The shared SDK v3 classifier: `$retryable` first, then the modeled error name,
 * then `$fault` and HTTP status. `$metadata.httpStatusCode` is a fallback
 * because a 400 can be throttling or validation depending on the service.
 */
export function awsSdkErrorClassification(error: unknown): Classification {
  const name = errorName(error)
  if (name === "AbortError") return "failure"
  if (hasRetryableMarker(error)) return "retryable"
  if (name !== undefined) {
    if (RETRYABLE_NAMES.has(name)) return "retryable"
    if (PERMANENT_NAMES.has(name)) return "failure"
  }

  const status = httpStatusCode(error)
  if (status !== undefined) {
    if (status === 408 || status === 429 || status >= 500) return "retryable"
    if (status >= 400) return "failure"
  }

  const faultValue = fault(error)
  if (faultValue === "server") return "retryable"
  if (faultValue === "client") return "failure"
  if (
    isRecord(error) &&
    typeof error.code === "string" &&
    RETRYABLE_CODES.has(error.code)
  ) {
    return "retryable"
  }
  return "failure"
}

function replayFor<Args>(
  args: Args,
  configured: AwsSdkReplay<Args> | undefined,
  command: unknown,
): OperationCapabilities["replay"] {
  if (typeof configured === "function") return configured(args)
  if (configured !== undefined) return configured
  const name = commandName(command)
  if (name === undefined) return "unknown"
  const base = name.endsWith("Command")
    ? name.slice(0, -"Command".length)
    : name
  return REPLAY_BY_COMMAND[base] ?? "unknown"
}

function disposeStreams(value: unknown): void {
  if (!isRecord(value)) return
  for (const key of ["Body", "Payload"] as const) {
    const stream = value[key]
    if (!isRecord(stream)) continue
    try {
      if (typeof stream.destroy === "function") {
        ;(stream.destroy as () => void).call(stream)
      } else if (typeof stream.cancel === "function") {
        void Promise.resolve(
          (stream.cancel as () => unknown).call(stream),
        ).catch(() => {})
      }
    } catch {
      // Disposal is best-effort; the runtime isolates a throwing dispose.
    }
  }
}

/**
 * Adapter for AWS SDK v3 clients. Capabilities are per command: reads are
 * replay-safe, non-idempotent sends are not, and everything else stays
 * `unknown` until the application declares otherwise.
 */
export function awsSdkAdapter<Args, Command>(
  options: AwsSdkAdapterOptions<Args, Command>,
): Adapter<Args, AwsSdkCommandOutput<Command>> {
  return Object.freeze({
    capabilities(args: Args): OperationCapabilities {
      const configured = options.replay
      if (configured !== undefined && typeof configured !== "function") {
        return { abort: "supported", replay: configured }
      }
      return {
        abort: "supported",
        replay: replayFor(args, configured, options.command(args)),
      }
    },
    execute(
      args: Args,
      context: ExecutionContext,
    ): Promise<AwsSdkCommandOutput<Command>> {
      const command = options.command(args)
      return options.client.send(
        command,
        context.signal === undefined
          ? undefined
          : { abortSignal: context.signal },
      )
    },
    classify(outcome: Outcome<AwsSdkCommandOutput<Command>>): Classification {
      if (outcome.status === "success") return "success"
      return (
        options.classifyError?.(outcome.error) ??
        awsSdkErrorClassification(outcome.error)
      )
    },
    dispose(
      outcome: Outcome<AwsSdkCommandOutput<Command>>,
      context: ExecutionContext,
    ): void | Promise<void> {
      if (outcome.status === "success") disposeStreams(outcome.value)
      return options.dispose?.(outcome, context)
    },
  })
}
