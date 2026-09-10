import type {
  Adapter,
  Classification,
  ExecutionContext,
  OperationCapabilities,
  Outcome,
} from "../../core/types.js"

export type FetchOperationArgs = Readonly<{
  url: RequestInfo | URL
  options?: RequestInit
}>

export type FetchReplay =
  | OperationCapabilities["replay"]
  | ((args: FetchOperationArgs) => OperationCapabilities["replay"])

export interface FetchAdapterOptions {
  readonly fetch?: typeof globalThis.fetch
  readonly replay?: FetchReplay
  readonly classifyResponse?: (response: Response) => Classification
  readonly classifyError?: (error: unknown) => Classification
}

function requestMethod(args: FetchOperationArgs): string {
  if (args.options?.method !== undefined) {
    return args.options.method.toUpperCase()
  }

  if (args.url instanceof Request) {
    return args.url.method.toUpperCase()
  }

  return "GET"
}

function defaultReplay(
  args: FetchOperationArgs,
): OperationCapabilities["replay"] {
  const method = requestMethod(args)
  if (method === "GET" || method === "HEAD") {
    return "safe"
  }

  if (method === "POST" || method === "PATCH") {
    return "unsafe"
  }

  return "unknown"
}

function defaultResponseClassification(response: Response): Classification {
  return response.status >= 500 ||
    response.status === 408 ||
    response.status === 429
    ? "retryable"
    : "success"
}

function urlSignal(url: RequestInfo | URL): AbortSignal | undefined {
  return url instanceof Request ? url.signal : undefined
}

function combinedSignal(
  first: AbortSignal | undefined,
  second: AbortSignal | undefined,
  third: AbortSignal | undefined,
): AbortSignal | undefined {
  const signals = [first, second, third].filter(
    (signal): signal is AbortSignal => signal !== undefined,
  )
  if (signals.length === 0) {
    return undefined
  }

  return signals.length === 1 ? signals[0] : AbortSignal.any(signals)
}

/**
 * Creates a fetch adapter with conservative per-request replay traits.
 * It performs no implicit retry or timeout; those remain operation policies.
 */
export function fetchAdapter(
  options: FetchAdapterOptions = {},
): Adapter<FetchOperationArgs, Response> {
  const fetchImplementation = options.fetch ?? globalThis.fetch
  if (fetchImplementation === undefined) {
    throw new Error("fetch is not available; provide FetchAdapterOptions.fetch")
  }

  return Object.freeze({
    capabilities(args: FetchOperationArgs): OperationCapabilities {
      const configured = options.replay
      const replay =
        typeof configured === "function"
          ? configured(args)
          : (configured ?? defaultReplay(args))
      return { abort: "supported", replay }
    },
    async execute(
      args: FetchOperationArgs,
      context: ExecutionContext,
    ): Promise<Response> {
      const signal = combinedSignal(
        urlSignal(args.url),
        args.options?.signal ?? undefined,
        context.signal,
      )
      return fetchImplementation(args.url, { ...args.options, signal })
    },
    classify(outcome: Outcome<Response>): Classification {
      if (outcome.status === "failure") {
        return options.classifyError?.(outcome.error) ?? "retryable"
      }

      return (
        options.classifyResponse?.(outcome.value) ??
        defaultResponseClassification(outcome.value)
      )
    },
  })
}
