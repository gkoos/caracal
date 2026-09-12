import { Cluster, Redis, type RedisOptions } from "ioredis"

export class CoordinatorUnavailableError extends Error {
  readonly coordination = "distributed"
  constructor(cause: unknown) {
    super("Redis coordination unavailable; command outcome may be unknown", {
      cause,
    })
    this.name = "CoordinatorUnavailableError"
  }
}

function attachErrorSink(emitter: {
  on(event: "error", listener: () => void): void
}): void {
  emitter.on("error", () => {
    /* Commands surface errors; never crash on an unhandled EventEmitter error. */
  })
}

/** Standalone Redis client. Caller owns it and must disconnect at shutdown. */
export function createCoordinationClient(
  url: string,
  commandTimeout = 1000,
): Redis {
  if (!Number.isSafeInteger(commandTimeout) || commandTimeout < 1)
    throw new RangeError("commandTimeout must be a positive integer")
  const client = new Redis(url, {
    lazyConnect: true,
    enableOfflineQueue: false,
    maxRetriesPerRequest: 0,
    autoResendUnfulfilledCommands: false,
    commandTimeout,
    connectTimeout: commandTimeout,
    retryStrategy: (attempt) => Math.min(attempt * 50, 1000),
  })
  attachErrorSink(client)
  return client
}

export interface ClusterNode {
  readonly host: string
  readonly port: number
}

/**
 * Redis Cluster client. Caller owns it and must disconnect at shutdown.
 *
 * All coordination keys use a hash-tag so that every key for a given
 * policy+operation+scope lands on the same cluster slot. Multi-key Lua
 * scripts (circuit breaker) are therefore cluster-safe without cross-slot
 * concerns.
 *
 * Call `connect()` before use and `disconnect()` at shutdown, matching the
 * standalone client contract.
 */
/**
 * Redis Cluster client for a secured cluster: pass credentials and TLS via
 * `connectionOptions`.  The coordination safeguards (`lazyConnect`, no offline
 * queue, no command replay, no per-request retries) are applied after them and
 * cannot be overridden, because the coordinators depend on those semantics.
 */
export function createCoordinationClusterClient(
  nodes: ReadonlyArray<ClusterNode>,
  commandTimeout = 1000,
  connectionOptions: RedisOptions = {},
): Cluster {
  if (!Number.isSafeInteger(commandTimeout) || commandTimeout < 1)
    throw new RangeError("commandTimeout must be a positive integer")
  if (!Array.isArray(nodes) || nodes.length === 0)
    throw new RangeError("nodes must be a non-empty array of { host, port }")
  if (
    typeof connectionOptions !== "object" ||
    connectionOptions === null ||
    Array.isArray(connectionOptions)
  )
    throw new TypeError("connectionOptions must be an ioredis options object")
  const cluster = new Cluster([...nodes], {
    lazyConnect: true,
    enableOfflineQueue: false,
    clusterRetryStrategy: (attempt) => Math.min(attempt * 50, 1000),
    redisOptions: {
      ...connectionOptions,
      commandTimeout,
      connectTimeout: commandTimeout,
      maxRetriesPerRequest: 0,
      autoResendUnfulfilledCommands: false,
    },
  })
  attachErrorSink(cluster)
  return cluster
}
