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
 * `connectionOptions`.
 *
 * The coordination safeguards are applied after the caller's options so they
 * cannot be overridden: no command replay, no per-request retries, and bounded
 * command/connect timeouts.  Caller options that are not safeguards -
 * credentials, TLS - still pass through.
 *
 * One safeguard is not attainable for cluster node connections: ioredis 6 keeps
 * `enableOfflineQueue` at its own default (`true`) for them, whether it is set at
 * cluster level, in `redisOptions`, or passed in `connectionOptions`.  The
 * coordinators' fail-fast behaviour therefore rests on the pinned timeout and
 * retry options rather than on a disabled queue.  See redis.md.
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
  // Verified against a live cluster: the pool gives node connections this object,
  // and everything pinned here lands on them - except `enableOfflineQueue`, which
  // ioredis 6 keeps at its own default (`true`) for cluster nodes because it
  // treats the flag as cluster-level. Setting it here, or at cluster level, or
  // passing it in `connectionOptions`, all leave the nodes at `true`, so it is
  // deliberately not claimed as a safeguard. See redis.md.
  const nodeOptions: RedisOptions = {
    ...connectionOptions,
    commandTimeout,
    connectTimeout: commandTimeout,
    maxRetriesPerRequest: 0,
    autoResendUnfulfilledCommands: false,
  }
  const cluster = new Cluster([...nodes], {
    lazyConnect: true,
    enableOfflineQueue: false,
    clusterRetryStrategy: (attempt) => Math.min(attempt * 50, 1000),
    redisOptions: nodeOptions,
  })
  attachErrorSink(cluster)
  return cluster
}
