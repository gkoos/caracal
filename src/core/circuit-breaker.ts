import { randomUUID } from "node:crypto"
import { emitRuntimeEvent } from "./runtime.js"
import { createScopeStateCache } from "./scope-state-cache.js"
import type { ExecutionContext, Next, Outcome, Policy } from "./types.js"

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

export type BreakerState = "closed" | "open" | "half-open"

/** Classification of an attempt outcome for circuit-breaker purposes. */
export type BreakerOutcome = "success" | "failure" | "ignored"

/**
 * Called after every attempt that the breaker observes.  Returning "ignored"
 * means the outcome neither counts toward nor clears failures.
 */
export type BreakerClassifier = (
  error: unknown,
  isSuccess: boolean,
) => BreakerOutcome

function classifyOutcome(
  context: ExecutionContext,
  classifier: BreakerClassifier | undefined,
  outcome: Outcome<unknown>,
): BreakerOutcome {
  if (classifier !== undefined) {
    return classifier(
      outcome.status === "failure" ? outcome.error : undefined,
      outcome.status === "success",
    )
  }

  const classification = context.classify(outcome)
  return classification === "retryable" ? "failure" : classification
}

export interface LocalBreakerOptions {
  /** Identifies this policy in events and introspection. */
  readonly name: string
  /**
   * Minimum number of observations in the window before the breaker may open.
   * Default: 5.
   */
  readonly minimumThroughput?: number
  /**
   * Fraction of failures (0–1 exclusive) that triggers opening.
   * Default: 0.5.
   */
  readonly failureThreshold?: number
  /**
   * How long (ms) the breaker stays open before entering half-open.
   * Default: 10 000.
   */
  readonly openMs?: number
  /**
   * Number of consecutive successes needed to close from half-open.
   * Default: 1.
   */
  readonly halfOpenSuccesses?: number
  /**
   * Maximum concurrent probes allowed in half-open state.
   * Default: 1.
   */
  readonly halfOpenProbes?: number
  /**
   * Sliding-window size (number of observations retained).
   * Default: 100.
   */
  readonly windowSize?: number
  /** Custom outcome classifier. Defaults to the adapter classification; retryable counts as failure. */
  readonly classify?: BreakerClassifier
}

export interface BreakerSnapshot {
  readonly coordination: "local"
  readonly state: BreakerState
  readonly failures: number
  readonly successes: number
  readonly observations: number
  /** Only meaningful in half-open; number of probes currently in-flight. */
  readonly probesInFlight: number
  /** Only meaningful in half-open; consecutive successes so far this epoch. */
  readonly halfOpenSuccesses: number
}

export class CircuitOpenError extends Error {
  constructor(
    readonly policyName: string,
    readonly coordination: "local" | "distributed",
    readonly scope: string,
  ) {
    super(`Circuit ${policyName} is open for scope "${scope}"`)
    this.name = "CircuitOpenError"
  }
}

// ---------------------------------------------------------------------------
// Defaults
// ---------------------------------------------------------------------------

const DEFAULT_MINIMUM_THROUGHPUT = 5
const DEFAULT_FAILURE_THRESHOLD = 0.5
const DEFAULT_OPEN_MS = 10_000
const DEFAULT_HALF_OPEN_SUCCESSES = 1
const DEFAULT_HALF_OPEN_PROBES = 1
const DEFAULT_WINDOW_SIZE = 100

/**
 * Upper bound on the window, which is both a memory bound and a cost bound:
 * `breakerObserveV1` scans every retained member to count the current epoch's
 * share of the window, inside a blocking script on a single-threaded server. The
 * lower bound alone allowed a window size that turns each observation into a
 * hundred-thousand-iteration Lua loop.
 */
const MAX_WINDOW_SIZE = 10_000

// The distributed coordinator compares the failure threshold as an integer
// numerator of thousandths (`wFail * 1000 >= numerator * wTotal`).  A threshold
// that rounds to 0 makes that comparison unconditionally true, so the breaker
// opens on a success-only window and re-opens after every recovery; a threshold
// that rounds to the full scale requires every observation to fail, so the
// breaker effectively never opens.  Both are rejected instead of silently
// reinterpreted, and the local breaker enforces the same bounds so one policy
// config works with either coordination.
const FAILURE_THRESHOLD_SCALE = 1000

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

/**
 * Rejects a failureThreshold the thousandths comparison cannot represent.
 *
 * Accepts `0.0005 <= failureThreshold < 0.9995`.  Anything else resolves to a
 * numerator of 0 or of the full scale, which changes what the threshold means
 * rather than how precisely it is expressed.
 */
function assertResolvableThreshold(failureThreshold: number): void {
  const numerator = Math.round(failureThreshold * FAILURE_THRESHOLD_SCALE)
  if (numerator < 1 || numerator >= FAILURE_THRESHOLD_SCALE)
    throw new RangeError(
      `failureThreshold must be at least 0.0005 and below 0.9995 (thresholds are resolved to thousandths); got ${failureThreshold}`,
    )
}

function validate(opts: LocalBreakerOptions): void {
  if (!opts.name.trim())
    throw new RangeError("Circuit breaker name must not be empty")

  const { minimumThroughput = DEFAULT_MINIMUM_THROUGHPUT } = opts
  if (!Number.isInteger(minimumThroughput) || minimumThroughput < 1)
    throw new RangeError("minimumThroughput must be a positive integer")

  const { failureThreshold = DEFAULT_FAILURE_THRESHOLD } = opts
  if (
    !Number.isFinite(failureThreshold) ||
    failureThreshold <= 0 ||
    failureThreshold >= 1
  )
    throw new RangeError("failureThreshold must be a number in (0, 1)")
  assertResolvableThreshold(failureThreshold)

  const { openMs = DEFAULT_OPEN_MS } = opts
  if (!Number.isInteger(openMs) || openMs < 1)
    throw new RangeError("openMs must be a positive integer")

  const { halfOpenSuccesses = DEFAULT_HALF_OPEN_SUCCESSES } = opts
  if (!Number.isInteger(halfOpenSuccesses) || halfOpenSuccesses < 1)
    throw new RangeError("halfOpenSuccesses must be a positive integer")

  const { halfOpenProbes = DEFAULT_HALF_OPEN_PROBES } = opts
  if (!Number.isInteger(halfOpenProbes) || halfOpenProbes < 1)
    throw new RangeError("halfOpenProbes must be a positive integer")

  const { windowSize = DEFAULT_WINDOW_SIZE } = opts
  if (
    !Number.isInteger(windowSize) ||
    windowSize < 1 ||
    windowSize > MAX_WINDOW_SIZE
  )
    throw new RangeError(
      `windowSize must be an integer between 1 and ${MAX_WINDOW_SIZE}`,
    )
}

// ---------------------------------------------------------------------------
// Sliding window (circular buffer of booleans: true = failure)
// ---------------------------------------------------------------------------

class SlidingWindow {
  readonly #size: number
  readonly #buf: boolean[]
  #head = 0
  #count = 0
  #failures = 0

  constructor(size: number) {
    this.#size = size
    this.#buf = new Array<boolean>(size).fill(false)
  }

  record(failure: boolean): void {
    const evicted = this.#buf[this.#head] === true
    if (this.#count === this.#size) {
      if (evicted) this.#failures--
    } else {
      this.#count++
    }
    this.#buf[this.#head] = failure
    if (failure) this.#failures++
    this.#head = (this.#head + 1) % this.#size
  }

  get count(): number {
    return this.#count
  }

  get failures(): number {
    return this.#failures
  }

  get successes(): number {
    return this.#count - this.#failures
  }

  reset(): void {
    this.#buf.fill(false)
    this.#head = 0
    this.#count = 0
    this.#failures = 0
  }
}

// ---------------------------------------------------------------------------
// Local circuit breaker factory
// ---------------------------------------------------------------------------

type LocalBreakerPolicy = Policy & {
  readonly coordination: "local"
  snapshot(): BreakerSnapshot
}

function local(options: LocalBreakerOptions): LocalBreakerPolicy {
  validate(options)

  const name = options.name
  const minimumThroughput =
    options.minimumThroughput ?? DEFAULT_MINIMUM_THROUGHPUT
  const failureThreshold = options.failureThreshold ?? DEFAULT_FAILURE_THRESHOLD
  const openMs = options.openMs ?? DEFAULT_OPEN_MS
  const halfOpenSuccessTarget =
    options.halfOpenSuccesses ?? DEFAULT_HALF_OPEN_SUCCESSES
  const halfOpenProbeLimit = options.halfOpenProbes ?? DEFAULT_HALF_OPEN_PROBES
  const windowSize = options.windowSize ?? DEFAULT_WINDOW_SIZE
  const classifier = options.classify

  let state: BreakerState = "closed"
  let generation = 0
  let openedAt = 0
  let halfOpenSuccessCount = 0
  let halfOpenProbesInFlight = 0
  const window = new SlidingWindow(windowSize)

  // ---------------------------------------------------------------------------
  // Transition helpers
  // ---------------------------------------------------------------------------

  function transitionToOpen(
    context: ExecutionContext,
    previousState: "closed" | "half-open",
  ): void {
    state = "open"
    generation++
    openedAt = Date.now()
    halfOpenSuccessCount = 0
    halfOpenProbesInFlight = 0
    window.reset()
    emitRuntimeEvent(context, {
      type: "breaker.state-changed",
      coordination: "local",
      policyName: name,
      scope: "process",
      state: "open",
      previousState,
    })
  }

  function transitionToHalfOpen(context: ExecutionContext): void {
    state = "half-open"
    generation++
    halfOpenSuccessCount = 0
    halfOpenProbesInFlight = 0
    window.reset()
    emitRuntimeEvent(context, {
      type: "breaker.state-changed",
      coordination: "local",
      policyName: name,
      scope: "process",
      state: "half-open",
      previousState: "open",
    })
  }

  function transitionToClosed(context: ExecutionContext): void {
    state = "closed"
    generation++
    halfOpenSuccessCount = 0
    halfOpenProbesInFlight = 0
    window.reset()
    emitRuntimeEvent(context, {
      type: "breaker.state-changed",
      coordination: "local",
      policyName: name,
      scope: "process",
      state: "closed",
      previousState: "half-open",
    })
  }

  function admit(
    context: ExecutionContext,
  ): "closed" | "half-open" | "rejected" {
    if (state === "closed") return "closed"

    if (state === "open") {
      if (Date.now() - openedAt >= openMs) {
        transitionToHalfOpen(context)
        // fall through to half-open admission below
      } else {
        emitRuntimeEvent(context, {
          type: "breaker.rejected",
          coordination: "local",
          policyName: name,
          scope: "process",
          state: "open",
        })
        return "rejected"
      }
    }

    // half-open
    if (halfOpenProbesInFlight >= halfOpenProbeLimit) {
      emitRuntimeEvent(context, {
        type: "breaker.rejected",
        coordination: "local",
        policyName: name,
        scope: "process",
        state: "half-open",
      })
      return "rejected"
    }

    halfOpenProbesInFlight++
    emitRuntimeEvent(context, {
      type: "breaker.probe-started",
      coordination: "local",
      policyName: name,
      scope: "process",
    })
    return "half-open"
  }

  function observe(
    context: ExecutionContext,
    admitted: "closed" | "half-open",
    admittedGeneration: number,
    settled: Outcome<unknown>,
  ): void {
    // Transitions reset the window and probe accounting. Older work must not
    // contribute observations or release a probe slot in the new generation.
    if (admittedGeneration !== generation || admitted !== state) return

    const outcome = classifyOutcome(context, classifier, settled)

    if (outcome === "ignored") {
      if (admitted === "half-open") {
        halfOpenProbesInFlight = Math.max(0, halfOpenProbesInFlight - 1)
      }
      return
    }

    const failure = outcome === "failure"

    emitRuntimeEvent(context, {
      type: "breaker.observation",
      coordination: "local",
      policyName: name,
      scope: "process",
      outcome,
    })

    if (admitted === "half-open") {
      halfOpenProbesInFlight = Math.max(0, halfOpenProbesInFlight - 1)

      if (failure) {
        transitionToOpen(context, "half-open")
        return
      }

      halfOpenSuccessCount++
      if (halfOpenSuccessCount >= halfOpenSuccessTarget) {
        transitionToClosed(context)
      }
      return
    }

    // admitted === "closed"
    window.record(failure)

    if (
      window.count >= minimumThroughput &&
      window.failures / window.count >= failureThreshold
    ) {
      transitionToOpen(context, "closed")
    }
  }

  // ---------------------------------------------------------------------------
  // Policy implementation
  // ---------------------------------------------------------------------------

  return Object.freeze({
    name,
    coordination: "local" as const,

    snapshot(): BreakerSnapshot {
      return {
        coordination: "local",
        state,
        failures: window.failures,
        successes: window.successes,
        observations: window.count,
        probesInFlight: halfOpenProbesInFlight,
        halfOpenSuccesses: halfOpenSuccessCount,
      }
    },

    async execute<Result>(
      context: ExecutionContext,
      next: Next<Result>,
    ): Promise<Result> {
      const admitted = admit(context)
      const admittedGeneration = generation

      if (admitted === "rejected") {
        throw new CircuitOpenError(name, "local", "process")
      }

      let isSuccess = false
      let value: Result | undefined
      let error: unknown

      try {
        value = await next(context)
        isSuccess = true
        return value
      } catch (err) {
        error = err
        throw err
      } finally {
        observe(
          context,
          admitted,
          admittedGeneration,
          isSuccess
            ? { status: "success", value }
            : { status: "failure", error },
        )
      }
    },
  })
}

export interface BreakerIdentity {
  readonly name: string
  readonly operation: string
  readonly scope: string
}

export type ObserveResult =
  | { readonly type: "stale"; readonly currentGeneration: number }
  | {
      readonly type: "observed"
      readonly generation: number
      readonly windowTotal: number
      readonly windowFailures: number
    }
  | {
      readonly type: "opened"
      readonly newGeneration: number
      readonly windowTotal: number
      readonly windowFailures: number
    }

export type AdmitProbeResult =
  | {
      readonly type: "rejected"
      readonly reason: "closed" | "open" | "probe-limit"
      readonly generation: number
    }
  | {
      readonly type: "admitted"
      readonly generation: number
      readonly probeCount: number
      readonly stateChanged: boolean
    }

export type SettleProbeResult =
  | { readonly type: "stale"; readonly generation: number }
  | {
      readonly type: "settled"
      readonly state: BreakerState
      readonly generation: number
    }
  | {
      readonly type: "transitioned"
      readonly newState: BreakerState
      readonly newGeneration: number
      readonly previousState: "half-open"
    }

/**
 * Policy-specific coordinator capability for `circuitBreaker.distributed()`.
 * The Redis implementation lives in `@gkoos/caracal/redis`; the memory implementation
 * lives in `test/support/memory-coordinator` and must not be a production export.
 */
export interface BreakerCoordinator {
  readState(
    identity: BreakerIdentity,
  ): Promise<{ state: BreakerState; generation: number } | null>

  observe(
    identity: BreakerIdentity,
    params: {
      readonly generation: number
      readonly outcome: "success" | "failure"
      readonly uuid: string
      readonly windowTtlMs: number
      readonly minimumThroughput: number
      readonly failureThresholdNumerator: number
      readonly windowSize: number
      readonly openMs: number
    },
  ): Promise<ObserveResult>

  admitProbe(
    identity: BreakerIdentity,
    params: {
      readonly probeToken: string
      readonly openMs: number
      readonly halfOpenProbes: number
      readonly probeLeaseTtlMs: number
    },
  ): Promise<AdmitProbeResult>

  settleProbe(
    identity: BreakerIdentity,
    params: {
      readonly probeToken: string
      /**
       * `"success"` and `"failure"` record an outcome; `"ignored"` releases the
       * probe's slot without recording anything, which is what the policy sends
       * when its classifier ignores the result.
       */
      readonly outcome: "success" | "failure" | "ignored"
      readonly generation: number
      readonly halfOpenSuccesses: number
      readonly openMs: number
      /**
       * Observation retention period, used as a floor for the CLOSED cleanup
       * TTL: the state hash must not expire before the window it governs, or
       * retained members would outlive their epoch.  Optional so that custom
       * coordinators keep compiling; the policy always supplies it and the
       * Redis coordinator falls back to `openMs × 2` when it is missing.
       */
      readonly windowTtlMs?: number
    },
  ): Promise<SettleProbeResult>
}

/**
 * Distributed (Redis-backed) circuit breaker options.
 *
 * The defaults are mutually consistent.  Overriding one timer or count usually
 * means revisiting the related ones; see the Redis coordination guide
 * (`docs/redis.md`, "Keeping the breaker knobs consistent") for the constraints
 * and the symptoms of getting them wrong.
 */
export interface DistributedBreakerOptions {
  /** Identifies this policy in events and introspection. */
  readonly name: string
  /** Redis-backed coordinator.  See `redisCircuitBreakerCoordinator` in `@gkoos/caracal/redis`. */
  readonly coordinator: BreakerCoordinator
  /**
   * Maps an execution context to the coordination scope key.
   * `scope: ctx => 'process'` is NOT local mode, it's still Redis-backed
   * with full coordinator-failure semantics.
   */
  readonly scope: (context: ExecutionContext) => string
  /**
   * Minimum observations in the window before the breaker may open.  Default: 20.
   *
   * Requires `windowSize >= minimumThroughput`; a smaller window can never
   * reach this count, so the breaker would never open.
   */
  readonly minimumThroughput?: number
  /** Failure fraction (0–1 exclusive) that triggers opening.  Default: 0.5. */
  readonly failureThreshold?: number
  /**
   * Sliding-window size (observation count).  Default: 100.
   *
   * Must be at least `minimumThroughput`: the count cap trims the window, so a
   * smaller window keeps the observed total below the opening threshold and the
   * breaker never opens.
   */
  readonly windowSize?: number
  /**
   * Observation retention period in ms.  Observations older than this are
   * pruned regardless of `windowSize`.  Default: max(openMs × 3, 60_000).
   *
   * Must be long enough to accumulate `minimumThroughput` observations at your
   * traffic rate.  If pruning fires first the window never fills and the
   * breaker never opens; the default is a proxy for that, not a measurement.
   */
  readonly windowTtlMs?: number
  /** How long (ms) the breaker stays open before half-open.  Default: 30_000. */
  readonly openMs?: number
  /** Maximum concurrent half-open probes per scope.  Default: 3. */
  readonly halfOpenProbes?: number
  /**
   * Probe successes needed to close from half-open.  Default: 2.
   *
   * Any probe failure resets progress back to OPEN, so a value that is large
   * relative to the probe rate keeps traffic throttled long after the
   * downstream recovered.
   */
  readonly halfOpenSuccesses?: number
  /**
   * Probe token TTL in ms.  A dead worker's probe expires without blocking
   * recovery.  Default: openMs × 2.
   *
   * Must exceed the slowest probe settle time (at least `timeoutMs`): if a live
   * token expires mid-probe the slot is re-issued, more than `halfOpenProbes`
   * probes run concurrently, and the late settle is dropped as stale.  It is
   * also the worst case a HALF_OPEN window stalls while crashed workers hold
   * every slot, so do not make it arbitrarily large.
   */
  readonly probeLeaseTtlMs?: number
  /**
   * What to do when the coordinator is unreachable and the last known state
   * for the scope was CLOSED (or no prior successful read has occurred).
   * `"fail-open"` allows the attempt through (default).
   * `"fail-closed"` rejects it with CircuitOpenError.
   *
   * If the last successfully-read state was OPEN or HALF_OPEN the attempt is
   * always rejected, regardless of this setting.  Admitting work into a
   * known-open breaker removes the protection it exists to provide.
   *
   * Coordinator unavailability during probe admission (after a successful
   * readState that returned OPEN/HALF_OPEN) also always fails closed.
   */
  readonly onCoordinatorError?: "fail-open" | "fail-closed"
  /** Custom outcome classifier.  Defaults to the adapter classification; retryable counts as failure. */
  readonly classify?: BreakerClassifier
}

const DEFAULT_DIST_MINIMUM_THROUGHPUT = 20
const DEFAULT_DIST_FAILURE_THRESHOLD = 0.5
const DEFAULT_DIST_WINDOW_SIZE = 100
const DEFAULT_DIST_OPEN_MS = 30_000
const DEFAULT_DIST_HALF_OPEN_PROBES = 3
const DEFAULT_DIST_HALF_OPEN_SUCCESSES = 2
const DEFAULT_DIST_ON_COORDINATOR_ERROR = "fail-open" as const

function validateDistributed(opts: DistributedBreakerOptions): void {
  if (typeof opts.name !== "string" || !opts.name.trim())
    throw new RangeError("Distributed circuit breaker name must not be empty")
  if (!opts.coordinator || typeof opts.coordinator !== "object")
    throw new TypeError("coordinator is required")
  if (typeof opts.scope !== "function")
    throw new TypeError("scope must be a function")

  const { minimumThroughput = DEFAULT_DIST_MINIMUM_THROUGHPUT } = opts
  if (!Number.isInteger(minimumThroughput) || minimumThroughput < 1)
    throw new RangeError("minimumThroughput must be a positive integer")

  const { failureThreshold = DEFAULT_DIST_FAILURE_THRESHOLD } = opts
  if (
    !Number.isFinite(failureThreshold) ||
    failureThreshold <= 0 ||
    failureThreshold >= 1
  )
    throw new RangeError("failureThreshold must be a number in (0, 1)")
  assertResolvableThreshold(failureThreshold)

  const { openMs = DEFAULT_DIST_OPEN_MS } = opts
  if (!Number.isInteger(openMs) || openMs < 1)
    throw new RangeError("openMs must be a positive integer")

  const { halfOpenSuccesses = DEFAULT_DIST_HALF_OPEN_SUCCESSES } = opts
  if (!Number.isInteger(halfOpenSuccesses) || halfOpenSuccesses < 1)
    throw new RangeError("halfOpenSuccesses must be a positive integer")

  const { halfOpenProbes = DEFAULT_DIST_HALF_OPEN_PROBES } = opts
  if (!Number.isInteger(halfOpenProbes) || halfOpenProbes < 1)
    throw new RangeError("halfOpenProbes must be a positive integer")

  const { windowSize = DEFAULT_DIST_WINDOW_SIZE } = opts
  if (
    !Number.isInteger(windowSize) ||
    windowSize < 1 ||
    windowSize > MAX_WINDOW_SIZE
  )
    throw new RangeError(
      `windowSize must be an integer between 1 and ${MAX_WINDOW_SIZE}`,
    )

  if (opts.windowTtlMs !== undefined) {
    if (!Number.isInteger(opts.windowTtlMs) || opts.windowTtlMs < 1)
      throw new RangeError("windowTtlMs must be a positive integer")
  }

  if (opts.probeLeaseTtlMs !== undefined) {
    if (!Number.isInteger(opts.probeLeaseTtlMs) || opts.probeLeaseTtlMs < 1)
      throw new RangeError("probeLeaseTtlMs must be a positive integer")
  }

  if (
    opts.onCoordinatorError !== undefined &&
    opts.onCoordinatorError !== "fail-open" &&
    opts.onCoordinatorError !== "fail-closed"
  )
    throw new TypeError(
      'onCoordinatorError must be "fail-open" or "fail-closed"',
    )
}

type DistributedBreakerPolicy = Policy & {
  readonly coordination: "distributed"
}

function distributed(
  options: DistributedBreakerOptions,
): DistributedBreakerPolicy {
  validateDistributed(options)

  const name = options.name
  const coordinator = options.coordinator
  const resolveScope = options.scope
  const minimumThroughput =
    options.minimumThroughput ?? DEFAULT_DIST_MINIMUM_THROUGHPUT
  const failureThreshold =
    options.failureThreshold ?? DEFAULT_DIST_FAILURE_THRESHOLD
  const failureThresholdNumerator = Math.round(
    failureThreshold * FAILURE_THRESHOLD_SCALE,
  )
  const windowSize = options.windowSize ?? DEFAULT_DIST_WINDOW_SIZE
  const openMs = options.openMs ?? DEFAULT_DIST_OPEN_MS
  const windowTtlMs = options.windowTtlMs ?? Math.max(openMs * 3, 60_000)
  const halfOpenProbes = options.halfOpenProbes ?? DEFAULT_DIST_HALF_OPEN_PROBES
  const halfOpenSuccesses =
    options.halfOpenSuccesses ?? DEFAULT_DIST_HALF_OPEN_SUCCESSES
  const probeLeaseTtlMs = options.probeLeaseTtlMs ?? openMs * 2
  const onCoordinatorError =
    options.onCoordinatorError ?? DEFAULT_DIST_ON_COORDINATOR_ERROR
  const classifier = options.classify

  // Per-(operation, scope) record of the last known NON-CLOSED state.
  // Used as a fallback when readState() fails: if the last confirmed state was
  // OPEN or HALF_OPEN the breaker must fail-closed regardless of
  // onCoordinatorError, because admitting traffic to a known-open breaker
  // removes the protection the breaker exists to provide.
  // CLOSED is not retained: it is indistinguishable from "never seen" for this
  // decision, and storing it would grow without bound with scope cardinality.
  const lastKnownState = createScopeStateCache()

  // Admission state tracked per-attempt within a single execute() call.
  type AdmissionState =
    | { readonly kind: "closed"; readonly generation: number }
    | {
        readonly kind: "probe"
        readonly probeToken: string
        readonly generation: number
      }

  return Object.freeze({
    name,
    coordination: "distributed" as const,

    async execute<Result>(
      context: ExecutionContext,
      next: Next<Result>,
    ): Promise<Result> {
      const scope = resolveScope(context)
      if (typeof scope !== "string" || !scope.trim())
        throw new TypeError(
          "circuitBreaker.distributed scope must be a non-empty string",
        )

      const identity: BreakerIdentity = {
        name,
        operation: context.operationName,
        scope,
      }

      // ---- ADMISSION ----

      let stateData: { state: BreakerState; generation: number } | null = null

      try {
        stateData = await coordinator.readState(identity)
        // Remember only non-closed states: CLOSED is indistinguishable from
        // "never seen" for the failure decision below, so storing it would grow
        // this process-local record with every scope ever observed.
        const observed = stateData?.state
        if (observed === "open" || observed === "half-open") {
          lastKnownState.remember(identity.operation, scope, observed)
        } else {
          lastKnownState.forget(identity.operation, scope)
        }
      } catch (readError) {
        emitRuntimeEvent(context, {
          type: "breaker.coordinator-error",
          coordination: "distributed",
          policyName: name,
          scope,
          operation: "admit",
          error: readError,
        })

        // If the last successfully-read state for this scope was OPEN or
        // HALF_OPEN, we know the breaker was non-closed before the outage.
        // Admitting work in that case removes the protection the breaker
        // provides, so we always fail-closed regardless of onCoordinatorError.
        const cached = lastKnownState.read(identity.operation, scope)
        const effectiveBehavior =
          cached === "open" || cached === "half-open"
            ? "fail-closed"
            : onCoordinatorError

        emitRuntimeEvent(context, {
          type: "breaker.degraded",
          coordination: "distributed",
          policyName: name,
          scope,
          reason: "coordinator-unavailable",
          behavior: effectiveBehavior,
        })

        if (effectiveBehavior === "fail-closed") {
          emitRuntimeEvent(context, {
            type: "breaker.rejected",
            coordination: "distributed",
            policyName: name,
            scope,
            state: "open",
          })
          throw new CircuitOpenError(name, "distributed", scope)
        }
        // fail-open: last known state was CLOSED (or no prior read succeeded).
        // Treat as CLOSED with generation 0.
        stateData = { state: "closed", generation: 0 }
      }

      // null from readState means missing hash key → implicit CLOSED / generation 0
      const reportedState: BreakerState = stateData?.state ?? "closed"
      const reportedGeneration: number = stateData?.generation ?? 0
      let admission: AdmissionState

      if (reportedState === "closed") {
        admission = { kind: "closed", generation: reportedGeneration }
      } else {
        // OPEN or HALF_OPEN: attempt probe admission
        const probeToken = randomUUID()
        let probeResult: AdmitProbeResult

        try {
          probeResult = await coordinator.admitProbe(identity, {
            probeToken,
            openMs,
            halfOpenProbes,
            probeLeaseTtlMs,
          })
        } catch (admitError) {
          // Coordinator unavailable during probe admission → always fail-closed
          emitRuntimeEvent(context, {
            type: "breaker.coordinator-error",
            coordination: "distributed",
            policyName: name,
            scope,
            operation: "admit",
            error: admitError,
          })
          emitRuntimeEvent(context, {
            type: "breaker.degraded",
            coordination: "distributed",
            policyName: name,
            scope,
            reason: "coordinator-unavailable",
            behavior: "fail-closed",
          })
          emitRuntimeEvent(context, {
            type: "breaker.rejected",
            coordination: "distributed",
            policyName: name,
            scope,
            state: reportedState === "open" ? "open" : "half-open",
          })
          throw new CircuitOpenError(name, "distributed", scope)
        }

        if (probeResult.type === "rejected") {
          if (probeResult.reason === "closed") {
            // Breaker closed between readState and admitProbe; treat as CLOSED
            lastKnownState.forget(identity.operation, scope)
            admission = { kind: "closed", generation: probeResult.generation }
          } else {
            // "open"        → openMs not yet elapsed; still OPEN
            // "probe-limit" → admitProbe atomically transitioned (or was) HALF_OPEN
            const rejState =
              probeResult.reason === "open"
                ? ("open" as const)
                : ("half-open" as const)
            emitRuntimeEvent(context, {
              type: "breaker.rejected",
              coordination: "distributed",
              policyName: name,
              scope,
              state: rejState,
              generation: probeResult.generation,
            })
            throw new CircuitOpenError(name, "distributed", scope)
          }
        } else {
          // Admitted as probe
          if (probeResult.stateChanged) {
            lastKnownState.remember(identity.operation, scope, "half-open")
            emitRuntimeEvent(context, {
              type: "breaker.state-changed",
              coordination: "distributed",
              policyName: name,
              scope,
              state: "half-open",
              previousState: "open",
              generation: probeResult.generation,
            })
          }
          emitRuntimeEvent(context, {
            type: "breaker.probe-started",
            coordination: "distributed",
            policyName: name,
            scope,
            generation: probeResult.generation,
          })
          admission = {
            kind: "probe",
            probeToken,
            generation: probeResult.generation,
          }
        }
      }

      // ---- EXECUTION ----

      let isSuccess = false
      let value: Result | undefined
      let thrownError: unknown

      try {
        value = await next(context)
        isSuccess = true
        return value
      } catch (err) {
        thrownError = err
        throw err
      } finally {
        // ---- SETTLEMENT ----
        const breakerOutcome = classifyOutcome(
          context,
          classifier,
          isSuccess
            ? { status: "success", value }
            : { status: "failure", error: thrownError },
        )
        // An ignored result is never recorded.  A probe still has to settle
        // though: releasing the slot it holds is what lets the next probe
        // through, instead of stalling the recovery window until the probe lease
        // elapses.  The local breaker frees its slot immediately, and a release
        // records nothing and does not advance recovery.
        if (breakerOutcome !== "ignored" || admission.kind === "probe") {
          const outcomeStr =
            breakerOutcome === "success"
              ? ("success" as const)
              : ("failure" as const)
          const settleOutcome =
            breakerOutcome === "ignored" ? ("ignored" as const) : outcomeStr

          if (admission.kind === "closed") {
            try {
              const result = await coordinator.observe(identity, {
                generation: admission.generation,
                outcome: outcomeStr,
                uuid: randomUUID(),
                windowTtlMs,
                minimumThroughput,
                failureThresholdNumerator,
                windowSize,
                openMs,
              })
              if (result.type === "stale") {
                emitRuntimeEvent(context, {
                  type: "breaker.observation-stale",
                  coordination: "distributed",
                  policyName: name,
                  scope,
                  attemptGeneration: admission.generation,
                  currentGeneration: result.currentGeneration,
                })
              } else {
                emitRuntimeEvent(context, {
                  type: "breaker.observation",
                  coordination: "distributed",
                  policyName: name,
                  scope,
                  outcome: outcomeStr,
                  // The epoch the observation was recorded under. Normally that
                  // is the generation the attempt was admitted with, but if the
                  // state hash was lost while its window survived, the script
                  // mints a new epoch and records the observation there - so
                  // reporting the admission generation would name an epoch this
                  // observation does not belong to.
                  generation:
                    result.type === "observed"
                      ? result.generation
                      : admission.generation,
                })
                if (result.type === "opened") {
                  lastKnownState.remember(identity.operation, scope, "open")
                  emitRuntimeEvent(context, {
                    type: "breaker.state-changed",
                    coordination: "distributed",
                    policyName: name,
                    scope,
                    state: "open",
                    previousState: "closed",
                    generation: result.newGeneration,
                  })
                }
              }
            } catch (observeError) {
              // Coordinator error during observation: drop the datapoint — do not
              // fail the caller, one lost observation has low impact.  Emit the
              // diagnostic event so operators can detect that the breaker has
              // stopped learning (e.g. sustained coordinator unavailability).
              emitRuntimeEvent(context, {
                type: "breaker.coordinator-error",
                coordination: "distributed",
                policyName: name,
                scope,
                operation: "observe",
                error: observeError,
              })
            }
          } else {
            // Probe settlement
            try {
              const result = await coordinator.settleProbe(identity, {
                probeToken: admission.probeToken,
                outcome: settleOutcome,
                generation: admission.generation,
                halfOpenSuccesses,
                openMs,
                windowTtlMs,
              })
              // A release reports nothing: it is not an observation, so there is
              // no breaker.observation event and no transition to announce.
              if (breakerOutcome !== "ignored") {
                if (result.type === "stale") {
                  emitRuntimeEvent(context, {
                    type: "breaker.observation-stale",
                    coordination: "distributed",
                    policyName: name,
                    scope,
                    attemptGeneration: admission.generation,
                    currentGeneration: result.generation,
                  })
                } else {
                  emitRuntimeEvent(context, {
                    type: "breaker.observation",
                    coordination: "distributed",
                    policyName: name,
                    scope,
                    outcome: outcomeStr,
                    generation: admission.generation,
                  })
                  if (result.type === "transitioned") {
                    if (result.newState === "closed") {
                      lastKnownState.forget(identity.operation, scope)
                    } else {
                      lastKnownState.remember(
                        identity.operation,
                        scope,
                        result.newState,
                      )
                    }
                    emitRuntimeEvent(context, {
                      type: "breaker.state-changed",
                      coordination: "distributed",
                      policyName: name,
                      scope,
                      state: result.newState,
                      previousState: "half-open",
                      generation: result.newGeneration,
                    })
                  }
                }
              }
            } catch (settleError) {
              // Coordinator error during probe settlement: drop the result — the
              // probe token expires via TTL without blocking recovery.  Emit the
              // diagnostic event so operators can detect the failure.
              emitRuntimeEvent(context, {
                type: "breaker.coordinator-error",
                coordination: "distributed",
                policyName: name,
                scope,
                operation: "settle-probe",
                error: settleError,
              })
            }
          }
        }
      }
    },
  })
}

export const circuitBreaker = Object.freeze({ local, distributed })
