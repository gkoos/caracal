/**
 * Serval: the opt-in, explicitly approximate coordination strategy.
 *
 * Import from `@gkoos/caracal/serval` and pass a coordinator to the policy you already
 * use - `circuitBreaker.distributed`, `rateLimit.distributed` or
 * `bulkhead.distributed` - so nothing about the strict coordinators changes:
 *
 * ```ts
 * import { createCoordinationClient } from "@gkoos/caracal/redis"
 * import { createServalBreaker } from "@gkoos/caracal/serval"
 *
 * const client = createCoordinationClient(process.env.CARACAL_REDIS_URL as string)
 * const breaker = createServalBreaker({
 *   namespace: "orders:prod",
 *   authority: redisCircuitBreakerCoordinator(client, { namespace: "orders:prod" }),
 * })
 * ```
 *
 * This entry point is **experimental**. It is exported so the strategy can be
 * evaluated and reviewed, not because its operating envelope is settled: read
 * `docs/serval/api.md` for the guarantees it makes, the deviations it documents and
 * the numbers `docs/serval/benchmarks.md` measures.
 */

export {
  createServalAuthoritySession as createServalSession,
  ServalSessionClosedError,
} from "./coordination/serval/session.js"
export {
  createServalBreaker,
  ServalAuthorityRefusalError,
} from "./coordination/serval/breaker.js"
export { createServalRateLimitCoordinator } from "./coordination/serval/rate-limit.js"
export { createServalBulkheadCoordinator } from "./coordination/serval/bulkhead.js"
export { ServalRegistryPressureError } from "./coordination/serval/cache.js"
export { redisRateLimitGrantIssuer } from "./coordination/redis/rate-limit.js"
export { redisBulkheadAllocationIssuer } from "./coordination/redis/bulkhead.js"

export type {
  ServalSession,
  ServalSessionOptions,
  ServalSessionState,
  ServalScopeRegistration,
} from "./coordination/serval/session.js"
export type {
  ServalBreakerCoordinator,
  ServalBreakerCoordinatorOptions,
  ServalBreakerOptions,
  ServalBreakerPayload,
  ServalStaleClosedBehavior,
} from "./coordination/serval/breaker.js"
export type {
  ServalRateGrantState,
  ServalRateLimitCoordinator,
  ServalRateLimitCoordinatorOptions,
  ServalRateLimitDiagnostics,
  ServalRateLimitMetrics,
} from "./coordination/serval/rate-limit.js"
export type {
  ServalBulkheadCoordinator,
  ServalBulkheadCoordinatorOptions,
  ServalBulkheadDiagnostics,
  ServalBulkheadMetrics,
  ServalBulkheadState,
  ServalRelinquishResult,
} from "./coordination/serval/bulkhead.js"
export type {
  BulkheadAllocationIssuer,
  BulkheadAllocationReply,
  BulkheadAllocationRequest,
} from "./coordination/redis/bulkhead.js"
export type {
  RateGrantReply,
  RateGrantRequest,
  RateLimitGrantIssuer,
} from "./coordination/redis/rate-limit.js"
export type { ServalAuthorityReader } from "./coordination/serval/reconciliation.js"
export type {
  ServalInvalidationPublisher,
  ServalNotificationTransport,
} from "./coordination/serval/notifications.js"
export type { ServalMetricsSnapshot } from "./coordination/serval/metrics.js"
