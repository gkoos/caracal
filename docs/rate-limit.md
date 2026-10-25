# Rate limiting

A rate limit bounds the arrival rate of adapter calls, using the Generic Cell Rate Algorithm (GCRA). Rate is the axis next to the [bulkhead](bulkhead.md)'s concurrency axis: a bulkhead limits how many calls run at once, a rate limit limits how fast new calls start.

GCRA keeps one scalar of state - the theoretical arrival time (TAT) of the next call - so it needs no background refill process and no stored clock drift. On each admission the TAT advances by one emission interval; a call whose arrival is more than the burst tolerance ahead of schedule is rejected. This is the same algorithm Envoy's local rate limiter uses, with a distributed state cell instead of a per-process one.

## Knobs

| Option | Meaning | Default | Bounds |
|---|---|---|---|
| `name` | Identifies the policy in events and introspection | required | non-empty |
| `rate` | Sustained rate in requests per second | required | finite, `0 < rate <= 1000`; resolved to the nearest whole millisecond, so sub-millisecond emission intervals cannot be expressed |
| `burst` | Maximum burst: how many calls may arrive clustered together before the strict rate applies | `1` (no burst) | integer `>= 1` |
| `coordinator` | Coordinator object | — (distributed only) | coordinator object |
| `scope` | `(context) => string` | — (distributed only) | function |

`rate` is resolved to `emissionIntervalMs = round(1000 / rate)`, and `burst` to `burstDelayMs = (burst - 1) * emissionIntervalMs`. A `rate` of 50 with `burst` of 10 means "50 requests per second on average, but allow 10 to land in the same instant".

## Local

Each process enforces its own rate independently. No coordinator or Redis connection is required.

```ts
import { rateLimit, operation } from "@gkoos/caracal"

const limiter = rateLimit.local({ name: "partner-api", rate: 50, burst: 10 })

const op = operation({ name: "orders", adapter, policies: [limiter] })

limiter.snapshot() // { coordination: "local", nextAllowedAt }
```

The returned policy carries `coordination` (`"local"` or `"distributed"`) and `snapshot()` is available on local policies only, reporting the timestamp of the next admissible call.

## Distributed

The rate is shared across every replica that shares the same coordination identity: `(namespace, policy name, operation name, scope)`.

```ts
import { rateLimit, operation } from "@gkoos/caracal"
import { createCoordinationClient, redisRateLimitCoordinator } from "@gkoos/caracal/redis"

const redis = createCoordinationClient(process.env.REDIS_URL!)
await redis.connect()

const sharedRate = rateLimit.distributed({
  name: "partner-api",
  coordinator: redisRateLimitCoordinator(redis, { namespace: "svc:prod" }),
  scope: (ctx) => `region:${String(ctx.metadata.region)}`,
  rate: 100,
  burst: 20,
})

const op = operation({ name: "orders", adapter, policies: [sharedRate] })

redis.disconnect()
```

The GCRA state lives in Redis under the scope's coordination key, so one scope's budget is enforced across the whole fleet. Unlike the local policy, the distributed policy has no `snapshot()`: the state lives in Redis.

`rate` and `burst` are the constructor baseline. A stored configuration record for the identity replaces both, so the rate can change without a redeploy; see [shared configuration](redis.md#shared-configuration). The GCRA theoretical arrival time is preserved across the change, so outstanding debt is not reset and a rate increase is not felt until that debt clears.

## Division across processes

The same asymmetry as the bulkhead and circuit breaker applies: **one local instance shared by several operations *merges* their arrivals into a single in-process budget, whereas the distributed policy keys by `(namespace, policy name, operation name, scope)` and keeps them separate.** Local state is per instance, not per name; two instances with the same name are independent, and each process enforces `rate` on its own - so forty replicas each configured with `rate: 100` admit up to 4000/s in aggregate. The distributed policy shares a single GCRA cell, so the fleet as a whole admits at most `rate` per second per scope.

## Reject, not wait

Admission is immediate-reject only: there is no queue and no waiting. A rejected call throws `RateLimitExceededError` carrying `retryAfterMs`, the time until the next call would be admissible. That hint composes with the existing retry delay path instead of introducing a second wait mechanism:

```ts
import { retry, rateLimit, RateLimitExceededError } from "@gkoos/caracal"

const limiter = rateLimit.distributed({ /* ... */ })

retry({
  maxAttempts: 3,
  delay: (attempt, ctx) =>
    ctx.error instanceof RateLimitExceededError ? ctx.error.retryAfterMs : 0,
})
```

`retryAfterMs` is the GCRA catch-up time, always a positive whole number of milliseconds on rejection. It also appears on the `ratelimit.rejected` event.

A refusal is not evidence about the dependency, so an enclosing [circuit breaker](circuit-breaker.md#ignored-results) does not record a `RateLimitExceededError` as a failure; `countAdmissionRejections` on the breaker records it anyway. A coordinator failure is not this case: the policy rethrows the coordinator's own error, which a breaker counts.

## Delegated quotas

`rateLimit.distributed` accepts any `RateLimitCoordinator`, so a coordinator may spend
locally held quota instead of asking Redis on every admission. Serval's delegated rate limiter
is exactly that (`docs/serval/README.md`): Redis issues a bounded grant of permits, the
process spends them without a round trip, and the ordinary admission path costs nothing while
stock remains.

Three properties are worth knowing before opting in:

- **It is not a second budget.** A grant is taken from the *same* GCRA state this page
  describes, so a strict call and a delegated grant draw on one bucket. Mixing the two paths
  for one identity is therefore safe, but it does not add capacity.
- **It is not exact GCRA.** Permits are issued in grants and spent locally, so admissions in
  any interval of length `L` are bounded by `R * (L + V) + B`, where `V` is the grant spend
  horizon - looser than the strict cell, and stated rather than implied.
- **A refusal still carries `retryAfterMs`**, from the server's own hint when the bucket is
  empty, or from a local estimate when the authority answered too late to be used. The retry
  recipe above works unchanged.

## Failure guarantees

- Failed or uncertain admission **fails closed**. No adapter call is started and no local fallback occurs. With the Redis coordinator the rejection is a `CoordinatorUnavailableError` (exported from `@gkoos/caracal/redis`, not the root); the policy rethrows whatever its coordinator threw, so a custom coordinator's own error type is what you see. There is no `onCoordinatorError` knob: rate-limit admission has no safe "fail-open", because allowing traffic when the limiter cannot be read removes the protection it exists to provide.
- **Abandoned-attempt accounting**: GCRA charges a call at the moment of admission, not at settlement. A call that is later abandoned (for example by a `timeout()`) does not refund its rate slot. The `ratelimit.admitted` event is the record of the charge.
- All configuration (`rate`, `burst`) must be consistent across every process using the same identity. Use stable, non-secret names and scopes; they are observable in the Redis keyspace. Empty values and components over 1024 UTF-8 bytes are rejected.

## Events

Relevant events: `ratelimit.admitted`, `ratelimit.rejected`, `ratelimit.degraded`.

See [events and observability](events-and-observability.md#rate-limit) for the full reference.
