# Serval fault lab: measured trade-offs and operating envelopes

This page reports what the reproducible fault lab measures, what the numbers mean, and
which configurations the evidence supports. It is the measurement behind
[Serval](README.md) and [its API](api.md): every claim those pages make about
cost, admission behaviour and tracked occupancy is checked here rather than asserted.

The lab is **not part of `npm test`**: it takes minutes, spawns real processes and
needs Redis, so it is a research tool rather than a gate.

```bash
npm run redis:up          # a local Redis
npm run bench:serval      # the default sweep
```

| Variable | Default | Meaning |
|---|---|---|
| `CARACAL_REDIS_URL` | `redis://127.0.0.1:6379` | the coordination server |
| `CARACAL_LAB_REPLICAS` | `1,4` | replica counts to sweep |
| `CARACAL_LAB_SEEDS` | `1` | paired seeds per cell |
| `CARACAL_LAB_OFFERS` | `24` | offers per replica per cell |
| `CARACAL_LAB_INTERVAL` | `10` | milliseconds between offers |
| `CARACAL_LAB_TARGET` | `4` | the fleet's concurrency target |

Each run writes raw per-seed results - revision, configuration, node version, per-run
metrics - to `bench/serval/results/`, so a claim can be re-derived from the artifacts
rather than from this page.

## What it compares

The same offered workload through four profiles, in the same cells, with the same
seeds:

| Profile | Admission path |
|---|---|
| `unprotected` | no policies at all - the reference |
| `strict` | the existing Redis coordinators, one round trip per admission |
| `serval` | the three Serval coordinators, no local guardrails |
| `serval-guarded` | Serval plus a local bulkhead at the target |

Cells: a healthy dependency, a slow one, Redis unavailable to every replica, the
**critical interaction** (slow dependency *and* Redis unavailable), and a single
partitioned replica among healthy ones. Every worker is a real process with its own
Redis connection; the dependency is a real HTTP server that accounts for the work it
actually does, and it ignores client disconnects on purpose.

## A representative run

Twenty offers per replica at 5ms, one and four replicas, one seed, on a developer
machine (Windows, local Redis, loopback dependency). Absolute latencies are therefore
not portable; the *ratios* and the admission outcomes are the point.

| Cell | Profile | Rep | Offered | Admitted | Rate-rej | Bulkhead-rej | Failed | Redis/offer | p95 ms | Tracked | Peak physical |
|---|---|---|---|---|---|---|---|---|---|---|---|
| healthy | unprotected | 1 | 20 | 20 | 0 | 0 | 0 | 0.00 | 2 | 0 | 1 |
| healthy | strict | 1 | 20 | 20 | 0 | 0 | 0 | **5.00** | 32 | 0 | 1 |
| healthy | serval | 1 | 20 | 20 | 0 | 0 | 0 | **1.55** | 3 | 1 | 1 |
| healthy | strict | 4 | 80 | 25 | 55 | 0 | 0 | 2.94 | 7 | 0 | 1 |
| healthy | serval | 4 | 80 | 24 | 56 | 0 | 0 | **1.40** | 6 | 4 | 1 |
| downstream-slow | strict | 4 | 80 | 51 | 25 | 0 | 0 | 4.06 | 63 | 0 | 4 |
| downstream-slow | serval | 4 | 80 | 50 | 26 | 0 | 0 | **1.99** | 60 | 4 | 4 |
| redis-outage | strict | 1 | 20 | 0 | 0 | 0 | 20 | 3.00 | 1 | 0 | 0 |
| redis-outage | serval | 1 | 20 | 0 | 0 | 0 | 20 | 3.05 | 1 | 0 | 0 |
| slow+partition | strict | 4 | 80 | 57 | 0 | 0 | 20 | 4.50 | 61 | 0 | 3 |
| slow+partition | serval | 4 | 80 | 57 | 0 | 0 | 20 | **1.98** | 57 | 3 | 3 |

Tracked occupancy is what the delegated bulkhead accounts for locally. The strict bulkhead keeps its leases in Redis, so its column reads 0 rather than the count a server-side read would give.

## What the numbers say

**The admission read really is removed, and that is most of the cost.** On a healthy
dependency one replica pays **1.55 Redis commands per offered request** with Serval
against **5.00** with the strict coordinators - a 69% reduction, and the same
admissions. The remaining cost is what the contract expects: the rate grant and
the bulkhead allocation on their slow paths, plus the breaker's settlement on every
call. Serval does not make settlement free, and this number is the honest measurement
of that.

**Under load the fleet ceiling is what binds, not the coordinator.** With four replicas
and 80 offers the strict and Serval profiles admitted 25 and 24: the rate limit (50/s,
burst 8) refuses the rest. Coordination strategy does not change how much work the fleet
accepts; it changes what that costs.

**A slow dependency is where the profiles diverge.** In this run Serval admitted 50 of 80
where strict admitted 51, with 26 rate refusals against 25 and no bulkhead refusals on
either path. The delegated rate limiter spends locally held stock, so it can refuse
before the strict GCRA would, and the integration suite pins that mechanism; how much
earlier it bites depends on the arrival pattern. What the table shows either way is the
cost: 1.99 Redis commands per offer against 4.06, for the same protection.

**A cold start without Redis admits nothing, on both paths.** The outage cells start each
worker against an unreachable authority, so no replica holds a verdict, a grant or an
allocation yet and every path has to read before it can serve. The strict coordinators fail
closed because they cannot read, and the delegated layers have nothing local to admit
from. This is a cold-start envelope rather than a settlement effect: observation and
settlement failures are reported as degradation and do not fail an otherwise successful
call, and the composition suite asserts that a warm delegated pipeline keeps admitting from
the authority it holds. If a cold start during an outage must be survivable, the answer is
a deployment choice: keep a local guardrail in front, or accept the fail-closed behaviour.
This is recorded as an operating envelope rather than smoothed over.

**A partition is per replica, and the fleet carries on.** With one of four replicas cut
off, the three healthy replicas kept admitting (57 of 80 on both paths) while the isolated
replica's 20 offers failed. Cost stayed lower with Serval (1.98 against 4.50 per offer).
No replica's failure was hidden: it appeared as failures, not as silent capacity.

**Tracked occupancy is measured at the client, physical concurrency at the dependency.**
In these cells the two agree - four replicas tracked 4 slots while the dependency worked
on 4 responses, and 3 against 3 under the partition - because every admitted request is
held until it settles. They diverge when work outlives its admission: the composition
suite pins the abandoned-attempt case, where the permit stays held until the adapter
settles even though the caller has gone, and the lab's adapter ignores cancellation for
the same reason. The safety claim is about *tracked* occupancy against the sum of the
local ceilings; the physical number is what the dependency actually did.

## Recommended operating envelopes

- **Keep the fleet target and the local guardrail independent.** The composed pipeline
  is bounded by the smallest layer; a local bulkhead at the target is what makes a
  per-process ceiling hold when the fleet's accounting is stale.
- **Expect the delegated rate limiter to refuse earlier than the strict one.** It admits
  from local stock, so a slow dependency exhausts that stock sooner. Size `grantSize`
  and `validForMs` for the traffic shape rather than for the peak rate.
- **Do not deploy the distributed breaker where an outage must be survivable.** Its
  settlement is awaited by design: that is what makes its evidence trustworthy and what
  makes a Redis outage visible to callers.
- **Watch three numbers in production**: Redis commands per *offered* request (the cost
  claim), the fleet's tracked occupancy against the sum of the local ceilings (the
  safety claim), and the dependency's own physical concurrency (the claim nothing can
  make on the fleet's behalf).

## Known limitations

- Single machine, loopback dependency, local Redis: absolute latencies are not portable
  and no network fault injector is used, so latency *distributions* are not
  representative of a real deployment.
- The lab drives a closed loop per replica (one offer at a time), which measures
  protection and cost but not queueing behaviour under an open-loop arrival process.
- Grant sizes, TTLs and reclaim grace are held at the values in `bench/serval/worker.ts`
  for comparability; sweeping them is a configuration exercise, not a new measurement.
- The cells are short: large enough to compare profiles against each other, too small to
  claim steady-state behaviour for a long-running fleet.
- The "real-process partition" cells isolate a replica by pointing it at an unreachable
  Redis, which models a partition of that replica rather than a network-level fault
  between replicas.

