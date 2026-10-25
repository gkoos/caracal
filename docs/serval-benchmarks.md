# Serval fault lab: measured trade-offs and operating envelopes

This page reports what the reproducible fault lab measures, what the numbers mean, and
which configurations the evidence supports. It is the measurement behind
[Serval](serval.md) and [its API](serval-api.md): every claim those pages make about
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
| healthy | unprotected | 1 | 20 | 20 | 0 | 0 | 0 | 0.00 | 26 | 0 | 1 |
| healthy | strict | 1 | 20 | 20 | 0 | 0 | 0 | **5.00** | 31 | 0 | 1 |
| healthy | serval | 1 | 20 | 20 | 0 | 0 | 0 | **1.70** | 32 | 0 | 1 |
| healthy | strict | 4 | 80 | 32 | 48 | 0 | 0 | 3.20 | 35 | 0 | 1 |
| healthy | serval | 4 | 80 | 32 | 48 | 0 | 0 | **1.68** | 36 | 0 | 1 |
| downstream-slow | strict | 4 | 80 | 64 | 7 | 1 | 0 | 4.71 | 75 | 0 | 4 |
| downstream-slow | serval | 4 | 80 | 56 | 16 | 0 | 0 | **2.19** | 74 | 4 | 4 |
| redis-outage | strict | 1 | 20 | 0 | 0 | 0 | 20 | 3.00 | 3 | 0 | 0 |
| redis-outage | serval | 1 | 20 | 0 | 0 | 0 | 20 | 3.10 | 4 | 0 | 0 |
| slow+partition | strict | 4 | 80 | 51 | 0 | 0 | 20 | 4.50 | 76 | 0 | 3 |
| slow+partition | serval | 4 | 80 | 50 | 1 | 0 | 20 | **2.19** | 68 | 4 | 3 |

## What the numbers say

**The admission read really is removed, and that is most of the cost.** On a healthy
dependency one replica pays **1.70 Redis commands per offered request** with Serval
against **5.00** with the strict coordinators - a 66% reduction, and the same
admissions. The remaining cost is what the RFC says it should be: the rate grant and
the bulkhead allocation on their slow paths, plus the breaker's **awaited settlement**
on every call. Serval does not make settlement free, and this number is the honest
measurement of that.

**Under load the fleet ceiling is what binds, not the coordinator.** With four replicas
and 80 offers the strict and Serval profiles both admit 32: the rate limit (50/s, burst
8) refuses the rest. Coordination strategy does not change how much work the fleet
accepts; it changes what that costs.

**A slow dependency is where the profiles diverge, and Serval is the stricter one.**
Serval admitted 56 of 80 where strict admitted 64, with 16 rate refusals instead of 7
and no bulkhead refusals instead of 1. The delegated rate limiter's local stock runs
out before the strict GCRA would refuse, so it protects earlier and admits less. That
is a *deliberate* trade: less work through a struggling dependency, at a lower Redis
cost. It is not free - it is also the case where a caller sees a refusal the strict
path would have admitted.

**Redis unavailable stops a composed pipeline, on both paths, and that is not a bug.**
The strict coordinators fail closed because they cannot read. Serval keeps its local
authority - the composition suite asserts exactly that for the rate limiter and the
bulkhead - but the breaker's outcome settlement is awaited, so a pipeline that includes
the breaker fails the call rather than pretending the outcome was recorded. The
measured consequence is that a full outage admits nothing on either path. If an outage
must be survivable, the answer is a deployment choice: leave the distributed breaker
out and rely on local protection, or accept the fail-closed behaviour. This is recorded
as an operating envelope rather than smoothed over.

**A partition is per replica, and the fleet carries on.** With one of four replicas cut
off, the three healthy replicas kept admitting (50-51 of 80) while the isolated
replica's 20 offers failed. Cost stayed lower with Serval (2.19 against 4.50 per
offer). No replica's failure was hidden: it appeared as failures, not as silent
capacity.

**Tracked occupancy is not physical concurrency, and the lab shows the gap.** In the
slow-downstream cell the four replicas tracked 4 slots between them while the dependency
was physically working on 4 responses - and in the outage cells the dependency's peak
was 0 because nothing was admitted. The two are measured independently on purpose: the
safety claim is about *tracked* occupancy against the sum of the local ceilings,
and the physical number is what the dependency actually did, including work that
outlived its admission (the lab's adapter ignores cancellation, so an abandoned attempt
is not mistaken for a stopped one).

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

