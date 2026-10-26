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
| `CARACAL_LAB_SEEDS` | `1` | arrival patterns to pair across profiles; more seeds sample more patterns |
| `CARACAL_LAB_OFFERS` | `24` | offers per replica per cell |
| `CARACAL_LAB_INTERVAL` | `10` | milliseconds between offers |
| `CARACAL_LAB_TARGET` | `4` | the fleet's concurrency target |
| `CARACAL_LAB_CELLS` | all cells | comma-separated cell names to run, for re-measuring one cell |

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
**critical interaction** (slow dependency *and* Redis unavailable), a single
partitioned replica among healthy ones, and a **warm outage**, where every replica is served
by the healthy authority first and cut off afterwards. Every worker is a real process with its own
Redis connection; the dependency is a real HTTP server that accounts for the work it
actually does, and it ignores client disconnects on purpose.

## A representative run

Twenty offers per replica at 5ms ±50% — the offered schedule is seeded, see
[paired load](#paired-load) — one and four replicas, one seed, on a developer
machine (Windows, local Redis, loopback dependency). Absolute latencies are therefore
not portable; the *ratios* and the admission outcomes are the point.

| Cell | Profile | Rep | Offered | Admitted | Rate-rej | Bulkhead-rej | Failed | Redis/offer | p95 ms | Tracked | Peak physical |
|---|---|---|---|---|---|---|---|---|---|---|---|
| healthy | unprotected | 1 | 20 | 20 | 0 | 0 | 0 | 0.00 | 2 | 0 | 1 |
| healthy | strict | 1 | 20 | 20 | 0 | 0 | 0 | **5.00** | 7 | 1 | 1 |
| healthy | serval | 1 | 20 | 20 | 0 | 0 | 0 | **1.50** | 4 | 1 | 1 |
| healthy | strict | 4 | 80 | 25 | 55 | 0 | 0 | 2.94 | 8 | 4 | 1 |
| healthy | serval | 4 | 80 | 24 | 56 | 0 | 0 | **1.41** | 7 | 4 | 1 |
| downstream-slow | strict | 4 | 80 | 52 | 21 | 0 | 0 | 4.21 | 67 | 4 | 4 |
| downstream-slow | serval | 4 | 80 | 57 | 19 | 0 | 0 | **2.16** | 60 | 4 | 4 |
| redis-outage | strict | 1 | 20 | 0 | 0 | 0 | 20 | 3.00 | 1 | 0 | 0 |
| redis-outage | serval | 1 | 20 | 0 | 0 | 0 | 20 | 3.05 | 1 | 0 | 0 |
| slow+partition | strict | 4 | 80 | 57 | 0 | 0 | 20 | 4.50 | 51 | 3 | 3 |
| slow+partition | serval | 4 | 80 | 55 | 1 | 0 | 20 | **1.95** | 50 | 3 | 3 |

Tracked occupancy is the peak the bulkhead itself reported, taken from its own `bulkhead.admitted` and `bulkhead.rejected` events for the delegated policy in each profile. The strict path counts one shared number in Redis that every replica reads a view of, so its row is the largest view across replicas rather than a sum; the delegated profiles count per process, and their rows are summed. Seeds are independent runs, so a peak is never summed across them. `unprotected` has no bulkhead, so its 0 is structural rather than measured.

## Paired load

Every profile in a cell and seed sees the same offered schedule, and the seed is what makes that
schedule what it is: the waits between offers come from a seeded generator, so one seed is one
arrival pattern and the same seed is the same pattern in every profile. The lab prints a digest of
the waits it asked for, per cell and seed, which is how the pairing is visible rather than asserted:

```text
paired load: one digest of the offered schedule per cell and seed
  healthy            seed 1: unprotected 4146829710 | strict 4146829710 | serval 4146829710 | serval-guarded 4146829710
  healthy            seed 2: unprotected 446446524 | strict 446446524 | serval 446446524 | serval-guarded 446446524
  warm-outage        seed 1: unprotected 2348087609 | strict 2348087609 | serval 2348087609 | serval-guarded 2348087609
```

Four profiles, one digest per cell: the load was not the difference between them. A different seed
gives a different digest, so `CARACAL_LAB_SEEDS=1,2,3` samples three arrival patterns rather than
repeating one run — which is what makes the arrival-pattern sensitivity below measurable instead of
asserted. The waits are what the worker *asks* for, not what it gets; wall-clock scheduling still
varies, and the digest reports the intended schedule, which is the part that has to match.

## A warm outage

The cold outage cells above answer what a *starting* process does without Redis. A running
process is in the other state: it has been served, and then its path to the authority goes
away. The warm-outage cell measures that — thirty offers per replica at 60ms, the authority
cut after six, one and four replicas, one seed, on the same machine as the run above. The
cold cell from the same run is quoted underneath for contrast.

| Cell | Profile | Rep | Offered | Admitted | of which after the cut | Rate-rej | Failed | Redis/offer | Tracked | Peak physical |
|---|---|---|---|---|---|---|---|---|---|---|
| warm-outage | unprotected | 1 | 30 | 30 | 24 | 0 | 0 | 0.00 | 2 | 0 | 1 |
| warm-outage | strict | 1 | 30 | 6 | **0** | 0 | 24 | 3.40 | 6 | 1 | 1 |
| warm-outage | serval | 1 | 30 | 8 | **2** | 0 | 22 | 2.27 | 3 | 1 | 1 |
| warm-outage | serval-guarded | 1 | 30 | 8 | **2** | 0 | 22 | 2.27 | 3 | 1 | 1 |
| warm-outage | strict | 4 | 120 | 24 | **0** | 0 | 96 | 3.40 | 8 | 3 | 1 |
| warm-outage | serval | 4 | 120 | 29 | **9** | 4 | 87 | 2.28 | 4 | 4 | 1 |
| warm-outage | serval-guarded | 4 | 120 | 30 | **9** | 3 | 87 | 2.28 | 5 | 4 | 1 |
| redis-outage | strict | 1 | 20 | 0 | 0 | 0 | 20 | 3.00 | 1 | 0 | 0 |
| redis-outage | serval | 1 | 20 | 0 | 0 | 0 | 20 | 3.05 | 1 | 0 | 0 |

What the warm numbers say:

- **A warm replica keeps serving from local authority.** The delegated pipeline admitted
  offers after the cut — two per replica with one replica, nine across four — while
  the strict path admitted none, because its coordinators need Redis for every admission and
  hold nothing across an outage.
- **What it holds is a grant, not a reservoir.** Those post-cut admits are exactly what the
  replica was holding: a four-permit grant, a four-slot allocation and a verdict with a
  one-second freshness allowance. The rest of the outage failed, and it failed as failures
  rather than as silent capacity — which is the honest reading of "keeps admitting from the
  authority it holds".
- **The cost and safety claims survive the fault.** Redis commands per offer stayed below the
  strict path (2.27 against 3.40), tracked occupancy stayed within the sum of the local
  ceilings, and physical concurrency stayed at 1.

## What the numbers say

**The admission read really is removed, and that is most of the cost.** On a healthy
dependency one replica pays **1.50 Redis commands per offered request** with Serval
against **5.00** with the strict coordinators - a 70% reduction, and the same
admissions. The remaining cost is what the contract expects: the rate grant and
the bulkhead allocation on their slow paths, plus the breaker's settlement on every
call. Serval does not make settlement free, and this number is the honest measurement
of that.

**Under load the fleet ceiling is what binds, not the coordinator.** With four replicas
and 80 offers the strict and Serval profiles admitted 25 and 24: the rate limit (50/s,
burst 8) refuses the rest. Coordination strategy does not change how much work the fleet
accepts; it changes what that costs.

**A slow dependency is where the profiles diverge.** In this run Serval admitted 57 of 80
where strict admitted 52, with 19 rate refusals against 21 — the opposite direction from an
earlier run of the same shape. That is the honest reading of the mechanism: the delegated
rate limiter spends locally held stock, so *which* path refuses first depends on the arrival
pattern rather than on the strategy being stricter, and the integration suite pins the
mechanism instead of the direction. `CARACAL_LAB_SEEDS` is what samples the pattern. What the
table shows either way is the cost: 2.16 Redis commands per offer against 4.21, for the same
protection.

**A cold start without Redis admits nothing, on both paths.** The cold outage cells start each
worker against an unreachable authority, so no replica holds a verdict, a grant or an
allocation yet and every path has to read before it can serve. The strict coordinators fail
closed because they cannot read, and the delegated layers have nothing local to admit
from. This is a cold-start envelope rather than a settlement effect: observation and
settlement failures are reported as degradation and do not fail an otherwise successful
call. If a cold start during an outage must be survivable, the answer is
a deployment choice: keep a local guardrail in front, or accept the fail-closed behaviour.
This is recorded as an operating envelope rather than smoothed over.

**A warm replica does admit from what it holds, and only while it holds it.** The warm-outage
cell above is the same fault against a replica that has already been served: the delegated
pipeline admitted a few offers after the cut and the strict one admitted none. That is the
difference between a path that caches authority and a path that reads it per admission — and
it is bounded by the local grant, not by the outage.

**A partition is per replica, and the fleet carries on.** With one of four replicas cut
off, the three healthy replicas kept admitting (57 of 80 on both paths) while the isolated
replica's 20 offers failed. Cost stayed lower with Serval (1.95 against 4.50 per offer).
No replica's failure was hidden: it appeared as failures, not as silent capacity.

**Tracked occupancy is measured at the client, physical concurrency at the dependency.**
In these cells the two agree - four replicas tracked 4 slots while the dependency worked
on 4 responses, and 3 against 3 under the partition - because every admitted request is
held until it settles. Two things bound what the column can show. It is reported by the
bulkhead itself, so the strict row is a shared Redis count read from one replica's view
rather than a per-process one; and the driver is a closed loop, one offer at a time per
replica, so the peak is capped by the replica count and the two columns *cannot* diverge
here. They diverge when work outlives its admission: the composition suite pins the
abandoned-attempt case, where the permit stays held until the adapter settles even though
the caller has gone, and the lab's adapter ignores cancellation for the same reason. The
safety claim is about *tracked* occupancy against the sum of the local ceilings; the
physical number is what the dependency actually did.

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
  protection and cost but not queueing behaviour under an open-loop arrival process. It also
  caps tracked occupancy at the replica count, so the tracked and physical columns cannot
  diverge here; that case is pinned by the composition suite instead.
- Grant sizes, TTLs and reclaim grace are held at the values in `bench/serval/worker.ts`
  for comparability; sweeping them is a configuration exercise, not a new measurement.
- The offered schedule is seeded, so profiles are paired on the load they were offered, but the
  waits are what the worker *asks* for rather than what it gets: a loaded machine adds jitter of
  its own, and the digest reports the intended schedule.
- The cells are short: large enough to compare profiles against each other, too small to
  claim steady-state behaviour for a long-running fleet.
- The "real-process partition" cells isolate a replica by pointing it at an unreachable
  Redis, which models a partition of that replica rather than a network-level fault
  between replicas.
- The warm-outage cell cuts a replica's own connection to Redis (`disconnect`, with no offline
  queue and no retries) rather than stopping the server, so it models that replica losing its
  path to the authority. The authority and the other replicas stay up, and commands already in
  flight are not replayed.
- The warm-outage cell's timing is its own (thirty offers at 60ms, cut after six), because a run
  has to outlive the one-second verdict and grant windows for the lapse to appear alongside the
  warm phase.

