---
"@gkoos/caracal": minor
---

Add `@gkoos/caracal/serval`: delegated coordination for the distributed circuit breaker, rate limiter
and bulkhead.

Each replica holds a bounded piece of the server's authority - a cached verdict, a quota grant, a slot
allocation - and enforces it locally, so an admission is usually decided without a Redis round trip.
The strict coordinators are unchanged and stay the default: opting in is a coordinator argument, and
nothing is loaded unless this entry point is imported.

What that buys, and what it costs, is measured rather than asserted. On a healthy dependency one
replica pays **1.50 Redis commands per offered request** against **5.00** with the strict coordinators,
for the same admissions. In exchange the strategy is explicitly approximate, and each deviation is
documented: a cached verdict is a fleet-wide opening seen through a per-replica window, unused quota
expires rather than being credited into a later window, and a replica that cannot renew keeps spending
the stock it already holds until its conservative deadline. Outage recovery, capacity return and
ambiguous authority are all specified rather than left to the implementation.

The entry point is **experimental**: it is exported so the strategy can be evaluated and reviewed, and
its API may change in a minor release until the operating envelope settles.

- [Serval: delegated coordination](docs/serval/README.md) - what it is, and the guarantees it keeps
- [Authority protocol](docs/serval/authority.md) - the normative contracts and their state machines
- [API reference](docs/serval/api.md) - exports, options, lifecycle, metrics and migration
- [Fault lab](docs/serval/benchmarks.md) - the measured trade-offs and known limitations
