# Caracal documentation

Start with the [project README](../README.md) for installation and a first operation, then pick a page:

| Page | What it covers |
|---|---|
| [Core API](core-api.md) | `operation`, the `Adapter` contract, policy composition, options and errors |
| [Timeouts and retries](timeout-and-retry.md) | what a deadline does and does not cover, retry pacing, declined retries |
| [Circuit breaker](circuit-breaker.md) | state machine, sliding window, threshold resolution, probes |
| [Bulkheads](bulkhead.md) | local queueing, distributed leases, admission expiry |
| [Events and observability](events-and-observability.md) | every event, its fields and its `reason` values |
| [Fetch adapter](fetch.md) | capability declaration, response classification, `Retry-After` |
| [Postgres adapter](postgres.md) | statement execution as an operation |
| [Redis coordination](redis.md) | scripts, topology, ACL grants, knob consistency |
| [Writing your own adapter](adapter-contracts.md) | the contract harness and what it checks |
| [Testing](testing.md) | this repository's suites and harnesses |
| [Architecture](architecture.md) | how the pieces fit together |
| [Local development](development.md) | setup, npm scripts, integration environments |
