# Testing

## Test suites

```sh
npm test                    # unit suite (test/unit) - fast, no external dependencies
npm run test:property       # property suite (test/property) - fast-check, honouring CARACAL_TEST_SEED
npm run test:fuzz           # fuzz suite (test/fuzz) - seeded random event-history generator
npm run test:generated      # both generated suites in one run, under one seed and one depth
npm run test:generated:deep # the same suites at a deeper case count (CARACAL_TEST_RUNS)
npm run test:integration    # integration suite (test/integration) - requires services and CARACAL_* URLs
npm run test:all            # check, then the generated and integration suites
```

`npm test` runs `test/unit` only; the other suites are selected explicitly by the commands above.

**Unit** tests cover the core operation runtime, all policies, and both adapters without any external dependencies. They run in under 3 seconds.

**Property** tests use fast-check to verify invariants across generated histories: the circuit breaker state machine, the local GCRA rate limiter (admission arithmetic, burst envelope, the exact `retryAfterMs`, and `snapshot()`), and local bulkhead permit accounting (occupancy, queue timeouts, lease reclaim, FIFO handoff). They derive from a random seed by default and honour `CARACAL_TEST_SEED` for replay.

**Fuzz** tests run a seeded random event-history generator against the circuit breaker to confirm no invalid state transitions occur under adversarial sequences. Both generated suites are built with `test/support/seed.ts`, which owns the seed, the depth multiplier, and the replay instruction every failure prints.

**Integration** tests run against real Valkey and PostgreSQL instances and exercise multi-process coordination scenarios that cannot be verified in-process. Each file skips itself unless its environment variable is set: `CARACAL_REDIS_URL` for Redis, `CARACAL_REDIS_CLUSTER_URLS` for cluster, and `CARACAL_POSTGRES_URL` for PostgreSQL. Starting the containers alone does not set them - see [Local development](development.md).

## Reproducible generated tests

Property and fuzz tests report their seed on failure. Replay a specific run:

```sh
CARACAL_TEST_SEED=123456 npm run test:generated
CARACAL_TEST_SEED=123456 npm run test:fuzz
CARACAL_TEST_SEED=123456 npm run test:property
```

On PowerShell:

```powershell
$env:CARACAL_TEST_SEED = "123456"
npm run test:generated
```

Every failure message includes the seed value and the exact replay command. With no seed set, each run draws a new one: the suites explore a different slice of the contract instead of re-checking a frozen history.

### Depth

`CARACAL_TEST_RUNS` multiplies every generated case count, so a suite can be run deeper without a second copy of the test. It defaults to `1`, which is what CI runs on every pull request:

```sh
CARACAL_TEST_RUNS=10 npm run test:generated
npm run test:generated:deep          # the same, defaulting to a depth of 10
```

A deep failure needs both variables to replay - the failure message prints them together:

```sh
CARACAL_TEST_SEED=123456 CARACAL_TEST_RUNS=10 npm run test:generated
```

`.github/workflows/nightly.yml` runs the deep suites every night with `CARACAL_TEST_SEED` set to the workflow run number, so each night draws different cases under a seed that can be replayed on demand. Scheduled runs never gate a pull request: a failure there is a signal to investigate, not a merge blocker.

### The mutation rule

A new invariant must be able to fail, so before opening a pull request revert the behaviour it guards - locally, never in the commit - and confirm the suite goes red. The rate-limit properties, for example, go red when `burstDelayMs` loses its `burst - 1` factor, when `round` becomes `floor`, or when `retryAfterMs` is off by one millisecond; the bulkhead properties go red when the released permit is reported after the handoff, when a lease stops reclaiming its holder, or when the queue hands off out of order.

## Integration test environments

See [Local development](development.md) for instructions on starting Valkey and PostgreSQL via Docker Compose, and for using `CARACAL_REDIS_URL` and `CARACAL_POSTGRES_URL` to point at existing instances.

## Redis cluster integration tests

Cluster coordination is verified against a real cluster. It is **not** part of CI: provisioning a cluster in a hosted runner is flaky, so it is a local gate instead.

```sh
npm run test:integration:cluster
```

That starts a three-master Valkey cluster with Docker Compose, sets `CARACAL_REDIS_CLUSTER_URLS`, and runs the suite. See [Local development](development.md) for the container details and for pointing the suite at an existing cluster.

The suite checks that every key of one breaker identity resolves to the same slot via `CLUSTER KEYSLOT`, that a distributed bulkhead rejects a second concurrent call at `limit: 1`, and that a distributed breaker opens, probes, and closes through a cluster client.

## Multi-process harness

The Redis integration tests use a multi-process harness that forks independent Node worker processes with IPC commands and readiness barriers. The parent records replies and reads Redis independently from the workers. It can:

- Kill workers mid-operation to verify lease expiry and orphan recovery
- Freeze worker event loops to simulate GC pauses and stalls
- Delay or drop Redis traffic through a test-only TCP proxy to verify timeout and reconnect behaviour

Integration tests cover: repeated concurrent admission, owner death and stall, lease expiry, renewal failure, stale releases, scoped cleanup, reconnects, circuit breaker probe races, stale generation rejection, and coordinator disconnection under both fail-open and fail-closed configurations.

The coordinator conformance suite additionally pins the Lua state machine itself: threshold boundary arithmetic, probe accounting and token TTL policy, then a model-based equivalence run that replays generated command sequences against both the in-memory coordinator and Redis and compares every result plus the final state. Divergences print the exact script and a replay instruction.

A script-transport group asserts the Redis transport directly: steady-state calls go out as `EVALSHA` with a SHA1 that matches the server's script cache, a cache miss falls back to `EVAL` exactly once and then returns to `EVALSHA`, and a flushed script cache never surfaces `NOSCRIPT` to a caller. Commands are attributed with a recording wrapper around the client, so the assertions do not depend on what else is running against the instance.

An ACL group does the same for permissions: it creates a Redis user with exactly the grant documented in [Redis coordination](redis.md#acl-recommendations), connects as that user, and runs a bulkhead lease plus the whole breaker lifecycle (observe to open, probe to half-open, settle to closed, settle back to open, and an epoch mint after an out-of-band `DEL`), verifies the `EVAL` fallback, and checks the grant stays narrow. A command the implementation starts using without a matching documentation row fails this suite by name. It skips itself when the target server cannot manage ACL users.

Every run uses unique randomly-generated key namespaces and deletes only its own keys. There is no `FLUSHDB`. One script-transport test deliberately runs `SCRIPT FLUSH` - the script cache only, never data - to prove that a cache miss does not surface `NOSCRIPT` to callers.
