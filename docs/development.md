# Local development

## Requirements

- Node.js 20 or newer.
- Docker Compose when running Redis/Valkey or PostgreSQL integration tests.

## Setup and checks

```sh
npm install
npm run check
```

`check` runs formatting verification, linting, type checking, the production build, and the unit test suite. It runs `precheck` first, which verifies the Node.js version against `engines` (`npm run node:check`). Use `npm run format` to apply the repository formatter instead of only checking it.

## npm scripts

| Script | Description |
|---|---|
| `npm run build` | Builds the ESM bundles, type declarations, and source maps into `dist/` with tsup. |
| `npm run clean` | Deletes `dist/` and `coverage/`. |
| `npm run typecheck` | Type-checks without emitting, using `tsc --noEmit`. |
| `npm run format` | Applies the Biome formatter to the repository. |
| `npm run format:check` | Verifies formatting without writing changes; used by CI. |
| `npm run lint` | Runs the Biome lint rules. |
| `npm run node:check` | Fails unless the running Node.js version satisfies `engines` (>= 20). |
| `npm run precheck` | Runs `node:check` automatically before `check`. |
| `npm test` | Unit suite (`test/unit`) - fast, no external dependencies. |
| `npm run test:watch` | Vitest in watch mode. |
| `npm run test:property` | Property suite (`test/property`); honours `CARACAL_TEST_SEED`. |
| `npm run test:fuzz` | Fuzz suite (`test/fuzz`); seeded random event-history generator. |
| `npm run test:integration` | Integration suite (`test/integration`); requires services and `CARACAL_*` URLs. |
| `npm run check` | `format:check` -> `lint` -> `typecheck` -> `build` -> unit tests. |
| `npm run test:all` | `check`, then the property, fuzz, and integration suites. |
| `npm run bench` | Builds, then runs the local-policy baseline and the Redis `EVAL`/`EVALSHA` script-transport comparison. |
| `npm run audit:bundle` | Builds, then asserts the public exports and the root bundle's dependencies. |
| `npm run changeset` | Records a changeset describing a pending release. |
| `npm run version` | Applies pending changesets: bumps the version and updates `CHANGELOG.md`. |
| `npm run redis:up` | Starts the disposable Valkey container. |
| `npm run redis:logs` | Follows the Valkey container logs. |
| `npm run redis:down` | Stops the Compose services. |
| `npm run test:integration:redis` | Starts Valkey unless `CARACAL_REDIS_URL` is set, runs the Redis integration files, and leaves the service running. |
| `npm run postgres:up` | Starts the PostgreSQL container and waits for readiness. |
| `npm run postgres:down` | Stops the Compose services. |
| `npm run test:integration:postgres` | Starts PostgreSQL, sets `CARACAL_POSTGRES_URL`, and runs the full integration suite. |
| `npm run test:integration:cluster` | Starts a three-master Valkey cluster, sets `CARACAL_REDIS_CLUSTER_URLS`, and runs the cluster suite. |
| `npm run redis:cluster:up` | Starts the local Valkey Cluster container (ports 7000-7002). |
| `npm run redis:cluster:logs` | Follows the Valkey Cluster container logs. |
| `npm run redis:cluster:down` | Stops the Valkey Cluster container. |

## Test commands

```sh
npm test                    # unit suite (test/unit) - fast, no external dependencies
npm run test:property       # property suite (test/property) - fast-check, CARACAL_TEST_SEED for replay
npm run test:fuzz           # fuzz suite (test/fuzz) - seeded random event-history generator
npm run test:integration    # integration suite (test/integration); services and CARACAL_* URLs required
npm run test:all            # check, then the property, fuzz, and integration suites
```

Each script selects its own directory (see the table above), so the fast unit suite runs without Docker. Integration files skip unless their environment variable is set.

## Reproducible generated tests

Property and fuzz tests report a seed in their failure output. Replay with the same command and `CARACAL_TEST_SEED`:

```sh
CARACAL_TEST_SEED=123456 npm run test:property
CARACAL_TEST_SEED=123456 npm run test:fuzz
```

On PowerShell:

```powershell
$env:CARACAL_TEST_SEED = "123456"
npm run test:fuzz
```

Every failure message includes the seed value and the exact replay command.

## Redis/Valkey integration environment

The local Compose file starts a disposable Valkey instance on `localhost:6379`; its data is held in a container tmpfs and disappears when the container stops.

Redis integration tests need `CARACAL_REDIS_URL`. Starting the container alone does not set it, so pass it explicitly or use the helper script:

```sh
npm run redis:up
CARACAL_REDIS_URL=redis://127.0.0.1:6379 npm run test:integration
npm run redis:down
```

`npm run test:integration:redis` starts Valkey with Compose, sets `CARACAL_REDIS_URL`, and runs the Redis integration suite, but intentionally leaves the service running so failures can be inspected. Use `npm run redis:logs` or `npm run redis:down` afterwards.

To run against an existing test server without starting Compose, export the URL yourself:

```sh
CARACAL_REDIS_URL=redis://redis.internal:6379 npm run test:integration
```

## PostgreSQL integration environment

```sh
npm run postgres:up
npm run test:integration:postgres
npm run postgres:down
```

`npm run test:integration:postgres` starts PostgreSQL with Compose, sets `CARACAL_POSTGRES_URL` (default `postgresql://caracal:caracal@127.0.0.1:5432/caracal`), and runs the full integration suite. Redis-dependent tests inside that suite still need `CARACAL_REDIS_URL`; set it to include them. The helper leaves the service running for inspection.

`CARACAL_POSTGRES_URL` overrides the connection string, but the helper still starts the local Compose service. To run against an existing instance without Compose, export the URL and use `npm run test:integration` directly.

## Redis Cluster integration environment

The cluster suite needs a real cluster, so it is a local gate rather than a CI job:

```sh
npm run test:integration:cluster
```

That starts a three-master Valkey cluster in one container (ports 7000-7002), sets `CARACAL_REDIS_CLUSTER_URLS`, runs `test/integration/redis-cluster.integration.test.ts`, and leaves the cluster running for inspection:

```sh
npm run redis:cluster:logs   # optional
npm run redis:cluster:down
```

`docker/cluster/start.sh` starts three `valkey-server` processes, forms the cluster with `valkey-cli --cluster create`, and announces `127.0.0.1:<port>` so redirects stay routable from the host.

To use an existing cluster, export the seed nodes and run the helper (it then skips Compose):

```sh
CARACAL_REDIS_CLUSTER_URLS=host-a:7000,host-b:7000,host-c:7000 npm run test:integration:cluster
```

## Benchmarks

```sh
npm run bench
```

Runs the baseline local-policy benchmark after a fresh build. Output shows median and p99 latency in µs and throughput in operations/second for each policy combination. These numbers are a floor for performance regression detection, not load benchmarks.

It then compares `EVAL` against `EVALSHA` for the Lua scripts the coordinators actually ship. The Lua bodies are captured from the wire through a byte-counting proxy, so the benchmark cannot drift from the implementation; it reports request bytes per call (measured from the socket), sequential and burst throughput, and server `usec_per_call` from `INFO commandstats`. This section needs a reachable Redis - start one with `npm run redis:up`, or point it elsewhere with `CARACAL_REDIS_URL` or `--redis-url=redis://host:6379`. Without one it prints a skip notice and the local numbers are unaffected. See [Redis coordination](redis.md) for the transport trade-off it quantifies.

## Bundle audit

```sh
npm run audit:bundle
```

Builds the package, then asserts the public surface: the root entry exports exactly the documented symbols, `dist/redis.js` exports the Redis coordinators, and `@gkoos/caracal/testing` exports the adapter contract runner. It also fails if `ioredis` or `pg` references leak into the root bundle. Run it after changing exports or adding dependencies.

## Releases

Releases are managed with [Changesets](https://github.com/changesets/changesets):

```sh
npm run changeset
```

Describe the change, pick a bump type, and commit the generated file in `.changeset/` alongside your code. Do not edit the version in `package.json` by hand.

On `main`, the `Version Packages` workflow (`.github/workflows/version.yml`) runs `npm run version`, which applies the pending changesets, and opens or updates a release pull request. Merging that pull request triggers `.github/workflows/publish.yml`, which publishes to npm with provenance, pushes the `v<version>` tag, and creates the GitHub release. The tag push is what downstream announcement workflows listen for.


