# Contributing to Caracal

Thank you for your interest in contributing. Please read this guide before opening an issue or pull request.

## Development setup

```sh
git clone https://github.com/gkoos/caracal.git
cd caracal
npm install
```

Services (Redis, Postgres) are managed via Docker Compose:

```sh
npm run redis:up      # start Valkey
npm run postgres:up   # start Postgres
```

The cluster suite manages its own three-master container: `npm run test:integration:cluster`.

## Running tests

```sh
npm test                    # unit suite (test/unit) - fast, no external dependencies
npm run test:property       # property suite (test/property) - CARACAL_TEST_SEED for replay
npm run test:fuzz           # fuzz suite (test/fuzz) - seeded event-history generator
npm run test:integration    # integration suite (test/integration) - services and CARACAL_* URLs required
npm run test:integration:cluster  # cluster suite - starts a local cluster, not run in CI
npm run test:all            # check, then the property, fuzz, and integration suites
```

The cluster suite needs a real cluster, so it is a local gate rather than a CI job.

See [`docs/testing.md`](docs/testing.md) for details on seed-based replay, integration setup, and the cluster suite.

## Code style

Formatting and linting are enforced by [Biome](https://biomejs.dev/). Run before committing:

```sh
npm run format    # auto-fix formatting
npm run lint      # check lint rules
```

The CI will fail on any format or lint violation.

## Submitting changes

1. Fork the repository and create a branch from `main`.
2. Make your changes and ensure `npm run test:all` passes locally.
3. Add a changeset describing your change:

   ```sh
   npm run changeset
   ```

4. Open a pull request against `main`. The CI will run automatically.

Adding a changeset is all that is needed to release a change. Once it reaches `main`, the [Version Packages](.github/workflows/version.yml) workflow opens a release pull request that bumps the version and updates the changelog; merging that pull request publishes to npm, pushes the `v<version>` tag, and creates the GitHub release. Do not bump the version in `package.json` by hand.

## Reporting bugs

Open a [GitHub issue](https://github.com/gkoos/caracal/issues) with a minimal reproduction. For security vulnerabilities, see [`SECURITY.md`](SECURITY.md).
