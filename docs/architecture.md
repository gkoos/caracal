# Architecture

Caracal is a single ESM package with deliberate exports. This is not a multi-package monorepo, package boundaries are deferred until independent release cycles or dependency requirements justify them.

## Subpath exports

`src/` contains only shippable application code. The root export is intentionally small and does not import Redis or the adapters as side effects. The published package also includes `src/` (see the `files` field) so consumers and auditors can read the original TypeScript; runtime entry points remain the built `dist/` files.

| Export | Responsibility |
|---|---|
| `caracal` | Core operation runtime, policies, capabilities, and types |
| `caracal/redis` | Redis-backed distributed coordination (standalone and cluster) |
| `caracal/fetch` | Fetch/HTTP adapter |
| `caracal/postgres` | PostgreSQL adapter |
| `caracal/testing` | Adapter contract test harness |

The core does not depend on any specific protocol or coordinator implementation. Adapters and coordinators are imported only by the subpath that needs them.

## Tree-shaking

- ESM only, no CJS bundle.
- All production modules are side-effect free.
- Optional integrations (Redis, fetch, postgres) are never re-exported from the root entry point.
- Import directly from the subpath: `import { fetchAdapter } from "caracal/fetch"`, not from `"caracal"`.