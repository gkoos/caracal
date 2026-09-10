/** PostgreSQL adapter entry point for node-postgres (`pg`) 8.x clients/pools. */

export type {
  PostgresAdapterOptions,
  PostgresQueryArgs,
  PostgresQueryable,
  PostgresReplay,
} from "./adapters/postgres/index.js"
export { postgresAdapter } from "./adapters/postgres/index.js"
