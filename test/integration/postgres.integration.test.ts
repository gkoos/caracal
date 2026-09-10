import { randomUUID } from "node:crypto"
import { Pool } from "pg"
import { afterAll, beforeAll, describe, expect, it } from "vitest"
import { coordinationKey } from "../../src/coordination/redis/keys.js"
import type { OperationEvent } from "../../src/index.js"
import {
  bulkhead,
  operation,
  retry,
  TimeoutError,
  timeout,
} from "../../src/index.js"
import { postgresAdapter } from "../../src/postgres.js"
import { createCoordinationClient, redisCoordinator } from "../../src/redis.js"
import { defineAdapterContractSuite } from "../harness/index.js"

const connectionString = process.env.CARACAL_POSTGRES_URL
const describePostgres =
  connectionString === undefined ? describe.skip : describe

// This suite runs through `npm run test:integration:postgres` or when callers
// explicitly provide CARACAL_POSTGRES_URL.
describePostgres("postgres adapter integration", () => {
  const pool = new Pool({ connectionString })
  it.skipIf(!process.env.CARACAL_REDIS_URL)(
    "retains a distributed permit for a real timed-out PostgreSQL query",
    async () => {
      const redis = createCoordinationClient(
        process.env.CARACAL_REDIS_URL as string,
      )
      const namespace = `test-${randomUUID()}`
      const key = coordinationKey(
        namespace,
        "bulkhead:pool",
        "postgres-bulkhead",
        "shared",
      )
      const blocker = await pool.connect()
      const lock = Math.floor(Math.random() * 1000000000)
      try {
        await redis.connect()
        await blocker.query("select pg_advisory_lock($1)", [lock])
        const events: OperationEvent[] = []
        const policy = bulkhead.distributed({
          name: "pool",
          limit: 1,
          leaseMs: 600,
          scope: () => "shared",
          coordinator: redisCoordinator(redis, { namespace }),
        })
        const op = operation({
          name: "postgres-bulkhead",
          adapter: postgresAdapter(pool),
          policies: [policy, timeout({ ms: 100 })],
          events: { emit: (e) => events.push(e) },
        })
        await expect(
          op.execute({
            sql: "select pg_advisory_xact_lock($1)",
            values: [lock],
          }),
        ).rejects.toBeInstanceOf(TimeoutError)
        await new Promise((resolve) => setTimeout(resolve, 900))
        expect(await redis.zcard(key)).toBe(1)
        await expect(op.execute({ sql: "select 1" })).rejects.toMatchObject({
          name: "BulkheadRejectedError",
        })
        await blocker.query("select pg_advisory_unlock($1)", [lock])
        await expect
          .poll(() => events.some((e) => e.type === "bulkhead.released"))
          .toBe(true)
        expect(await redis.zcard(key)).toBe(0)
      } finally {
        await blocker.query("select pg_advisory_unlock_all()")
        blocker.release()
        if (redis.status === "ready") await redis.del(key)
        redis.disconnect()
      }
    },
  )

  const suite = defineAdapterContractSuite({
    name: "postgres",
    adapter: postgresAdapter(pool),
    success: {
      args: { sql: "select 42 as value" },
      assertResult: (result) => {
        expect(result.rows).toEqual([{ value: 42 }])
      },
    },
    capabilities: [
      {
        args: { sql: "select 1" },
        expected: { abort: "unsupported", replay: "unknown" },
      },
      {
        args: { sql: "select 1", replay: "safe" },
        expected: { abort: "unsupported", replay: "safe" },
      },
    ],
    abort: {
      args: { sql: "select pg_sleep(0.05)" },
      async verify({ controller, execute }) {
        const pending = execute()
        controller.abort(new Error("caller cancelled"))
        await expect(pending).resolves.toMatchObject({ command: "SELECT" })
      },
    },
  })
  for (const check of suite.checks) it(check.name, check.run)

  it("keeps timed-out work occupying a one-connection pool until underlying settlement", async () => {
    const limited = new Pool({ connectionString, max: 1 })
    const blocker = await pool.connect()
    const events: OperationEvent[] = []
    try {
      await blocker.query("select pg_advisory_lock(734204)")
      const subject = operation({
        name: "postgres-pressure",
        adapter: postgresAdapter(limited),
        policies: [timeout({ ms: 100 })],
        events: { emit: (event) => events.push(event) },
      })
      await expect(
        subject.execute({ sql: "select pg_advisory_xact_lock(734204)" }),
      ).rejects.toBeInstanceOf(TimeoutError)
      expect(events.some((event) => event.type === "attempt.settled")).toBe(
        false,
      )
      const queued = limited.query("select 1")
      expect(limited.waitingCount).toBe(1)
      await blocker.query("select pg_advisory_unlock(734204)")
      await queued
      expect(
        events.filter((event) => event.type === "attempt.settled"),
      ).toHaveLength(1)
      expect(
        events.map((event) => event.type).indexOf("execution.settled"),
      ).toBeLessThan(
        events.map((event) => event.type).indexOf("attempt.settled"),
      )
    } finally {
      await blocker.query("select pg_advisory_unlock_all()")
      blocker.release()
      await limited.end()
    }
  })

  it("classifies real SQL errors and bounds retries for explicitly safe work", async () => {
    const events: OperationEvent[] = []
    const subject = operation({
      name: "postgres-errors",
      adapter: postgresAdapter(pool),
      policies: [retry({ maxAttempts: 2 })],
      events: { emit: (event) => events.push(event) },
    })
    await expect(
      subject.execute({
        sql: "do $$ begin raise exception 'test serialization' using errcode = '40001'; end $$",
        replay: "safe",
      }),
    ).rejects.toMatchObject({ code: "40001" })
    expect(
      events.filter((event) => event.type === "attempt.started"),
    ).toHaveLength(2)
    events.length = 0
    await expect(
      subject.execute({ sql: "select 1/0", replay: "safe" }),
    ).rejects.toMatchObject({ code: "22012" })
    expect(
      events.filter((event) => event.type === "attempt.started"),
    ).toHaveLength(1)
  })

  it("reports loss of its own database connection without replaying unknown work", async () => {
    const client = await pool.connect()
    client.on("error", () => {})
    const events: OperationEvent[] = []
    try {
      const subject = operation({
        name: "postgres-disconnect",
        adapter: postgresAdapter(client),
        policies: [retry({ maxAttempts: 3 })],
        events: { emit: (event) => events.push(event) },
      })
      await expect(
        subject.execute({
          sql: "select pg_terminate_backend(pg_backend_pid())",
        }),
      ).rejects.toMatchObject({ code: "57P01" })
      expect(
        events.filter((event) => event.type === "attempt.started"),
      ).toHaveLength(1)
      expect(
        events.find((event) => event.type === "attempt.settled"),
      ).toMatchObject({ classification: "retryable" })
    } finally {
      client.release(true)
    }
  })

  beforeAll(async () => {
    await pool.query("select 1")
  })

  afterAll(async () => {
    await pool.end()
  })

  it("executes a real query through the generic operation runtime", async () => {
    const subject = operation({
      name: "postgres-integration",
      adapter: postgresAdapter(pool),
    })

    const result = await subject.execute({
      sql: "select $1::int as value",
      values: [42],
      replay: "safe",
    })

    expect(result.rows).toEqual([{ value: 42 }])
  })
})
