import { describe, expect, it } from "vitest"

import { operation, retry, TimeoutError, timeout } from "../../src/index.js"
import { postgresAdapter } from "../../src/postgres.js"

class FakePgError extends Error {
  constructor(readonly code: string) {
    super(code)
  }
}

describe("postgresAdapter", () => {
  it.each(["unsafe", "unknown"] as const)(
    "never replays %s queries after transient failure",
    async (replay) => {
      let calls = 0
      const error = new FakePgError("40001")
      const subject = operation({
        name: "no-replay",
        adapter: postgresAdapter({
          query: async () => {
            calls++
            throw error
          },
        }),
        policies: [retry({ maxAttempts: 3 })],
      })
      await expect(subject.execute({ sql: "select 1", replay })).rejects.toBe(
        error,
      )
      expect(calls).toBe(1)
    },
  )
  it("requires explicit replay declarations and accurately reports no portable abort support", () => {
    const adapter = postgresAdapter({
      query: async () => ({
        rows: [],
        rowCount: 0,
        command: "SELECT",
        oid: 0,
        fields: [],
      }),
    })

    expect(adapter.capabilities({ sql: "select 1" })).toEqual({
      abort: "unsupported",
      replay: "unknown",
    })
    expect(adapter.capabilities({ sql: "select 1", replay: "safe" })).toEqual({
      abort: "unsupported",
      replay: "safe",
    })
  })

  it("classifies documented transient SQLSTATE failures as retryable", () => {
    const adapter = postgresAdapter({
      query: async () => ({
        rows: [],
        rowCount: 0,
        command: "SELECT",
        oid: 0,
        fields: [],
      }),
    })

    expect(
      adapter.classify?.({
        status: "failure",
        error: new FakePgError("40001"),
      }),
    ).toBe("retryable")
    expect(
      adapter.classify?.({
        status: "failure",
        error: new FakePgError("08006"),
      }),
    ).toBe("retryable")
    expect(
      adapter.classify?.({
        status: "failure",
        error: new FakePgError("22012"),
      }),
    ).toBe("failure")
  })

  it("retries a serialization failure only when the application declares the query replay-safe", async () => {
    let calls = 0
    const adapter = postgresAdapter({
      query: async () => {
        calls += 1
        if (calls === 1) {
          throw new FakePgError("40001")
        }
        return {
          rows: [{ value: 1 }],
          rowCount: 1,
          command: "SELECT",
          oid: 0,
          fields: [],
        }
      },
    })
    const subject = operation({
      name: "postgres-retry",
      adapter,
      policies: [retry({ maxAttempts: 2 })],
    })

    await expect(
      subject.execute({ sql: "select 1", replay: "safe" }),
    ).resolves.toMatchObject({ rowCount: 1 })
    expect(calls).toBe(2)
  })

  it("times out the caller without claiming that the query was cancelled", async () => {
    let resolveQuery!: (value: {
      rows: never[]
      rowCount: number
      command: string
      oid: number
      fields: never[]
    }) => void
    const query = new Promise<{
      rows: never[]
      rowCount: number
      command: string
      oid: number
      fields: never[]
    }>((resolve) => {
      resolveQuery = resolve
    })
    const subject = operation({
      name: "postgres-timeout",
      adapter: postgresAdapter({ query: async () => query }),
      policies: [timeout({ ms: 5 })],
    })

    await expect(
      subject.execute({ sql: "select pg_sleep(1)" }),
    ).rejects.toBeInstanceOf(TimeoutError)
    resolveQuery({
      rows: [],
      rowCount: 0,
      command: "SELECT",
      oid: 0,
      fields: [],
    })
  })
})
