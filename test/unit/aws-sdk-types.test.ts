import { DynamoDBClient, GetItemCommand } from "@aws-sdk/client-dynamodb"
import { describe, expect, it } from "vitest"

import { awsSdkAdapter } from "../../src/aws-sdk.js"

/**
 * The adapter is typed structurally, so this test is the proof that a real SDK v3
 * client satisfies it. Constructing a client and a command does not make a
 * network call; the capability read is enough to exercise the inferred types.
 */
describe("AWS SDK client compatibility", () => {
  it("accepts a real service client and command without casts", () => {
    const client = new DynamoDBClient({ region: "us-east-1", maxAttempts: 1 })
    const adapter = awsSdkAdapter({
      client,
      command: (args: { table: string; id: string }) =>
        new GetItemCommand({
          TableName: args.table,
          Key: { id: { S: args.id } },
        }),
    })

    expect(adapter.capabilities({ table: "orders", id: "42" })).toEqual({
      abort: "supported",
      replay: "safe",
    })
  })
})
