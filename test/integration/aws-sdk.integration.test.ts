import { randomUUID } from "node:crypto"

import {
  CreateTableCommand,
  DeleteTableCommand,
  DynamoDBClient,
  GetItemCommand,
  PutItemCommand,
} from "@aws-sdk/client-dynamodb"
import {
  CreateQueueCommand,
  DeleteQueueCommand,
  GetQueueAttributesCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
  SQSClient,
} from "@aws-sdk/client-sqs"
import { afterAll, beforeAll, describe, expect, it } from "vitest"

import type { OperationEvent } from "../../src/index.js"
import { operation, retry, TimeoutError, timeout } from "../../src/index.js"
import { awsSdkAdapter } from "../../src/aws-sdk.js"
import { startAwsFaultProxy } from "../support/aws-fault-proxy.js"

const awsEndpoint = process.env.CARACAL_AWS_ENDPOINT
const describeAws = awsEndpoint === undefined ? describe.skip : describe

function clientConfig(target: string) {
  return {
    endpoint: target,
    region: "us-east-1",
    // Caracal owns retries; an SDK retry would hide attempts from the policies.
    maxAttempts: 1,
    credentials: { accessKeyId: "test", secretAccessKey: "test" },
  }
}

function sqsConfig(target: string) {
  return {
    ...clientConfig(target),
    // The explicit endpoint already wins in the SDK's queue-url middleware, but
    // disable the QueueUrl override explicitly so a future change cannot make
    // these tests bypass the fault proxy.
    useQueueUrlAsEndpoint: false,
  }
}

async function waitFor(
  condition: () => boolean,
  timeoutMs: number,
): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    if (condition()) return
    await new Promise((resolve) => setTimeout(resolve, 25))
  }
  throw new Error("condition was not met before the deadline")
}

async function waitForEndpoint(target: string): Promise<void> {
  const deadline = Date.now() + 20_000
  while (Date.now() < deadline) {
    try {
      const response = await fetch(`${target}/_localstack/health`)
      if (response.ok) return
    } catch {
      // Not listening yet.
    }
    await new Promise((resolve) => setTimeout(resolve, 200))
  }
  throw new Error(`AWS endpoint ${target} did not become ready`)
}

describeAws("AWS SDK adapter integration", () => {
  const target = awsEndpoint ?? "http://127.0.0.1:4566"
  const suffix = randomUUID().slice(0, 8)
  const queueName = `caracal-${suffix}`
  const tableName = `caracal-${suffix}`
  let queueUrl = ""

  const sqs = new SQSClient(sqsConfig(target))
  const dynamo = new DynamoDBClient(clientConfig(target))

  beforeAll(async () => {
    await waitForEndpoint(target)
    const queue = await sqs.send(
      new CreateQueueCommand({ QueueName: queueName }),
    )
    queueUrl = queue.QueueUrl ?? ""
    await dynamo.send(
      new CreateTableCommand({
        TableName: tableName,
        KeySchema: [{ AttributeName: "id", KeyType: "HASH" }],
        AttributeDefinitions: [{ AttributeName: "id", AttributeType: "S" }],
        ProvisionedThroughput: {
          ReadCapacityUnits: 1,
          WriteCapacityUnits: 1,
        },
      }),
    )
  }, 30_000)

  afterAll(async () => {
    await sqs
      .send(new DeleteQueueCommand({ QueueUrl: queueUrl }))
      .catch(() => undefined)
    await dynamo
      .send(new DeleteTableCommand({ TableName: tableName }))
      .catch(() => undefined)
    sqs.destroy()
    dynamo.destroy()
  })

  it("executes a real DynamoDB command", async () => {
    const subject = operation({
      name: "aws-dynamo-get",
      adapter: awsSdkAdapter({
        client: dynamo,
        command: (args: { id: string }) =>
          new GetItemCommand({
            TableName: tableName,
            Key: { id: { S: args.id } },
          }),
      }),
    })

    const result = await subject.execute({ id: "missing" })
    expect(result.Item).toBeUndefined()
  })

  it("classifies a real ConditionalCheckFailedException as permanent", async () => {
    const put = () =>
      new PutItemCommand({
        TableName: tableName,
        Item: { id: { S: "duplicate" } },
        ConditionExpression: "attribute_not_exists(id)",
      })
    await dynamo.send(put())

    const events: OperationEvent[] = []
    const subject = operation({
      name: "aws-dynamo-conditional",
      adapter: awsSdkAdapter({
        client: dynamo,
        command: put,
        // The caller declares the conditional write safe; the condition failure
        // is still permanent for this operation.
        replay: "safe",
      }),
      policies: [retry({ maxAttempts: 3 })],
      events: { emit: (event) => events.push(event) },
    })

    await expect(subject.execute({})).rejects.toMatchObject({
      name: "ConditionalCheckFailedException",
    })
    expect(
      events.filter((event) => event.type === "retry.declined"),
    ).toMatchObject([{ reason: "not-retryable" }])
  })

  it("cancels an in-flight request on timeout", async () => {
    const proxy = await startAwsFaultProxy({
      upstream: target,
      mode: { kind: "hang" },
    })
    const client = new SQSClient(sqsConfig(proxy.url))
    try {
      const subject = operation({
        name: "aws-abort",
        adapter: awsSdkAdapter({
          client,
          command: () =>
            new ReceiveMessageCommand({
              QueueUrl: queueUrl,
              WaitTimeSeconds: 20,
            }),
        }),
        policies: [timeout({ ms: 100 })],
      })

      await expect(subject.execute({})).rejects.toBeInstanceOf(TimeoutError)
      await waitFor(() => proxy.aborted === 1, 2_000)
      expect(proxy.aborted).toBe(1)
    } finally {
      await proxy.close()
      client.destroy()
    }
  })

  it("makes one HTTP request per Caracal attempt", async () => {
    const proxy = await startAwsFaultProxy({
      upstream: target,
      mode: { kind: "fail-before", requests: 1 },
    })
    const client = new DynamoDBClient(clientConfig(proxy.url))
    try {
      const subject = operation({
        name: "aws-retry-read",
        adapter: awsSdkAdapter({
          client,
          command: () =>
            new GetItemCommand({
              TableName: tableName,
              Key: { id: { S: "missing" } },
            }),
        }),
        policies: [retry({ maxAttempts: 2 })],
      })

      const result = await subject.execute({})
      expect(result.Item).toBeUndefined()
      // Two attempts, two HTTP requests: the SDK did not retry inside either.
      expect(proxy.requests).toBe(2)
    } finally {
      await proxy.close()
      client.destroy()
    }
  })

  it("does not replay an SQS send the upstream accepted", async () => {
    const proxy = await startAwsFaultProxy({
      upstream: target,
      mode: { kind: "fail-after-upstream", requests: 1 },
    })
    const client = new SQSClient(sqsConfig(proxy.url))
    const events: OperationEvent[] = []
    try {
      const subject = operation({
        name: "aws-send-once",
        adapter: awsSdkAdapter({
          client,
          command: () =>
            new SendMessageCommand({
              QueueUrl: queueUrl,
              MessageBody: `once-${suffix}`,
            }),
        }),
        policies: [retry({ maxAttempts: 3 })],
        events: { emit: (event) => events.push(event) },
      })

      await expect(subject.execute({})).rejects.toBeDefined()
      expect(proxy.requests).toBe(1)
      expect(
        events.filter((event) => event.type === "retry.declined"),
      ).toMatchObject([{ reason: "replay-unsafe" }])

      const attributes = await sqs.send(
        new GetQueueAttributesCommand({
          QueueUrl: queueUrl,
          AttributeNames: ["ApproximateNumberOfMessages"],
        }),
      )
      expect(attributes.Attributes?.ApproximateNumberOfMessages).toBe("1")
    } finally {
      await proxy.close()
      client.destroy()
    }
  })
})
