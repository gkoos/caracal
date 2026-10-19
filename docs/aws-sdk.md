# AWS SDK v3 adapter

`@gkoos/caracal/aws-sdk` wraps any AWS SDK v3 client. It has **no runtime dependency on the SDK**: the client is structural, so the adapter only needs `send(command, { abortSignal })`, and you install only the service packages you use.

```ts
import { DynamoDBClient, GetItemCommand } from "@aws-sdk/client-dynamodb"
import { circuitBreaker, operation, retry, timeout } from "@gkoos/caracal"
import { awsSdkAdapter } from "@gkoos/caracal/aws-sdk"

const dynamo = new DynamoDBClient({ region: "eu-west-1", maxAttempts: 1 })

const items = operation({
  name: "dynamo-items",
  adapter: awsSdkAdapter({
    client: dynamo,
    command: (args: { table: string; id: string }) =>
      new GetItemCommand({
        TableName: args.table,
        Key: { id: { S: args.id } },
      }),
  }),
  policies: [circuitBreaker.local({ name: "dynamo" }), timeout({ ms: 1_000 }), retry({ maxAttempts: 2 })],
})

const result = await items.execute({ table: "orders", id: "42" })
```

## Adapter options

```ts
awsSdkAdapter({
  client: dynamo,                         // any SDK v3 client
  command: (args) => new GetItemCommand(args), // one command per attempt
  replay: "safe",                         // optional override of the per-command default
  classifyError: (error) => "failure",    // optional service-specific addition
  dispose: (outcome, context) => {},      // optional cleanup after the default stream disposal
})
```

| Option | Required | Purpose |
|---|---|---|
| `client` | yes | The SDK client. Only `send()` is used. |
| `command` | yes | Builds the command. Called during capability resolution when `replay` is not fixed, and again per attempt; keep it pure and cheap. |
| `replay` | no | Fixed value or `(args) => replay`; overrides the command table. |
| `classifyError` | no | Returns a classification for a thrown error, overriding the shared classifier. |
| `dispose` | no | Runs after the built-in `Body`/`Payload` stream cleanup for an abandoned result. |

The options are typed as `AwsSdkAdapterOptions<Args, Command>`. The client is any `AwsSdkClient` - a structural `send` contract - and `AwsSdkReplay` is the fixed or per-args replay declaration. No SDK type is imported by the package.

## Commands

`command(args)` is called once for every attempt, and once more during capability resolution when `replay` is not a fixed value. A single-attempt execution therefore constructs two commands, and a policy that rejects admission can construct one without sending it. Keep the factory pure and cheap: no side effects, no one-shot state, and no idempotency token generation. Put idempotency tokens in `args`, not in the factory:

```ts
const token = randomUUID()
await orders.execute({ token, ... })

command: (args) => new CreateOrderCommand({ ClientToken: args.token, ... })
```

If the factory generated a new token per attempt, every retry would be a different operation and the token would protect nothing.

## Cancellation

The adapter declares `abort: "supported"` and passes `{ abortSignal: context.signal }` to `send`. A timeout or caller abort therefore reaches the SDK's HTTP handler and cancels the in-flight request. A custom `requestHandler` must honour the signal for that claim to remain true.

## SDK retries

Set **`maxAttempts: 1`** on every client used with Caracal's `retry` policy:

```ts
new DynamoDBClient({ region: "eu-west-1", maxAttempts: 1 })
```

The SDK retries inside a single `send()` by default (three attempts in standard mode). Caracal would see one attempt, the breaker would record one observation for up to three HTTP requests, and `retry.declined` would no longer describe the whole retry behaviour. The adapter does not change your client configuration; this is the caller's contract.

## Replay safety

Capabilities are decided per command. A blanket `replay: "safe"` would duplicate non-idempotent sends; a blanket `"unsafe"` would refuse to retry a read. The default table covers common commands:

| Command class | SDK commands | Default `replay` |
|---|---|---|
| Read-only | `GetItem`, `BatchGetItem`, `Query`, `Scan`, `GetObject`, `HeadObject`, `HeadBucket`, `ListObjectsV2`, `ListBuckets`, `ListTables`, `DescribeTable`, `GetQueueUrl`, `GetQueueAttributes` | `"safe"` |
| Non-idempotent | `SendMessage`, `SendMessageBatch`, `Publish`, `PublishBatch`, `StartExecution`, `StartSyncExecution`, `StartWorkflowExecution`, `Invoke`, `InvokeAsync`, `PutEvents`, `PutRecord`, `PutRecords`, `SendEmail`, `SendRawEmail`, `SendTemplatedEmail` | `"unsafe"` |
| Everything else | `PutItem`, `UpdateItem`, `DeleteItem`, `Create*`, `ReceiveMessage`, `ChangeMessageVisibility`, … | `"unknown"` |

`"unknown"` blocks retry exactly like `"unsafe"`. Override it when your application has the guarantee:

```ts
awsSdkAdapter({
  client: dynamo,
  command: (args) => new PutItemCommand(args),
  replay: (args) => (args.conditional ? "safe" : "unknown"),
})
```

### Conditional writes

A conditional `PutItem` can be safe to repeat, but the second attempt may return `ConditionalCheckFailedException` after the first write succeeded. The adapter classifies that as `"failure"` because it cannot distinguish "already applied" from a genuine conflict. Declare `replay: "safe"` only if the caller treats that error as success for this operation:

```ts
try {
  await items.execute({ ...put })
} catch (error) {
  if (error instanceof ConditionalCheckFailedException) return "already applied"
  throw error
}
```

Client tokens (`ClientToken`, idempotency keys) are the cleaner option where the service supports them: the service returns the original result for a repeated token.

`constructor.name` is the lookup key, so a bundler that renames command classes can defeat the table. Pass `replay` explicitly in that case. The adapter never infers a guarantee from the command's input.

## Classification

The shared classifier reads structured error data, never message text:

| Signal | Classification |
|---|---|
| `$retryable` present on a modeled error, including an empty `{}` | `retryable` |
| `ThrottlingException`, `ThrottledException`, `Throttling`, `RequestThrottled`, `RequestThrottledException`, `ProvisionedThroughputExceededException`, `RequestLimitExceeded`, `TooManyRequestsException`, `SlowDown` | `retryable` |
| `RequestTimeout`, `RequestTimeoutException`, `TimeoutError`, `InternalServerError`, `InternalFailure`, `ServiceUnavailable` | `retryable` |
| HTTP `408`, `429`, or any `5xx` fallback | `retryable` |
| `ConditionalCheckFailedException`, `ValidationException`, `AccessDeniedException`, `UnauthorizedOperation`, `ResourceNotFoundException`, `NoSuchKey`, `NoSuchBucket`, `EntityAlreadyExistsException`, `InvalidParameterValue`, `SerializationException` | `failure` |
| `AbortError` | `failure` |
| Unknown 4xx, unknown shape, non-object throw | `failure` |

`$metadata.httpStatusCode` is a fallback, not the primary signal: a `400` can be throttling or validation depending on the service, so the modeled `name` and `$retryable` are checked first. `$fault` (`"server"` / `"client"`) is used when neither is present. Override with `classifyError` for service-specific errors:

```ts
awsSdkAdapter({
  client: dynamo,
  command: (args) => new TransactWriteItemsCommand(args),
  classifyError: (error) =>
    error instanceof TransactionCanceledException ? "failure" : awsSdkErrorClassification(error),
})
```

`awsSdkErrorClassification` is exported for exactly this composition.

## Streaming responses

When `retry` or `timeout` abandons a settled result, the adapter destroys a `Body` stream (`destroy()`) or a `Payload` stream (`cancel()` / `destroy()`) before running your `dispose` hook. This covers S3 `GetObject` and Lambda `Invoke`: without it, every retried success would hold a socket and buffer until garbage collection.

```ts
awsSdkAdapter({
  client: s3,
  command: (args) => new GetObjectCommand(args),
  dispose: (outcome) => {
    // The body has already been destroyed; do any application-level cleanup here.
  },
})
```

If you consume the body inside a custom adapter, this default never runs because the adapter promise does not settle until you return.

## Why capabilities are per command

Caracal's rule is that capabilities are declared, never inferred. An SDK-wide declaration would be a lie in one direction: marking `SendMessage` safe duplicates messages, and marking `GetItem` unsafe refuses to retry a read that is always safe to repeat. `capabilities(args)` exists for exactly this case, so the AWS adapter derives `replay` from the command and lets the caller override it where the guarantee depends on application context.

## Running the integration tests

```sh
npm run test:integration:aws
```

Requires Docker. The helper starts Floci (`floci/floci`, port `4566`), sets `CARACAL_AWS_ENDPOINT`, and runs the suite against real SQS and DynamoDB commands. Floci accepts any non-empty credentials; the suite uses `test`/`test`, so no real AWS account or credential is involved. The service is left running for inspection:

```sh
npm run floci:down
```

To use an existing endpoint, export `CARACAL_AWS_ENDPOINT` and run the suite directly; the helper then skips Compose. The suite covers the real SDK wire path: endpoint wiring, abort over a socket, one HTTP request per Caracal attempt, duplicate suppression for a non-idempotent send, and classification of errors produced by the emulator.

