import {
  ConditionalCheckFailedException,
  ProvisionedThroughputExceededException,
} from "@aws-sdk/client-dynamodb"
import { describe, expect, it, vi } from "vitest"

import type { ExecutionContext, OperationEvent } from "../../src/index.js"
import { operation, retry, TimeoutError, timeout } from "../../src/index.js"
import { awsSdkAdapter, awsSdkErrorClassification } from "../../src/aws-sdk.js"
import {
  defineAdapterContractSuite,
  runAdapterContractSuite,
} from "../harness/index.js"

class GetItemCommand {
  constructor(readonly input: Record<string, unknown> = {}) {}
}

class PutItemCommand {
  constructor(readonly input: Record<string, unknown> = {}) {}
}

class SendMessageCommand {
  constructor(readonly input: Record<string, unknown> = {}) {}
}

class StartExecutionCommand {
  constructor(readonly input: Record<string, unknown> = {}) {}
}

class UnknownCommand {
  constructor(readonly input: Record<string, unknown> = {}) {}
}

type Sent = {
  command: unknown
  options: { abortSignal?: AbortSignal } | undefined
}

function fakeClient(script: Array<() => unknown>) {
  const sent: Sent[] = []
  return {
    sent,
    async send(command: unknown, options?: { abortSignal?: AbortSignal }) {
      sent.push({ command, options })
      const next = script.shift()
      if (next === undefined) throw new Error("unexpected send")
      return next()
    },
  }
}

function hangUntilAborted(options?: {
  abortSignal?: AbortSignal
}): Promise<never> {
  return new Promise((_, reject) => {
    const signal = options?.abortSignal
    if (signal === undefined) return
    if (signal.aborted) {
      reject(signal.reason)
      return
    }
    signal.addEventListener("abort", () => reject(signal.reason), {
      once: true,
    })
  })
}

describe("awsSdkAdapter capabilities", () => {
  it.each([
    ["GetItemCommand", new GetItemCommand(), "safe"],
    ["an unknown command", new UnknownCommand(), "unknown"],
    ["PutItemCommand", new PutItemCommand(), "unknown"],
    ["SendMessageCommand", new SendMessageCommand(), "unsafe"],
    ["StartExecutionCommand", new StartExecutionCommand(), "unsafe"],
  ] as const)("derives replay for %s", (_name, command, expected) => {
    const adapter = awsSdkAdapter({
      client: fakeClient([]),
      command: () => command,
    })

    expect(adapter.capabilities({})).toEqual({
      abort: "supported",
      replay: expected,
    })
  })

  it("uses a fixed replay override", () => {
    const adapter = awsSdkAdapter({
      client: fakeClient([]),
      command: () => new PutItemCommand(),
      replay: "safe",
    })

    expect(adapter.capabilities({}).replay).toBe("safe")
  })

  it("uses a per-args replay override", () => {
    const adapter = awsSdkAdapter({
      client: fakeClient([]),
      command: () => new PutItemCommand(),
      replay: (args: { conditional: boolean }) =>
        args.conditional ? "safe" : "unknown",
    })

    expect(adapter.capabilities({ conditional: true }).replay).toBe("safe")
    expect(adapter.capabilities({ conditional: false }).replay).toBe("unknown")
  })

  it("leaves an unrecognised command unknown", () => {
    const adapter = awsSdkAdapter({
      client: fakeClient([]),
      command: () => new UnknownCommand(),
    })

    expect(adapter.capabilities({}).replay).toBe("unknown")
  })
})

describe("awsSdkAdapter execution", () => {
  it("returns the client result", async () => {
    const client = fakeClient([() => ({ Item: { id: { S: "42" } } })])
    const subject = operation({
      name: "aws-result",
      adapter: awsSdkAdapter({
        client,
        command: () => new GetItemCommand(),
      }),
    })

    await expect(subject.execute({})).resolves.toEqual({
      Item: { id: { S: "42" } },
    })
    expect(client.sent).toHaveLength(1)
  })

  it("constructs a command for capability resolution and again for execution", async () => {
    let built = 0
    const client = fakeClient([() => "ok"])
    const adapter = awsSdkAdapter({
      client,
      command: () => {
        built += 1
        return new GetItemCommand()
      },
    })

    expect(adapter.capabilities({}).replay).toBe("safe")
    expect(built).toBe(1)

    built = 0
    const subject = operation({ name: "aws-factory-calls", adapter })
    await subject.execute({})
    expect(built).toBeGreaterThanOrEqual(2)
    expect(client.sent).toHaveLength(1)
  })

  it("builds a fresh command for every attempt", async () => {
    const client = fakeClient([
      () => {
        throw new ProvisionedThroughputExceededException({
          message: "slow down",
          $metadata: {},
        })
      },
      () => "ok",
    ])
    const subject = operation({
      name: "aws-fresh-command",
      adapter: awsSdkAdapter({
        client,
        command: (args: { id: string }) => new GetItemCommand({ id: args.id }),
      }),
      policies: [retry({ maxAttempts: 2 })],
    })

    await expect(subject.execute({ id: "42" })).resolves.toBe("ok")
    expect(client.sent).toHaveLength(2)
    expect(client.sent[0]?.command).toBeInstanceOf(GetItemCommand)
    expect(client.sent[1]?.command).toBeInstanceOf(GetItemCommand)
    expect(client.sent[0]?.command).not.toBe(client.sent[1]?.command)
  })

  it("forwards the operation signal to send", async () => {
    const controller = new AbortController()
    const client = fakeClient([() => "ok"])
    const subject = operation({
      name: "aws-signal",
      adapter: awsSdkAdapter({
        client,
        command: () => new GetItemCommand(),
      }),
    })

    await subject.execute({}, { signal: controller.signal })
    const signal = client.sent[0]?.options?.abortSignal
    expect(signal).toBeDefined()
    controller.abort()
    expect(signal?.aborted).toBe(true)
  })

  it("aborts the send when a timeout fires", async () => {
    const sent: Sent[] = []
    const subject = operation({
      name: "aws-timeout",
      adapter: awsSdkAdapter({
        client: {
          send(command: unknown, options?: { abortSignal?: AbortSignal }) {
            sent.push({ command, options })
            return hangUntilAborted(options)
          },
        },
        command: () => new GetItemCommand(),
      }),
      policies: [timeout({ ms: 10 })],
    })

    await expect(subject.execute({})).rejects.toBeInstanceOf(TimeoutError)
    expect(sent[0]?.options?.abortSignal?.aborted).toBe(true)
  })

  it("propagates the caller's abort reason", async () => {
    const controller = new AbortController()
    const subject = operation({
      name: "aws-abort",
      adapter: awsSdkAdapter({
        client: {
          send(_command: unknown, options?: { abortSignal?: AbortSignal }) {
            return hangUntilAborted(options)
          },
        },
        command: () => new GetItemCommand(),
      }),
    })

    const pending = subject.execute({}, { signal: controller.signal })
    controller.abort()
    await expect(pending).rejects.toBe(controller.signal.reason)
  })
})

describe("awsSdkError classification", () => {
  it("classifies a successful result", () => {
    const adapter = awsSdkAdapter({
      client: fakeClient([]),
      command: () => new GetItemCommand(),
    })

    expect(adapter.classify?.({ status: "success", value: "ok" })).toBe(
      "success",
    )
  })

  it.each([
    [
      "ProvisionedThroughputExceededException",
      new ProvisionedThroughputExceededException({
        message: "slow down",
        $metadata: { httpStatusCode: 400 },
      }),
      "retryable",
    ],
    [
      "a $retryable throttle marker",
      { name: "SomethingNew", $retryable: { throttling: true } },
      "retryable",
    ],
    [
      "a $retryable transient marker",
      { name: "SomethingNew", $retryable: { errorType: "transient" } },
      "retryable",
    ],
    [
      "ThrottlingException",
      { name: "ThrottlingException", $metadata: { httpStatusCode: 400 } },
      "retryable",
    ],
    [
      "SlowDown",
      { name: "SlowDown", $metadata: { httpStatusCode: 503 } },
      "retryable",
    ],
    [
      "RequestThrottled",
      { name: "RequestThrottled", $metadata: { httpStatusCode: 400 } },
      "retryable",
    ],
    [
      "Throttling",
      { name: "Throttling", $metadata: { httpStatusCode: 400 } },
      "retryable",
    ],
    [
      "an empty $retryable marker",
      {
        name: "SomeModeledError",
        $retryable: {},
        $metadata: { httpStatusCode: 400 },
      },
      "retryable",
    ],
    [
      "an unknown 5xx",
      { name: "SomethingNew", $metadata: { httpStatusCode: 503 } },
      "retryable",
    ],
    ["a server fault", { name: "SomethingNew", $fault: "server" }, "retryable"],
    [
      "a transient network code",
      { name: "Error", code: "ECONNRESET" },
      "retryable",
    ],
    [
      "ConditionalCheckFailedException",
      new ConditionalCheckFailedException({
        message: "already exists",
        $metadata: { httpStatusCode: 400 },
      }),
      "failure",
    ],
    [
      "ValidationException",
      { name: "ValidationException", $metadata: { httpStatusCode: 400 } },
      "failure",
    ],
    [
      "an unknown 4xx",
      { name: "SomethingNew", $metadata: { httpStatusCode: 400 } },
      "failure",
    ],
    ["a client fault", { name: "SomethingNew", $fault: "client" }, "failure"],
    ["AbortError", { name: "AbortError" }, "failure"],
    ["an unknown throw", new Error("boom"), "failure"],
  ] as const)("classifies %s", (_name, error, expected) => {
    expect(awsSdkErrorClassification(error)).toBe(expected)
  })

  it("lets a factory classifier override the shared one", () => {
    const adapter = awsSdkAdapter({
      client: fakeClient([]),
      command: () => new GetItemCommand(),
      classifyError: () => "ignored",
    })

    expect(
      adapter.classify?.({
        status: "failure",
        error: { name: "ThrottlingException" },
      }),
    ).toBe("ignored")
  })
})

describe("awsSdkAdapter retry decisions", () => {
  const retryable = () =>
    new ProvisionedThroughputExceededException({
      message: "slow down",
      $metadata: {},
    })

  it("retries a replay-safe read", async () => {
    const client = fakeClient([
      () => {
        throw retryable()
      },
      () => "ok",
    ])
    const subject = operation({
      name: "aws-retry-read",
      adapter: awsSdkAdapter({
        client,
        command: () => new GetItemCommand(),
      }),
      policies: [retry({ maxAttempts: 2 })],
    })

    await expect(subject.execute({})).resolves.toBe("ok")
    expect(client.sent).toHaveLength(2)
  })

  it("declines to replay a non-idempotent send", async () => {
    const error = retryable()
    const client = fakeClient([
      () => {
        throw error
      },
      () => "ok",
    ])
    const events: OperationEvent[] = []
    const subject = operation({
      name: "aws-unsafe-send",
      adapter: awsSdkAdapter({
        client,
        command: () => new SendMessageCommand(),
      }),
      policies: [retry({ maxAttempts: 3 })],
      events: { emit: (event) => events.push(event) },
    })

    await expect(subject.execute({})).rejects.toBe(error)
    expect(client.sent).toHaveLength(1)
    expect(
      events.filter((event) => event.type === "retry.declined"),
    ).toMatchObject([{ reason: "replay-unsafe" }])
  })

  it("declines to replay an undeclared write", async () => {
    const error = retryable()
    const client = fakeClient([
      () => {
        throw error
      },
      () => "ok",
    ])
    const events: OperationEvent[] = []
    const subject = operation({
      name: "aws-unknown-write",
      adapter: awsSdkAdapter({
        client,
        command: () => new PutItemCommand(),
      }),
      policies: [retry({ maxAttempts: 3 })],
      events: { emit: (event) => events.push(event) },
    })

    await expect(subject.execute({})).rejects.toBe(error)
    expect(client.sent).toHaveLength(1)
    expect(
      events.filter((event) => event.type === "retry.declined"),
    ).toMatchObject([{ reason: "replay-unsafe" }])
  })

  it("retries a conditional write the caller declared safe", async () => {
    const client = fakeClient([
      () => {
        throw retryable()
      },
      () => "ok",
    ])
    const subject = operation({
      name: "aws-conditional-write",
      adapter: awsSdkAdapter({
        client,
        command: () => new PutItemCommand(),
        replay: "safe",
      }),
      policies: [retry({ maxAttempts: 2 })],
    })

    await expect(subject.execute({})).resolves.toBe("ok")
    expect(client.sent).toHaveLength(2)
  })

  it("does not retry a permanent failure", async () => {
    const error = new ConditionalCheckFailedException({
      message: "already exists",
      $metadata: {},
    })
    const client = fakeClient([
      () => {
        throw error
      },
    ])
    const events: OperationEvent[] = []
    const subject = operation({
      name: "aws-permanent",
      adapter: awsSdkAdapter({
        client,
        command: () => new GetItemCommand(),
      }),
      policies: [retry({ maxAttempts: 3 })],
      events: { emit: (event) => events.push(event) },
    })

    await expect(subject.execute({})).rejects.toBe(error)
    expect(client.sent).toHaveLength(1)
    expect(
      events.filter((event) => event.type === "retry.declined"),
    ).toMatchObject([{ reason: "not-retryable" }])
  })
})

describe("awsSdkAdapter disposal", () => {
  it("disposes Body and Payload streams before the caller hook", () => {
    const order: string[] = []
    const adapter = awsSdkAdapter({
      client: fakeClient([]),
      command: () => new GetItemCommand(),
      dispose: () => {
        order.push("custom")
      },
    })

    adapter.dispose?.(
      {
        status: "success",
        value: {
          Body: { destroy: () => order.push("body") },
          Payload: {
            cancel: () => {
              order.push("payload")
              return Promise.resolve()
            },
          },
        },
      },
      {} as ExecutionContext,
    )

    expect(order).toEqual(["body", "payload", "custom"])
  })

  it("runs the caller hook for a failure outcome", () => {
    const custom = vi.fn()
    const adapter = awsSdkAdapter({
      client: fakeClient([]),
      command: () => new GetItemCommand(),
      dispose: custom,
    })

    adapter.dispose?.(
      { status: "failure", error: new Error("boom") },
      {} as ExecutionContext,
    )

    expect(custom).toHaveBeenCalledTimes(1)
  })
})

describe("awsSdkAdapter contract", () => {
  it("satisfies the runner-agnostic adapter contract", async () => {
    const sent: Sent[] = []
    let calls = 0
    const client = {
      async send(command: unknown, options?: { abortSignal?: AbortSignal }) {
        calls += 1
        sent.push({ command, options })
        if (calls === 1) return { Item: {} }
        if (calls === 2) return hangUntilAborted(options)
        return { Item: {} }
      },
    }
    const adapter = awsSdkAdapter({
      client,
      command: (args: { id: string }) => new GetItemCommand({ id: args.id }),
    })
    const suite = defineAdapterContractSuite({
      name: "aws-sdk",
      adapter,
      success: {
        args: { id: "1" },
        assertResult: (result) => expect(result).toEqual({ Item: {} }),
      },
      capabilities: [
        {
          args: { id: "1" },
          expected: { abort: "supported", replay: "safe" },
        },
      ],
      classifications: [
        {
          outcome: { status: "success", value: { Item: {} } },
          expected: "success",
        },
        {
          outcome: {
            status: "failure",
            error: new ProvisionedThroughputExceededException({
              message: "",
              $metadata: {},
            }),
          },
          expected: "retryable",
        },
        {
          outcome: {
            status: "failure",
            error: new ConditionalCheckFailedException({
              message: "",
              $metadata: {},
            }),
          },
          expected: "failure",
        },
      ],
      abort: {
        args: { id: "1" },
        verify: async ({ controller, execute }) => {
          const pending = execute()
          controller.abort()
          await expect(pending).rejects.toBe(controller.signal.reason)
          expect(sent).toHaveLength(2)
        },
      },
    })

    await runAdapterContractSuite(suite)
  })
})
