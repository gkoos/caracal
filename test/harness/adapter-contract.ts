import { operation } from "../../src/core/operation.js"
import type {
  Adapter,
  Classification,
  OperationCapabilities,
  OperationEvent,
  Outcome,
} from "../../src/core/types.js"

export interface AdapterContractSuccess<Args, Result> {
  readonly args: Args
  readonly assertResult?: (result: Result) => void | Promise<void>
}

export interface AdapterContractCapabilityCase<Args> {
  readonly args: Args
  readonly expected: OperationCapabilities
}

export interface AdapterContractClassificationCase<Result> {
  readonly outcome: Outcome<Result>
  readonly expected: Classification
}

export interface AdapterContractAbortCase<Args> {
  readonly args: Args
  readonly verify: (controls: {
    readonly controller: AbortController
    readonly execute: () => Promise<unknown>
  }) => void | Promise<void>
}

export interface AdapterContractOptions<Args, Result> {
  readonly name: string
  readonly adapter: Adapter<Args, Result>
  readonly success: AdapterContractSuccess<Args, Result>
  readonly capabilities: readonly AdapterContractCapabilityCase<Args>[]
  readonly classifications?: readonly AdapterContractClassificationCase<Result>[]
  readonly abort?: AdapterContractAbortCase<Args>
}

export interface AdapterContractCheck {
  readonly name: string
  run(): Promise<void>
}

export interface AdapterContractSuite {
  readonly name: string
  readonly checks: readonly AdapterContractCheck[]
}

function assertEqual<T>(actual: T, expected: T, message: string): void {
  if (!Object.is(actual, expected)) {
    throw new Error(
      `${message}: expected ${String(expected)}, received ${String(actual)}`,
    )
  }
}

function assertLifecycle(events: readonly OperationEvent[]): void {
  const types = events.map((event) => event.type)
  const expected = [
    "execution.started",
    "attempt.started",
    "attempt.settled",
    "execution.settled",
  ]
  if (
    types.length !== expected.length ||
    types.some((type, index) => type !== expected[index])
  ) {
    throw new Error(
      `expected operation lifecycle ${expected.join(" -> ")}; received ${types.join(" -> ")}`,
    )
  }
}

/**
 * Returns runner-agnostic checks for a third-party adapter. Register each
 * check with the application's test runner; this module imports no test runner.
 */
export function defineAdapterContractSuite<Args, Result>(
  options: AdapterContractOptions<Args, Result>,
): AdapterContractSuite {
  const checks: AdapterContractCheck[] = options.capabilities.map(
    (capabilityCase, index) => ({
      name: `${options.name}: capabilities ${index + 1}`,
      async run(): Promise<void> {
        const actual = options.adapter.capabilities(capabilityCase.args)
        assertEqual(
          actual.abort,
          capabilityCase.expected.abort,
          "abort capability",
        )
        assertEqual(
          actual.replay,
          capabilityCase.expected.replay,
          "replay capability",
        )
      },
    }),
  )

  checks.push({
    name: `${options.name}: successful operation lifecycle`,
    async run(): Promise<void> {
      const events: OperationEvent[] = []
      const subject = operation({
        name: `adapter-contract:${options.name}`,
        adapter: options.adapter,
        events: { emit: (event) => events.push(event) },
      })
      const result = await subject.execute(options.success.args, {
        executionId: "adapter-contract",
      })
      await options.success.assertResult?.(result)
      assertLifecycle(events)
    },
  })

  for (const [index, classificationCase] of (
    options.classifications ?? []
  ).entries()) {
    checks.push({
      name: `${options.name}: classification ${index + 1}`,
      async run(): Promise<void> {
        const actual =
          options.adapter.classify?.(classificationCase.outcome) ??
          (classificationCase.outcome.status === "success"
            ? "success"
            : "failure")
        assertEqual(
          actual,
          classificationCase.expected,
          "outcome classification",
        )
      },
    })
  }

  if (options.abort !== undefined) {
    checks.push({
      name: `${options.name}: abort behavior`,
      async run(): Promise<void> {
        const controller = new AbortController()
        const subject = operation({
          name: `adapter-contract:${options.name}`,
          adapter: options.adapter,
        })
        await options.abort?.verify({
          controller,
          execute: () =>
            subject.execute(options.abort?.args as Args, {
              signal: controller.signal,
            }),
        })
      },
    })
  }

  return Object.freeze({ name: options.name, checks: Object.freeze(checks) })
}

export async function runAdapterContractSuite(
  suite: AdapterContractSuite,
): Promise<void> {
  for (const check of suite.checks) {
    await check.run()
  }
}
