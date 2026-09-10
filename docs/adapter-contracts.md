# Writing your own adapter

An adapter is an object that declares its capabilities and executes underlying work. You can wrap any async operation - a queue client, a gRPC stub, an SDK call - in an adapter and apply any Caracal policy to it.

## The Adapter interface

```ts
import type { Adapter } from "caracal"

const myAdapter: Adapter<MyArgs, MyResult> = {
  capabilities(args) {
    return {
      abort: "supported",   // or "unsupported"
      replay: "safe",       // or "unsafe" | "unknown"
    }
  },
  async execute(args, context) {
    return doWork(args, { signal: context.signal })
  },
  // Optional - default classifies errors as "failure", successes as "success"
  classify(outcome) {
    if (outcome.status === "failure") {
      return isTransient(outcome.error) ? "retryable" : "failure"
    }
    return "success"
  },
}
```

### Capabilities

Capabilities are declared per invocation, before any policy runs, and may vary based on `args`:

| Capability | Values | Effect |
|---|---|---|
| `abort` | `"supported"` | Caracal will cancel `context.signal` when a timeout fires or the caller aborts. The adapter must honor it. |
| `abort` | `"unsupported"` | Caracal will not signal cancellation. The caller still receives `TimeoutError` on time, but underlying work continues. |
| `replay` | `"safe"` | Retry may issue another attempt. |
| `replay` | `"unsafe"` | `"unknown"` | Retry will not issue another attempt regardless of the outcome classification. |

Declare these accurately, **Caracal never infers them**. An adapter that ignores `context.signal` but declares `abort: "supported"` will cause timeouts to appear to work while the underlying work continues silently.

### Classification

The `classify` method maps each settled outcome to one of four values:

| Classification | Effect |
|---|---|
| `"success"` | The attempt succeeded. |
| `"failure"` | A non-transient failure. Recorded by the circuit breaker; retry will not retry it. |
| `"retryable"` | A transient failure. Retry will attempt again if `replay: "safe"` and attempts remain. |
| `"ignored"` | Not recorded by the circuit breaker and does not trigger retry. |

If `classify` is omitted, thrown errors become `"failure"` and resolved values become `"success"`.

### ExecutionContext

The `context` object passed to `execute` contains:

| Field | Description |
|---|---|
| `signal` | Combined `AbortSignal` from timeout and caller, or `undefined`. Check before and during long work. |
| `attempt` | Current attempt number, starting at 1. |
| `metadata` | The `Record<string, unknown>` passed by the caller to `operation.execute()`. |
| `operationName` | The name of the wrapping operation. |

## Contract test harness

`caracal/testing` exports a runner-agnostic harness that verifies your adapter's declared behaviour. It imports no test runner, you can wire the generated checks into whichever framework you use.

```ts
import { defineAdapterContractSuite } from "caracal/testing"

const suite = defineAdapterContractSuite({
  name: "my-adapter",
  adapter: myAdapter,

  // A call that should succeed
  success: {
    args: { id: "42" },
    assertResult: (result) => expect(result.id).toBe("42"), // optional
  },

  // Capability declarations to verify
  capabilities: [
    { args: { id: "42" }, expected: { abort: "supported", replay: "safe" } },
    { args: { method: "POST", id: "42" }, expected: { abort: "supported", replay: "unsafe" } },
  ],

  // Classification cases to verify
  classifications: [
    { outcome: { status: "failure", error: new NetworkError() }, expected: "retryable" },
    { outcome: { status: "failure", error: new ValidationError() }, expected: "failure" },
  ],

  // Optional: verify cancellation behaviour
  abort: {
    args: { id: "42" },
    verify: async ({ controller, execute }) => {
      controller.abort()
      await expect(execute()).rejects.toThrow()
    },
  },
})

// Register with any test runner
for (const check of suite.checks) {
  it(check.name, () => check.run())
}
```

The harness checks that:

- `capabilities()` returns the values you declared for each set of args
- A successful call resolves and, if `assertResult` is provided, the result passes your assertion
- Each `classifications` entry produces the expected classification from `classify`
- If an `abort` scenario is provided, cancellation behaves as described in your `verify` function

The harness does not guarantee correctness in all edge cases, it verifies the contract as you have configured it. Think of it as a baseline, not a complete test suite. You should write additional tests for your adapter's specific error handling, edge cases, and any classification logic beyond the basics.

`caracal/testing` has no runtime dependencies beyond `caracal` itself and imports no test framework. The internal test helpers under `test/support/` (in-memory coordinators, worker harness) are not exported and are not available to adapter authors.