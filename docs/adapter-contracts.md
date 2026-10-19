# Writing your own adapter

An adapter is an object that declares its capabilities and executes underlying work. You can wrap any async operation - a queue client, a gRPC stub, an SDK call - in an adapter and apply any Caracal policy to it.

## The Adapter interface

```ts
import type { Adapter } from "@gkoos/caracal"

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
  // Optional - clean up a settled result the runtime abandons
  dispose(outcome, context) {
    if (outcome.status === "success") {
      outcome.value.release()
    }
  },
}
```

### Capabilities

Capabilities are declared per invocation, before any policy runs, and may vary based on `args`:

| Capability | Values | Effect |
|---|---|---|
| `abort` | `"supported"` | Caracal may cancel the attempt by aborting `context.signal` - a timeout firing, or a distributed lease that could not be renewed. The adapter must honor it. |
| `abort` | `"unsupported"` | Caracal will not *generate* cancellation: on timeout the caller still receives `TimeoutError` while the underlying work continues, and a lost lease is reported without aborting. The caller's own `signal` is still propagated on `context.signal`, so the caller can abort the attempt either way. |
| `replay` | `"safe"` | Retry may issue another attempt. |
| `replay` | `"unsafe"` | Retry will not issue another attempt regardless of the outcome classification. |
| `replay` | `"unknown"` | Same as `"unsafe"`: without a guarantee that a repeat is safe, retry stays at a single attempt. |

Declare these accurately, **Caracal never infers them**. An adapter that ignores `context.signal` but declares `abort: "supported"` will cause timeouts to appear to work while the underlying work continues silently.

Likewise, `replay: "safe"` reports an existing guarantee; it does not make an unsafe side effect idempotent. A request key is sufficient only when the downstream enforces it for the invocation being repeated. See [who provides idempotency](timeout-and-retry.md#who-provides-idempotency) and the fetch and PostgreSQL examples linked there.

### Classification

The `classify` method maps each settled outcome to one of four values:

| Classification | Effect |
|---|---|
| `"success"` | The attempt succeeded. |
| `"failure"` | A non-transient failure. Recorded by the circuit breaker; retry will not retry it. |
| `"retryable"` | A transient failure. Retry will attempt again if `replay: "safe"` and attempts remain. |
| `"ignored"` | Not recorded by the circuit breaker and does not trigger retry. |

If `classify` is omitted, thrown errors become `"failure"` and resolved values become `"success"`.

`classify` is called more than once for the same outcome: the operation classifies it to fill in the `attempt.settled` event, `retry` classifies it to decide whether to try again, and the circuit breaker classifies it to record the observation. It must therefore be pure - counting, logging or memoising inside it will see each attempt two or three times, and the calls must not depend on one another. When no event sink is configured the operation skips its own call, so the count is two rather than three.

### Disposal

If a settled result holds a body or handle - a response stream, a pooled connection - declare the optional `dispose` method so the runtime can release it when it abandons the result rather than returning it to the caller. Caracal invokes `dispose` when `retry` abandons an attempt to try again - including when the `delay` that paces it throws, since the caller receives the delay's failure and not the attempt's - or when an outer `timeout` supersedes a result that settled after its deadline, passing the abandoned `Outcome` and the attempt's `ExecutionContext`.

`dispose` follows the same isolation discipline as event sinks: it is fire-and-forget, it is never awaited on the caller path, and a synchronous throw or a rejected promise never changes what the caller sees. Return nothing (or `undefined`) when the outcome has nothing to release - an error outcome for an adapter that only disposes values is the common case.

### Methods and `this`

Caracal calls every adapter method **on the adapter**, so a method can reach the adapter's own state through `this`. This holds for `classify` and `dispose` too, even though you never call them yourself - the runtime calls `classify` from `retry` and from the circuit breaker, and `dispose` from whichever policy abandons the result:

```ts
class PooledAdapter implements Adapter<Query, Row[]> {
  #pool: Pool

  constructor(pool: Pool) {
    this.#pool = pool
  }

  capabilities() {
    return { abort: "unsupported", replay: "safe" } as const
  }

  execute(query: Query) {
    return this.#pool.query(query)
  }

  classify(outcome) {
    // `this.#pool` is available here: the classifier is not detached.
    return this.#pool.isTransient(outcome) ? "retryable" : "failure"
  }

  dispose(outcome) {
    if (outcome.status === "success") this.#pool.release(outcome.value)
  }
}
```

An object literal works the same way, so a method may read a sibling method through `this`. Nothing else about the contract changes: `classify` still has to be pure, and `dispose` still cannot report a failure.

### ExecutionContext

The `context` object passed to `execute` contains:

| Field | Description |
|---|---|
| `signal` | Combined `AbortSignal` from timeout and caller, or `undefined`. Check before and during long work. |
| `attempt` | Current attempt number, starting at 1. |
| `metadata` | The `Record<string, unknown>` passed by the caller to `operation.execute()`. |
| `operationName` | The name of the wrapping operation. |
| `executionId` | The execution identifier, shared by every attempt of one `execute()` call. |
| `capabilities` | The capabilities declared for this invocation. |
| `classify` | The classifier Caracal will use: the adapter's `classify`, or the default. Call it to classify an outcome exactly as the retry and breaker policies will. |

## Contract test harness

`@gkoos/caracal/testing` exports a runner-agnostic harness that verifies your adapter's declared behaviour. It imports no test runner, you can wire the generated checks into whichever framework you use.

```ts
import { defineAdapterContractSuite } from "@gkoos/caracal/testing"

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
- The successful call emits exactly the operation lifecycle, in order: `execution.started`, `attempt.started`, `attempt.settled`, `execution.settled`

The lifecycle check is stricter than it looks: it asserts the *whole* event list, so an adapter that emits extra lifecycle events fails it. That happens if `execute` wraps a Caracal `operation` of its own - the inner operation's events are recorded by the outer sink. Retrying *inside* `execute` without Caracal is fine; the check counts attempts, not retries.

`runAdapterContractSuite(suite)` runs every generated check in order and rejects on the first failure. Use it when your runner consumes promises directly - `node:test`, or a bespoke harness - instead of registering each entry in `suite.checks` as its own test case.

The harness does not guarantee correctness in all edge cases, it verifies the contract as you have configured it. Think of it as a baseline, not a complete test suite. You should write additional tests for your adapter's specific error handling, edge cases, and any classification logic beyond the basics.

`@gkoos/caracal/testing` has no runtime dependencies beyond `@gkoos/caracal` itself and imports no test framework. The internal test helpers under `test/support/` (in-memory coordinators, worker harness) are not exported and are not available to adapter authors.

## Scripted adapter

`createScriptedAdapter` builds an adapter whose calls settle from a fixed script. Use it to reproduce retry, breaker, bulkhead and timeout behaviour without a network service, and to assert on the attempts the runtime made.

```ts
import { operation, retry } from "@gkoos/caracal"
import { createScriptedAdapter } from "@gkoos/caracal/testing"

const adapter = createScriptedAdapter<void, string>({
  capabilities: { abort: "supported", replay: "safe" },
  classify: (outcome) =>
    outcome.status === "failure" ? "retryable" : "success",
  outcomes: [
    { status: "failure", error: new Error("unavailable") },
    { status: "success", value: "recovered", delayMs: 20 },
  ],
})

const subject = operation({
  name: "orders.create",
  adapter,
  policies: [retry({ maxAttempts: 3 })],
})

await subject.execute(undefined)
adapter.calls.map((call) => call.context.attempt) // [1, 2]
```

`ScriptedStep` is the script entry union: a `success` with a `value`, a `failure` with an `error`, or a `hang`. A `success` or `failure` may carry `delayMs`; a `hang` never settles on its own. `ScriptedAdapterOptions` takes the script as `outcomes`, a `capabilities` object or `(args) => capabilities` function, an optional `classify`, and an optional `dispose` hook for outcomes the runtime abandons.

The returned `ScriptedAdapter` extends `Adapter` with `calls`, a frozen snapshot of `ScriptedCall` records (`index`, `args`, `context`). Read it after the execution you are asserting on; a held reference does not update.

### Queue lifetime and exhaustion

Each `execute()` reserves one entry synchronously, before awaiting anything, so concurrent calls consume entries in invocation order even when they settle in a different order. The queue belongs to the adapter instance and continues across retries and later operations: create a fresh adapter for an independent scenario. Capability reads, classification and disposal never consume entries.

An exhausted script rejects with an error naming the operation and the missing call; it never repeats the final entry. Exhausted attempts are not recorded in `calls`. The script and each entry are copied at creation, so mutating the array or an entry afterwards does not change behaviour; the `value` and `error` payloads keep their identity.

`delayMs` is validated eagerly: it must be an integer between `0` and `2_147_483_647`, the largest delay `setTimeout` honours.

### Cancellation

An entry honours `context.signal` by default, including a signal that is already aborted, and clears its timer and listener on every settlement. Add `abort: "ignore"` to an entry to make that entry ignore cancellation and settle (or hang) regardless. This is independent of the declared capabilities: it is how a test models an adapter that claims `abort: "supported"` and then does not honour it.

A cancelled entry remains consumed. A cancellation that prevents the runtime from invoking the adapter at all consumes nothing.

### Capability mismatches and the contract harness

The scripted adapter is also the negative control for the contract harness. `defineAdapterContractSuite` runs its checks against one adapter instance in a fixed order, so its queue is shared. Consumption is per execution that reaches the adapter, not per check:

1. `capabilities` checks call no `execute()`, so they consume nothing.
2. The successful lifecycle calls `execute()` once, so it consumes one entry.
3. `classifications` checks call `classify` only, so they consume nothing.
4. The `abort` check consumes as many entries as the consumer's `verify` causes to reach the adapter: one if it calls `execute()` before aborting, none if it aborts first. The harness only hands `verify` an `execute` closure; it never calls it itself.
5. The abandoned-result disposal check consumes two entries when the adapter defines `dispose`, because its wrapper classifies every outcome `retryable` and retries once. Both attempts have to reach and settle, so a `hang` left here stalls the check.

Budget the script in that order, and remember that a check consuming fewer entries than expected shifts every check after it. A test that asserts cancellation fails when the adapter ignores it, which is how the harness is shown to catch a mis-declared adapter:

```ts
const adapter = createScriptedAdapter<void, string>({
  capabilities: { abort: "supported", replay: "safe" },
  outcomes: [
    { status: "success", value: "ok" },       // lifecycle: its one execute()
    { status: "hang", abort: "ignore" },      // abort: consumed only if verify executes first
  ],
})
```

The harness only catches a mismatch when the consumer's `abort.verify` asserts cancellation. It does not prove that an adapter honours abort or replay in production, and it never infers capabilities from scripted behaviour - declare them as the adapter would.
