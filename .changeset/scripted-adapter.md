---
"@gkoos/caracal": minor
---

Add `createScriptedAdapter` to `@gkoos/caracal/testing`: a runner-agnostic adapter whose calls settle from a fixed script of successes, failures, delays and hangs, with per-call capabilities, optional classification and an optional disposal hook. Use it to reproduce retry, breaker, timeout and bulkhead behaviour without a network service or a test-runner dependency, and to assert on the attempts the runtime made.

Each `execute()` reserves one entry synchronously, so concurrent calls consume entries in invocation order; the queue belongs to the adapter instance and continues across retries and later operations. Capability reads, classification and disposal never consume entries, an exhausted script rejects instead of repeating its final entry, and `calls` exposes a frozen snapshot of each call's index, arguments and attempt context. The script and its entries are copied at creation, delays are validated eagerly against the largest delay `setTimeout` honours, and an entry can ignore cancellation with `abort: "ignore"` - independently of the declared capabilities - so the contract harness can be shown to catch a deliberately mis-declared adapter.
