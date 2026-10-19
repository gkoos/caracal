---
"@gkoos/caracal": minor
---

Add `@gkoos/caracal/aws-sdk`: one adapter factory for any AWS SDK v3 client, with no runtime dependency on the SDK. Capabilities are decided per command - read-only commands such as `GetItem` and `GetObject` are `replay: "safe"`, non-idempotent commands such as `SendMessage`, `Publish` and `StartExecution` are `"unsafe"`, and everything else stays `"unknown"` until the application declares it. `abort: "supported"` is real: the operation signal is passed to `send` as `{ abortSignal }`.

The shared classifier reads `$retryable`, the modeled error name, `$fault` and the HTTP status, in that order; `awsSdkErrorClassification` is exported for service-specific overrides through `classifyError`. Abandoned streaming outputs have their `Body`/`Payload` destroyed before the caller's `dispose` hook runs.

A Floci-backed integration suite (`npm run test:integration:aws`) covers endpoint wiring, abort over a socket, one HTTP request per Caracal attempt, duplicate suppression for an accepted `SendMessage`, and classification of real emulator errors. It uses `test`/`test` credentials; no AWS account is involved.