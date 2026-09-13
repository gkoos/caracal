---
"@gkoos/caracal": patch
---

Fixed

- an operation with no event sink no longer builds events. Both `emitRuntimeEvent` and the operation's own emitter constructed the event object - including a `Date.now()` call - before checking whether a sink would receive it, so four lifecycle events per execution were allocated for nobody. `test/unit/no-sink-fast-path.test.ts` pins it and `npm run bench:gate` measures it
- `npm run bench` no longer fails when the Redis script cache is already warm: capturing a script body used to depend on the server choosing `EVAL`, which it does not when it already has the SHA
