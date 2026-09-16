---
"@gkoos/caracal": minor
---

The local bulkhead gains an optional `leaseMs` permit lease. A holder that has not settled after `leaseMs` is aborted, so an `abort: "supported"` adapter settles and releases its permit instead of wedging the bulkhead; an `abort: "unsupported"` holder keeps its permit and the expiry is reported as a `bulkhead.lease-lost` event. No default, so behavior is unchanged unless configured.
