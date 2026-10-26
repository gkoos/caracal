---
"@gkoos/caracal": minor
---

The bulkhead's `maximumSlots` bounds the initial allocation, not only growth.

The first allocation requested `initialSlots` directly, so `initialSlots: 8` with `maximumSlots: 2`
allocated eight slots — and the authority could not correct it, because the ceiling is a per-replica
intent the server never hears about. Only the fleet limit is clamped server-side. A replica that
allocated past its own ceiling held that capacity for the life of the grant, and eight concurrent
admissions succeeded where two were intended.

The start is now clamped to the replica's effective ceiling, which also covers a fleet limit below
`initialSlots`: that value is discovered per scope rather than known at construction. A configuration
where `initialSlots` exceeds a *configured* `maximumSlots` is refused outright — the two knobs
contradict each other, and silently resolving it would hide a caller error.

Existing callers are unaffected: every pair in the repository already keeps `initialSlots` at or
below `maximumSlots`.
