---
"@gkoos/caracal": minor
---

Only a reply that verified the cached verdict moves the Serval breaker's freshness allowance, and it
is measured from the request's send time.

A replica holding a cached CLOSED verdict renews its freshness allowance when the authority answers.
That renewal used to happen for any reply the session did not reject, including one whose verdict was
older than the record the replica already held - a settlement that raced a transition it had not yet
seen. Extending the allowance there let a replica admit on a superseded reply, past the deadline the
authority granted, which is the opposite direction from the guarantee the rest of the surface holds.

The window now moves only for a reply that verified the verdict the replica holds: a record the
session applied, or one it reported as unchanged, which means the authority answered with exactly
what is cached. A superseded reply leaves the window where it was, so the deadline a replica enforces
is never later than the one it was granted. The session distinguishes the two in its `ignored`
reason, which was previously a single `stale`, so a caller caching freshness can tell a verification
from a reply that describes a past observation. The window is anchored to the send time as well, so a
delayed reply cannot extend it either.
