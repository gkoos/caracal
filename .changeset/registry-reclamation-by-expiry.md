---
"@gkoos/caracal": minor
---

A scope registry entry is reclaimed once it holds nothing, including when nobody asks about it again.

The rate limiter and the bulkhead each bound the number of scopes they track, and refuse a new scope
rather than dropping live accounting when the bound is reached. Reclamation only accepted a scope
whose recorded stock or allocation was already zero, but expiry is lazy: stock and allocations are
expired when the scope is used again, and an idle scope is never used again. An expired-but-untouched
scope therefore occupied its entry forever, and after `maximumScopes` of them every new scope was
refused — while the process held no live quota or capacity at all.

Reclamation now uses the same expiry predicate the admission uses. Stock is live only until its
deadline; an allocation is held only until its validity plus the grace window passes, after which the
server's reservation is gone; and a retained operation stops pinning a scope once its reply could no
longer be adopted, which is the point `authority.md` calls "until the operation can no longer be
replayed". Every reclaimable entry is dropped in one pass, so pressure settles the registry in a
single scan, and permits discarded with a reclaimed scope are counted as expired rather than
forgotten.
