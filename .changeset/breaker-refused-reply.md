---
"@gkoos/caracal": minor
---

The cached-verdict breaker no longer admits from a reply the session refused.

`refresh` served the authority's answer whatever the session did with it. A reply that arrived after
its freshness allowance, or one that a newer verdict the replica already holds supersedes, was
refused as a record and then used as the admission decision anyway — so an operation could run while
a cached OPEN verdict stood, or on a reply that had outlived the window it was granted. A read in
flight across a transition reproduces it directly.

The reply now has to be adoptable: applied, or reported unchanged. A refused reply leaves the cached
verdict to answer — a protective OPEN or HALF_OPEN is returned as it is — and a stale CLOSED follows
the configured behaviour instead of borrowing freshness, which under the default
`staleClosed: "refresh"` is a refusal, because the read has already happened and did not produce
usable authority. A reply that merely failed to be *cached* (registry pressure, or no incarnation
adopted yet) is still served: it carries the authority's current verdict.

`admitProbe` has the same shape and is left as it stands: a superseded `admitted` reply can still
grant one probe, bounded by the probe lease and by the authority's own probe accounting.
