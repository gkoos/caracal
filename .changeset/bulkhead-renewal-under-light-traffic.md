---
"@gkoos/caracal": minor
---

The delegated bulkhead renews its allocation under light traffic.

Admission takes a fast path whenever a free slot exists, and renewal lived only in `maintain`, which
that path never reaches. One holder at a time therefore never renewed: the allocation ran out its
validity untouched, and the next admission after it lapsed was refused for the grace window — with
the defaults, ten seconds — while the process held no slots at all. The renewal had been due for a
third of the validity window by then, and a burst that emptied the pool was the only thing that ever
asked for it.

The fast path now nudges a renewal when one is due, in the background: the admission keeps its
no-round-trip property, and repeated nudges join the modification already in flight. `maintain` is
called with `demand: false`, so the nudge renews and never grows the allocation.
