---
"@gkoos/caracal": minor
---

A frozen capacity return always has a modification that settles it.

`relinquish` freezes the free slots before the reduction is sent, which is what keeps a server-side
reallocation from preceding this process's own freeze. When another modification already held the
wire, the reduction was never submitted: the freeze was recorded, the retained operation belonged to
the other action, and the slots stayed frozen and unusable for the life of the grant — held by this
replica and returned to nobody. The fleet kept counting them, and `adopt`'s bound only ever reduces
the count, so a later grant inherited them.

The intent is now submitted by the modification machinery itself. A freeze made while another
operation holds the wire is submitted as soon as that one settles, a return the authority refused
stays pending and is retried by the same nudge an admission already performs, and a freeze dies with
the grant that granted it instead of being inherited by the next one. A return no longer outlives the
allocation it belongs to, and the capacity it claims always comes back to the fleet.
