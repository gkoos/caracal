---
"@gkoos/caracal": minor
---

Remove the bulkhead's `reconciliations` and `unfrozenAfterReconciliation` metrics.

Both were declared and initialised to zero, and nothing ever incremented them: the bulkhead has no
snapshot reader and no reconciliation path, so the recovery they named does not exist. A counter that
always reads zero invites an alert on a path that never runs, and a frozen return is settled by
retrying the same operation instead — the modification machinery owns it.

The session's own `reconciliations` counter is unaffected. It is a different counter on a different
type, and it does work.
