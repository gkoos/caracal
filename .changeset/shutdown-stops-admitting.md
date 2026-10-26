---
"@gkoos/caracal": minor
---

Shutdown stops admitting, and the session's own refusals are decisions.

The bulkhead checked `closed` once, at the entry to an admission, and never again: an offer parked
inside its admission — waiting on the allocation it had just requested — was admitted after `close()`
began. Closing also returned only what the scope held when it started, so capacity that arrived while
it was closing stayed with the process until its grace window lapsed. Admission now re-checks closure
after every await, and closing makes a second pass over what arrived during the first.

The breaker kept working after `session.close()`: the registration shortcut returned early for an
identity registered before the close, and the closed session's error was not one the policy could
read, so `onCoordinatorError` applied — whose default runs the call. The coordinator now checks
closure before that shortcut, and reports a closed session or a registry that cannot hold the scope
as `ServalAuthorityRefusalError`, which the policy rejects. The shared session and registry keep their
own error types: they may not import a policy module, so the translation happens at the coordinator,
the layer that knows which policy is asking.

Note for callers of the breaker coordinator: a full registry now surfaces as
`ServalAuthorityRefusalError` with `ServalRegistryPressureError` as its `cause`, rather than as the
registry error itself. The rate-limit and bulkhead coordinators still throw it directly.
