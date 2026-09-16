---
"@gkoos/caracal": minor
---

An outer `timeout` now disposes a result that settles after its deadline has fired. When the timeout supersedes the inner pipeline, the losing value or error is disposed via the adapter's `dispose` hook instead of being leaked, closing the remaining part of the "definition of abandoned" from #44.
