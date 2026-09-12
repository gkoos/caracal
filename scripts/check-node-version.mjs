import { pathToFileURL } from "node:url"

/**
 * Minimum Node.js version: `AbortSignal.any` is used by the runtime, the
 * timeout and bulkhead policies, and the fetch adapter, and it landed in
 * 20.3.0.  A major-only check accepts 20.0-20.2, which install cleanly and then
 * throw at runtime.
 */
export const minimumNodeVersion = "20.3.0"

/** True when `version` (a `process.versions.node` string) meets the minimum. */
export function satisfiesMinimumNodeVersion(
  version,
  minimum = minimumNodeVersion,
) {
  const [major, minor, patch] = String(version ?? "")
    .split(".")
    .map(Number)
  const [minMajor, minMinor, minPatch] = minimum.split(".").map(Number)
  if (![major, minor, patch].every(Number.isInteger)) return false
  if (major !== minMajor) return major > minMajor
  if (minor !== minMinor) return minor > minMinor
  return patch >= minPatch
}

function check() {
  if (!satisfiesMinimumNodeVersion(process.versions.node)) {
    throw new Error(
      `Caracal requires Node.js >=${minimumNodeVersion}; found ${process.version}`,
    )
  }

  console.log(
    `Node.js ${process.version} satisfies Caracal's >=${minimumNodeVersion} requirement.`,
  )
}

// Only run when executed directly, so the predicate above stays importable.
if (import.meta.url === pathToFileURL(process.argv[1] ?? "").href) check()
