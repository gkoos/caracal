#!/usr/bin/env node
/**
 * Keeps SECURITY.md's supported-versions table in step with the released line.
 *
 * `npm run version` runs this straight after `changeset version`, so the
 * Version Packages pull request carries the update. That matters because the
 * package-contract test asserts the table and `package.json` agree - without
 * this step the release pull request fails its own Check job.
 */
import { readFileSync, writeFileSync } from "node:fs"

const root = new URL("../", import.meta.url)
const { version } = JSON.parse(
  readFileSync(new URL("package.json", root), "utf8"),
)
const [major, minor] = String(version).split(".")
const path = new URL("SECURITY.md", root)
const source = readFileSync(path, "utf8")

const rowPattern = /^(\| )\d+\.\d+\.x(\s*\|.*)$/m
if (!rowPattern.test(source)) {
  throw new Error(
    "SECURITY.md has no supported-versions row (| 0.x.x | ...) to update",
  )
}

const updated = source.replace(rowPattern, `$1${major}.${minor}.x$2`)
if (updated === source) {
  console.log(`SECURITY.md already lists ${major}.${minor}.x`)
} else {
  writeFileSync(path, updated)
  console.log(`SECURITY.md supported versions -> ${major}.${minor}.x`)
}
