import { expect, it } from "vitest"
import { coordinationKey } from "../../src/coordination/redis/keys.js"

function hashTag(key: string): string {
  return key.slice(key.indexOf("{") + 1, key.indexOf("}"))
}

it("isolates namespaces, scopes and delimiter-like identities", () => {
  const identities = [
    ["a", "b", "c", "d"],
    ["a:b", "c", "d", "e"],
    ["a", "b:c", "d", "e"],
    ["a", "b", "c", "{d}"],
  ]
  expect(
    new Set(identities.map(([n, p, o, s]) => coordinationKey(n, p, o, s))).size,
  ).toBe(4)
  expect(() => coordinationKey("a", "b", "c", "")).toThrow()
  expect(() => coordinationKey("a", "b", "c", "x".repeat(1025))).toThrow()
})

it("gives every suffix of one identity the same hash-tag", () => {
  const keys = ["leases", "breaker", "observations", "probes"].map((suffix) =>
    coordinationKey("ns", "breaker:name", "op", "scope", suffix),
  )
  const tags = new Set(keys.map(hashTag))

  expect(tags.size).toBe(1)
  const [tag] = tags
  expect(tag).toMatch(/^[0-9a-f]{64}$/)

  for (const key of keys) {
    expect(key).toMatch(/^caracal:v1:\{[0-9a-f]{64}\}:/)
  }

  expect(tag).not.toBe(
    hashTag(coordinationKey("ns", "breaker:name", "op", "other-scope")),
  )
})
