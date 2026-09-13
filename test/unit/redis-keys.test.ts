import { expect, it } from "vitest"
import {
  COORDINATION_KEY_CACHE_LIMIT,
  coordinationKey,
  coordinationKeyCacheSize,
} from "../../src/coordination/redis/keys.js"

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

it("derives a key once per identity and bounds what it remembers", () => {
  const first = coordinationKey("ns", "bulkhead:name", "op", "scope")
  const second = coordinationKey("ns", "bulkhead:name", "op", "scope")
  expect(second).toBe(first)

  const before = coordinationKeyCacheSize()
  coordinationKey("ns", "bulkhead:name", "op", "scope")
  expect(coordinationKeyCacheSize()).toBe(before)

  // Bounded: the cache must not grow with the number of scopes ever seen, which
  // is what the documented cardinality requirement is protecting.
  for (let i = 0; i < COORDINATION_KEY_CACHE_LIMIT + 50; i += 1) {
    coordinationKey("ns", "bulkhead:name", "op", `scope-${i}`)
  }
  expect(coordinationKeyCacheSize()).toBeLessThanOrEqual(
    COORDINATION_KEY_CACHE_LIMIT,
  )

  // A cached identity still returns a correct key after eviction pressure.
  expect(coordinationKey("ns", "bulkhead:name", "op", "scope")).toBe(first)
})

it("still validates inputs it has never seen", () => {
  expect(() => coordinationKey("ns", "bulkhead:name", "op", "")).toThrow()
  expect(() =>
    coordinationKey("ns", "bulkhead:name", "op", "x".repeat(1025)),
  ).toThrow()
  expect(() =>
    coordinationKey("ns", "bulkhead:name", "op", "scope", ""),
  ).toThrow()
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
