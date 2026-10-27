/**
 * Unit tests for the bounded Serval authority registry.
 *
 * `docs/serval/README.md` Contract 5 is the contract: three record classes, pinned and
 * protective records that pressure never evicts, replay tombstones that outlive
 * their operation, and an observable refusal instead of dropped accounting.
 */

import { describe, expect, it } from "vitest"
import {
  ServalRegistryPressureError,
  createServalAuthorityCache,
} from "../../../src/serval/cache.js"

describe("serval authority cache", () => {
  it("refuses a new key rather than evicting pinned accounting", () => {
    const cache = createServalAuthorityCache<number>({
      capacity: 2,
      clock: () => 0,
    })
    cache.register({
      key: "grant-a",
      recordClass: "pinned",
      value: 1,
      retainedUntil: 100,
    })
    cache.register({
      key: "grant-b",
      recordClass: "pinned",
      value: 2,
      retainedUntil: 100,
    })

    expect(() =>
      cache.register({
        key: "grant-c",
        recordClass: "pinned",
        value: 3,
        retainedUntil: 100,
      }),
    ).toThrow(ServalRegistryPressureError)

    expect(cache.size).toBe(2)
    expect(cache.refusals).toBe(1)
    expect(cache.evictions).toBe(0)
    expect(cache.read("grant-a")?.value).toBe(1)
    expect(cache.read("grant-b")?.value).toBe(2)
  })

  it("evicts a disposable entry to make room, never a protective one", () => {
    let now = 0
    const cache = createServalAuthorityCache<string>({
      capacity: 2,
      clock: () => now,
    })
    cache.register({
      key: "open-verdict",
      recordClass: "protective",
      value: "open",
      retainedUntil: 0,
    })
    cache.register({
      key: "idle",
      recordClass: "disposable",
      value: "idle",
      retainedUntil: 10,
    })

    now = 1_000
    cache.register({
      key: "new",
      recordClass: "pinned",
      value: "new",
      retainedUntil: 2_000,
    })

    expect(cache.read("open-verdict")?.value).toBe("open")
    expect(cache.has("idle")).toBe(false)
    expect(cache.evictions).toBe(1)
  })

  it("keeps a tombstone until its replay window closes", () => {
    let now = 0
    const cache = createServalAuthorityCache<number>({
      capacity: 2,
      clock: () => now,
    })
    cache.registerTombstone("op-1", 50)

    expect(cache.read("op-1")).toMatchObject({
      recordClass: "pinned",
      tombstone: true,
      value: undefined,
    })
    expect(cache.sweep(49)).toBe(0)
    expect(cache.has("op-1")).toBe(true)

    now = 50
    expect(cache.sweep()).toBe(1)
    expect(cache.has("op-1")).toBe(false)
  })

  it("replaces an entry, reclassifies it and releases it", () => {
    const cache = createServalAuthorityCache<string>({ capacity: 1 })

    cache.register({
      key: "verdict",
      recordClass: "disposable",
      value: "closed",
      retainedUntil: 10,
    })
    expect(cache.read("verdict")?.recordClass).toBe("disposable")

    expect(cache.retain("verdict", "pinned", 20)).toBe(true)
    expect(cache.read("verdict")).toMatchObject({
      recordClass: "pinned",
      value: "closed",
      retainedUntil: 20,
    })

    expect(cache.retain("absent", "pinned", 20)).toBe(false)
    expect(cache.release("verdict")).toBe(true)
    expect(cache.release("verdict")).toBe(false)
    expect(cache.size).toBe(0)
  })

  it("never grows past its capacity when only pinned entries arrive", () => {
    const cache = createServalAuthorityCache<number>({
      capacity: 4,
      clock: () => 0,
    })
    for (let index = 0; index < 4; index += 1) {
      cache.register({
        key: `pinned-${index}`,
        recordClass: "pinned",
        value: index,
        retainedUntil: 1_000,
      })
    }

    for (let index = 4; index < 40; index += 1) {
      expect(() =>
        cache.register({
          key: `pinned-${index}`,
          recordClass: "pinned",
          value: index,
          retainedUntil: 1_000,
        }),
      ).toThrow(ServalRegistryPressureError)
    }

    expect(cache.size).toBe(4)
    expect(cache.refusals).toBe(36)
    expect(cache.keys()).toHaveLength(4)
  })

  it("rejects an unusable capacity", () => {
    expect(() => createServalAuthorityCache({ capacity: 0 })).toThrow(
      RangeError,
    )
    expect(() => createServalAuthorityCache({ capacity: 1.5 })).toThrow(
      RangeError,
    )
  })
})
