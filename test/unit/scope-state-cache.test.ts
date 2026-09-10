import { describe, expect, it } from "vitest"
import { createScopeStateCache } from "../../src/core/scope-state-cache.js"

describe("scopeStateCache", () => {
  it("stores and reads retained states", () => {
    const cache = createScopeStateCache()
    expect(cache.size()).toBe(0)

    cache.remember("op", "region:eu", "open")
    expect(cache.read("op", "region:eu")).toBe("open")
    expect(cache.size()).toBe(1)

    cache.remember("op", "region:eu", "half-open")
    expect(cache.read("op", "region:eu")).toBe("half-open")
    expect(cache.size()).toBe(1)
  })

  it("forgets a scope without affecting the others", () => {
    const cache = createScopeStateCache()
    cache.remember("op", "a", "open")
    cache.remember("op", "b", "half-open")

    cache.forget("op", "a")

    expect(cache.read("op", "a")).toBeUndefined()
    expect(cache.read("op", "b")).toBe("half-open")
    expect(cache.size()).toBe(1)
  })

  it("isolates the same scope across operations", () => {
    const cache = createScopeStateCache()
    cache.remember("A", "region:eu", "open")

    expect(cache.read("B", "region:eu")).toBeUndefined()

    cache.remember("B", "region:eu", "half-open")

    expect(cache.read("A", "region:eu")).toBe("open")
    expect(cache.read("B", "region:eu")).toBe("half-open")
    expect(cache.size()).toBe(2)
  })

  it("does not accumulate when closed scopes are forgotten", () => {
    const cache = createScopeStateCache()
    for (let i = 0; i < 1_000; i++) {
      cache.remember("op", `tenant:${i}`, "open")
      cache.forget("op", `tenant:${i}`)
    }

    expect(cache.size()).toBe(0)
  })
})
