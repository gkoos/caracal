/**
 * The bounded Serval authority registry.
 *
 * Every entry is classified, and the classification decides what pressure may do
 * to it:
 *
 * - `disposable` - expired or unreferenced state. Evictable at will.
 * - `pinned` - live grants, holders, pending requests, consumed accounting and
 *   replay tombstones. Never evicted to satisfy the bound.
 * - `protective` - last-known protective knowledge such as a cached OPEN verdict.
 *   Never silently downgraded to a permissive cold start.
 *
 * A tombstone is a pinned record with no payload: it keeps a retired operation
 * identifier protected for its replay window and is dropped only when that window
 * closes.
 *
 * When the registry cannot admit a new key it refuses, with an observable error,
 * rather than dropping accounting. `docs/serval.md` (decision 5) is the contract.
 */

export type ServalCacheClass = "disposable" | "pinned" | "protective"

export class ServalRegistryPressureError extends Error {
  readonly coordination = "distributed"

  constructor(message: string) {
    super(message)
    this.name = "ServalRegistryPressureError"
  }
}

export interface ServalCacheEntry<Value> {
  readonly key: string
  readonly recordClass: ServalCacheClass
  readonly value: Value | undefined
  readonly retainedUntil: number
  readonly tombstone: boolean
}

export interface ServalAuthorityCacheOptions {
  readonly capacity: number
  /** Monotonic milliseconds. Injected so tests stay deterministic. */
  readonly clock?: () => number
}

export interface ServalAuthorityCache<Value> {
  readonly capacity: number
  /** Retained entries. */
  readonly size: number
  /** Registrations refused because the registry was full. */
  readonly refusals: number
  /** Entries dropped by `sweep`, never by pressure. */
  readonly evictions: number
  register(entry: {
    readonly key: string
    readonly recordClass: ServalCacheClass
    readonly value: Value
    readonly retainedUntil: number
  }): void
  /** Registers a payload-free pinned record for a retired identifier. */
  registerTombstone(key: string, retainedUntil: number): void
  /** Pins or reclassifies an existing entry. Returns false when it is absent. */
  retain(
    key: string,
    recordClass: ServalCacheClass,
    retainedUntil: number,
  ): boolean
  read(key: string): ServalCacheEntry<Value> | undefined
  has(key: string): boolean
  /** Releases an entry explicitly, e.g. when its accounting settles. */
  release(key: string): boolean
  /** Drops disposable entries and tombstones past retention. */
  sweep(now?: number): number
  keys(): readonly string[]
  clear(): void
}

export function createServalAuthorityCache<Value>(
  options: ServalAuthorityCacheOptions,
): ServalAuthorityCache<Value> {
  const { capacity } = options
  if (!Number.isSafeInteger(capacity) || capacity < 1) {
    throw new RangeError(
      "A Serval registry capacity must be a positive integer",
    )
  }
  const clock = options.clock ?? (() => Date.now())

  const entries = new Map<string, ServalCacheEntry<Value>>()
  let refusals = 0
  let evictions = 0

  function sweep(now: number): number {
    let dropped = 0
    for (const [key, entry] of entries) {
      const evictable = entry.tombstone || entry.recordClass === "disposable"
      if (evictable && now >= entry.retainedUntil) {
        entries.delete(key)
        dropped += 1
      }
    }
    evictions += dropped
    return dropped
  }

  function admit(key: string): void {
    if (entries.size < capacity) return

    // Pressure is answered by dropping what is disposable, never by evicting
    // accounting: a pinned grant or protective verdict outlives the bound.
    sweep(clock())
    if (entries.size < capacity) return

    refusals += 1
    throw new ServalRegistryPressureError(
      `The Serval authority registry is full (${capacity} entries); refusing to register ${key}`,
    )
  }

  return {
    capacity,
    get size() {
      return entries.size
    },
    get refusals() {
      return refusals
    },
    get evictions() {
      return evictions
    },

    register(entry) {
      if (!entries.has(entry.key)) {
        admit(entry.key)
      }
      entries.set(entry.key, {
        key: entry.key,
        recordClass: entry.recordClass,
        value: entry.value,
        retainedUntil: entry.retainedUntil,
        tombstone: false,
      })
    },

    registerTombstone(key, retainedUntil) {
      if (!entries.has(key)) {
        admit(key)
      }
      entries.set(key, {
        key,
        recordClass: "pinned",
        value: undefined,
        retainedUntil,
        tombstone: true,
      })
    },

    retain(key, recordClass, retainedUntil) {
      const entry = entries.get(key)
      if (entry === undefined) return false
      entries.set(key, {
        ...entry,
        recordClass,
        retainedUntil,
      })
      return true
    },

    read(key) {
      return entries.get(key)
    },

    has(key) {
      return entries.has(key)
    },

    release(key) {
      return entries.delete(key)
    },

    sweep(now = clock()) {
      return sweep(now)
    },

    keys() {
      return [...entries.keys()]
    },

    clear() {
      entries.clear()
    },
  }
}
