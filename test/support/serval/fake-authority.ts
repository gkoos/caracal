import {
  servalScopeKey,
  type ServalAuthorityRecord,
  type ServalScopeIdentity,
} from "../../../src/coordination/serval/authority.js"
import type {
  ServalInvalidation,
  ServalInvalidationKind,
} from "../../../src/coordination/serval/notifications.js"
import type {
  ServalAuthorityReader,
  ServalAuthoritySnapshot,
} from "../../../src/coordination/serval/reconciliation.js"

/**
 * A scriptable stand-in for the authoritative Redis state.
 *
 * It holds versioned records, stamps the server's own clock reading on each one,
 * and can be told to fail reads, rewind its ledger (a restore from a stale
 * snapshot) or change incarnation (a failover). Nothing here interprets policy
 * semantics: it is the authority side of the protocol only.
 */

export interface FakeAuthorityOptions {
  readonly incarnation?: string
  /** The server's clock. Injected so a test controls remaining validity. */
  readonly serverTime?: () => number
  readonly configRevision?: number
}

export interface PublishOptions {
  readonly epoch?: number
  readonly version?: number
  readonly configRevision?: number
  /** Lifetime measured from the server's clock reading at publish time. */
  readonly validForMs?: number
  readonly payload?: unknown
  readonly incarnation?: string
}

export interface FakeAuthority {
  readonly reader: ServalAuthorityReader
  /** Snapshot reads attempted. */
  readonly reads: number
  incarnation(): string
  configurationRevision(): number
  publish(
    identity: ServalScopeIdentity,
    options?: PublishOptions,
  ): ServalAuthorityRecord
  record(identity: ServalScopeIdentity): ServalAuthorityRecord | undefined
  records(): readonly ServalAuthorityRecord[]
  withdraw(identity: ServalScopeIdentity): boolean
  /** Fails the next `count` reads. */
  failReads(count: number, error?: unknown): void
  /** Holds every read until `releaseReads`, so an in-flight state is visible. */
  holdReads(): void
  releaseReads(): void
  /** Rewinds every stored version, as a restore from a stale snapshot does. */
  rewindVersions(by?: number): void
  /** Switches the authority lifecycle, as a failover does. */
  failover(incarnation?: string): string
}

/** The invalidation a published record would announce. */
export function invalidationOf(
  record: ServalAuthorityRecord,
  kind: ServalInvalidationKind = "authority",
): ServalInvalidation {
  return { identity: record.identity, version: record.version, kind }
}

export function createFakeAuthority(
  options: FakeAuthorityOptions = {},
): FakeAuthority {
  const serverTime = options.serverTime ?? (() => Date.now())
  const records = new Map<string, ServalAuthorityRecord>()
  const versions = new Map<string, number>()

  let incarnation = options.incarnation ?? "authority-1"
  const configRevision = options.configRevision ?? 1
  let reads = 0
  let failures = 0
  let failureError: unknown
  let failovers = 1
  let gate: Promise<void> | undefined
  let releaseGate: (() => void) | undefined

  function snapshotRecord(
    record: ServalAuthorityRecord,
  ): ServalAuthorityRecord {
    return { ...record, version: { ...record.version } }
  }

  return {
    reader: {
      async readSnapshot(): Promise<ServalAuthoritySnapshot> {
        reads += 1
        if (gate !== undefined) {
          await gate
        }
        if (failures > 0) {
          failures -= 1
          throw failureError ?? new Error("the fake authority is unavailable")
        }
        return {
          authorityIncarnation: incarnation,
          serverTime: serverTime(),
          configRevision,
          records: [...records.values()].map(snapshotRecord),
        }
      },
    },

    get reads() {
      return reads
    },

    incarnation() {
      return incarnation
    },

    configurationRevision() {
      return configRevision
    },

    publish(identity, publishOptions = {}) {
      const key = servalScopeKey(identity)
      const version = publishOptions.version ?? (versions.get(key) ?? 0) + 1
      versions.set(key, version)

      const issuedAt = serverTime()
      const record: ServalAuthorityRecord = {
        identity,
        version: {
          authorityIncarnation: publishOptions.incarnation ?? incarnation,
          epoch: publishOptions.epoch ?? 0,
          version,
          configRevision: publishOptions.configRevision ?? configRevision,
        },
        issuedAt,
        expiresAt: issuedAt + (publishOptions.validForMs ?? 60_000),
        payload: publishOptions.payload,
      }
      records.set(key, record)
      return snapshotRecord(record)
    },

    record(identity) {
      const record = records.get(servalScopeKey(identity))
      return record === undefined ? undefined : snapshotRecord(record)
    },

    records() {
      return [...records.values()].map(snapshotRecord)
    },

    withdraw(identity) {
      return records.delete(servalScopeKey(identity))
    },

    failReads(count, error) {
      failures = count
      failureError = error
    },

    holdReads() {
      if (gate !== undefined) return
      gate = new Promise<void>((resolve) => {
        releaseGate = resolve
      })
    },

    releaseReads() {
      releaseGate?.()
      releaseGate = undefined
      gate = undefined
    },

    rewindVersions(by = 1) {
      for (const [key, record] of records) {
        records.set(key, {
          ...record,
          version: {
            ...record.version,
            version: Math.max(0, record.version.version - by),
          },
        })
      }
    },

    failover(next) {
      failovers += 1
      incarnation = next ?? `authority-${failovers}`
      return incarnation
    },
  }
}
