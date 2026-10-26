/**
 * Unit tests for the shared Serval session.
 *
 * The session is policy-agnostic infrastructure: identity, the shared scope
 * registry, the bounded authority registry, notification handling, reconciliation
 * and lifecycle. `docs/serval/README.md` and `docs/serval/authority.md` are the contract;
 * the breaker, rate limiter and bulkhead all reuse this session unchanged.
 */

import { describe, expect, it } from "vitest"
import { ServalRegistryPressureError } from "../../src/coordination/serval/cache.js"
import { createServalMetrics } from "../../src/coordination/serval/metrics.js"
import { encodeServalInvalidation } from "../../src/coordination/serval/notifications.js"
import {
  ServalSessionClosedError,
  createServalAuthoritySession,
} from "../../src/coordination/serval/session.js"
import { readSource } from "../support/package-surface.js"
import { createDeterministicScheduler } from "../support/serval/deterministic-scheduler.js"
import {
  createFakeAuthority,
  invalidationOf,
} from "../support/serval/fake-authority.js"
import {
  TEST_IDENTITY,
  createSessionHarness,
  identityFor,
} from "../support/serval/session-harness.js"

describe("serval session identity", () => {
  it("keeps an explicit replica id and gives every session a fresh incarnation", () => {
    const scheduler = createDeterministicScheduler(0)
    const authority = createFakeAuthority()
    let generated = 0
    const options = {
      namespace: "ns",
      reader: authority.reader,
      scheduler,
      replicaId: "replica-7",
      idFactory: () => {
        generated += 1
        return `generated-${generated}`
      },
    }

    const first = createServalAuthoritySession(options)
    const second = createServalAuthoritySession(options)

    expect(first.replicaId).toBe("replica-7")
    expect(second.replicaId).toBe("replica-7")
    // A restart is a new session, so it is never entitled to the previous
    // incarnation's grants.
    expect(first.replicaIncarnation).toBe("generated-1")
    expect(second.replicaIncarnation).toBe("generated-2")
    expect(first.replicaIncarnation).not.toBe(second.replicaIncarnation)
  })

  it("requires a namespace and validates its numeric options", () => {
    const authority = createFakeAuthority()
    const base = { namespace: "ns", reader: authority.reader }

    expect(() =>
      createServalAuthoritySession({ ...base, namespace: "  " }),
    ).toThrow(TypeError)
    expect(() =>
      createServalAuthoritySession({ ...base, registryCapacity: 0 }),
    ).toThrow(RangeError)
    expect(() =>
      createServalAuthoritySession({ ...base, reconciliationIntervalMs: 0 }),
    ).toThrow(RangeError)
    expect(() =>
      createServalAuthoritySession({ ...base, safetyMarginMs: -1 }),
    ).toThrow(RangeError)
  })
})

describe("serval session scope registry", () => {
  it("registers one scope per identity and refcounts its handles", () => {
    const harness = createSessionHarness()

    const first = harness.session.registerScope(TEST_IDENTITY)
    const second = harness.session.registerScope(TEST_IDENTITY)

    expect(first.key).toBe(second.key)
    expect(harness.session.scopeCount).toBe(1)

    // The scope is released only when the last handle is.
    expect(first.unregister()).toBe(false)
    expect(harness.session.scopeCount).toBe(1)
    expect(second.unregister()).toBe(true)
    expect(harness.session.scopeCount).toBe(0)
    // A handle releases once.
    expect(first.unregister()).toBe(false)
    expect(harness.session.unregisterScope(first.key)).toBe(false)
  })

  it("isolates scopes by every part of the identity", () => {
    const harness = createSessionHarness()

    const keys = [
      harness.session.registerScope(TEST_IDENTITY).key,
      harness.session.registerScope(identityFor("tenant-b")).key,
      harness.session.registerScope({ ...TEST_IDENTITY, policyName: "refunds" })
        .key,
      harness.session.registerScope({ ...TEST_IDENTITY, operation: "refund" })
        .key,
      harness.session.registerScope({
        ...TEST_IDENTITY,
        policyKind: "rate-limit",
      }).key,
    ]

    expect(new Set(keys).size).toBe(5)
    expect(harness.session.scopeCount).toBe(5)
  })

  it("refuses a new scope when the registry is full, with an observable error", () => {
    const harness = createSessionHarness({ registryCapacity: 2 })
    const first = harness.session.registerScope(identityFor("a"))
    harness.session.registerScope(identityFor("b"))

    expect(() => harness.session.registerScope(identityFor("c"))).toThrow(
      ServalRegistryPressureError,
    )
    expect(harness.session.scopeCount).toBe(2)
    expect(harness.session.metrics().counters.registryRefusals).toBe(1)

    // The refusal left no half-registered scope behind.
    first.unregister()
    expect(harness.session.registerScope(identityFor("c")).key).toBeDefined()
    expect(harness.session.scopeCount).toBe(2)
  })

  it("rejects an invalid identity and any registration after close", async () => {
    const harness = createSessionHarness()

    expect(() =>
      harness.session.registerScope({ ...TEST_IDENTITY, scope: "  " }),
    ).toThrow(TypeError)

    await harness.session.close()
    expect(() => harness.session.registerScope(TEST_IDENTITY)).toThrow(
      ServalSessionClosedError,
    )
  })
})

describe("serval session lifecycle", () => {
  it("moves from uninitialized to valid on a successful cold start", async () => {
    const harness = createSessionHarness({ reconciliationIntervalMs: 10_000 })
    harness.session.registerScope(TEST_IDENTITY)
    harness.authority.publish(TEST_IDENTITY)

    expect(harness.session.state).toBe("uninitialized")
    await harness.session.start()

    expect(harness.session.state).toBe("valid")
    expect(harness.session.authorityIncarnation).toBe(
      harness.authority.incarnation(),
    )
    expect(harness.notifications.subscribed).toBe(true)
    expect(harness.session.metrics().counters.reconciliations).toBe(1)
  })

  it("reports disconnected when the cold start read fails and recovers", async () => {
    const harness = createSessionHarness({ reconciliationIntervalMs: 10_000 })
    harness.authority.failReads(1)

    await harness.session.start()
    expect(harness.session.state).toBe("disconnected")
    expect(harness.session.metrics().counters.reconciliationFailures).toBe(1)

    await harness.session.refresh("periodic")
    expect(harness.session.state).toBe("valid")
  })

  it("retries a subscription that could not be opened", async () => {
    const harness = createSessionHarness({ reconciliationIntervalMs: 10_000 })
    harness.notifications.failNextOpen()
    harness.authority.publish(TEST_IDENTITY)

    await harness.session.start()
    await harness.session.whenIdle()

    // Hints are optional, but a failed subscription is retried rather than left
    // silently down, and reconciliation kept working throughout.
    expect(harness.notifications.opens).toBe(2)
    expect(harness.notifications.subscribed).toBe(true)
    expect(harness.session.state).toBe("valid")
  })

  it("reports disconnected while the subscription is down and refreshes on reconnect", async () => {
    const harness = createSessionHarness({ reconciliationIntervalMs: 10_000 })
    await harness.session.start()
    expect(harness.session.state).toBe("valid")

    harness.notifications.disconnect()
    expect(harness.session.state).toBe("disconnected")

    harness.notifications.reconnect()
    await harness.session.whenIdle()
    expect(harness.session.state).toBe("valid")
    expect(harness.session.metrics().counters.reconnects).toBe(1)
  })

  it("reports stale once the last reconciliation is older than the allowance", async () => {
    const harness = createSessionHarness({
      reconciliationIntervalMs: 10_000,
      staleAfterMs: 50,
    })
    await harness.session.start()
    expect(harness.session.state).toBe("valid")

    await harness.scheduler.advanceBy(100)
    expect(harness.session.state).toBe("stale")
  })

  it("reports refreshing while a reconciliation is in flight", async () => {
    const harness = createSessionHarness({ reconciliationIntervalMs: 10_000 })
    await harness.session.start()

    harness.authority.holdReads()
    const pending = harness.session.refresh("periodic")
    expect(harness.session.state).toBe("refreshing")

    harness.authority.releaseReads()
    await pending
    expect(harness.session.state).toBe("valid")
  })

  it("keeps the refresh loop running and stops it on close", async () => {
    const harness = createSessionHarness({ reconciliationIntervalMs: 100 })
    harness.session.registerScope(TEST_IDENTITY)
    harness.authority.publish(TEST_IDENTITY)
    await harness.session.start()
    // Registering the scope asked for a refresh; let it run so the baseline is
    // the steady state rather than a coalesced read.
    await harness.scheduler.advanceBy(0)
    await harness.session.whenIdle()
    const readsAfterStart = harness.authority.reads

    await harness.scheduler.advanceBy(300)
    expect(harness.authority.reads - readsAfterStart).toBe(3)
    expect(harness.scheduler.pending()).toBe(1)

    await harness.session.close()
    expect(harness.session.state).toBe("closed")
    expect(harness.scheduler.pending()).toBe(0)
    expect(harness.notifications.subscribed).toBe(false)
    expect(harness.notifications.closes).toBe(1)
  })

  it("refuses work after close and releases the registry", async () => {
    const harness = createSessionHarness({ reconciliationIntervalMs: 100 })
    harness.session.registerScope(TEST_IDENTITY)
    harness.authority.publish(TEST_IDENTITY)
    await harness.session.start()
    const record = harness.authority.record(TEST_IDENTITY)

    await harness.session.close()

    expect(harness.session.scopeCount).toBe(0)
    expect(harness.session.readAuthority(TEST_IDENTITY)).toBeUndefined()
    expect(
      harness.session.applyAuthority({
        record: record as NonNullable<typeof record>,
        requestStartedAt: 0,
        validForMs: 1_000,
        receivedAt: 0,
      }),
    ).toEqual({ type: "rejected", reason: "closed" })
  })
})

describe("serval session authority application", () => {
  async function startedHarness(options: { safetyMarginMs?: number } = {}) {
    const harness = createSessionHarness({
      safetyMarginMs: options.safetyMarginMs ?? 0,
      reconciliationIntervalMs: 10_000,
    })
    harness.session.registerScope(TEST_IDENTITY)
    harness.authority.publish(TEST_IDENTITY)
    await harness.session.start()
    return harness
  }

  it("applies a reply anchored to its send time", async () => {
    const harness = await startedHarness()
    const record = harness.authority.publish(TEST_IDENTITY, {
      validForMs: 5_000,
    })
    const requestStartedAt = harness.scheduler.now()

    expect(
      harness.session.applyAuthority({
        record,
        requestStartedAt,
        validForMs: 100,
        receivedAt: requestStartedAt + 100,
      }),
    ).toEqual({ type: "applied", configurationChanged: false })

    const cached = harness.session.readAuthority(TEST_IDENTITY)
    expect(cached?.usableUntil).toBe(requestStartedAt + 100)
    expect(cached?.record.version.version).toBe(record.version.version)
  })

  it("rejects a reply that arrives after the send-time-anchored deadline", async () => {
    const harness = await startedHarness()
    const record = harness.authority.publish(TEST_IDENTITY)
    const requestStartedAt = harness.scheduler.now()

    expect(
      harness.session.applyAuthority({
        record,
        requestStartedAt,
        validForMs: 100,
        receivedAt: requestStartedAt + 101,
      }),
    ).toEqual({ type: "rejected", reason: "late" })

    // A late reply cannot extend authority: the cached record is unchanged.
    expect(
      harness.session.readAuthority(TEST_IDENTITY)?.record.version.version,
    ).toBe(1)
  })

  it("subtracts the safety margin from the reply deadline", async () => {
    const harness = await startedHarness({ safetyMarginMs: 25 })
    const record = harness.authority.publish(TEST_IDENTITY)
    const requestStartedAt = harness.scheduler.now()

    expect(
      harness.session.applyAuthority({
        record,
        requestStartedAt,
        validForMs: 100,
        receivedAt: requestStartedAt + 75,
      }),
    ).toEqual({ type: "applied", configurationChanged: false })
    expect(harness.session.readAuthority(TEST_IDENTITY)?.usableUntil).toBe(
      requestStartedAt + 75,
    )
  })

  it("ignores a record that does not advance the version", async () => {
    const harness = await startedHarness()
    const cached = harness.authority.record(TEST_IDENTITY)
    const requestStartedAt = harness.scheduler.now()

    // The authority answered with exactly what we hold: ignored, but a verification of the
    // current verdict rather than a superseded reply.
    expect(
      harness.session.applyAuthority({
        record: cached as NonNullable<typeof cached>,
        requestStartedAt,
        validForMs: 100,
        receivedAt: requestStartedAt,
      }),
    ).toEqual({ type: "ignored", reason: "unchanged" })
  })

  it("distinguishes a superseded record from an unchanged one", async () => {
    const harness = await startedHarness()
    const cached = harness.authority.record(TEST_IDENTITY) as NonNullable<
      ReturnType<typeof harness.authority.record>
    >
    const requestStartedAt = harness.scheduler.now()

    // An older version describes a past observation, so a caller caching freshness must be able to
    // tell it apart from a record that confirms what we hold.
    expect(
      harness.session.applyAuthority({
        record: {
          ...cached,
          version: { ...cached.version, version: cached.version.version - 1 },
        },
        requestStartedAt,
        validForMs: 100,
        receivedAt: requestStartedAt,
      }),
    ).toEqual({ type: "ignored", reason: "superseded" })
  })

  it("ignores a record for an unregistered identity and a malformed record", async () => {
    const harness = await startedHarness()
    const requestStartedAt = harness.scheduler.now()
    const other = harness.authority.publish(identityFor("tenant-b"))

    expect(
      harness.session.applyAuthority({
        record: other,
        requestStartedAt,
        validForMs: 100,
        receivedAt: requestStartedAt,
      }),
    ).toEqual({ type: "ignored", reason: "unregistered" })

    expect(
      harness.session.applyAuthority({
        record: { ...other, expiresAt: other.issuedAt - 1 },
        requestStartedAt,
        validForMs: 100,
        receivedAt: requestStartedAt,
      }),
    ).toEqual({ type: "ignored", reason: "malformed" })
  })

  it("retires the current incarnation and refuses its replies", async () => {
    const harness = await startedHarness()
    const incarnation = harness.authority.incarnation()
    const requestStartedAt = harness.scheduler.now()

    harness.session.retireIncarnation(incarnation)
    expect(harness.session.state).toBe("uncertain")
    expect(harness.session.authorityIncarnation).toBeUndefined()

    const record = harness.authority.publish(TEST_IDENTITY, { incarnation })
    expect(
      harness.session.applyAuthority({
        record,
        requestStartedAt,
        validForMs: 100,
        receivedAt: requestStartedAt,
      }),
    ).toEqual({ type: "rejected", reason: "retired-incarnation" })
    expect(harness.session.metrics().counters.incarnationRetirements).toBe(1)
  })

  it("treats an unfamiliar incarnation as uncertain rather than adopting it", async () => {
    const harness = await startedHarness()
    const requestStartedAt = harness.scheduler.now()
    const record = harness.authority.publish(TEST_IDENTITY, {
      incarnation: "authority-other",
    })

    expect(
      harness.session.applyAuthority({
        record,
        requestStartedAt,
        validForMs: 100,
        receivedAt: requestStartedAt,
      }),
    ).toEqual({ type: "rejected", reason: "unknown-incarnation" })
    expect(harness.session.state).toBe("uncertain")
    expect(harness.session.authorityIncarnation).toBe(
      harness.authority.incarnation(),
    )
  })

  it("reports a configuration change and moves the revision forward", async () => {
    const harness = await startedHarness()
    const requestStartedAt = harness.scheduler.now()
    const record = harness.authority.publish(TEST_IDENTITY, {
      configRevision: 9,
    })

    expect(
      harness.session.applyAuthority({
        record,
        requestStartedAt,
        validForMs: 100,
        receivedAt: requestStartedAt,
      }),
    ).toEqual({ type: "applied", configurationChanged: true })
    expect(harness.session.configurationRevision).toBe(9)
  })
})

describe("serval session reconciliation", () => {
  it("caches a snapshot record with its remaining validity as a client deadline", async () => {
    const harness = createSessionHarness({
      safetyMarginMs: 0,
      reconciliationIntervalMs: 10_000,
    })
    harness.session.registerScope(TEST_IDENTITY)
    harness.authority.publish(TEST_IDENTITY, { validForMs: 5_000 })

    await harness.session.start()

    const cached = harness.session.readAuthority(TEST_IDENTITY)
    // The read's send time (1000) plus the record's remaining validity (5000).
    expect(cached?.usableUntil).toBe(6_000)
    expect(cached?.record.version.version).toBe(1)
  })

  it("never turns an absolute server timestamp into a client deadline", async () => {
    const scheduler = createDeterministicScheduler(1_000)
    const authority = createFakeAuthority({
      serverTime: () => scheduler.now() + 1_000_000,
    })
    const session = createServalAuthoritySession({
      namespace: "ns",
      reader: authority.reader,
      scheduler,
      safetyMarginMs: 0,
      reconciliationIntervalMs: 10_000,
    })
    session.registerScope(TEST_IDENTITY)
    authority.publish(TEST_IDENTITY, { validForMs: 5_000 })

    await session.start()

    // The server's clock is a million milliseconds ahead of the client's, yet only
    // the duration crosses: the deadline is the client's own send time plus 5000.
    expect(session.readAuthority(TEST_IDENTITY)?.usableUntil).toBe(6_000)
  })

  it("ignores a snapshot record whose remaining validity is inside the margin", async () => {
    const harness = createSessionHarness({
      safetyMarginMs: 100,
      reconciliationIntervalMs: 10_000,
    })
    harness.session.registerScope(TEST_IDENTITY)
    harness.authority.publish(TEST_IDENTITY, { validForMs: 100 })

    await harness.session.start()
    expect(harness.session.readAuthority(TEST_IDENTITY)).toBeUndefined()
  })

  it("skips a snapshot record stamped with another incarnation", async () => {
    const harness = createSessionHarness({ reconciliationIntervalMs: 10_000 })
    harness.session.registerScope(TEST_IDENTITY)
    harness.authority.publish(TEST_IDENTITY, { incarnation: "authority-old" })

    await harness.session.start()

    expect(harness.session.authorityIncarnation).toBe("authority-1")
    expect(harness.session.readAuthority(TEST_IDENTITY)).toBeUndefined()
  })

  it("clears the cache and retires the old incarnation on a failover", async () => {
    const harness = createSessionHarness({ reconciliationIntervalMs: 10_000 })
    harness.session.registerScope(TEST_IDENTITY)
    harness.authority.publish(TEST_IDENTITY)
    await harness.session.start()
    expect(harness.session.readAuthority(TEST_IDENTITY)).toBeDefined()

    const next = harness.authority.failover()
    await harness.session.refresh("periodic")

    expect(harness.session.authorityIncarnation).toBe(next)
    // The old incarnation's records are gone: a new authority re-issues them.
    expect(harness.session.readAuthority(TEST_IDENTITY)).toBeUndefined()
    expect(harness.session.metrics().counters.incarnationRetirements).toBe(1)

    harness.authority.publish(TEST_IDENTITY)
    await harness.session.refresh("periodic")
    expect(
      harness.session.readAuthority(TEST_IDENTITY)?.record.version
        .authorityIncarnation,
    ).toBe(next)
  })

  it("flags a rewound ledger as a rollback and keeps the newer record", async () => {
    const harness = createSessionHarness({ reconciliationIntervalMs: 10_000 })
    harness.session.registerScope(TEST_IDENTITY)
    harness.authority.publish(TEST_IDENTITY)
    await harness.session.start()
    harness.authority.publish(TEST_IDENTITY)
    await harness.session.refresh("periodic")
    expect(
      harness.session.readAuthority(TEST_IDENTITY)?.record.version.version,
    ).toBe(2)

    harness.authority.rewindVersions(2)
    await harness.session.refresh("periodic")

    expect(harness.session.state).toBe("uncertain")
    expect(harness.session.metrics().counters.rollbacksSuspected).toBe(1)
    // Monotonic state application: the older snapshot never overwrites the cache.
    expect(
      harness.session.readAuthority(TEST_IDENTITY)?.record.version.version,
    ).toBe(2)

    // Only an authoritative reconciliation that sees no rollback clears it.
    harness.authority.publish(TEST_IDENTITY, { version: 5 })
    await harness.session.refresh("periodic")
    expect(harness.session.state).toBe("valid")
    expect(
      harness.session.readAuthority(TEST_IDENTITY)?.record.version.version,
    ).toBe(5)
  })

  it("coalesces many invalidations into one refresh", async () => {
    const harness = createSessionHarness({ reconciliationIntervalMs: 10_000 })
    harness.session.registerScope(TEST_IDENTITY)
    harness.authority.publish(TEST_IDENTITY)
    await harness.session.start()
    await harness.scheduler.advanceBy(0)
    await harness.session.whenIdle()
    const readsAfterStart = harness.authority.reads

    for (let index = 0; index < 5; index += 1) {
      harness.notifications.deliverInvalidation(
        invalidationOf(harness.authority.publish(TEST_IDENTITY)),
      )
    }
    expect(harness.session.metrics().counters.invalidationsApplied).toBe(5)

    await harness.scheduler.advanceBy(0)
    await harness.session.whenIdle()

    expect(harness.authority.reads - readsAfterStart).toBe(1)
    expect(
      harness.session.readAuthority(TEST_IDENTITY)?.record.version.version,
    ).toBe(6)
  })

  it("ignores a duplicated and a reordered invalidation", async () => {
    const harness = createSessionHarness({ reconciliationIntervalMs: 10_000 })
    harness.session.registerScope(TEST_IDENTITY)
    const first = harness.authority.publish(TEST_IDENTITY)
    await harness.session.start()
    await harness.scheduler.advanceBy(0)
    await harness.session.whenIdle()

    const second = harness.authority.publish(TEST_IDENTITY)
    harness.notifications.deliverTwice(
      encodeServalInvalidation(invalidationOf(second)),
    )
    harness.notifications.deliverInvalidation(invalidationOf(first))

    const { counters } = harness.session.metrics()
    expect(counters.invalidationsReceived).toBe(3)
    // A hint is not a state change: both deliveries of the newer version coalesce
    // into one refresh, and the older one is ignored outright.
    expect(counters.invalidationsApplied).toBe(2)
    expect(counters.invalidationsIgnored).toBe(1)

    await harness.scheduler.advanceBy(0)
    await harness.session.whenIdle()
    expect(
      harness.session.readAuthority(TEST_IDENTITY)?.record.version.version,
    ).toBe(2)

    // Once the cache has advanced, the same hint no longer advances anything.
    harness.notifications.deliverInvalidation(invalidationOf(second))
    expect(harness.session.metrics().counters.invalidationsIgnored).toBe(2)
  })

  it("drops an invalidation for an identity this process does not hold", async () => {
    const harness = createSessionHarness({ reconciliationIntervalMs: 10_000 })
    await harness.session.start()
    const reads = harness.authority.reads

    harness.notifications.deliverInvalidation(
      invalidationOf(harness.authority.publish(identityFor("other"))),
    )

    expect(harness.session.metrics().counters.invalidationsIgnored).toBe(1)
    expect(harness.authority.reads).toBe(reads)
  })

  it("counts a malformed payload and does not refresh", async () => {
    const harness = createSessionHarness({ reconciliationIntervalMs: 10_000 })
    harness.session.registerScope(TEST_IDENTITY)
    await harness.session.start()
    const reads = harness.authority.reads

    harness.notifications.deliver("{not json")
    harness.notifications.deliver(
      encodeServalInvalidation(
        invalidationOf(harness.authority.publish(TEST_IDENTITY)),
      ).replace('"v":1', '"v":99'),
    )

    expect(harness.session.metrics().counters.invalidationsMalformed).toBe(2)
    expect(harness.authority.reads).toBe(reads)
  })

  it("ignores an invalidation from a retired incarnation", async () => {
    const harness = createSessionHarness({ reconciliationIntervalMs: 10_000 })
    harness.session.registerScope(TEST_IDENTITY)
    harness.authority.publish(TEST_IDENTITY)
    await harness.session.start()

    const incarnation = harness.authority.incarnation()
    harness.session.retireIncarnation(incarnation)
    harness.notifications.deliverInvalidation(
      invalidationOf(harness.authority.publish(TEST_IDENTITY, { incarnation })),
    )

    expect(harness.session.metrics().counters.invalidationsIgnored).toBe(1)
  })

  it("treats an invalidation from an unfamiliar incarnation as uncertain", async () => {
    const harness = createSessionHarness({ reconciliationIntervalMs: 10_000 })
    harness.session.registerScope(TEST_IDENTITY)
    harness.authority.publish(TEST_IDENTITY)
    await harness.session.start()

    harness.notifications.deliverInvalidation(
      invalidationOf(
        harness.authority.publish(TEST_IDENTITY, {
          incarnation: "authority-other",
        }),
      ),
    )

    expect(harness.session.state).toBe("uncertain")
    expect(harness.session.metrics().counters.invalidationsIgnored).toBe(1)
  })
})

describe("serval session diagnostics", () => {
  it("reports per-scope diagnostics and drops diagnostics for untracked scopes", async () => {
    const harness = createSessionHarness({
      registryCapacity: 2,
      reconciliationIntervalMs: 10_000,
    })
    const scope = harness.session.registerScope(TEST_IDENTITY)
    harness.authority.publish(TEST_IDENTITY)
    await harness.session.start()

    const snapshot = harness.session.metrics()
    expect(snapshot.registrySize).toBe(1)
    expect(snapshot.scopes).toEqual([
      {
        key: scope.key,
        applied: 1,
        ignored: 0,
        rejected: 0,
        lastAppliedAt: 1_000,
      },
    ])

    // A diagnostics entry for an untracked scope is dropped and counted, never
    // stored: the map stays bounded by the scope registry.
    const metrics = createServalMetrics()
    metrics.observeScope("untracked", "applied")
    expect(metrics.snapshot().counters.diagnosticsDropped).toBe(1)
    expect(metrics.snapshot().scopes).toEqual([])
  })

  it("keeps policy algorithms out of the shared session", () => {
    const shared = [
      "session.ts",
      "cache.ts",
      "notifications.ts",
      "reconciliation.ts",
      "metrics.ts",
      "scheduler.ts",
    ]

    for (const file of shared) {
      const source = readSource(`src/coordination/serval/${file}`)
      expect(source, `${file} must not name a policy kind`).not.toMatch(
        /"(circuit-breaker|rate-limit|bulkhead)"/,
      )
      expect(source, `${file} must not import a policy module`).not.toMatch(
        /core\/(circuit-breaker|rate-limit|bulkhead)/,
      )
    }
  })
})
