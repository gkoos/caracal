import { randomUUID } from "node:crypto"
import { afterAll, beforeAll, describe, expect, it } from "vitest"
import {
  createCoordinationClient,
  redisCircuitBreakerCoordinator,
} from "../../../src/redis.js"
import { createServalBreakerReader } from "../../../src/serval/breaker.js"
import { encodeServalInvalidation } from "../../../src/serval/notifications.js"
import type { ServalAuthorityReader } from "../../../src/serval/reconciliation.js"
import { createServalAuthoritySession } from "../../../src/serval/session.js"

/**
 * The authority is replaced under a running replica.
 *
 * Every other Serval test holds the authority still. This one moves it: the
 * authority's incarnation changes and the state it answers from is gone, so the
 * replica has to adopt the replacement through reconciliation and refuse anything
 * still arriving from the incarnation that has been retired - including a hint
 * that would otherwise take it back.
 *
 * The incarnation path is unit-tested (`ServalIncarnationTracker`) but had no
 * end-to-end witness, and the sandbox cannot supply one: it runs a constant
 * incarnation, so a failover is never exercised there. This is that witness.
 *
 *   npm run test:integration:redis
 */

const url = process.env.CARACAL_REDIS_URL
const IDENTITY = { name: "failover", operation: "charge", scope: "global" }
const SCOPE = {
  policyKind: "circuit-breaker" as const,
  policyName: IDENTITY.name,
  operation: IDENTITY.operation,
  scope: IDENTITY.scope,
}

describe.skipIf(!url)("serval authority failover", () => {
  const client = url ? createCoordinationClient(url) : undefined

  beforeAll(async () => {
    await client?.connect()
  })

  afterAll(() => {
    client?.disconnect()
  })

  it("adopts the replacement, and a retired incarnation cannot take it back", async () => {
    const namespace = `serval-failover-${randomUUID()}`
    const authority = redisCircuitBreakerCoordinator(client as never, {
      namespace,
    })

    // The authority's identity as the reader reports it. Replacing the authority
    // process is a change here; the reader is the seam that reports it, which is
    // why a real failover can be driven without restarting anything.
    let incarnation = "authority-a"
    const reader: ServalAuthorityReader = {
      readSnapshot: (options) =>
        createServalBreakerReader({
          scopes: () => [SCOPE],
          now: () => Date.now(),
          authority,
          freshnessMs: 1_000,
          authorityIncarnation: incarnation,
        }).readSnapshot(options),
    }

    // A transport the test drives itself, so a hint can be delivered after the
    // incarnation it refers to has been retired.
    let deliver: ((payload: string) => void) | undefined
    const transport = {
      async open(handlers: {
        onMessage: (payload: string) => void
        onStatus: (status: "connected" | "disconnected") => void
      }) {
        deliver = handlers.onMessage
        handlers.onStatus("connected")
      },
      async close() {},
    }

    const session = createServalAuthoritySession({
      namespace,
      reader,
      transport,
      replicaId: "replica-1",
      reconciliationIntervalMs: 60_000,
      staleAfterMs: 5_000,
    })
    session.registerScope(SCOPE)

    // The authority is open before the replica ever reads it, so what the replica
    // holds is a verdict rather than a default.
    const opened = await authority.observe(
      {
        name: IDENTITY.name,
        operation: IDENTITY.operation,
        scope: IDENTITY.scope,
      },
      {
        generation: 0,
        outcome: "failure",
        uuid: randomUUID(),
        windowTtlMs: 60_000,
        minimumThroughput: 1,
        failureThresholdNumerator: 500,
        windowSize: 20,
        openMs: 60_000,
        closedCleanupMs: 60_000,
      },
    )
    expect(opened).toMatchObject({ type: "opened" })

    await session.start()
    await session.whenIdle()

    // Cold start: the first snapshot is what adopts an incarnation.
    expect(session.authorityIncarnation).toBe("authority-a")
    expect(session.state).toBe("valid")
    expect(session.readAuthority(SCOPE)).toBeDefined()

    // A record from the incumbent, kept for the late-arrival cases below.
    const incumbent = await reader.readSnapshot({
      replicaId: session.replicaId,
      replicaIncarnation: session.replicaIncarnation,
    })
    const incumbentRecord = incumbent.records[0]
    expect(incumbentRecord).toBeDefined()
    if (incumbentRecord === undefined) return

    // The verdict changes before the replacement takes over, so adopting the new
    // incarnation is observable rather than a no-op. Driving it through the
    // coordinator keeps the test on documented API.
    const probeToken = randomUUID()
    const admitted = await authority.admitProbe(
      {
        name: IDENTITY.name,
        operation: IDENTITY.operation,
        scope: IDENTITY.scope,
      },
      { probeToken, openMs: 0, halfOpenProbes: 1, probeLeaseTtlMs: 5_000 },
    )
    expect(admitted).toMatchObject({ type: "admitted" })
    await authority.settleProbe(
      {
        name: IDENTITY.name,
        operation: IDENTITY.operation,
        scope: IDENTITY.scope,
      },
      {
        probeToken,
        outcome: "success",
        generation: admitted.type === "admitted" ? admitted.generation : 0,
        halfOpenSuccesses: 1,
        openMs: 60_000,
        closedCleanupMs: 60_000,
        windowTtlMs: 60_000,
      },
    )

    // The authority is replaced: a different incarnation, answering differently.
    incarnation = "authority-b"
    const replaced = await reader.readSnapshot({
      replicaId: session.replicaId,
      replicaIncarnation: session.replicaIncarnation,
    })
    expect(replaced.authorityIncarnation).toBe("authority-b")
    expect(replaced.records[0]?.payload).toMatchObject({ state: "closed" })

    // Reconciliation is the only place an incarnation is adopted.
    await session.refresh("reconnect")
    await session.whenIdle()
    expect(session.authorityIncarnation).toBe("authority-b")
    expect(session.state).toBe("valid")

    // The replica is holding the replacement's verdict, not the one it cached from
    // the incumbent: applying the same record again changes nothing.
    const replacementRecord = replaced.records[0]
    expect(replacementRecord).toBeDefined()
    if (replacementRecord === undefined) return
    expect(
      session.applyAuthority({
        record: replacementRecord,
        requestStartedAt: Date.now(),
        validForMs: 1_000,
        receivedAt: Date.now(),
      }),
    ).toMatchObject({ type: "ignored", reason: "unchanged" })

    // A late reply from the retired incarnation is refused, not adopted: it carries
    // a real record, and only the incarnation in its version is stale.
    expect(
      session.applyAuthority({
        record: {
          ...incumbentRecord,
          version: {
            ...incumbentRecord.version,
            authorityIncarnation: "authority-a",
          },
        },
        requestStartedAt: Date.now(),
        validForMs: 1_000,
        receivedAt: Date.now(),
      }),
    ).toMatchObject({ type: "rejected", reason: "retired-incarnation" })
    expect(session.authorityIncarnation).toBe("authority-b")

    // A late hint cannot take it back either: the same guard covers notifications,
    // so the session stays on the replacement.
    expect(deliver).toBeDefined()
    deliver?.(
      encodeServalInvalidation({
        identity: SCOPE,
        version: {
          ...incumbentRecord.version,
          authorityIncarnation: "authority-a",
        },
        kind: "authority",
      }),
    )
    await session.whenIdle()
    expect(session.authorityIncarnation).toBe("authority-b")
    expect(session.state).toBe("valid")

    // The hint was seen and refused on its incarnation rather than dropped as
    // garbage, and the reply was rejected on the same basis: the counters separate
    // "ignored for the right reason" from "never processed".
    const counters = session.metrics().counters
    expect(counters.reconciliations).toBeGreaterThanOrEqual(1)
    expect(counters.incarnationRetirements).toBeGreaterThanOrEqual(1)
    expect(counters.authorityRejected).toBeGreaterThanOrEqual(1)
    expect(counters.invalidationsReceived).toBeGreaterThanOrEqual(1)
    expect(counters.invalidationsIgnored).toBeGreaterThanOrEqual(1)
    expect(counters.invalidationsMalformed).toBe(0)

    // And the replica never lost its verdict across the swap: it held one before
    // reconciliation and holds the replacement's after.
    expect(session.readAuthority(SCOPE)).toBeDefined()

    await session.close()
  })
})
