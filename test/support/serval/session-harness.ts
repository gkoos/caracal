import type { ServalScopeIdentity } from "../../../src/coordination/serval/authority.js"
import {
  createServalAuthoritySession,
  type ServalSession,
} from "../../../src/coordination/serval/session.js"
import {
  createDeterministicScheduler,
  type DeterministicScheduler,
} from "./deterministic-scheduler.js"
import { createFakeAuthority, type FakeAuthority } from "./fake-authority.js"
import {
  createFakeTransport,
  type FakeTransport,
} from "./notification-transport.js"

/**
 * The session wired to deterministic collaborators: a virtual clock, a scriptable
 * authority and a transport whose delivery the test controls.
 *
 * The clock starts at a non-zero value so an accidental dependence on absolute
 * time shows up as a wrong deadline rather than passing by coincidence.
 */

/** The identity most tests use: one policy scope of one operation. */
export const TEST_IDENTITY: ServalScopeIdentity = Object.freeze({
  policyKind: "circuit-breaker",
  policyName: "payments",
  operation: "charge",
  scope: "tenant-a",
})

export function identityFor(
  scope: string,
  policyName = "payments",
): ServalScopeIdentity {
  return {
    policyKind: "circuit-breaker",
    policyName,
    operation: "charge",
    scope,
  }
}

export interface SessionHarnessOptions {
  /** Omit the transport to test a reconciliation-only session. */
  readonly withTransport?: boolean
  readonly registryCapacity?: number
  readonly reconciliationIntervalMs?: number
  readonly staleAfterMs?: number
  readonly safetyMarginMs?: number
  readonly initialNow?: number
  readonly transport?: FakeTransport
}

export interface SessionHarness {
  readonly scheduler: DeterministicScheduler
  readonly authority: FakeAuthority
  readonly notifications: FakeTransport
  readonly session: ServalSession
  start(): Promise<void>
  close(): Promise<void>
}

export function createSessionHarness(
  options: SessionHarnessOptions = {},
): SessionHarness {
  const scheduler = createDeterministicScheduler(options.initialNow ?? 1_000)
  const authority = createFakeAuthority({ serverTime: () => scheduler.now() })
  const notifications = options.transport ?? createFakeTransport()

  const session = createServalAuthoritySession({
    namespace: "serval-test",
    reader: authority.reader,
    transport:
      options.withTransport === false ? undefined : notifications.transport,
    scheduler,
    replicaId: "replica-1",
    replicaIncarnation: "replica-1#1",
    registryCapacity: options.registryCapacity,
    reconciliationIntervalMs: options.reconciliationIntervalMs,
    staleAfterMs: options.staleAfterMs,
    safetyMarginMs: options.safetyMarginMs,
  })

  return {
    scheduler,
    authority,
    notifications,
    session,
    start: () => session.start(),
    close: () => session.close(),
  }
}
