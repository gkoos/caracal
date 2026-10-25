import type { BreakerIdentity } from "../../../src/core/circuit-breaker.js"
import type { ServalScopeIdentity } from "../../../src/coordination/serval/authority.js"
import {
  createServalBreaker,
  breakerScopeIdentity,
  type ServalBreakerCoordinator,
  type ServalStaleClosedBehavior,
} from "../../../src/coordination/serval/breaker.js"
import {
  decodeServalInvalidation,
  type ServalInvalidation,
} from "../../../src/coordination/serval/notifications.js"
import {
  createFakeBreakerAuthority,
  type FakeBreakerAuthority,
} from "./fake-breaker-authority.js"

/** The breaker identity the breaker tests use. */
export const TEST_BREAKER_IDENTITY: BreakerIdentity = Object.freeze({
  name: "payments",
  operation: "charge",
  scope: "tenant-a",
})

let harnessSequence = 0

function nextHarnessName(): string {
  harnessSequence += 1
  return `serval-breaker-${harnessSequence}`
}

export interface BreakerHarnessOptions {
  /** Shared by replicas that must coordinate with each other. */
  readonly namespace?: string
  readonly freshnessMs?: number
  readonly staleClosed?: ServalStaleClosedBehavior
  readonly reconciliationIntervalMs?: number
  readonly registryCapacity?: number
  readonly initialNow?: number
  /** Shared by replicas that must see the same clock. */
  readonly scheduler?: DeterministicScheduler
  /** Shared by replicas that must see the same authority. */
  readonly authority?: FakeBreakerAuthority
  readonly withTransport?: boolean
  readonly replicaId?: string
}

export interface BreakerHarness {
  readonly scheduler: DeterministicScheduler
  readonly authority: FakeBreakerAuthority
  readonly notifications: FakeTransport
  readonly session: ServalSession
  readonly coordinator: ServalBreakerCoordinator
  /** Every invalidation this replica published. */
  readonly published: ServalInvalidation[]
  readonly identity: BreakerIdentity
  start(): Promise<void>
  close(): Promise<void>
}

/**
 * The cached-verdict breaker wired to deterministic collaborators: the session, the
 * scriptable strict authority, a controllable transport and a capturing publisher.
 */
export function createBreakerHarness(
  options: BreakerHarnessOptions = {},
): BreakerHarness {
  const scheduler =
    options.scheduler ??
    createDeterministicScheduler(options.initialNow ?? 1_000)
  const authority =
    options.authority ??
    createFakeBreakerAuthority({ now: () => scheduler.now() })
  const notifications = createFakeTransport()
  const published: ServalInvalidation[] = []

  const breaker = createServalBreaker({
    namespace: options.namespace ?? nextHarnessName(),
    authority: authority.coordinator,
    transport:
      options.withTransport === false ? undefined : notifications.transport,
    publisher: {
      async publish(payload) {
        const invalidation = decodeServalInvalidation(payload)
        if (invalidation !== undefined) {
          published.push(invalidation)
        }
      },
    },
    scheduler,
    replicaId: options.replicaId ?? "replica-1",
    replicaIncarnation: `${options.replicaId ?? "replica-1"}#1`,
    freshnessMs: options.freshnessMs,
    staleClosed: options.staleClosed,
    reconciliationIntervalMs: options.reconciliationIntervalMs,
    registryCapacity: options.registryCapacity,
  })

  return {
    scheduler,
    authority,
    notifications,
    session: breaker.session,
    coordinator: breaker.coordinator,
    published,
    identity: TEST_BREAKER_IDENTITY,
    async start() {
      // A deployment registers the scopes its policies use, so the cold start
      // caches a verdict for each of them instead of learning them one at a time.
      breaker.session.registerScope(breakerScopeIdentity(TEST_BREAKER_IDENTITY))
      await breaker.session.start()
    },
    close: () => breaker.session.close(),
  }
}

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
