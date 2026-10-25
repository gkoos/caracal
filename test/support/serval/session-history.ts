import type { ServalScopeIdentity } from "../../../src/coordination/serval/authority.js"
import { encodeServalInvalidation } from "../../../src/coordination/serval/notifications.js"
import { ServalRegistryPressureError } from "../../../src/coordination/serval/cache.js"
import {
  createServalAuthoritySession,
  type ServalScopeRegistration,
} from "../../../src/coordination/serval/session.js"
import { mulberry32, randInt } from "../seed.js"
import {
  createDeterministicScheduler,
  type DeterministicScheduler,
} from "./deterministic-scheduler.js"
import {
  createFakeAuthority,
  invalidationOf,
  type FakeAuthority,
} from "./fake-authority.js"
import {
  createFakeTransport,
  type FakeTransport,
} from "./notification-transport.js"
import { identityFor } from "./session-harness.js"

/**
 * A seeded adversarial history for the session.
 *
 * It mixes the events the contract names - reordered and duplicated
 * invalidations, failed reads, a disconnected subscription and a reconnect,
 * registration pressure, send-time-anchored replies and clock advances - and
 * checks the invariants after every step against an oracle it maintains itself.
 *
 * The oracle is what makes a failure meaningful: it recomputes each reply's
 * conservative deadline and each cached version independently, so a session that
 * lengthened authority, regressed a version, or grew without bound fails here.
 */

export interface SessionHistoryOptions {
  readonly seed: number
  readonly steps: number
}

const CAPACITY = 4
const INTERVAL_MS = 50
const SCOPES: readonly ServalScopeIdentity[] = [
  identityFor("alpha"),
  identityFor("beta"),
  identityFor("gamma"),
]

interface Expected {
  readonly version: number
  readonly usableUntil: number
}

export async function runSessionHistory(
  options: SessionHistoryOptions,
): Promise<readonly string[]> {
  const { seed, steps } = options
  const rng = mulberry32(seed)
  const violations: string[] = []

  const scheduler: DeterministicScheduler = createDeterministicScheduler(1_000)
  const authority: FakeAuthority = createFakeAuthority({
    serverTime: () => scheduler.now(),
  })
  const notifications: FakeTransport = createFakeTransport()

  const session = createServalAuthoritySession({
    namespace: `serval-history-${seed}`,
    reader: authority.reader,
    transport: notifications.transport,
    scheduler,
    replicaId: "replica-1",
    replicaIncarnation: "replica-1#1",
    registryCapacity: CAPACITY,
    reconciliationIntervalMs: INTERVAL_MS,
    staleAfterMs: 500,
    safetyMarginMs: 0,
    idFactory: () => `history-${seed}`,
  })

  const handles = SCOPES.map((identity) => session.registerScope(identity))
  for (const identity of SCOPES) {
    authority.publish(identity)
  }
  await session.start()

  /** The version and deadline the session must hold for each scope. */
  const expected = new Map<string, Expected>()
  const highestVersion = new Map<string, number>()
  const held: ServalScopeRegistration[] = []
  let lastHint: string | undefined

  function checkInvariants(step: number): void {
    if (session.scopeCount > CAPACITY) {
      violations.push(`step ${step}: the registry grew past its capacity`)
    }
    if (scheduler.pending() > 2) {
      // One periodic refresh plus at most one coalesced refresh is the bound.
      violations.push(`step ${step}: ${scheduler.pending()} timers pending`)
    }

    for (const identity of SCOPES) {
      const cached = session.readAuthority(identity)
      const highest = highestVersion.get(identity.scope)
      if (cached === undefined) continue

      // Monotonic state application: a hint, a duplicate or a rewound snapshot
      // must never move a cached version backwards.
      if (highest !== undefined && cached.record.version.version < highest) {
        violations.push(
          `step ${step}: ${identity.scope} regressed to version ${cached.record.version.version}`,
        )
      }
      highestVersion.set(identity.scope, cached.record.version.version)

      if (!(cached.usableUntil > 0)) {
        violations.push(
          `step ${step}: ${identity.scope} holds an unusable deadline`,
        )
      }

      const oracle = expected.get(identity.scope)
      if (
        oracle !== undefined &&
        cached.record.version.version === oracle.version &&
        cached.usableUntil !== oracle.usableUntil
      ) {
        violations.push(
          `step ${step}: ${identity.scope} deadline ${cached.usableUntil}, oracle ${oracle.usableUntil}`,
        )
      }
    }

    const snapshot = session.metrics()
    for (const [name, value] of Object.entries(snapshot.counters)) {
      if (!Number.isSafeInteger(value) || value < 0) {
        violations.push(`step ${step}: counter ${name} is ${value}`)
      }
    }
    if (snapshot.registrySize > CAPACITY) {
      violations.push(
        `step ${step}: the reported registry size exceeds capacity`,
      )
    }
  }

  for (let step = 0; step < steps; step += 1) {
    const identity = SCOPES[
      randInt(rng, 0, SCOPES.length - 1)
    ] as ServalScopeIdentity
    const action = randInt(rng, 0, 6)

    if (action === 0) {
      notifications.deliverInvalidation(
        invalidationOf(authority.publish(identity)),
      )
    } else if (action === 1) {
      // A duplicated hint, and one reordered after a newer hint.
      const record = authority.publish(identity)
      const payload = encodeServalInvalidation(invalidationOf(record))
      notifications.deliverTwice(payload)
      if (lastHint !== undefined) {
        notifications.deliver(lastHint)
      }
      lastHint = payload
    } else if (action === 2) {
      // A partition: reads fail and the subscription drops.
      authority.failReads(randInt(rng, 1, 2))
      notifications.disconnect()
    } else if (action === 3) {
      notifications.reconnect()
    } else if (action === 4) {
      // A reply anchored to its send time, delivered late or on time.
      const record = authority.publish(identity)
      const requestStartedAt = scheduler.now()
      const validForMs = randInt(rng, 5, 200)
      const receivedAt = requestStartedAt + randInt(rng, 0, validForMs + 40)
      const outcome = session.applyAuthority({
        record,
        requestStartedAt,
        validForMs,
        receivedAt,
      })

      const expectedLate = receivedAt > requestStartedAt + validForMs
      if (expectedLate && outcome.type !== "rejected") {
        violations.push(`step ${step}: a late reply was accepted`)
      }
      if (
        !expectedLate &&
        outcome.type === "rejected" &&
        outcome.reason === "late"
      ) {
        violations.push(`step ${step}: an on-time reply was refused as late`)
      }
      if (outcome.type === "applied") {
        expected.set(identity.scope, {
          version: record.version.version,
          usableUntil: requestStartedAt + validForMs,
        })
      }
    } else if (action === 5) {
      // Registration pressure: two extra scopes are requested, so at least one is
      // refused once the registry is full, and every handle is released again.
      for (const suffix of ["a", "b"]) {
        try {
          held.push(
            session.registerScope(identityFor(`extra-${step}-${suffix}`)),
          )
        } catch (error) {
          if (!(error instanceof ServalRegistryPressureError)) {
            violations.push(
              `step ${step}: registration failed with ${String(error)}`,
            )
          }
        }
      }
      while (held.length > 0) {
        held.pop()?.unregister()
      }
      if (session.scopeCount > CAPACITY) {
        violations.push(`step ${step}: the registry grew past its capacity`)
      }
    } else {
      await scheduler.advanceBy(randInt(rng, 0, INTERVAL_MS * 2))
    }

    await scheduler.advanceBy(randInt(rng, 0, 20))
    await session.whenIdle()
    checkInvariants(step)

    // A reconnect must converge: after two refresh intervals the session is no
    // longer reporting a disconnected authority.
    if (action === 3) {
      await scheduler.advanceBy(INTERVAL_MS * 2)
      await session.whenIdle()
      if (session.state === "disconnected") {
        violations.push(
          `step ${step}: the session stayed disconnected after a reconnect`,
        )
      }
    }
  }

  // Bounded cleanup: closing releases every timer and every scope.
  await session.close()
  if (scheduler.pending() !== 0) {
    violations.push("close left timers pending")
  }
  if (session.scopeCount !== 0) {
    violations.push("close left scopes registered")
  }
  for (const handle of handles) {
    handle.unregister()
  }
  const readsAfterClose = authority.reads
  await scheduler.advanceBy(1_000)
  if (authority.reads !== readsAfterClose) {
    violations.push("a closed session kept reading the authority")
  }

  return violations
}
