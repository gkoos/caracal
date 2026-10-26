/**
 * Unit tests for the delegated bulkhead.
 *
 * `docs/serval/README.md` contracts 6, 7, 9 and 11 are the contract: Redis allocates a slot
 * budget, the process admits within it without a round trip, allocation validity and
 * holder lifetime are separate clocks, a renewal is not a grant, and returned
 * capacity is frozen before it is sent. The authority here is a server model with its
 * own oracle, so a client bug that provokes a server-side breach fails the test.
 */

import { describe, expect, it } from "vitest"
import { bulkhead, BulkheadRejectedError, operation } from "../../src/index.js"
import { ServalRegistryPressureError } from "../../src/coordination/serval/cache.js"
import { redisBulkheadAllocationIssuer } from "../../src/coordination/redis/bulkhead.js"
import { createFakeAllocationAuthority } from "../support/serval/fake-allocation-authority.js"
import {
  TEST_BULKHEAD_IDENTITY,
  createBulkheadHarness,
  createManualClock,
} from "../support/serval/bulkhead-harness.js"

describe("serval bulkhead local allocation", () => {
  it("admits a whole allocation with one round trip", async () => {
    const harness = createBulkheadHarness({ limit: 8, initialSlots: 4 })

    for (let index = 0; index < 4; index += 1) {
      expect((await harness.acquire()).allowed).toBe(true)
    }

    // One allocation for four admissions: the round trip is not per admission.
    expect(harness.authority.calls.modify).toBe(1)
    expect(harness.coordinator.metrics()).toMatchObject({
      admissions: 4,
      allocations: 1,
      localRefusals: 0,
    })
    expect(harness.authority.violations()).toEqual([])
  })

  it("grows the allocation on demand and refuses when the fleet is full", async () => {
    const harness = createBulkheadHarness({ limit: 4, initialSlots: 2 })

    // Two slots are granted; the third admission asks for more and gets the rest.
    expect((await harness.acquire()).allowed).toBe(true)
    expect((await harness.acquire()).allowed).toBe(true)
    expect((await harness.acquire()).allowed).toBe(true)
    expect(harness.coordinator.diagnostics(harness.identity)).toMatchObject({
      allocated: 4,
      active: 3,
      freeSpendable: 1,
    })

    expect((await harness.acquire()).allowed).toBe(true)
    // The fleet's ceiling is reached, so the fifth admission is refused locally.
    expect((await harness.acquire()).allowed).toBe(false)
    expect(harness.authority.granted()).toBe(4)
    expect(harness.authority.violations()).toEqual([])
  })

  it("never admits more than the allocation it holds", async () => {
    const harness = createBulkheadHarness({ limit: 4, initialSlots: 4 })

    const decisions = await Promise.all(
      Array.from({ length: 6 }, () => harness.acquire()),
    )
    await harness.whenIdle()

    expect(decisions.filter((decision) => decision.allowed)).toHaveLength(4)
    const diagnostics = harness.coordinator.diagnostics(harness.identity)
    expect(diagnostics?.active).toBe(4)
    expect(diagnostics?.active).toBeLessThanOrEqual(diagnostics?.allocated ?? 0)
    expect(harness.authority.peakGranted()).toBe(4)
  })

  it("stops new admissions when its allocation is reclaimed, and keeps its holders", async () => {
    const clock = createManualClock()
    const authority = createFakeAllocationAuthority({
      now: () => clock.now(),
      limit: 4,
      graceMs: 0,
    })
    const shared = {
      limit: 4,
      validityMs: 100,
      graceMs: 0,
      redistributionIntervalMs: 0,
      clock,
      authority,
    }
    const first = createBulkheadHarness({
      ...shared,
      initialSlots: 4,
      replicaId: "replica-1",
    })
    const second = createBulkheadHarness({
      ...shared,
      initialSlots: 4,
      replicaId: "replica-2",
    })

    const tokens = [1, 2, 3, 4].map((index) => first.token(index))
    for (const token of tokens) {
      expect((await first.acquire(token)).allowed).toBe(true)
    }
    expect(authority.granted()).toBe(4)

    // The validity lapses, the reservation is reclaimed, and another replica takes
    // the whole fleet budget. The first replica holds four running holders and no
    // capacity at all.
    clock.advanceBy(200)
    expect((await second.acquire()).allowed).toBe(true)
    await first.whenIdle()

    expect((await first.acquire(first.token(5))).allowed).toBe(false)
    const diagnostics = first.coordinator.diagnostics(first.identity)
    expect(diagnostics?.allocated).toBe(0)
    expect(diagnostics?.active).toBe(4)
    expect(diagnostics?.freeSpendable).toBe(0)
    expect(authority.violations()).toEqual([])

    // Its holders are still tracked and still renew: allocation validity and holder
    // lifetime are separate clocks.
    expect((await first.renew(tokens[0] as string)).allowed).toBe(true)
    expect((await first.release(tokens[0] as string)).allowed).toBe(true)
    expect(first.coordinator.diagnostics(first.identity)?.active).toBe(3)
  })

  it("does not count a renewal as capacity", async () => {
    const harness = createBulkheadHarness({
      limit: 4,
      initialSlots: 4,
      validityMs: 1_000,
      renewLeadMs: 900,
    })
    for (let index = 0; index < 4; index += 1) await harness.acquire()
    await harness.whenIdle()

    const granted = harness.authority.granted()
    for (let index = 0; index < 5; index += 1) {
      harness.advanceBy(100)
      // The pool is exhausted, so each admission consults the authority - and the
      // only thing it can do is renew.
      await harness.acquire()
      await harness.whenIdle()
    }

    expect(harness.coordinator.metrics().renewals).toBeGreaterThan(0)
    expect(harness.authority.granted()).toBe(granted)
    expect(harness.coordinator.diagnostics(harness.identity)?.allocated).toBe(4)
    expect(harness.authority.violations()).toEqual([])
  })

  it("renews under light traffic before the allocation lapses", async () => {
    // One holder at a time, so a free slot is always available: the admission fast path serves
    // every offer and the pool never empties, which is exactly the traffic that never asked for a
    // renewal. The allocation still has to be renewed before its validity ends, or the next
    // admission after it lapses is refused for the grace window.
    const harness = createBulkheadHarness({
      limit: 4,
      initialSlots: 4,
      validityMs: 1_000,
      renewLeadMs: 500,
      graceMs: 1_000,
    })

    for (let offer = 0; offer < 16; offer += 1) {
      const token = harness.token(offer)
      expect((await harness.acquire(token)).allowed).toBe(true)
      await harness.release(token)
      // 150ms steps: the offer after the deadline lands past the server's expiry too, so the
      // renewal this traffic never asks for cannot succeed by luck.
      harness.advanceBy(150)
    }
    await harness.whenIdle()

    // The renewal fell due at 1475 and ran in the background rather than waiting for the lapse.
    expect(harness.coordinator.metrics().renewals).toBeGreaterThan(0)
    const diagnostics = harness.coordinator.diagnostics(harness.identity)
    expect(diagnostics).toMatchObject({ state: "allocated", allocated: 4 })
    expect(diagnostics?.deadline ?? 0).toBeGreaterThan(harness.now())
    expect(harness.authority.violations()).toEqual([])
  })

  it("keeps the deadline anchored to the renewal's send time", async () => {
    const harness = createBulkheadHarness({
      limit: 2,
      initialSlots: 2,
      validityMs: 2_000,
      renewLeadMs: 1_000,
    })
    await harness.acquire()
    await harness.acquire()
    await harness.whenIdle()

    // The renewal falls due at 1 975, and its reply is delayed by 500ms.
    harness.advanceBy(1_000)
    harness.authority.holdNext()
    const pending = harness.acquire(harness.token(3))
    harness.advanceBy(500)
    harness.authority.release()
    await expect(pending).resolves.toMatchObject({ allowed: false })
    await harness.whenIdle()

    // The renewal was sent at 2 000, so its deadline is 2 000 + 2 000 - margin: the
    // 500ms the reply took never becomes authority.
    expect(harness.coordinator.metrics().renewals).toBe(1)
    expect(harness.coordinator.diagnostics(harness.identity)?.deadline).toBe(
      2_000 + 2_000 - 25,
    )
  })

  it("adopts a replacement grant whose revision restarts at one", async () => {
    // A replacement allocation is a new grant, and the server numbers it from 1 again: the
    // revision is a per-grant counter. Judging staleness by it keeps the retired grant's number
    // and rejects every reply the new grant sends, so the replica can never allocate again.
    const harness = createBulkheadHarness({ limit: 4, initialSlots: 2 })
    expect((await harness.acquire("token-1")).allowed).toBe(true)
    await harness.whenIdle()
    await harness.release("token-1")
    await harness.whenIdle()

    // Returning the whole allocation retires the grant on the server.
    expect(
      (await harness.coordinator.relinquish(TEST_BULKHEAD_IDENTITY)).type,
    ).toBe("returned")
    await harness.whenIdle()
    expect(
      harness.coordinator.diagnostics(TEST_BULKHEAD_IDENTITY),
    ).toMatchObject({
      allocated: 0,
      active: 0,
    })

    // The next allocation is a new grant at revision 1, and it must be adopted.
    expect((await harness.acquire("token-2")).allowed).toBe(true)
    await harness.whenIdle()
    expect(harness.coordinator.metrics().staleReplies).toBe(0)
    expect(
      harness.coordinator.diagnostics(TEST_BULKHEAD_IDENTITY)?.active,
    ).toBe(1)
  })
})

describe("serval bulkhead freeze-before-return", () => {
  it("freezes returned slots before the request, and never spends them again", async () => {
    const harness = createBulkheadHarness({ limit: 8, initialSlots: 4 })
    await harness.acquire()
    await harness.whenIdle()
    expect(harness.coordinator.diagnostics(harness.identity)).toMatchObject({
      allocated: 4,
      active: 1,
      freeSpendable: 3,
    })

    // The authority is unreachable, so the outcome is unknown: the two slots stay
    // frozen and are no longer spendable.
    harness.authority.failNext(1)
    await expect(
      harness.coordinator.relinquish(harness.identity, 2),
    ).resolves.toEqual({ type: "frozen", slots: 2, reason: "unavailable" })

    expect(harness.coordinator.diagnostics(harness.identity)).toMatchObject({
      allocated: 4,
      active: 1,
      frozenPendingReturn: 2,
      freeSpendable: 1,
    })
    expect(harness.authority.granted()).toBe(4)

    // Only the single unfrozen slot can be spent.
    expect((await harness.acquire()).allowed).toBe(true)
    expect((await harness.acquire()).allowed).toBe(false)
    expect(harness.coordinator.metrics().frozenSlots).toBe(2)
  })

  it("settles a lost reply by retrying the same operation", async () => {
    const harness = createBulkheadHarness({ limit: 8, initialSlots: 4 })
    await harness.acquire()
    await harness.whenIdle()

    // The reduction committed on the server and the reply was lost.
    harness.authority.commitThenFailNext()
    await expect(
      harness.coordinator.relinquish(harness.identity, 2),
    ).resolves.toMatchObject({ type: "frozen" })
    expect(harness.coordinator.metrics().frozenSlots).toBe(2)
    // The server already returned them: the fleet can see two slots free.
    expect(harness.authority.granted()).toBe(2)

    // The next admission, once the retry backoff has lapsed, retries the same
    // operation in the background; the server returns its recorded result and the
    // frozen slots are retired rather than restored.
    harness.advanceBy(200)
    expect((await harness.acquire()).allowed).toBe(true)
    await harness.whenIdle()

    expect(harness.authority.calls.duplicates).toBeGreaterThan(0)
    expect(harness.coordinator.metrics().frozenSlots).toBe(0)
    expect(harness.coordinator.diagnostics(harness.identity)).toMatchObject({
      allocated: 2,
      active: 2,
      frozenPendingReturn: 0,
    })
    expect(harness.authority.violations()).toEqual([])
  })

  it("does not freeze the same slots twice", async () => {
    const harness = createBulkheadHarness({ limit: 8, initialSlots: 4 })
    await harness.acquire()
    await harness.whenIdle()

    harness.authority.failNext(1)
    await harness.coordinator.relinquish(harness.identity, 3)
    const second = await harness.coordinator.relinquish(harness.identity, 3)

    expect(second).toEqual({ type: "frozen", slots: 3, reason: "unknown" })
    expect(harness.coordinator.metrics().frozenSlots).toBe(3)
    expect(
      harness.coordinator.diagnostics(harness.identity)?.freeSpendable,
    ).toBe(0)
  })

  it("returns every free slot on close", async () => {
    const harness = createBulkheadHarness({ limit: 8, initialSlots: 4 })
    await harness.acquire()
    await harness.acquire()
    await harness.whenIdle()

    await harness.close()

    // Only free slots are returned: the two holders keep theirs, so the fleet still
    // accounts for them and shutdown never pretends otherwise.
    expect(harness.authority.granted()).toBe(2)
    expect(harness.coordinator.diagnostics(harness.identity)).toMatchObject({
      allocated: 2,
      active: 2,
      state: "allocated",
    })
    // Shutdown stops admitting, whatever capacity is left.
    expect((await harness.acquire()).allowed).toBe(false)
  })

  it("refuses an admission that was in flight when shutdown began", async () => {
    const harness = createBulkheadHarness({ limit: 4, initialSlots: 4 })

    // The allocation this offer triggers is held, so the offer is parked inside its admission
    // when shutdown starts.
    harness.authority.holdNext()
    const pending = harness.acquire(harness.token(1))
    const closing = harness.close()
    harness.authority.release()

    // Closure wins over a continuation that was already in flight.
    await expect(pending).resolves.toMatchObject({ allowed: false })
    await closing

    // And nothing is left behind for the server to reclaim: the allocation that arrived during
    // shutdown is returned by the pass that follows it.
    expect(harness.authority.granted()).toBe(0)
    expect(harness.coordinator.diagnostics(harness.identity)).toMatchObject({
      allocated: 0,
      active: 0,
    })
  })

  it("submits a return frozen while another modification held the wire", async () => {
    const harness = createBulkheadHarness({
      limit: 4,
      initialSlots: 4,
      validityMs: 2_000,
      renewLeadMs: 1_000,
    })
    for (let index = 0; index < 4; index += 1) await harness.acquire()
    await harness.whenIdle()

    // The renewal falls due, and its reply is held so that it holds the wire.
    harness.advanceBy(1_000)
    harness.authority.holdNext()
    const pending = harness.acquire(harness.token(5))

    // One holder finishes, so there is a free slot to return while the renewal is in flight.
    await harness.release(harness.token(1))
    const returned = harness.coordinator.relinquish(harness.identity, 1)
    harness.authority.release()

    // The freeze waits for the renewal and is submitted as soon as it settles, so the slot
    // reaches the fleet instead of being held here and unusable.
    await expect(returned).resolves.toMatchObject({
      type: "returned",
      slots: 1,
    })
    // The demand that was waiting on the wire is served from the re-granted capacity.
    await pending
    await harness.whenIdle()

    expect(harness.coordinator.metrics().returns).toBe(1)
    expect(harness.coordinator.metrics().frozenSlots).toBe(0)
    expect(harness.coordinator.diagnostics(harness.identity)).toMatchObject({
      frozenPendingReturn: 0,
    })
    expect(harness.authority.violations()).toEqual([])
  })

  it("retries a return the authority refused instead of abandoning the freeze", async () => {
    const harness = createBulkheadHarness({ limit: 8, initialSlots: 4 })
    await harness.acquire()
    await harness.whenIdle()

    // The authority refuses the reduction, and the retry cannot reach it either.
    harness.authority.refuseNext("revision")
    harness.authority.failNext(1)
    await expect(
      harness.coordinator.relinquish(harness.identity, 2),
    ).resolves.toMatchObject({ type: "frozen" })

    // The intent survives both: the slots are still frozen, and the fleet still counts them.
    expect(harness.coordinator.metrics().frozenSlots).toBe(2)
    expect(harness.authority.granted()).toBe(4)

    // The next admission nudges it, and the slots come back rather than being frozen for the
    // life of the grant.
    harness.advanceBy(200)
    await harness.acquire()
    await harness.whenIdle()

    expect(harness.authority.granted()).toBe(2)
    expect(harness.coordinator.metrics().returns).toBe(1)
    expect(harness.coordinator.diagnostics(harness.identity)).toMatchObject({
      allocated: 2,
      frozenPendingReturn: 0,
    })
    expect(harness.authority.violations()).toEqual([])
  })

  it("drops a frozen return when the allocation lapses", async () => {
    const harness = createBulkheadHarness({
      limit: 8,
      initialSlots: 4,
      validityMs: 1_000,
      graceMs: 0,
    })
    await harness.acquire()
    await harness.whenIdle()

    // The authority is unreachable, so the return stays frozen and pending.
    harness.authority.failNext(1)
    await expect(
      harness.coordinator.relinquish(harness.identity, 2),
    ).resolves.toMatchObject({ type: "frozen" })
    expect(harness.coordinator.metrics().frozenSlots).toBe(2)

    // The allocation lapses, and the next admission takes a fresh grant. The freeze belonged to
    // the grant that is gone, so the new one does not inherit it.
    harness.advanceBy(3_000)
    await harness.acquire()
    await harness.whenIdle()

    expect(harness.coordinator.metrics().frozenSlots).toBe(0)
    expect(harness.coordinator.diagnostics(harness.identity)).toMatchObject({
      allocated: 4,
      frozenPendingReturn: 0,
    })
  })
})

describe("serval bulkhead boundaries", () => {
  it("refuses a scope its registry cannot hold rather than dropping holders", async () => {
    const harness = createBulkheadHarness({
      limit: 4,
      initialSlots: 4,
      maximumScopes: 1,
    })
    await harness.acquire()

    const other = { ...TEST_BULKHEAD_IDENTITY, scope: "tenant-b" }
    await expect(
      harness.coordinator.command(
        other,
        "acquire",
        "other",
        harness.leaseMs,
        4,
      ),
    ).rejects.toBeInstanceOf(ServalRegistryPressureError)
    expect(harness.coordinator.metrics().registryRefusals).toBe(1)
  })

  it("never reclaims a scope whose allocation is still valid", async () => {
    const harness = createBulkheadHarness({
      limit: 4,
      initialSlots: 4,
      validityMs: 60_000,
      maximumScopes: 1,
    })
    await harness.acquire(harness.token(1))
    await harness.release(harness.token(1))
    await harness.whenIdle()
    // No holders, but the allocation is live: those slots are this process's to spend.
    expect(harness.coordinator.diagnostics(harness.identity)).toMatchObject({
      allocated: 4,
      active: 0,
    })

    const other = { ...TEST_BULKHEAD_IDENTITY, scope: "tenant-b" }
    await expect(
      harness.coordinator.command(
        other,
        "acquire",
        "other",
        harness.leaseMs,
        4,
      ),
    ).rejects.toBeInstanceOf(ServalRegistryPressureError)
    expect(harness.coordinator.metrics().registryRefusals).toBe(1)
  })

  it("reclaims a scope whose allocation has lapsed without being touched again", async () => {
    const harness = createBulkheadHarness({
      limit: 4,
      initialSlots: 4,
      validityMs: 1_000,
      graceMs: 0,
      maximumScopes: 1,
    })
    await harness.acquire(harness.token(1))
    await harness.release(harness.token(1))
    await harness.whenIdle()
    expect(harness.coordinator.diagnostics(harness.identity)?.allocated).toBe(4)

    // The allocation lapses, and nobody asks again, so nothing has cleared it locally. The
    // server's reservation is gone with it, so the scope holds nothing.
    harness.advanceBy(2_000)
    const other = { ...TEST_BULKHEAD_IDENTITY, scope: "tenant-b" }
    await expect(
      harness.coordinator.command(
        other,
        "acquire",
        "other",
        harness.leaseMs,
        4,
      ),
    ).resolves.toMatchObject({ allowed: true })
    expect(harness.coordinator.metrics().registryRefusals).toBe(0)
    expect(harness.coordinator.metrics().scopes).toBe(1)
    expect(harness.coordinator.diagnostics(harness.identity)).toBeUndefined()
  })

  it("rejects invalid parameters as the strict coordinator does", async () => {
    const harness = createBulkheadHarness()
    await expect(
      harness.coordinator.command(harness.identity, "acquire", "", 1_000, 8),
    ).rejects.toThrow("Invalid bulkhead lease action or token")
    await expect(
      harness.coordinator.command(harness.identity, "acquire", "token", 0, 8),
    ).rejects.toThrow("Invalid bulkhead lease duration or limit")
    await expect(
      harness.coordinator.command(
        harness.identity,
        "acquire",
        "token",
        1_000,
        0,
      ),
    ).rejects.toThrow("Invalid bulkhead lease duration or limit")

    expect(() => createBulkheadHarness({ validityMs: 0 })).toThrow("validityMs")
    expect(() => createBulkheadHarness({ graceMs: -1 })).toThrow("graceMs")
    expect(() => createBulkheadHarness({ initialSlots: 0 })).toThrow(
      "initialSlots",
    )
    expect(() => createBulkheadHarness({ maximumScopes: 0 })).toThrow(
      "maximumScopes",
    )
  })

  it("reports an unknown token as an unknown holder", async () => {
    const harness = createBulkheadHarness({ limit: 4, initialSlots: 2 })
    await harness.acquire()

    expect((await harness.renew("never-acquired")).allowed).toBe(false)
    expect((await harness.release("never-acquired")).allowed).toBe(false)
    expect(harness.coordinator.metrics().admissions).toBe(1)
  })
})

describe("serval bulkhead in the policy path", () => {
  it("admits within the allocation and rejects beyond it", async () => {
    const harness = createBulkheadHarness({ limit: 2, initialSlots: 2 })
    const policy = bulkhead.distributed({
      name: TEST_BULKHEAD_IDENTITY.name,
      limit: 2,
      coordinator: harness.coordinator,
      scope: () => TEST_BULKHEAD_IDENTITY.scope,
    })

    let release: (() => void) | undefined
    const held = new Promise<void>((resolve) => {
      release = resolve
    })
    const slow = operation({
      name: TEST_BULKHEAD_IDENTITY.operation,
      adapter: {
        capabilities: () => ({ abort: "supported", replay: "safe" }) as const,
        execute: async () => {
          await held
          return "ok"
        },
      },
      policies: [policy],
    })
    const quick = operation({
      name: TEST_BULKHEAD_IDENTITY.operation,
      adapter: {
        capabilities: () => ({ abort: "supported", replay: "safe" }) as const,
        execute: async () => "ok",
      },
      policies: [policy],
    })

    // Two executions hold both slots until they settle.
    const first = slow.execute(undefined)
    const second = slow.execute(undefined)
    await new Promise((resolve) => setTimeout(resolve, 0))

    const rejected = await quick.execute(undefined).catch((error) => error)
    expect(rejected).toBeInstanceOf(BulkheadRejectedError)
    expect((rejected as BulkheadRejectedError).coordination).toBe("distributed")
    expect(harness.coordinator.metrics().localRefusals).toBeGreaterThan(0)

    release?.()
    await expect(first).resolves.toBe("ok")
    await expect(second).resolves.toBe("ok")
    await harness.whenIdle()
    expect(harness.coordinator.metrics().admissions).toBe(2)
  })
})

describe("serval bulkhead allocation issuer", () => {
  // The issuer validates before it touches the client, so a stub is enough here.
  const issuer = redisBulkheadAllocationIssuer({} as never, { namespace: "ns" })

  it("rejects ids that would break the reservation member format", async () => {
    const request = {
      identity: TEST_BULKHEAD_IDENTITY,
      action: "allocate" as const,
      replicaId: "host-1",
      operationId: "op-1",
      slots: 1,
      limit: 4,
      validityMs: 1_000,
    }

    // Reservation members are `a|replicaId|grantId|slots` and the server's pruning pass
    // splits them on `|`. An id that carries the delimiter would make its own
    // reservation unreadable, so the capacity it holds would never be released.
    await expect(
      issuer.modify({ ...request, replicaId: "host|1" }),
    ).rejects.toThrow("Invalid bulkhead replica id")
    await expect(
      issuer.modify({ ...request, operationId: "op|1" }),
    ).rejects.toThrow("Invalid bulkhead allocation operation id")
  })
})
