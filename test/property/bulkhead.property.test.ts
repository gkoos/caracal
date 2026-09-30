/**
 * Property tests for the local bulkhead's permit accounting.
 *
 * `docs/bulkhead.md` is the contract under test: a permit is held until the
 * adapter promise settles, a waiter never waits past `queue.timeoutMs`, a
 * refusal other than `lease-lost` means the adapter never ran, and `leaseMs`
 * reclaims an abort-honoring holder while an abort-unsupported (or abort-
 * ignoring) holder keeps its permit.
 *
 * Each case drives a generated interleaving of arrivals, settles, queue
 * timeouts and lease expiries on a fake clock, then checks the whole trace:
 * permits are never over-admitted, never double-released, and never leak.
 */

import * as fc from "fast-check"
import { describe, expect, it, vi } from "vitest"
import type { ExecutionContext, OperationEvent } from "../../src/index.js"
import { bulkhead, BulkheadRejectedError, operation } from "../../src/index.js"
import { Deferred } from "../support/deferred.js"
import { createGeneratedSuite } from "../support/seed.js"

const suite = createGeneratedSuite({
  name: "bulkhead-permits",
  command: "npm run test:property",
  maxCases: 2000,
})

const NAME = "pool"

/** How a call's adapter behaves: what it claims, and whether it honors abort. */
type Mode = "unsupported" | "supported" | "ignoring"

/**
 * `settle` is the work's duration after arrival, or `null` for work that never
 * settles; `modes` is per call. `chain` keeps the per-call arrays the same
 * length as the arrival count.
 */
const specArb = fc.integer({ min: 2, max: 4 }).chain((calls) =>
  fc.record({
    limit: fc.integer({ min: 1, max: 3 }),
    queue: fc.option(
      fc.record({
        limit: fc.integer({ min: 1, max: 2 }),
        timeoutMs: fc.integer({ min: 1, max: 30 }),
      }),
      { nil: undefined },
    ),
    leaseMs: fc.option(fc.integer({ min: 1, max: 30 }), { nil: undefined }),
    gaps: fc.array(fc.integer({ min: 0, max: 8 }), {
      minLength: calls - 1,
      maxLength: calls - 1,
    }),
    settle: fc.array(
      fc.option(fc.integer({ min: 0, max: 20 }), { nil: null }),
      {
        minLength: calls,
        maxLength: calls,
      },
    ),
    modes: fc.array(
      fc.constantFrom<Mode>("unsupported", "supported", "ignoring"),
      {
        minLength: calls,
        maxLength: calls,
      },
    ),
  }),
)

type Spec = {
  readonly limit: number
  readonly queue:
    | { readonly limit: number; readonly timeoutMs: number }
    | undefined
  readonly leaseMs: number | undefined
  readonly gaps: readonly number[]
  readonly settle: readonly (number | null)[]
  readonly modes: readonly Mode[]
}

/** One bulkhead event, with its clock time normalised to the case's origin. */
interface Step {
  readonly type:
    | "admitted"
    | "rejected"
    | "waited"
    | "released"
    | "lease-lost"
    | "degraded"
  readonly at: number
  /** The execution that emitted the event, so a step belongs to one call. */
  readonly executionId: string
  readonly occupancy: number | undefined
  readonly reason: string | undefined
}

interface Terminal {
  readonly outcome: "resolved" | "rejected"
  readonly at: number
  /** The `BulkheadRejectedError.reason`, or `undefined` on success. */
  readonly reason: string | undefined
}

interface Trace {
  /** Arrival times, relative to the case origin. */
  readonly arrivals: readonly number[]
  readonly steps: readonly Step[]
  /** The clock time each adapter body started, or `null` if it never ran. */
  readonly started: readonly (number | null)[]
  /** The execution id each started call ran under, or `null` if it never ran. */
  readonly executionIds: readonly (string | null)[]
  /** How each caller's promise ended, or `null` if it never settled. */
  readonly terminal: readonly (Terminal | null)[]
  readonly occupancy: number
  readonly waiting: number
}

function reasonOf(error: unknown): string {
  return error instanceof BulkheadRejectedError ? error.reason : "unexpected"
}

/** Drives one generated schedule on a fake clock and records the trace. */
async function drive(spec: Spec): Promise<Trace> {
  const policy = bulkhead.local({
    name: NAME,
    limit: spec.limit,
    queue: spec.queue,
    leaseMs: spec.leaseMs,
  })

  const arrivals: number[] = [0]
  let cursor = 0
  for (const gap of spec.gaps) {
    cursor += gap
    arrivals.push(cursor)
  }

  const zero = Date.now()
  const events: OperationEvent[] = []
  const started: Array<number | null> = arrivals.map(() => null)
  const executionIds: Array<string | null> = arrivals.map(() => null)
  const terminal: Array<Terminal | null> = arrivals.map(() => null)
  const gates = arrivals.map(() => new Deferred<void>())

  const op = operation({
    name: "work",
    adapter: {
      capabilities: (index: number) => ({
        abort:
          spec.modes[index] === "unsupported"
            ? ("unsupported" as const)
            : ("supported" as const),
        replay: "safe" as const,
      }),
      execute: async (index: number, context: ExecutionContext) => {
        started[index] = Date.now() - zero
        executionIds[index] = context.executionId
        const gate = gates[index]
        if (gate === undefined) {
          throw new Error(`unknown call ${index}`)
        }

        // An "ignoring" adapter claims abort support but never settles on it,
        // which is what a real adapter does when abort is advisory only.
        if (spec.modes[index] !== "supported") {
          await gate.promise
          return "ok"
        }

        await new Promise<void>((resolve, reject) => {
          const abort = () => reject(context.signal?.reason)
          if (context.signal?.aborted === true) {
            abort()
            return
          }
          context.signal?.addEventListener("abort", abort, { once: true })
          void gate.promise.then(
            () => resolve(),
            () => resolve(),
          )
        })
        return "ok"
      },
    },
    policies: [policy],
    events: { emit: (event) => events.push(event) },
  })

  const actions: Array<{
    at: number
    kind: "arrive" | "settle"
    index: number
  }> = []
  for (const [index, at] of arrivals.entries()) {
    actions.push({ at, kind: "arrive", index })
    const delay = spec.settle[index]
    if (delay !== null && delay !== undefined) {
      actions.push({ at: at + delay, kind: "settle", index })
    }
  }
  actions.sort((left, right) => left.at - right.at || left.index - right.index)

  let clock = 0
  for (const action of actions) {
    await vi.advanceTimersByTimeAsync(action.at - clock)
    clock = action.at

    if (action.kind === "arrive") {
      const index = action.index
      void op.execute(index).then(
        () => {
          terminal[index] = {
            outcome: "resolved",
            at: Date.now() - zero,
            reason: undefined,
          }
        },
        (error: unknown) => {
          terminal[index] = {
            outcome: "rejected",
            at: Date.now() - zero,
            reason: reasonOf(error),
          }
        },
      )
    } else {
      gates[action.index]?.resolve()
    }

    await vi.advanceTimersByTimeAsync(0)
  }

  // Past every queue timeout and lease, so a call that can still settle does.
  const horizon =
    3 *
      Math.max(
        spec.leaseMs ?? 0,
        spec.queue?.timeoutMs ?? 0,
        ...spec.settle.map((delay) => delay ?? 0),
        1,
      ) +
    10
  await vi.advanceTimersByTimeAsync(horizon)

  const steps: Step[] = []
  for (const event of events) {
    const at = event.at - zero
    switch (event.type) {
      case "bulkhead.admitted":
        steps.push({
          type: "admitted",
          at,
          executionId: event.context.executionId,
          occupancy: event.occupancy,
          reason: event.reason,
        })
        break
      case "bulkhead.rejected":
        steps.push({
          type: "rejected",
          at,
          executionId: event.context.executionId,
          occupancy: event.occupancy,
          reason: event.reason,
        })
        break
      case "bulkhead.waited":
        steps.push({
          type: "waited",
          at,
          executionId: event.context.executionId,
          occupancy: event.occupancy,
          reason: event.reason,
        })
        break
      case "bulkhead.released":
        steps.push({
          type: "released",
          at,
          executionId: event.context.executionId,
          occupancy: event.occupancy,
          reason: event.reason,
        })
        break
      case "bulkhead.lease-lost":
        steps.push({
          type: "lease-lost",
          at,
          executionId: event.context.executionId,
          occupancy: event.occupancy,
          reason: event.reason,
        })
        break
      case "bulkhead.degraded":
        steps.push({
          type: "degraded",
          at,
          executionId: event.context.executionId,
          occupancy: event.occupancy,
          reason: event.reason,
        })
        break
      default:
        break
    }
  }

  const snapshot = policy.snapshot()
  return {
    arrivals,
    steps,
    started,
    executionIds,
    terminal,
    occupancy: snapshot.occupancy,
    waiting: snapshot.waiting,
  }
}

function withFakeTimers<Result>(body: () => Promise<Result>): Promise<Result> {
  vi.useFakeTimers()
  return body().finally(() => {
    vi.useRealTimers()
  })
}

describe(`local bulkhead - generated permit schedules (seed=${suite.seed} replay="${suite.replay}")`, () => {
  suite.itProperty(
    "keeps permit accounting consistent with every event it emits",
    specArb,
    async (spec: Spec) => {
      await withFakeTimers(async () => {
        const trace = await drive(spec)
        let live = 0

        for (const step of trace.steps) {
          if (step.type === "admitted") {
            live += 1
          }
          if (step.type === "released") {
            live -= 1
          }

          const context = `${step.type} at=${step.at} live=${live} limit=${spec.limit}`
          // A lease-lost holder keeps its permit, so live never goes below zero
          // and never exceeds the limit.
          expect(live, context).toBeGreaterThanOrEqual(0)
          expect(live, context).toBeLessThanOrEqual(spec.limit)

          if (step.occupancy !== undefined) {
            // Every event reports the occupancy the caller can reconstruct from
            // the event stream, so a sink can trust either one.
            expect(step.occupancy, `occupancy on ${context}`).toBe(live)
          }
        }

        expect(live, "live permits at the horizon").toBe(trace.occupancy)
        expect(trace.occupancy, "final occupancy").toBeLessThanOrEqual(
          spec.limit,
        )
        // Every waiter is either granted or timed out by the horizon.
        expect(trace.waiting, "waiters at the horizon").toBe(0)
      })
    },
    { runs: 100 },
  )

  suite.itProperty(
    "never runs the adapter for a refusal, and never hangs a reclaimable holder",
    specArb,
    async (spec: Spec) => {
      await withFakeTimers(async () => {
        const trace = await drive(spec)

        for (const [index, mode] of spec.modes.entries()) {
          const settle = spec.settle[index]
          const startedAt = trace.started[index]
          const end = trace.terminal[index]
          const context = `call=${index} mode=${mode} settle=${String(settle)} leaseMs=${String(spec.leaseMs)} queue=${JSON.stringify(spec.queue)} limit=${spec.limit}`

          if (end === null) {
            // Still pending: only possible when the permit cannot be reclaimed
            // and the work never settles. This is the documented behaviour, not
            // a leak - there is no lease and no abort, so nothing can free it.
            expect(
              startedAt,
              `pending call never started: ${context}`,
            ).not.toBeNull()
            expect(
              settle,
              `pending call whose work settled: ${context}`,
            ).toBeNull()
            expect(
              spec.leaseMs === undefined || mode !== "supported",
              `abort-honoring holder was never reclaimed: ${context}`,
            ).toBe(true)
            continue
          }

          expect(
            end.at,
            `terminal before arrival: ${context}`,
          ).toBeGreaterThanOrEqual(trace.arrivals[index] ?? 0)

          if (end.outcome === "resolved") {
            expect(
              startedAt,
              `resolved call never started: ${context}`,
            ).not.toBeNull()
            expect(
              end.reason,
              `resolved call carried a reason: ${context}`,
            ).toBeUndefined()
            continue
          }

          expect(
            ["capacity", "wait-timeout", "lease-lost"],
            `unexpected rejection: ${context}`,
          ).toContain(end.reason)

          if (end.reason === "lease-lost") {
            // A lease-lost refusal is always a started call on an abort-honoring
            // holder: the permit was held and the work had begun.
            expect(
              startedAt,
              `lease-lost without a start: ${context}`,
            ).not.toBeNull()
            expect(
              mode,
              `lease-lost on an abort-ignoring adapter: ${context}`,
            ).toBe("supported")
            expect(
              spec.leaseMs,
              `lease-lost without a lease: ${context}`,
            ).toBeDefined()
            if (startedAt === null || spec.leaseMs === undefined) {
              continue
            }
            expect(
              end.at - startedAt,
              `lease reclaimed earlier than leaseMs: ${context}`,
            ).toBeGreaterThanOrEqual(spec.leaseMs)
          } else {
            // The adapter never ran, so the dependency saw no traffic.
            expect(
              startedAt,
              `refusal that ran the adapter: ${context}`,
            ).toBeNull()
          }
        }
      })
    },
    { runs: 100 },
  )

  suite.itProperty(
    "bounds a waiter by the queue timeout and never loses a freed permit",
    specArb,
    async (spec: Spec) => {
      await withFakeTimers(async () => {
        const trace = await drive(spec)
        const queue = spec.queue

        for (const step of trace.steps) {
          if (step.type === "rejected") {
            expect(
              ["capacity", "wait-timeout"],
              `local rejection at=${step.at}`,
            ).toContain(step.reason)
          }
          if (step.type === "rejected" && step.reason === "capacity") {
            // A capacity refusal is only possible at the limit: nothing is
            // refused while a permit is free.
            expect(step.occupancy, `capacity refusal at=${step.at}`).toBe(
              spec.limit,
            )
          }
        }

        for (const [index, end] of trace.terminal.entries()) {
          if (end === null || end.reason !== "wait-timeout") {
            continue
          }
          expect(
            queue,
            `wait-timeout without a queue: call=${index}`,
          ).toBeDefined()
          if (queue === undefined) {
            continue
          }
          // The wait ends exactly at the queue timeout: no early give-up and no
          // silently renewed wait.
          expect(
            end.at - (trace.arrivals[index] ?? 0),
            `call=${index} queue.timeoutMs=${queue.timeoutMs}`,
          ).toBe(queue.timeoutMs)
        }

        // A release while a call is queued hands the permit on. Without this a
        // permit can leak and every later waiter times out in turn.
        const waitingAt = (at: number): number => {
          let count = 0
          for (const [index, arrival] of trace.arrivals.entries()) {
            if (arrival > at) {
              continue
            }
            const end = trace.terminal[index]
            if (end !== null && end.at <= at) {
              continue
            }
            const startedAt = trace.started[index]
            if (startedAt === null || startedAt > at) {
              count += 1
            }
          }
          return count
        }

        for (const step of trace.steps) {
          if (step.type !== "released" || waitingAt(step.at) === 0) {
            continue
          }
          expect(
            trace.started.some(
              (startedAt) => startedAt !== null && startedAt >= step.at,
            ),
            `release at=${step.at} left a waiter unserved`,
          ).toBe(true)
        }

        if (spec.queue === undefined) {
          for (const step of trace.steps) {
            if (step.type === "rejected") {
              expect(step.reason, `refusal without a queue at=${step.at}`).toBe(
                "capacity",
              )
            }
          }
        }
      })
    },
    { runs: 100 },
  )

  suite.itProperty(
    "reports a lease it cannot reclaim, and reclaims the holder it can",
    specArb,
    async (spec: Spec) => {
      await withFakeTimers(async () => {
        const trace = await drive(spec)
        const stepsOfType = (type: Step["type"]): Step[] =>
          trace.steps.filter((step) => step.type === type)
        const leaseLost = stepsOfType("lease-lost")

        for (const [index, mode] of spec.modes.entries()) {
          // Only a permit holder can lose a lease: a refusal never ran.
          const admitted = trace.started[index] !== null
          const holdsForever = spec.settle[index] === null
          if (spec.leaseMs === undefined || !holdsForever || !admitted) {
            continue
          }
          const context = `call=${index} mode=${mode} leaseMs=${spec.leaseMs} limit=${spec.limit}`
          const executionId = trace.executionIds[index] ?? null

          // Either way this holder's expiry is reported, so a sink can see that
          // a permit sat expired instead of being retained silently. The event
          // is attributed by execution id: a bare count lets one holder's
          // expiry stand in for another's that was never reported.
          expect(
            leaseLost.filter((step) => step.executionId === executionId).length,
            `no lease-lost event for this holder: ${context}`,
          ).toBeGreaterThan(0)

          if (mode === "supported") {
            // The abort settles the work, so the permit comes back.
            expect(
              trace.terminal[index]?.reason,
              `abort-honoring holder was not reclaimed: ${context}`,
            ).toBe("lease-lost")
          } else {
            // Abort is advisory: the holder keeps its permit and its caller
            // stays pending. The docs call this out - there is no fencing.
            expect(
              trace.terminal[index],
              `abort-ignoring holder was reclaimed: ${context}`,
            ).toBeNull()
            expect(
              trace.occupancy,
              `permit reclaimed without an abort: ${context}`,
            ).toBeGreaterThanOrEqual(1)
          }
        }

        // Every permit has one owner and one end: an execution reports a lost
        // lease once, and gives up its permit once. Scoped by execution id, so a
        // duplicated or misattributed event cannot pass.
        for (const type of ["lease-lost", "released"] as const) {
          const perExecution = new Map<string, number>()
          for (const step of stepsOfType(type)) {
            perExecution.set(
              step.executionId,
              (perExecution.get(step.executionId) ?? 0) + 1,
            )
          }
          for (const [executionId, count] of perExecution) {
            expect(
              count,
              `${type} reported ${count} times: execution=${executionId}`,
            ).toBe(1)
          }
        }

        // A call that was admitted and never settled still holds its permit at
        // the end, so the snapshot cannot fall below how many such calls there
        // are: a holder reclaimed while its caller stayed pending cannot hide
        // behind another holder that kept its permit.
        const heldToTheEnd = trace.started.filter(
          (startedAt, index) =>
            startedAt !== null && trace.terminal[index] === null,
        ).length
        expect(
          trace.occupancy,
          `occupancy below ${heldToTheEnd} unsettled holders`,
        ).toBeGreaterThanOrEqual(heldToTheEnd)
      })
    },
    { runs: 100 },
  )

  suite.itProperty(
    "serves waiters in arrival order, and never makes a call wait without a queue",
    specArb,
    async (spec: Spec) => {
      await withFakeTimers(async () => {
        const trace = await drive(spec)
        const waiters: Array<{ index: number; startedAt: number }> = []
        trace.started.forEach((startedAt, index) => {
          const arrival = trace.arrivals[index]
          if (
            startedAt !== null &&
            arrival !== undefined &&
            startedAt > arrival
          ) {
            waiters.push({ index, startedAt })
          }
        })

        // FIFO, no barging: a later arrival never starts before an earlier
        // waiter when both had to wait.
        for (let position = 1; position < waiters.length; position++) {
          const previous = waiters[position - 1]
          const current = waiters[position]
          if (previous === undefined || current === undefined) {
            continue
          }
          expect(
            current.startedAt,
            `waiter ${current.index} overtook waiter ${previous.index} (limit=${spec.limit})`,
          ).toBeGreaterThanOrEqual(previous.startedAt)
        }

        // Immediate rejection is the only option without a queue.
        if (spec.queue === undefined) {
          expect(
            waiters,
            `a call waited without a queue (limit=${spec.limit})`,
          ).toHaveLength(0)
        }
      })
    },
    { runs: 100 },
  )
})

/**
 * Pinned schedules. The generator above is broad but seed-dependent, so the
 * orderings that matter most are also written out by hand: a mutation in the
 * handoff order is then a deterministic failure, not a lucky draw.
 */
describe("local bulkhead - pinned schedules", () => {
  it("history: a freed permit goes to the earliest waiter, not to the latest", async () => {
    await withFakeTimers(async () => {
      const trace = await drive({
        limit: 1,
        queue: { limit: 2, timeoutMs: 100 },
        leaseMs: undefined,
        gaps: [0, 0],
        settle: [10, null, null],
        modes: ["unsupported", "unsupported", "unsupported"],
      })

      // The first call holds the only permit until it settles; the two later
      // arrivals queue behind it.
      expect(trace.started).toEqual([0, 10, null])
      expect(trace.terminal[0]?.outcome).toBe("resolved")
      // The second call is the one the released permit reached, and it never
      // settles, so it is still holding the permit at the horizon.
      expect(trace.terminal[1]).toBeNull()
      // The third waiter timed out in the queue instead of overtaking it.
      expect(trace.terminal[2]?.reason).toBe("wait-timeout")
      expect(trace.occupancy).toBe(1)
      expect(trace.waiting).toBe(0)
    })
  })
})
