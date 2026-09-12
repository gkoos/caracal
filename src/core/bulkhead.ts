import { randomUUID } from "node:crypto"
import {
  MAX_TIMER_MS,
  admissionSignal,
  emitRuntimeEvent,
  withAdmissionSignal,
  withSignal,
} from "./runtime.js"
import type { ExecutionContext, Next, Policy } from "./types.js"

export class BulkheadRejectedError extends Error {
  constructor(
    readonly coordination: "local" | "distributed",
    readonly policyName: string,
    readonly scope: string,
    readonly reason: string,
  ) {
    super(`Bulkhead ${policyName} rejected: ${reason}`)
    this.name = "BulkheadRejectedError"
  }
}
export interface LocalBulkheadOptions {
  readonly name: string
  readonly limit: number
  readonly queue?: { readonly limit: number; readonly timeoutMs: number }
}
/** Policy-specific capability supplied by caracal/redis. */
export interface BulkheadCoordinator {
  command(
    identity: { name: string; operation: string; scope: string },
    action: "acquire" | "renew" | "release",
    token: string,
    leaseMs: number,
    limit: number,
  ): Promise<{ allowed: boolean; occupancy: number }>
}
export interface DistributedBulkheadOptions {
  readonly name: string
  readonly limit: number
  readonly coordinator: BulkheadCoordinator
  readonly scope: (context: ExecutionContext) => string
  readonly leaseMs?: number
}
function validate(name: string, limit: number) {
  if (!name.trim() || !Number.isSafeInteger(limit) || limit < 1)
    throw new RangeError("Bulkhead needs a name and positive integer limit")
}
function event(
  context: ExecutionContext,
  coordination: "local" | "distributed",
  policyName: string,
  scope: string,
  type:
    | "admitted"
    | "rejected"
    | "waited"
    | "released"
    | "lease-lost"
    | "degraded",
  occupancy?: number,
  reason?: string,
) {
  emitRuntimeEvent(context, {
    type: `bulkhead.${type}`,
    coordination,
    policyName,
    scope,
    ...(occupancy === undefined ? {} : { occupancy }),
    ...(reason === undefined ? {} : { reason }),
  })
}
function local(options: LocalBulkheadOptions): Policy & {
  readonly coordination: "local"
  snapshot(): { coordination: "local"; occupancy: number; waiting: number }
} {
  const { name, limit } = options
  const queue = options.queue && { ...options.queue }
  validate(name, limit)
  if (
    queue &&
    (!Number.isSafeInteger(queue.limit) ||
      queue.limit < 1 ||
      !Number.isSafeInteger(queue.timeoutMs) ||
      queue.timeoutMs < 1 ||
      queue.timeoutMs > MAX_TIMER_MS)
  )
    throw new RangeError("Invalid bounded queue")
  let occupancy = 0
  const waiting: (() => void)[] = []
  return Object.freeze({
    name,
    phase: "attempt" as const,
    coordination: "local" as const,
    snapshot: () => ({
      coordination: "local" as const,
      occupancy,
      waiting: waiting.length,
    }),
    async execute<Result>(
      context: ExecutionContext,
      next: Next<Result>,
    ): Promise<Result> {
      const signal = admissionSignal(context)
      signal?.throwIfAborted()
      const reject = (reason: string) => {
        event(context, "local", name, "process", "rejected", occupancy, reason)
        return new BulkheadRejectedError("local", name, "process", reason)
      }
      if (occupancy >= limit) {
        if (!queue || waiting.length >= queue.limit) throw reject("capacity")
        event(context, "local", name, "process", "waited", occupancy)
        await new Promise<void>((resolve, fail) => {
          const cleanup = () => {
            clearTimeout(timer)
            signal?.removeEventListener("abort", abort)
            const i = waiting.indexOf(grant)
            if (i >= 0) waiting.splice(i, 1)
          }
          const grant = () => {
            cleanup()
            occupancy++
            resolve()
          }
          const abort = () => {
            cleanup()
            event(
              context,
              "local",
              name,
              "process",
              "rejected",
              occupancy,
              "cancelled",
            )
            fail(signal?.reason)
          }
          const timer = setTimeout(() => {
            cleanup()
            fail(reject("wait-timeout"))
          }, queue.timeoutMs)
          waiting.push(grant)
          signal?.addEventListener("abort", abort, { once: true })
          if (signal?.aborted) abort()
        })
      } else occupancy++
      event(context, "local", name, "process", "admitted", occupancy)
      try {
        signal?.throwIfAborted()
        return await next(context)
      } finally {
        occupancy--
        // Report the released permit before handing it to the queued successor:
        // granting first would make this event's occupancy already include the
        // next admission, so the released/admitted pair would look inverted.
        event(context, "local", name, "process", "released", occupancy)
        waiting[0]?.()
      }
    },
  })
}
function distributed(
  options: DistributedBulkheadOptions,
): Policy & { readonly coordination: "distributed" } {
  const {
    name,
    limit,
    coordinator,
    scope: resolveScope,
    leaseMs = 30000,
  } = options
  validate(name, limit)
  if (!Number.isSafeInteger(leaseMs) || leaseMs < 100 || leaseMs > 86400000)
    throw new RangeError("leaseMs must be 100..86400000")
  return Object.freeze({
    name,
    phase: "attempt" as const,
    coordination: "distributed" as const,
    async execute<Result>(
      context: ExecutionContext,
      next: Next<Result>,
    ): Promise<Result> {
      admissionSignal(context)?.throwIfAborted()
      const scope = resolveScope(context)
      if (typeof scope !== "string" || !scope.trim())
        throw new TypeError("Invalid bulkhead scope")
      const identity = { name, operation: context.operationName, scope }
      const token = randomUUID()
      const command = (action: "acquire" | "renew" | "release") =>
        coordinator.command(identity, action, token, leaseMs, limit)
      let admitted: { allowed: boolean; occupancy: number }
      const started = performance.now()
      try {
        admitted = await command("acquire")
      } catch (error) {
        event(
          context,
          "distributed",
          name,
          scope,
          "degraded",
          undefined,
          "admission-unknown",
        )
        event(
          context,
          "distributed",
          name,
          scope,
          "rejected",
          undefined,
          "coordinator-unavailable",
        )
        throw error
      }
      if (!admitted.allowed) {
        event(
          context,
          "distributed",
          name,
          scope,
          "rejected",
          admitted.occupancy,
          "capacity",
        )
        throw new BulkheadRejectedError("distributed", name, scope, "capacity")
      }
      let stopped = false
      let lost = false
      let timer: ReturnType<typeof setTimeout> | undefined
      let deadlineTimer: ReturnType<typeof setTimeout> | undefined
      let deadline = started + leaseMs
      const controller = new AbortController()
      const lose = () => {
        if (lost || stopped) return
        lost = true
        clearTimeout(timer)
        event(context, "distributed", name, scope, "lease-lost")
        event(
          context,
          "distributed",
          name,
          scope,
          "degraded",
          undefined,
          "lease-uncertain",
        )
        controller.abort(
          new BulkheadRejectedError("distributed", name, scope, "lease-lost"),
        )
      }
      const watch = () => {
        clearTimeout(deadlineTimer)
        deadlineTimer = setTimeout(
          lose,
          Math.max(0, deadline - performance.now()),
        )
      }
      const renew = async () => {
        const sent = performance.now()
        try {
          if (
            !(await command("renew")).allowed ||
            performance.now() >= deadline
          ) {
            lose()
            return
          }
          deadline = sent + leaseMs
        } catch {
          lose()
          return
        }
        if (!stopped && !lost) {
          watch()
          timer = setTimeout(() => void renew(), leaseMs / 3)
        }
      }
      try {
        if (performance.now() >= deadline)
          throw new BulkheadRejectedError(
            "distributed",
            name,
            scope,
            "admission-expired",
          )
        admissionSignal(context)?.throwIfAborted()
        event(
          context,
          "distributed",
          name,
          scope,
          "admitted",
          admitted.occupancy,
        )
        watch()
        timer = setTimeout(() => void renew(), leaseMs / 3)
        return await next(
          withAdmissionSignal(
            context.capabilities.abort === "supported"
              ? withSignal(
                  context,
                  context.signal
                    ? AbortSignal.any([context.signal, controller.signal])
                    : controller.signal,
                )
              : context,
            controller.signal,
          ),
        )
      } finally {
        stopped = true
        clearTimeout(timer)
        clearTimeout(deadlineTimer)
        try {
          const result = await command("release")
          event(
            context,
            "distributed",
            name,
            scope,
            "released",
            result.occupancy,
            result.allowed ? undefined : "already-expired-or-released",
          )
        } catch {
          event(
            context,
            "distributed",
            name,
            scope,
            "degraded",
            undefined,
            "release-unknown",
          )
        }
      }
    },
  })
}
export const bulkhead = Object.freeze({ local, distributed })
