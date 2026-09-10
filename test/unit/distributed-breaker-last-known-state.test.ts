import { describe, expect, it } from "vitest"
import type {
  Adapter,
  AdmitProbeResult,
  BreakerCoordinator,
  BreakerIdentity,
  ObserveResult,
  SettleProbeResult,
} from "../../src/index.js"
import { CircuitOpenError, circuitBreaker, operation } from "../../src/index.js"

type State = "closed" | "open" | "half-open"

/**
 * Coordinator stub with scripted per-identity state, probe settlement, and an
 * outage switch, so the last-known-state behaviour can be exercised without
 * Redis.
 */
class ScriptedCoordinator implements BreakerCoordinator {
  outage = false
  settleResult: SettleProbeResult = {
    type: "settled",
    state: "half-open",
    generation: 1,
  }
  admitResult: AdmitProbeResult | undefined

  constructor(
    private readonly stateFor: (identity: BreakerIdentity) => State,
  ) {}

  async readState(identity: BreakerIdentity) {
    if (this.outage) {
      throw new Error("coordinator unavailable")
    }
    return { state: this.stateFor(identity), generation: 1 }
  }

  async observe(): Promise<ObserveResult> {
    return {
      type: "observed",
      generation: 1,
      windowTotal: 1,
      windowFailures: 0,
    }
  }

  async admitProbe(identity: BreakerIdentity): Promise<AdmitProbeResult> {
    if (this.admitResult !== undefined) {
      return this.admitResult
    }
    return {
      type: "admitted",
      generation: 1,
      probeCount: 1,
      stateChanged: this.stateFor(identity) === "open",
    }
  }

  async settleProbe(): Promise<SettleProbeResult> {
    return this.settleResult
  }
}

const okAdapter: Adapter<undefined, string> = {
  capabilities: () => ({ abort: "unsupported", replay: "safe" }),
  execute: async () => "ok",
}

function makePolicy(coordinator: BreakerCoordinator) {
  return circuitBreaker.distributed({
    name: "breaker",
    coordinator,
    scope: () => "region:eu",
    onCoordinatorError: "fail-open",
  })
}

describe("distributed breaker — last known state", () => {
  it("fails closed during an outage after a scope was seen open", async () => {
    const coordinator = new ScriptedCoordinator(() => "open")
    const subject = operation({
      name: "op",
      adapter: okAdapter,
      policies: [makePolicy(coordinator)],
    })

    await expect(subject.execute(undefined)).resolves.toBe("ok")

    coordinator.outage = true
    await expect(subject.execute(undefined)).rejects.toBeInstanceOf(
      CircuitOpenError,
    )
  })

  it("forgets a scope once a probe closes the breaker", async () => {
    const coordinator = new ScriptedCoordinator(() => "half-open")
    coordinator.settleResult = {
      type: "transitioned",
      newState: "closed",
      newGeneration: 2,
      previousState: "half-open",
    }
    const subject = operation({
      name: "op",
      adapter: okAdapter,
      policies: [makePolicy(coordinator)],
    })

    await expect(subject.execute(undefined)).resolves.toBe("ok")

    coordinator.outage = true
    await expect(subject.execute(undefined)).resolves.toBe("ok")
  })

  it("forgets a scope when probe admission reports it closed", async () => {
    const coordinator = new ScriptedCoordinator(() => "open")
    coordinator.admitResult = {
      type: "rejected",
      reason: "closed",
      generation: 2,
    }
    const subject = operation({
      name: "op",
      adapter: okAdapter,
      policies: [makePolicy(coordinator)],
    })

    await expect(subject.execute(undefined)).resolves.toBe("ok")

    coordinator.outage = true
    await expect(subject.execute(undefined)).resolves.toBe("ok")
  })

  it("does not fail closed for scopes that were only ever closed", async () => {
    const coordinator = new ScriptedCoordinator(() => "closed")
    const policy = circuitBreaker.distributed({
      name: "breaker",
      coordinator,
      scope: (context) => `tenant:${context.metadata.id}`,
      onCoordinatorError: "fail-open",
    })
    const subject = operation({
      name: "op",
      adapter: okAdapter,
      policies: [policy],
    })

    for (let i = 0; i < 500; i++) {
      await expect(
        subject.execute(undefined, { metadata: { id: i } }),
      ).resolves.toBe("ok")
    }

    coordinator.outage = true
    await expect(
      subject.execute(undefined, { metadata: { id: 501 } }),
    ).resolves.toBe("ok")
  })

  it("isolates last known state per operation on a shared policy instance", async () => {
    const coordinator = new ScriptedCoordinator((identity) =>
      identity.operation === "B" ? "open" : "closed",
    )
    const policy = makePolicy(coordinator)
    const opA = operation({ name: "A", adapter: okAdapter, policies: [policy] })
    const opB = operation({ name: "B", adapter: okAdapter, policies: [policy] })

    await expect(opB.execute(undefined)).resolves.toBe("ok")
    await expect(opA.execute(undefined)).resolves.toBe("ok")

    coordinator.outage = true
    await expect(opB.execute(undefined)).rejects.toBeInstanceOf(
      CircuitOpenError,
    )
    await expect(opA.execute(undefined)).resolves.toBe("ok")
  })
})
