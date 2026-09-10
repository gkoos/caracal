import { createHash } from "node:crypto"
import { describe, expect, it } from "vitest"
import {
  evalScript,
  type ScriptClient,
  scriptSha,
} from "../../src/coordination/redis/eval-script.js"

const SCRIPT = "return ARGV[1]"
const NO_SCRIPT = "NOSCRIPT No matching script. Please use EVAL."

interface Recording {
  client: ScriptClient
  evalCalls: unknown[][]
  evalshaCalls: unknown[][]
}

function recordingClient(options: { evalshaError?: unknown } = {}): Recording {
  const evalCalls: unknown[][] = []
  const evalshaCalls: unknown[][] = []
  const client: ScriptClient = {
    eval: async (script, numberOfKeys, ...args) => {
      evalCalls.push([script, numberOfKeys, ...args])
      return "eval-reply"
    },
    evalsha: async (sha, numberOfKeys, ...args) => {
      evalshaCalls.push([sha, numberOfKeys, ...args])
      if (options.evalshaError !== undefined) throw options.evalshaError
      return "evalsha-reply"
    },
  }
  return { client, evalCalls, evalshaCalls }
}

describe("evalScript", () => {
  it("sends EVALSHA with the script SHA1, keys and arguments", async () => {
    const { client, evalCalls, evalshaCalls } = recordingClient()

    const result = await evalScript(client, SCRIPT, 2, "k1", "k2", 7)

    expect(result).toBe("evalsha-reply")
    expect(evalCalls).toHaveLength(0)
    expect(evalshaCalls).toEqual([[scriptSha(SCRIPT), 2, "k1", "k2", 7]])
    expect(scriptSha(SCRIPT)).toBe(
      createHash("sha1").update(SCRIPT).digest("hex"),
    )
  })

  it("reuses the memoised SHA1 across scripts and clients", async () => {
    const first = recordingClient()
    const second = recordingClient()

    await evalScript(first.client, SCRIPT, 1, "k")
    await evalScript(second.client, SCRIPT, 1, "k")

    expect(first.evalshaCalls[0]?.[0]).toBe(scriptSha(SCRIPT))
    expect(second.evalshaCalls[0]?.[0]).toBe(scriptSha(SCRIPT))
    expect(scriptSha(SCRIPT)).toBe(scriptSha(`${SCRIPT}`))
  })

  it("falls back to EVAL when the server reports a missing script", async () => {
    const { client, evalCalls, evalshaCalls } = recordingClient({
      evalshaError: new Error(NO_SCRIPT),
    })

    const result = await evalScript(client, SCRIPT, 1, "k")

    expect(result).toBe("eval-reply")
    expect(evalshaCalls).toHaveLength(1)
    expect(evalCalls).toEqual([[SCRIPT, 1, "k"]])
  })

  it("treats a NOSCRIPT rejection that is not an Error the same way", async () => {
    const { client, evalCalls } = recordingClient({ evalshaError: NO_SCRIPT })

    await expect(evalScript(client, SCRIPT, 0)).resolves.toBe("eval-reply")

    expect(evalCalls).toEqual([[SCRIPT, 0]])
  })

  it("propagates errors that are not NOSCRIPT without calling EVAL", async () => {
    const timeout = new Error("Command timed out")
    const { client, evalCalls } = recordingClient({ evalshaError: timeout })

    await expect(evalScript(client, SCRIPT, 1, "k")).rejects.toBe(timeout)

    expect(evalCalls).toHaveLength(0)
  })

  it("propagates a failure of the EVAL fallback without retrying EVALSHA", async () => {
    const evalCalls: unknown[][] = []
    let evalshaCalls = 0
    const client: ScriptClient = {
      eval: async (script, numberOfKeys, ...args) => {
        evalCalls.push([script, numberOfKeys, ...args])
        throw new Error("ERR coordinator unavailable")
      },
      evalsha: async () => {
        evalshaCalls += 1
        throw new Error(NO_SCRIPT)
      },
    }

    await expect(evalScript(client, SCRIPT, 1, "k")).rejects.toThrow(
      "ERR coordinator unavailable",
    )

    expect(evalshaCalls).toBe(1)
    expect(evalCalls).toHaveLength(1)
  })

  it("uses EVAL when the client has no EVALSHA capability", async () => {
    const evalCalls: unknown[][] = []
    const client: ScriptClient = {
      eval: async (script, numberOfKeys, ...args) => {
        evalCalls.push([script, numberOfKeys, ...args])
        return "eval-reply"
      },
    }

    const result = await evalScript(client, SCRIPT, 1, "k")

    expect(result).toBe("eval-reply")
    expect(evalCalls).toEqual([[SCRIPT, 1, "k"]])
  })
})
