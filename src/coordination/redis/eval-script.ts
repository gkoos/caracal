import { createHash } from "node:crypto"

/**
 * Minimal Lua-script capability required by the Redis coordinators.
 *
 * `eval` is always required.  `evalsha` is optional so structural test doubles
 * and clients that only expose `eval` keep working; when it is missing the
 * script body is sent with `eval`.
 */
export interface ScriptClient {
  eval(
    script: string,
    numberOfKeys: number,
    ...args: (string | number)[]
  ): Promise<unknown>
  evalsha?(
    sha: string,
    numberOfKeys: number,
    ...args: (string | number)[]
  ): Promise<unknown>
}

const scriptHashes = new Map<string, string>()

/** SHA1 of a script body.  Redis keys its script cache by exactly this value. */
export function scriptSha(script: string): string {
  const cached = scriptHashes.get(script)
  if (cached !== undefined) return cached
  const sha = createHash("sha1").update(script).digest("hex")
  scriptHashes.set(script, sha)
  return sha
}

function errorMessage(error: unknown): string {
  if (typeof error === "string") return error
  if (error instanceof Error) return error.message
  return ""
}

/**
 * Executes a Lua script, sending the body only when it has to.
 *
 * The script cache is keyed by SHA1, so the steady state is `EVALSHA` - a
 * 40-byte hash instead of kilobytes of Lua on every call.  The full body goes
 * out with `EVAL` only when the client cannot send `EVALSHA`, or when the
 * server answers `NOSCRIPT`: the script cache lost the script to a restart, a
 * `SCRIPT FLUSH`, or (Redis 7.4 and later) LRU eviction under memory pressure.
 *
 * Retrying through `EVAL` is safe because Redis rejects an unknown SHA1 before
 * executing anything, so the failed command is known not to have run.  This is
 * deliberately narrower than the coordinator's command-timeout rule, where the
 * outcome is unknown and a replay is never allowed.  A timeout error is not a
 * `NOSCRIPT` error and propagates unchanged.
 */
export async function evalScript(
  client: ScriptClient,
  script: string,
  numberOfKeys: number,
  ...args: (string | number)[]
): Promise<unknown> {
  const evalsha = client.evalsha
  if (typeof evalsha !== "function") {
    return client.eval(script, numberOfKeys, ...args)
  }
  try {
    return await evalsha.call(client, scriptSha(script), numberOfKeys, ...args)
  } catch (error) {
    if (!errorMessage(error).includes("NOSCRIPT")) throw error
    return client.eval(script, numberOfKeys, ...args)
  }
}
