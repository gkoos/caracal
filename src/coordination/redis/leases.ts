import { CoordinatorUnavailableError } from "./client.js"
import { evalScript, type ScriptClient } from "./eval-script.js"
import { leaseV1 } from "./scripts.js"
export interface RedisScriptClient extends ScriptClient {
  hmget(key: string, ...fields: string[]): Promise<(string | null)[]>
}
/** Internal policy-specific capability, not a public distributed lock API. */
export async function leaseCommand(
  client: RedisScriptClient,
  key: string,
  action: "acquire" | "renew" | "release",
  token: string,
  ttl: number,
  limit: number,
): Promise<boolean> {
  if (
    !["acquire", "renew", "release"].includes(action) ||
    !token ||
    token.length > 256
  )
    throw new TypeError("Invalid lease action or token")
  if (
    ![ttl, limit].every((value) => Number.isSafeInteger(value) && value > 0) ||
    ttl > 86400000
  )
    throw new RangeError("Invalid lease TTL or limit")
  try {
    const result = await evalScript(
      client,
      leaseV1,
      1,
      key,
      action,
      token,
      ttl,
      limit,
    )
    if (result !== 0 && result !== 1)
      throw new Error("Invalid Redis script reply")
    return result === 1
  } catch (error) {
    throw new CoordinatorUnavailableError(error)
  }
}
