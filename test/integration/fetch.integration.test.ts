import { once } from "node:events"
import { createServer } from "node:http"

import { afterEach, beforeEach, describe, expect, it } from "vitest"
import { fetchAdapter } from "../../src/fetch.js"
import { operation, retry } from "../../src/index.js"

let server: ReturnType<typeof createServer>
let baseUrl: string
let attempts = 0

beforeEach(async () => {
  attempts = 0
  server = createServer((request, response) => {
    if (request.url === "/flaky") {
      attempts += 1
      response.statusCode = attempts === 1 ? 503 : 200
      response.end(attempts === 1 ? "unavailable" : "ok")
      return
    }

    response.statusCode = 200
    response.end("ok")
  })
  server.listen(0, "127.0.0.1")
  await once(server, "listening")
  const address = server.address()
  if (address === null || typeof address === "string") {
    throw new Error("Expected a TCP server address")
  }
  baseUrl = `http://127.0.0.1:${address.port}`
})

afterEach(async () => {
  server.close()
  await once(server, "close")
})

describe("fetch adapter integration", () => {
  it("classifies a real 503 response and retries through Node fetch", async () => {
    const subject = operation({
      name: "real-fetch",
      adapter: fetchAdapter(),
      policies: [retry({ maxAttempts: 2 })],
    })

    const response = await subject.execute({ url: `${baseUrl}/flaky` })

    expect(response.status).toBe(200)
    expect(await response.text()).toBe("ok")
    expect(attempts).toBe(2)
  })

  it("retries a replay-safe POST whose body can be re-sent", async () => {
    const subject = operation({
      name: "post-retry",
      adapter: fetchAdapter({ replay: () => "safe" }),
      policies: [retry({ maxAttempts: 2 })],
    })

    const response = await subject.execute({
      url: `${baseUrl}/flaky`,
      options: { method: "POST", body: "payload" },
    })

    // A string body is re-sent on the second attempt, so the declared
    // idempotency is all that is needed.
    expect(response.status).toBe(200)
    expect(attempts).toBe(2)
  })

  it("cannot replay a Request object, which is single-use", async () => {
    const subject = operation({
      name: "request-retry",
      adapter: fetchAdapter({ replay: () => "safe" }),
      policies: [retry({ maxAttempts: 2 })],
    })
    const request = new Request(`${baseUrl}/flaky`, {
      method: "POST",
      body: "payload",
    })

    // Documented limitation: the first attempt consumes the Request, so the
    // second cannot be constructed from it, whatever `replay` says.
    await expect(subject.execute({ url: request })).rejects.toBeInstanceOf(
      TypeError,
    )
    expect(attempts).toBe(1)
  })
})
