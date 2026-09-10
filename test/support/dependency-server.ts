import { createServer, type ServerResponse } from "node:http"
/** Parent-owned work accounting; a client disconnect does not finish server work. */
export async function dependencyServer() {
  const pending = new Set<ServerResponse>()
  let maximum = 0
  let started = 0
  const server = createServer((_request, response) => {
    pending.add(response)
    started++
    maximum = Math.max(maximum, pending.size)
  })
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve))
  const address = server.address()
  if (!address || typeof address === "string")
    throw new Error("No server address")
  const release = () => {
    for (const response of pending) response.end("done")
    pending.clear()
  }
  return {
    url: `http://127.0.0.1:${address.port}`,
    get active() {
      return pending.size
    },
    get maximum() {
      return maximum
    },
    get started() {
      return started
    },
    release,
    async close() {
      release()
      server.closeAllConnections()
      await new Promise<void>((resolve, reject) =>
        server.close((error) => (error ? reject(error) : resolve())),
      )
    },
  }
}
