import { connect, createServer, type Socket } from "node:net"
export async function redisProxy(target: URL) {
  const sockets = new Set<Socket>()
  let blocked = false
  let delayMs = 0
  const timers = new Set<ReturnType<typeof setTimeout>>()
  const server = createServer((incoming) => {
    if (blocked) {
      incoming.destroy()
      return
    }
    const outgoing = connect(Number(target.port || 6379), target.hostname)
    for (const socket of [incoming, outgoing]) {
      sockets.add(socket)
      socket.on("error", () => {
        incoming.destroy()
        outgoing.destroy()
      })
      socket.on("close", () => {
        sockets.delete(socket)
        incoming.destroy()
        outgoing.destroy()
      })
    }
    incoming.on("data", (data) => {
      const timer = setTimeout(() => {
        timers.delete(timer)
        if (!outgoing.destroyed) outgoing.write(data)
      }, delayMs)
      timers.add(timer)
    })
    outgoing.pipe(incoming)
  })
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve))
  const address = server.address()
  if (!address || typeof address === "string")
    throw new Error("Missing proxy port")
  const url = new URL(target)
  url.hostname = "127.0.0.1"
  url.port = String(address.port)
  return {
    url: url.toString(),
    delay(ms: number) {
      delayMs = ms
    },
    disconnect() {
      blocked = true
      for (const socket of sockets) socket.destroy()
    },
    restore() {
      blocked = false
    },
    async close() {
      for (const timer of timers) clearTimeout(timer)
      for (const socket of sockets) socket.destroy()
      await new Promise<void>((resolve) => server.close(() => resolve()))
    },
  }
}
