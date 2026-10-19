import { once } from "node:events"
import {
  createServer,
  request as httpRequest,
  type IncomingMessage,
  type ServerResponse,
} from "node:http"

export type AwsFaultMode =
  | { readonly kind: "forward" }
  | {
      readonly kind: "fail-before"
      readonly requests: number
      readonly status?: number
      readonly errorType?: string
    }
  | {
      readonly kind: "fail-after-upstream"
      readonly requests: number
      readonly status?: number
      readonly errorType?: string
    }
  | { readonly kind: "hang" }

export interface AwsFaultProxy {
  /** The endpoint the SDK client should use. */
  readonly url: string
  /** HTTP requests that reached the proxy. */
  readonly requests: number
  /** Requests the client cancelled before the proxy answered. */
  readonly aborted: number
  reset(): void
  close(): Promise<void>
}

function sendAwsError(
  response: ServerResponse,
  status: number,
  errorType: string,
): void {
  const body = JSON.stringify({
    __type: errorType,
    message: "injected by the AWS integration proxy",
  })
  response.writeHead(status, {
    "content-type": "application/x-amz-json-1.0",
    "x-amzn-errortype": errorType,
    "content-length": Buffer.byteLength(body),
  })
  response.end(body)
}

/**
 * A transparent proxy in front of Floci. It counts requests and can fail before
 * forwarding, fail after the upstream accepted the request, or hold the response
 * open so a client abort is observable on the wire.
 */
export async function startAwsFaultProxy(options: {
  readonly upstream: string
  readonly mode: AwsFaultMode
}): Promise<AwsFaultProxy> {
  let requests = 0
  let aborted = 0

  const server = createServer(
    (incoming: IncomingMessage, outgoing: ServerResponse) => {
      const mode = options.mode
      requests += 1
      const current = requests
      outgoing.on("close", () => {
        if (!outgoing.writableEnded) aborted += 1
      })

      if (mode.kind === "hang") {
        incoming.resume()
        return
      }

      if (mode.kind === "fail-before" && current <= mode.requests) {
        incoming.resume()
        sendAwsError(
          outgoing,
          mode.status ?? 500,
          mode.errorType ?? "InternalServerException",
        )
        return
      }

      const upstream = new URL(options.upstream)
      const proxyRequest = httpRequest(
        {
          hostname: upstream.hostname,
          port: upstream.port,
          path: incoming.url ?? "/",
          method: incoming.method,
          headers: incoming.headers,
        },
        (upstreamResponse) => {
          if (mode.kind === "fail-after-upstream" && current <= mode.requests) {
            // Drain the upstream response so the request is fully accepted,
            // then report a lost response to the client.
            upstreamResponse.resume()
            upstreamResponse.on("end", () => {
              sendAwsError(
                outgoing,
                mode.status ?? 500,
                mode.errorType ?? "InternalServerException",
              )
            })
            return
          }

          outgoing.writeHead(
            upstreamResponse.statusCode ?? 502,
            upstreamResponse.headers,
          )
          upstreamResponse.pipe(outgoing)
        },
      )

      proxyRequest.on("error", () => {
        if (!outgoing.headersSent) {
          sendAwsError(outgoing, 502, "InternalServerException")
        } else {
          outgoing.destroy()
        }
      })
      incoming.pipe(proxyRequest)
    },
  )

  server.listen(0, "127.0.0.1")
  await once(server, "listening")
  const address = server.address()
  if (address === null || typeof address === "string") {
    throw new Error("Expected a TCP address for the AWS fault proxy")
  }

  return {
    url: `http://127.0.0.1:${address.port}`,
    get requests() {
      return requests
    },
    get aborted() {
      return aborted
    },
    reset() {
      requests = 0
      aborted = 0
    },
    async close() {
      server.closeAllConnections()
      server.close()
      await once(server, "close")
    },
  }
}
