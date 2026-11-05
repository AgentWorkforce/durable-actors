import { Command } from "commander"
import { once } from "node:events"
import type { IncomingMessage, ServerResponse } from "node:http"
import type { AddressInfo } from "node:net"
import { fileURLToPath } from "node:url"
import open from "open"
import type { Connect, PreviewServer } from "vite"

import { connectionHelp } from "./connection.js"
import { ControlPlaneClient, createControlPlaneClient } from "./control-plane.js"

function registerObserveCommand(program: Command): void {
    program
        .command("observe")
        .description("Open the local observability UI")
        .option("--no-open", "print the UI URL without opening a browser")
        .addHelpText("after", connectionHelp)
        .action(async (options: { open: boolean }) => {
            const observer = new Observer(createControlPlaneClient(process.env, fetch), open)
            const result = await observer.start(options.open)
            const stop = () => {
                process.exitCode ??= 0
                process.off("SIGINT", stop)
                process.off("SIGTERM", stop)
                void observer.close()
            }
            process.once("SIGINT", stop)
            process.once("SIGTERM", stop)
            console.log("Connected to the control plane.")
            console.log(`Observe: ${result.url}`)
            console.log("Press Ctrl+C to stop.")
            if (options.open && !result.browserOpened)
                console.error("Could not open your browser. Open the URL above manually.")
        })
}

class Observer {
    private server: PreviewServer | undefined

    constructor(
        private readonly client: Pick<
            ControlPlaneClient,
            | "checkConnection"
            | "listActors"
            | "getMetrics"
            | "listQueueWaits"
            | "listWebSockets"
            | "getState"
            | "listStateHistory"
        > &
            Partial<
                Pick<
                    ControlPlaneClient,
                    | "openActorStream"
                    | "openRequestStream"
                    | "openLogStream"
                    | "listRequests"
                    | "listResidency"
                    | "listLogs"
                    | "getLogHistogram"
                >
            >,
        private readonly openBrowser: (url: string) => Promise<unknown>,
        private readonly assetDirectory = new URL(
            "./",
            import.meta.resolve("durable-actors-observer/standalone/index.html")
        )
    ) {}

    async start(launchBrowser = true): Promise<{ url: string; browserOpened: boolean }> {
        await this.client.checkConnection()
        this.server = await this.startServer()
        const url = `http://127.0.0.1:${(this.server.httpServer.address() as AddressInfo).port}`
        const browserOpened = launchBrowser
            ? await this.openBrowser(url).then(
                  () => true,
                  () => false
              )
            : false
        return { url, browserOpened }
    }

    async close(): Promise<void> {
        await this.server?.close()
        this.server = undefined
    }

    private async startServer(): Promise<PreviewServer> {
        // Load Vite only when starting the observer to keep other CLI commands fast.
        const { preview } = await import("vite")
        return preview({
            configFile: false,
            envFile: false,
            root: fileURLToPath(this.assetDirectory),
            publicDir: false,
            appType: "mpa",
            logLevel: "silent",
            build: { outDir: "." },
            preview: { host: "127.0.0.1", port: 0, cors: false },
            plugins: [
                {
                    name: "actor-observer-api",
                    configurePreviewServer: server => {
                        server.middlewares.use((request, response, next) => {
                            void this.respond(request, response, next).catch(() => {
                                if (response.headersSent) response.destroy()
                                else response.writeHead(500).end()
                            })
                        })
                    }
                }
            ]
        })
    }

    private async respond(
        request: IncomingMessage,
        response: ServerResponse,
        next: Connect.NextFunction
    ): Promise<void> {
        response.setHeader("cache-control", "no-store")
        response.setHeader("x-content-type-options", "nosniff")
        response.setHeader(
            "content-security-policy",
            "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'"
        )
        const host = `127.0.0.1:${(this.server!.httpServer.address() as AddressInfo).port}`
        if (
            request.headers.host !== host ||
            (request.headers.origin && request.headers.origin !== `http://${host}`) ||
            request.headers["sec-fetch-site"] === "cross-site"
        ) {
            response.writeHead(403).end()
            return
        }
        const url = new URL(request.url!, `http://${host}`)
        const pathname = url.pathname
        if (request.method !== "GET" && request.method !== "HEAD") {
            response.writeHead(405, { allow: "GET, HEAD" }).end()
            return
        }
        if (pathname === "/api/observe/events") {
            await this.proxyEvents(request, response, this.client.openActorStream?.bind(this.client))
            return
        }
        if (pathname === "/api/observe/requests/events") {
            await this.proxyEvents(
                request,
                response,
                this.client.openRequestStream
                    ? signal => this.client.openRequestStream!(signal, url.searchParams.get("after") ?? undefined)
                    : undefined
            )
            return
        }
        if (pathname === "/api/observe/logs/events") {
            await this.proxyEvents(
                request,
                response,
                this.client.openLogStream ? signal => this.client.openLogStream!(signal, url.searchParams) : undefined
            )
            return
        }
        const history = {
            "/api/observe/residency": this.client.listResidency,
            "/api/observe/state": this.client.getState,
            "/api/observe/state/history": this.client.listStateHistory,
            "/api/observe/requests": this.client.listRequests,
            "/api/observe/metrics": this.client.getMetrics,
            "/api/observe/queue-waits": this.client.listQueueWaits,
            "/api/observe/websockets": this.client.listWebSockets,
            "/api/observe/logs": this.client.listLogs,
            "/api/observe/logs/histogram": this.client.getLogHistogram
        }[pathname]
        if (history) {
            await this.requestHistory(request, response, signal => history.call(this.client, url.searchParams, signal))
            return
        }
        if (pathname === "/api/observe/actors") {
            await this.actorInventory(request, response)
            return
        }
        if (pathname === "/api/observe/connection") {
            await this.connectionStatus(request, response)
            return
        }
        next()
    }

    private async proxyEvents(
        request: IncomingMessage,
        response: ServerResponse,
        openStream?: (signal: AbortSignal) => Promise<Response>
    ): Promise<void> {
        if (request.method === "HEAD") {
            response.writeHead(200, { "content-type": "text/event-stream" }).end()
            return
        }
        const controller = new AbortController()
        const disconnect = () => controller.abort()
        response.once("close", disconnect)
        let reader: ReadableStreamDefaultReader<Uint8Array> | undefined
        try {
            if (!openStream) throw new Error("Streaming is not supported")
            const upstream = await openStream(controller.signal)
            if (controller.signal.aborted) {
                await upstream.body?.cancel()
                return
            }
            reader = upstream.body!.getReader()
            const cancel = () => {
                void reader?.cancel().catch(() => {})
            }
            controller.signal.addEventListener("abort", cancel, { once: true })
            response.writeHead(200, { "content-type": "text/event-stream", "x-accel-buffering": "no" })
            response.flushHeaders()
            while (!controller.signal.aborted) {
                const { value, done } = await reader.read()
                if (done) break
                if (!response.write(value)) await once(response, "drain", { signal: controller.signal })
            }
            response.end()
        } catch {
            if (!response.destroyed) {
                if (response.headersSent) response.end("event: error\ndata: Inventory unavailable\n\n")
                else
                    response
                        .writeHead(503, { "content-type": "application/json" })
                        .end(JSON.stringify({ error: "Live inventory unavailable" }))
            }
        } finally {
            controller.abort()
            response.off("close", disconnect)
            await reader?.cancel().catch(() => {})
        }
    }

    private async requestHistory(
        request: IncomingMessage,
        response: ServerResponse,
        read: (signal: AbortSignal) => Promise<unknown>
    ): Promise<void> {
        const controller = new AbortController()
        const disconnect = () => controller.abort()
        response.once("close", disconnect)
        try {
            const result = await read(controller.signal)
            response
                .writeHead(200, { "content-type": "application/json" })
                .end(request.method === "HEAD" ? undefined : JSON.stringify(result))
        } catch {
            if (!response.destroyed)
                response
                    .writeHead(503, { "content-type": "application/json" })
                    .end(JSON.stringify({ error: "Request history unavailable" }))
        } finally {
            response.off("close", disconnect)
        }
    }

    private async actorInventory(request: IncomingMessage, response: ServerResponse): Promise<void> {
        let status = 200
        let result: unknown
        try {
            result = await this.client.listActors()
        } catch {
            status = 503
            result = { error: "Actor inventory unavailable" }
        }
        response.writeHead(status, { "content-type": "application/json" })
        response.end(request.method === "HEAD" ? undefined : JSON.stringify(result))
    }

    private async connectionStatus(request: IncomingMessage, response: ServerResponse): Promise<void> {
        let status = 200
        let result: unknown = { connected: true }
        try {
            await this.client.checkConnection()
        } catch {
            status = 503
            result = { error: "Control plane connection failed" }
        }
        response.writeHead(status, { "content-type": "application/json" })
        response.end(request.method === "HEAD" ? undefined : JSON.stringify(result))
    }
}

export { Observer, registerObserveCommand }
