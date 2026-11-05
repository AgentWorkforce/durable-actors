import { validateProjectId } from "../actor/identity.js"
import { authorizationHeaders } from "../client/clientSettings.js"

import { connection } from "./connection.js"

interface ControlPlaneConnection {
    projectId: string
    controlPlaneUrl: string
    credential: string | undefined
}

class ControlPlaneClient {
    constructor(
        readonly connection: ControlPlaneConnection,
        private readonly request: typeof fetch,
        private readonly projectsPath = "/v1/projects"
    ) {}

    async checkConnection(): Promise<void> {
        await this.requestJson("GET", `${this.projectPath()}/observe/actors`, undefined, 10_000)
    }

    registerDeployment(deployment: unknown): Promise<unknown> {
        return this.requestJson("PUT", `${this.projectPath()}/deployment`, deployment, 150_000)
    }

    getContract(): Promise<unknown> {
        return this.requestJson("GET", `${this.projectPath()}/deployment/contract`)
    }

    listActors(): Promise<unknown> {
        return this.requestJson("GET", `${this.projectPath()}/observe/actors`)
    }

    listResidency(query: URLSearchParams, signal?: AbortSignal): Promise<unknown> {
        return this.requestJson("GET", `${this.projectPath()}/observe/residency?${query}`, undefined, 30_000, signal)
    }

    async openActorStream(signal: AbortSignal): Promise<Response> {
        return this.openStream(`${this.projectPath()}/observe/events`, signal)
    }

    async openRequestStream(signal: AbortSignal, after?: string): Promise<Response> {
        const query = after ? `?${new URLSearchParams({ after })}` : ""
        return this.openStream(`${this.projectPath()}/observe/requests/events${query}`, signal)
    }

    getState(query: URLSearchParams, signal?: AbortSignal): Promise<unknown> {
        return this.requestJson("GET", `${this.projectPath()}/observe/state?${query}`, undefined, 30_000, signal)
    }

    listStateHistory(query: URLSearchParams, signal?: AbortSignal): Promise<unknown> {
        return this.requestJson(
            "GET",
            `${this.projectPath()}/observe/state/history?${query}`,
            undefined,
            30_000,
            signal
        )
    }

    listRequests(query: URLSearchParams, signal?: AbortSignal): Promise<unknown> {
        return this.requestJson(
            "GET",
            `${this.projectPath()}/observe/requests${query.size ? `?${query}` : ""}`,
            undefined,
            30_000,
            signal
        )
    }

    getMetrics(query: URLSearchParams, signal?: AbortSignal): Promise<unknown> {
        return this.requestJson(
            "GET",
            `${this.projectPath()}/observe/metrics${query.size ? `?${query}` : ""}`,
            undefined,
            30_000,
            signal
        )
    }

    listQueueWaits(query: URLSearchParams, signal?: AbortSignal): Promise<unknown> {
        return this.requestJson(
            "GET",
            `${this.projectPath()}/observe/queue-waits${query.size ? `?${query}` : ""}`,
            undefined,
            30_000,
            signal
        )
    }

    listLogs(query: URLSearchParams, signal?: AbortSignal): Promise<unknown> {
        return this.requestJson(
            "GET",
            `${this.projectPath()}/observe/logs${query.size ? `?${query}` : ""}`,
            undefined,
            30_000,
            signal
        )
    }

    getLogHistogram(query: URLSearchParams, signal?: AbortSignal): Promise<unknown> {
        return this.requestJson(
            "GET",
            `${this.projectPath()}/observe/logs/histogram?${query}`,
            undefined,
            30_000,
            signal
        )
    }

    async openLogStream(signal: AbortSignal, query: URLSearchParams): Promise<Response> {
        return this.openStream(`${this.projectPath()}/observe/logs/events${query.size ? `?${query}` : ""}`, signal)
    }

    listWebSockets(query: URLSearchParams, signal?: AbortSignal): Promise<unknown> {
        return this.requestJson(
            "GET",
            `${this.projectPath()}/observe/websockets${query.size ? `?${query}` : ""}`,
            undefined,
            30_000,
            signal
        )
    }

    private async openStream(path: string, signal: AbortSignal): Promise<Response> {
        const response = await this.request(`${this.connection.controlPlaneUrl}${path}`, {
            signal,
            redirect: "error",
            headers: { ...authorizationHeaders(this.connection.credential), accept: "text/event-stream" }
        })
        if (!response.ok || !response.headers.get("content-type")?.startsWith("text/event-stream") || !response.body) {
            await response.body?.cancel()
            throw new Error("Live inventory is unavailable")
        }
        return response
    }

    private projectPath(): string {
        return `${this.projectsPath}/${encodeURIComponent(validateProjectId(this.connection.projectId))}`
    }

    private async requestJson(
        method: "GET" | "PUT",
        pathname: string,
        body?: unknown,
        timeoutMs = 30_000,
        signal?: AbortSignal
    ): Promise<unknown> {
        const { controlPlaneUrl, credential } = this.connection
        const response = await this.request(`${controlPlaneUrl}${pathname}`, {
            method,
            headers: {
                ...authorizationHeaders(credential),
                ...(body === undefined ? {} : { "content-type": "application/json" })
            },
            body: body === undefined ? undefined : JSON.stringify(body),
            signal: signal ? AbortSignal.any([signal, AbortSignal.timeout(timeoutMs)]) : AbortSignal.timeout(timeoutMs),
            redirect: "error"
        }).catch(() => {
            throw new Error(
                `Cannot complete ${method} ${pathname} at ${controlPlaneUrl}. Check the URL and runtime.${method === "GET" ? "" : " The request may have reached the server."}`
            )
        })
        return readResponse(response, method, `${controlPlaneUrl}${pathname}`)
    }
}

function createControlPlaneClient(env: NodeJS.ProcessEnv, request: typeof fetch): ControlPlaneClient {
    return new ControlPlaneClient(connection(env), request)
}

async function readResponse(response: Response, method: string, url: string): Promise<unknown> {
    const result: unknown = await response.json().catch(() => {
        if (response.ok) throw new Error(`Control plane returned invalid JSON (HTTP ${response.status}).`)
        return undefined
    })
    if (!response.ok) {
        const hint =
            response.status === 404 && url.endsWith("/deployment/contract")
                ? "\nCheck the control-plane URL and DURABLE_ACTORS_PROJECT_ID. For local development, run durable-actors dev in the actor project and wait for Ready."
                : ""
        throw new Error(
            `Control-plane request failed (HTTP ${response.status}): ${errorMessage(result) ?? response.statusText}\n${method} ${url}${hint}`
        )
    }
    return result
}

function errorMessage(result: unknown): string | undefined {
    if (!result || typeof result !== "object" || !("error" in result)) return undefined
    const error = result.error
    if (!error || typeof error !== "object" || !("message" in error)) return undefined
    return typeof error.message === "string" ? error.message : undefined
}

export { ControlPlaneClient, createControlPlaneClient }
export type { ControlPlaneConnection }
