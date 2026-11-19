import { spawn } from "node:child_process"
import path from "node:path"
import type { Readable } from "node:stream"

function runtimeEnvironment(executable: string): NodeJS.ProcessEnv {
    return {
        ...process.env,
        PATH: `${path.dirname(executable)}${path.delimiter}${process.env.PATH ?? ""}`,
        DURABLE_ACTORS_PROCESS_ROLE: process.env.DURABLE_ACTORS_PROCESS_ROLE ?? "control_plane",
        DURABLE_ACTORS_PARENT_LIFETIME_STDIN: "1"
    }
}

// This subprocess exists solely to run the native Rust control plane.
function startRustRuntime(
    command: string,
    args: string[],
    env: NodeJS.ProcessEnv,
    parentLifetime: boolean,
    readiness = false
) {
    const child = spawn(command, args, {
        env,
        stdio: [parentLifetime ? "pipe" : "inherit", "inherit", "inherit", ...(readiness ? (["pipe"] as const) : [])]
    })
    const exited = new Promise<number>((resolve, reject) => {
        const interrupt = () => child.kill("SIGINT")
        const terminate = () => child.kill("SIGTERM")
        const parentClosed = () => child.stdin?.end()
        process.on("SIGINT", interrupt)
        process.on("SIGTERM", terminate)
        if (parentLifetime && process.env.DURABLE_ACTORS_PARENT_LIFETIME_STDIN) {
            process.stdin.resume()
            process.stdin.on("end", parentClosed)
        }
        const cleanup = () => {
            process.off("SIGINT", interrupt)
            process.off("SIGTERM", terminate)
            process.stdin.off("end", parentClosed)
            if (parentLifetime) process.stdin.pause()
        }
        child.once("error", error => {
            cleanup()
            reject(error)
        })
        child.once("exit", (code, signal) => {
            cleanup()
            resolve(code ?? (signal === "SIGINT" ? 130 : 1))
        })
    })
    return { child, exited, readiness: readiness ? (child.stdio[3] as Readable) : undefined }
}

async function runtimeConnection(readiness: Readable, exited: Promise<number>): Promise<unknown> {
    const connection = readJson(readiness)
    return Promise.race([
        connection,
        // The exit event can arrive before the readiness pipe drains, so readiness written before exit still counts.
        exited.then(code =>
            connection.catch(() => {
                throw new Error(`Local actors exited before readiness (${code}).`)
            })
        )
    ])
}

async function readJson(stream: Readable): Promise<unknown> {
    const chunks: Buffer[] = []
    for await (const chunk of stream) chunks.push(Buffer.from(chunk))
    if (chunks.length === 0) throw new Error("Local actors exited without readiness information.")
    return JSON.parse(Buffer.concat(chunks).toString("utf8"))
}

export { runtimeConnection, runtimeEnvironment, startRustRuntime }
