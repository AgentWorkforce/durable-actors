import { format } from "node:util"

import type { LogLevel } from "./protocol.js"

const LOG_LIMIT_BYTES = 256 * 1024

const levels = [
    ["debug", "debug"],
    ["log", "info"],
    ["info", "info"],
    ["warn", "warn"],
    ["error", "error"]
] as const satisfies readonly (readonly [keyof Console, LogLevel])[]

/** Gives console output to `write` instead of stdout, until the returned function puts the console back. */
function captureConsole(write: (level: LogLevel, message: string) => void): () => void {
    const original = levels.map(([method]) => [method, console[method]] as const)
    for (const [method, level] of levels)
        console[method] = (...args: unknown[]) => write(level, truncateLog(format(...args)))
    return () => {
        for (const [method, log] of original) console[method] = log
    }
}

function truncateLog(message: string): string {
    if (message.length * 3 <= LOG_LIMIT_BYTES) return message
    const bytes = Buffer.from(message)
    return bytes.length <= LOG_LIMIT_BYTES ? message : bytes.subarray(0, LOG_LIMIT_BYTES).toString()
}

export { captureConsole }
