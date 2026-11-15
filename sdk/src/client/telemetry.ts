import type { TelemetrySink } from "../client-runtime/telemetry.js"

export { LatencyTimeline } from "../client-runtime/telemetry.js"

const stderrTelemetry: TelemetrySink = event => {
    if (process.env.DURABLE_ACTORS_TELEMETRY === "1") process.stderr.write(`${JSON.stringify(event)}\n`)
}

export { stderrTelemetry }
