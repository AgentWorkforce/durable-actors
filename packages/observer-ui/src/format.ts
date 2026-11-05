const count = new Intl.NumberFormat("en-US")
const decimal = new Intl.NumberFormat("en-US", { maximumFractionDigits: 1 })
const fine = new Intl.NumberFormat("en-US", { maximumSignificantDigits: 2 })
const percent = new Intl.NumberFormat("en-US", { style: "percent", minimumFractionDigits: 1, maximumFractionDigits: 1 })
const signedPercent = new Intl.NumberFormat("en-US", { style: "percent", signDisplay: "exceptZero", minimumFractionDigits: 1, maximumFractionDigits: 1 })

const clock = new Intl.DateTimeFormat("en-US", { hour: "2-digit", minute: "2-digit", hourCycle: "h23" })
const dateTime = new Intl.DateTimeFormat("en-US", { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit", hourCycle: "h23" })
const preciseClock = new Intl.DateTimeFormat("en-US", { hour: "2-digit", minute: "2-digit", second: "2-digit", hourCycle: "h23" })
const preciseDateTime = new Intl.DateTimeFormat("en-US", { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit", second: "2-digit", hourCycle: "h23" })

const timeSpan = (day: Intl.DateTimeFormat, time: Intl.DateTimeFormat) => (fromMs: number, toMs: number) =>
    `${day.format(fromMs)} – ${new Date(fromMs).toDateString() === new Date(toMs).toDateString() ? time.format(toMs) : day.format(toMs)}`

export const formatClock = (ms: number) => clock.format(ms)
export const formatPreciseClock = (ms: number) => preciseClock.format(ms)
export const formatPreciseDateTime = (ms: number) => preciseDateTime.format(ms)
export const formatTick = (ms: number, spanMs: number) => (spanMs < 86_400_000 ? clock : dateTime).format(ms)
export const formatTimeSpan = timeSpan(dateTime, clock)
export const formatPreciseTimeSpan = timeSpan(preciseDateTime, preciseClock)
export const formatCount = (value: number) => count.format(value)
export const formatDecimal = (value: number) => decimal.format(value)
export const formatPercent = (ratio: number) => percent.format(ratio)
export const formatSignedPercent = (ratio: number) => signedPercent.format(ratio)
export const formatMsValue = (ms: number) => (ms < 10 ? fine.format(ms) : count.format(Math.round(ms)))
export const formatMs = (ms: number) => `${formatMsValue(ms)}\u00a0ms`
export const formatMegabytes = (bytes: number) => decimal.format(bytes / 1_000_000)
export const formatPlural = (value: number, noun: string) => `${count.format(value)} ${value === 1 ? noun : `${noun}s`}`

export function formatDuration(ms: number): string {
    if (ms < 60_000) return `${Math.round(ms / 1000)}\u00a0s`
    const minutes = Math.round(ms / 60_000)
    if (minutes < 60) return `${minutes}\u00a0min`
    if (minutes < 2880) return `${decimal.format(minutes / 60)}\u00a0h`
    return `${decimal.format(minutes / 1440)}\u00a0d`
}

export function formatAgo(ms: number, now: number): string {
    const seconds = Math.max(0, Math.round((now - ms) / 1000))
    if (seconds < 60) return `${seconds}\u00a0s ago`
    if (seconds < 3600) return `${Math.floor(seconds / 60)}\u00a0min ago`
    if (seconds < 86_400) return `${Math.floor(seconds / 3600)}\u00a0h ago`
    return `${Math.floor(seconds / 86_400)}\u00a0d ago`
}
