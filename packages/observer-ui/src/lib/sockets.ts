import type { SocketSession } from "../types/Sockets.js"
import type { TimeSpan } from "../types/TimeRange.js"

// Sessions with no disconnect that the inventory no longer reports are treated as closed at their last activity.
export function sessionSpan(session: SocketSession, open: boolean): TimeSpan | null {
    const fromMs = session.openedAtMs ?? session.lastSeenMs
    if (fromMs === null) return null
    return { fromMs, toMs: session.closedAtMs ?? (open ? Infinity : (session.lastSeenMs ?? fromMs)) }
}
