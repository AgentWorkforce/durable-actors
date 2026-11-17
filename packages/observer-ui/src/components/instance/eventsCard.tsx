import { useState } from "react"

import { useEventHistory } from "../../hooks/useEventHistory.js"
import { killEvents } from "../../lib/instanceEvents.js"
import type { RecordedEvent } from "../../types/Events.js"
import type { TimeWindow } from "../../types/TimeRange.js"
import { SectionCard } from "../ui/sectionCard.js"

import { EventList, EventRowSkeletons } from "./eventList.js"

const recorded: RecordedEvent["type"][] = ["kill"]

export type EventsCardProps = {
    actorName: string
    actorId: string
    timeWindow: TimeWindow
}

// Loading and unloading are routine; listed here they would bury the kills.
export function EventsCard({ actorName, actorId, timeWindow }: EventsCardProps) {
    const [expanded, setExpanded] = useState(false)
    const { history, isError } = useEventHistory(recorded, timeWindow, actorName, actorId)
    const kills = history?.events.map(event => event.item)
    const events = kills && killEvents(kills, timeWindow)
    return (
        <SectionCard title="Events" description="Sandbox kills." busy={!events && !isError} className="pb-3">
            {isError ? (
                <p className="py-4 text-[13px] text-muted-foreground">Couldn't reach the observer API.</p>
            ) : events ? (
                <EventList events={events} noun="event" expanded={expanded} onToggle={() => setExpanded(!expanded)} />
            ) : (
                <EventRowSkeletons />
            )}
        </SectionCard>
    )
}
