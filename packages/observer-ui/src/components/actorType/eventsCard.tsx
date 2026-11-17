import { useState } from "react"

import { useEventHistory } from "../../hooks/useEventHistory.js"
import { killEvents } from "../../lib/instanceEvents.js"
import type { RecordedEvent } from "../../types/Events.js"
import type { TimeWindow } from "../../types/TimeRange.js"
import { EventList, EventRowSkeletons } from "../instance/eventList.js"
import { SectionCard } from "../ui/sectionCard.js"

const recorded: RecordedEvent["type"][] = ["kill"]

export type ActorTypeEventsCardProps = {
    actorName: string
    timeWindow: TimeWindow
}

// Residency changes are left to the Residency card: across every instance they bury the kills.
export function ActorTypeEventsCard({ actorName, timeWindow }: ActorTypeEventsCardProps) {
    const [expanded, setExpanded] = useState(false)
    const { history, isError } = useEventHistory(recorded, timeWindow, actorName)
    const kills = history?.events.map(event => event.item)
    const events = kills && killEvents(kills, timeWindow)
    return (
        <SectionCard title="Events" description="Sandbox kills across all instances." busy={!events && !isError} className="pb-3">
            {isError ? (
                <p className="py-4 text-[13px] text-muted-foreground">Couldn't reach the observer API.</p>
            ) : events ? (
                <EventList events={events} noun="event" showInstance expanded={expanded} onToggle={() => setExpanded(!expanded)} />
            ) : (
                <EventRowSkeletons showInstance />
            )}
        </SectionCard>
    )
}
