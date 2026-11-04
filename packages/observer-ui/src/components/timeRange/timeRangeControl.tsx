import { Pause, Play } from "lucide-react"

import { useSetTimeRange, useTimeWindow } from "../../client/TimeRangeContext.js"
import { toggleLive } from "../../lib/timeRange.js"
import { Button } from "../ui/button.js"

import { TimeRangePicker } from "./timeRangePicker.js"

export function TimeRangeControl() {
    const timeWindow = useTimeWindow()
    const setRange = useSetTimeRange()
    return (
        <div className="flex items-center gap-2">
            <TimeRangePicker timeWindow={timeWindow} onChange={setRange} />
            <LiveButton live={timeWindow.live} onToggle={() => setRange(toggleLive(timeWindow))} />
        </div>
    )
}

function LiveButton({ live, onToggle }: { live: boolean; onToggle: () => void }) {
    const label = live ? "Pause live updates" : "Resume live updates"
    return (
        <Button variant="outline" size="icon-sm" aria-label={label} title={label} onClick={onToggle}>
            {live ? <Pause /> : <Play />}
        </Button>
    )
}
