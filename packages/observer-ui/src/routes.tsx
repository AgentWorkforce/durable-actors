import { Navigate, Route, Routes } from "react-router"

import { TimeRangeProvider } from "./client/TimeRangeContext.js"
import { ActorTypePage } from "./components/actorType/actorTypePage.js"
import { InstancePage } from "./components/instance/instancePage.js"
import { Overview } from "./components/overview/overview.js"
import { pinnedLastHour } from "./lib/timeRange.js"

export function ObserverRoutes() {
    return (
        <Routes>
            <Route index element={<Overview />} />
            <Route path="actors/:actorName" element={<ActorTypePage />} />
            <Route
                path="actors/:actorName/instances/:actorId"
                element={
                    <TimeRangeProvider initialRange={pinnedLastHour}>
                        <InstancePage />
                    </TimeRangeProvider>
                }
            />
            <Route path="*" element={<Navigate to=".." replace />} />
        </Routes>
    )
}
