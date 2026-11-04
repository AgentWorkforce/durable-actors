import { HashRouter } from "react-router"

import { DataProvider } from "./client/DatatContext.js"
import { TimeRangeProvider } from "./client/TimeRangeContext.js"
import { ObserverRoutes } from "./routes.js"

export function App() {
    return (
        <main className="min-h-screen px-8 pt-7 pb-8 font-satoshi antialiased">
            <TimeRangeProvider>
                <DataProvider>
                    <HashRouter>
                        <ObserverRoutes />
                    </HashRouter>
                </DataProvider>
            </TimeRangeProvider>
        </main>
    )
}
export default App
