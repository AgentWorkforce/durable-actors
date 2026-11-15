import { pathToFileURL } from "node:url"

import { loadActorEntrypoint } from "./actor-entrypoint.js"

await loadActorEntrypoint(pathToFileURL(process.argv[2]!).href)
// The module may have left timers or servers running, which would keep this process alive.
process.exit(0)
