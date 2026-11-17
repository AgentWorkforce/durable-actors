# Durable Actors Observer

React 19 components for embedding Durable Actors observability in a hosted app.

```tsx
import { HttpObserverClient, Overview, WebSocketObserver } from "durable-actors-observer"
import "durable-actors-observer/styles.css"
import "durable-actors-observer/theme.css"

const projectId = "my-project"
const client = new HttpObserverClient(`/api/projects/${encodeURIComponent(projectId)}/observe`)

export function Observability() {
    return (
        <>
            <Overview client={client} onSelectActor={actorName => console.log(actorName)} />
            <WebSocketObserver client={client} />
        </>
    )
}
```

Import `styles.css` once. The optional `theme.css` supplies light and dark theme tokens; omit it when the host supplies those tokens. Apply the `dark` class to an ancestor for dark mode.

The package exports `ActorObserver`, `RequestObserver`, `Overview`, `WebSocketObserver`, `ConsoleApp`, `RequestTimeline`, `SocketTimeline`, `TimeRangePicker`, and `FilterCombobox`, along with their named props types. It also exports the shared badge, button, calendar, command, drawer, input, popover, sheet, and table components from the package root. Use React's `ComponentProps<typeof Button>` (and equivalent) for primitive props.

Pass your own `ObserverClient` to connect these views to a hosted backend, or configure `HttpObserverClient` with your API prefix and fetch implementation. React remains a peer dependency. `ConsoleApp` provides the complete navigation shell and requires a `toggleTheme` callback; individual views can be placed inside your own navigation.

`Overview` shows a compact list of actor types below the runtime summary. Select a type to inspect its instances. The list has no metric columns, filters, or threshold legend.

Control-plane endpoints require a project in the path: `/v1/projects/{project_id}/observe/...`. The hosted backend should authorize access to that project and proxy the routes using its server-side admin credential. The CLI's `/api/observe` proxy uses its configured `DURABLE_ACTORS_PROJECT_ID`. Request traces include a required `projectId`; older stored traces without one are excluded from project results.

The observer version follows the SDK and runtime version. Run `bun run --bun release:prepare <version>` at the repository root before publishing a GitHub release. Release preflight checks all versions, and the workflow publishes the observer before the SDK. The SDK's `workspace:*` dependency becomes the exact observer version when packed.

The localhost-only `durable-actors dev` runtime allows observability requests without a secret, even when a secret is configured for application routes. Hosted runtimes retain their configured admin authentication.

## Invocation waterfall

`RequestObserver` opens with a waterfall of retained method calls and WebSocket events, including inside an actor instance. Instance inspection combines identity, residency, and queue metrics in a compact header; Requests, State, and WebSockets views preserve inspection controls when switching. Calls to the same operation on the same actor instance share one row, with separate rows for method calls and WebSocket events. Each invocation has its own marker on a shared relative time axis, making interleaved methods visible together. Rows show call counts and summed durations; hover or select a marker to inspect identifiers, outcomes, queue wait, and the gap or overlap with preceding calls on that instance. Switch to Table for the tabular view. History opens by default and automatically loads every retained event matching the selected time range and filters, across all pages, in both views. The loaded count shows progress; interrupted loading keeps the fetched events visible and offers a retry for the remaining pages. Live mode shows the latest 500 events and supports pause; use History for a complete time range.

Use the Zoom slider to adjust the visible window smoothly from the full loaded history down to 1 ms, hold Ctrl or Command while scrolling over the waterfall to zoom around the cursor, or drag across the time axis to select a range. Scroll zoom keeps the slider synchronized and captures browser zoom only over the waterfall while the modifier key is held. Once zoomed, use the horizontal scrollbar beneath the requests, a horizontal trackpad gesture, or Shift+mouse-wheel to move through time. Operation labels stay in place. The slider supports arrow keys, Home, and End. Moving the slider fully left restores the full loaded history. The chosen window stays on the same timestamps as live calls arrive or older history loads. Only calls intersecting the window are shown; durations and queue timings still describe the full invocation.

Method rows stay in alphabetical order by actor type, instance, transport, and method. The inventory supplies published RPC methods, and the waterfall includes WebSocket callbacks even with zero calls. An inspected instance always shows all its methods. The global Requests view includes method rows only for instances represented in the loaded live or saved requests, so idle inventory does not add rows. Zooming and panning leave those rows in place. Custom clients can include optional `methods` names on each inventory actor; direct `RequestTimeline` embeds can supply `lanes` for methods without records. Without a deployment contract, rows cover observed operations.

Request details open in a right-side [shadcn Drawer](https://ui.shadcn.com/docs/components/radix/drawer), powered by Vaul. Dismiss with the close button, Escape, an outside click, or a swipe to the right; focus returns to the selected invocation. The portal stays within the observer's theme scope, and long details scroll below the header.

Durations include queue wait, actor processing, and persistence. Gaps are calculated only from the calls loaded in the current view; filtered or unretained calls are not included. The waterfall uses request traces, so internal helper calls within one invocation are not separate spans. Embed `RequestTimeline` directly with `records` and an `onSelect(record, trigger)` callback when providing your own request controls.

## Actor instances and residency

Actor pages list every known instance, ordered by its latest request activity, including dormant instances. The Last active column shows a relative time with the exact timestamp on hover; unavailable timestamps sort last. Activity timestamps include request admission and completion, and do not advance on lifecycle heartbeats.

Residency and WebSocket timelines share the same axis, row labels, bars, and time scale. The residency timeline shows one lane per instance in the same order as the list. Green segments mean in memory, purple means dormant, and hatching means unknown. Search and state filters apply to both views. Hover or focus a segment for its timestamps and duration; select a segment or instance to inspect it. The shared time range controls the timeline, while the inventory continues to list all known instances.

Residency uses lifecycle reports, including immediate notifications when the executor loads or evicts an instance. Missing reports become unknown after 35 seconds; stopped hosts remain dormant until a later activation. Time before retained evidence is blank. ClickHouse history follows analytics retention; the local and Postgres analytics runtimes retain up to 100,000 lifecycle reports for 24 hours in memory, and their inventory lasts for the runtime process (up to 10,000 instances). Timeline queries return the latest 10,000 reports and show a notice when truncated. Older deployments without activity timestamps display Unknown until the next request.

Custom observer clients can implement `listResidency` to enable the timeline. `HttpObserverClient` and the TypeScript and Python observer proxies use the authenticated project endpoint `/v1/projects/{project_id}/observe/residency?actorName=…`. `ResidencyTimeline` and its props and data types are also exported for embedding.

## Events

An actor's page and each instance's page list its sandbox kills, newest first: sandboxes that stopped without the platform asking, which needs Kubernetes sandboxes. They are read from `/v1/projects/{project_id}/observe/events/history?types=kill`, which the TypeScript and Python observer proxies serve as `events/history`, and arrive live as `kill` events.

## Persisted actor state

Select an actor instance in `ActorObserver` to inspect persisted fields, JSON types, nested values, and version diffs. Request and connection links filter the existing request history. The standard `HttpObserverClient` and `durable-actors observe` include this capability; custom clients implement `getState` and `listStateHistory` to enable it.

Values come from the actor's existing snapshot objects in the configured storage bucket, including dormant actors. Analytics stores only a state-version signal. Request events trigger a refresh; polling recovers from missed events and asynchronous bucket uploads. History uses rebuildable metadata-only sidecars beside snapshots and retains no duplicate state bodies. Available versions follow the bucket's snapshot retention/lifecycle policy.

Attribution identifies the committing request. A reentrant commit may include mutations from other interleaved requests; the UI labels this explicitly. Declared field types describe the current deployment schema, which may differ from older stored values. Inspection is read-only and uses the existing authenticated project observability API.
