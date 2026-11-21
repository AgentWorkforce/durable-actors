# MCP

The control plane serves an [MCP](https://modelcontextprotocol.io) endpoint so coding agents can inspect and call actors in your organization. It exposes the [HTTP API](openapi.md) as tools and uses the same credentials.

```
<control-plane-url>/mcp
```

With `durable-actors dev`, the endpoint is at `http://127.0.0.1:7100/mcp` and needs no credential. When `DURABLE_ACTORS_SECRET` is set on the server, agents send it as a bearer token.

## Claude Code

```sh
claude mcp add --transport http durable-actors http://127.0.0.1:7100/mcp
```

With a secret:

```sh
claude mcp add --transport http durable-actors https://actors.example.com/mcp \
  --header "Authorization: Bearer $DURABLE_ACTORS_SECRET"
```

## Codex

```sh
codex mcp add durable-actors --url http://127.0.0.1:7100/mcp
```

With a secret, which Codex reads from the environment when it starts:

```sh
codex mcp add durable-actors --url https://actors.example.com/mcp \
  --bearer-token-env-var DURABLE_ACTORS_SECRET
```

## Cursor

Add the server to `~/.cursor/mcp.json`, or to `.cursor/mcp.json` in one repository:

```json
{
  "mcpServers": {
    "durable-actors": { "url": "http://127.0.0.1:7100/mcp" }
  }
}
```

With a secret, which Cursor reads from the environment it starts in:

```json
{
  "mcpServers": {
    "durable-actors": {
      "url": "https://actors.example.com/mcp",
      "headers": { "Authorization": "Bearer ${env:DURABLE_ACTORS_SECRET}" }
    }
  }
}
```

For a control plane serving multiple organizations, an authenticated proxy sends `x-durable-actors-organization-id` with the server credential. A single-organization control plane selects its organization automatically.

## Tools

| Tool | Description |
| --- | --- |
| `list_projects` | Deployed projects in your organization |
| `describe_deployment` | Actor classes, their methods, and TypeScript declarations for every type |
| `list_actors` | Actor classes, then a page of instances with their status and open connections |
| `get_actor_state` | An instance's saved state, latest or by version, whole or at a JSON Pointer |
| `list_state_history` | An instance's saved state versions |
| `list_requests` | Request history with outcomes, timing, and errors |
| `list_logs` | Console output from actors |
| `get_health` | Request counts, success rate, p95 latency, and the longest queue waits |
| `get_actor_resources` | An instance's CPU, memory, and SQLite database size over time |
| `list_websocket_sessions` | WebSocket sessions, each open, closed, or lost |
| `invoke_actor` | Call an actor method. Runs real code and can change state. |
| `send_feedback` | Send the user's feedback to the Terse team, published on the [events topic](configuration.md#published-events) |

Pass a `projectId` from `list_projects` to the other tools. List tools return pages: pass `nextCursor` back as `cursor` for the next one.
