# Installing ray-mcp into an AI agent

This guide walks through building `ray-mcp` and connecting it to an MCP-capable
AI agent (Claude Desktop, Claude Code, Cursor, or any MCP client). It also shows
how to verify the connection by hand.

> **Status:** early development. Today the server exposes three read-only tools
> over the **stdio** transport: `ray_capabilities` (server/binding info, no cluster
> call), and `ray_cluster_list` / `ray_cluster_get` (read your RayClusters). Job and
> service tools, and write/destructive tiers, are being added iteratively. HTTP
> transport arrives later. The install steps below won't change as tools are added —
> you point your agent at the same binary.

## 1. Build the binary

```sh
git clone https://github.com/risjai/ray-mcp.git
cd ray-mcp
go build -o ray-mcp ./cmd/ray-mcp     # produces ./ray-mcp
```

Requires Go 1.26.3 (see `go.mod`). Note the **absolute path** to the binary —
your agent config needs it:

```sh
echo "$(pwd)/ray-mcp"
```

## 2. Flags you'll likely set

| Flag | Default | What it does |
|------|---------|--------------|
| `--default-namespace <ns>` | in-cluster ns, else `default` | namespace used when a tool omits one |
| `--context <name>` | current kubeconfig context | which kubeconfig context to bind |
| `--kubeconfig <path>` | `$KUBECONFIG` / discovery | credentials source |
| `--allow-mutations` | off | register write tools (create/update/scale) |
| `--allow-destructive` | off | register destructive tools (delete) — also needs `--allow-mutations` |
| `--log-level <level>` | `info` | `debug`/`info`/`warn`/`error` (logs go to **stderr**) |

Read tools are always on. By default the server is **read-only** — you must opt
into mutations explicitly. Full flag reference: design spec §9.

> The server **always boots**, even with no reachable cluster — so your agent
> always connects and `ray_capabilities` always works. The cluster connection is
> made **lazily**, on the first `ray_cluster_*` call; if the kubeconfig can't reach
> a cluster, those tools return a clean error (and retry once it's fixed — no
> restart needed). Point `--context`/`--kubeconfig` at a context where
> `kubectl --context <name> get rayclusters` works.

## 3. Connect your agent

MCP clients launch the server as a subprocess and speak JSON-RPC over its
stdin/stdout. You give the client a command + args.

### Claude Desktop

Edit `claude_desktop_config.json` (macOS:
`~/Library/Application Support/Claude/claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "ray-mcp": {
      "command": "/absolute/path/to/ray-mcp",
      "args": ["--default-namespace", "default"]
    }
  }
}
```

Restart Claude Desktop. `ray-mcp` should appear in the tools (🔌) menu, exposing
`ray_capabilities`.

### Claude Code

```sh
claude mcp add ray-mcp /absolute/path/to/ray-mcp --default-namespace default
```

Then in a session, ask the agent to "call ray_capabilities" — or run
`/mcp` to see it listed.

### Cursor / other MCP clients

Use the same shape — a stdio server with `command` = the binary path and `args`
= your flags. Consult your client's MCP docs for the config file location.

## 4. Verify by hand (no agent needed)

You can drive the server directly to confirm it works before wiring an agent.
Pipe JSON-RPC frames into it over stdio:

```sh
{ printf '%s\n' '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"cli","version":"0"}}}';
  sleep 0.4;
  printf '%s\n' '{"jsonrpc":"2.0","method":"notifications/initialized"}';
  printf '%s\n' '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"ray_capabilities","arguments":{}}}';
  sleep 0.4;
} | ./ray-mcp --default-namespace demo --allow-mutations 2>/dev/null
```

The last line is the tool result — structured + a human summary:

```json
{"jsonrpc":"2.0","id":2,"result":{"content":[{"type":"text","text":"ray-mcp 0.0.0-dev | context (current-context) | default-namespace demo | tiers: read,write | KubeRay tested v1.6.1"}],"structuredContent":{"defaultNamespace":"demo","enabledTiers":["read","write"],"kubeContext":"(current-context)","kubeRayTested":"v1.6.1","serverVersion":"0.0.0-dev"}}}
```

Note `enabledTiers` became `["read","write"]` because we passed
`--allow-mutations`; without it you'd see `["read"]`.

## Troubleshooting

- **Agent shows no tools / "server failed to start"** — run the binary yourself
  with the same flags; errors print to **stderr**. A bad `--http-addr` bind or an
  invalid flag value makes the server refuse to boot with an explanatory message.
- **Garbled output / protocol errors** — under stdio, **stdout is the JSON-RPC
  wire**. Don't redirect anything else to the server's stdout. All logs already go
  to stderr by design.
- **Can't reach the cluster** — that affects cluster tools (as they land), not
  `ray_capabilities`, which is config-only and makes no live call. Verify
  `kubectl --context <name> get rayclusters` works.

## What's next

As cluster/job/service tools land, they appear automatically — rebuild the binary
and your agent picks them up on restart. To follow or contribute, see
[CONTRIBUTING.md](../CONTRIBUTING.md) and `tasks/plan.md` (in the source tree).
