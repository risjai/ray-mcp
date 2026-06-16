# ray-mcp
MCP server for managing Ray Clusters and Jobs. Written in Go for native k8s support.

> **Status:** early development. The walking skeleton runs today: a single
> read-only `ray_capabilities` tool over the stdio transport. Cluster/job/service
> tools are being added iteratively. Full docs land later (see `tasks/plan.md`).

## Quickstart (skeleton)

```sh
go build -o ray-mcp ./cmd/ray-mcp
./ray-mcp --default-namespace <ns>     # speaks MCP JSON-RPC over stdio
```

`ray_capabilities` reports the server version, bound kubeconfig context, default
namespace, enabled tool tiers, and the CI-tested KubeRay version — no live cluster
call. Write tools register only with `--allow-mutations` (and `--allow-destructive`
for the destructive tier). Under stdio, **stdout is the JSON-RPC wire**; all logs go
to stderr.
