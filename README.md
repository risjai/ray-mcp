# ray-mcp
MCP server for managing Ray Clusters and Jobs. Written in Go for native k8s support.

> **Status:** early development. The walking skeleton runs today: a single
> read-only `ray_capabilities` tool over the stdio transport. Cluster/job/service
> tools are being added iteratively.

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

## Testing

A five-tier pyramid; the fast loop needs no Docker:

```sh
make test          # tiers 1/3/4 (unit, dashboard httptest, MCP) — fast, no Docker
make test-envtest  # tier 2: KubeRay adapter against envtest (apiserver+etcd, no Docker)
make e2e           # tier 5: real kind + KubeRay cluster (needs Docker + kind)
make pre-push      # all runnable tiers, before pushing a cluster-touching change
```

**Before adding a new tool or raising a PR**, read **[CONTRIBUTING.md](CONTRIBUTING.md)**
— it covers the test pyramid, which tier proves what, the "adding a tool" checklist,
and the project invariants (hexagonal imports, the stdio/stdout rule, the read-only
dashboard). The full rationale lives in
[`docs/specs/ray-mcp-testing-strategy.md`](docs/specs/ray-mcp-testing-strategy.md).

## Contributing

See **[CONTRIBUTING.md](CONTRIBUTING.md)** for setup, the testing workflow, and PR
guidelines.
