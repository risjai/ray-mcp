# ray-mcp

**An MCP server for [Ray](https://www.ray.io/) on Kubernetes.** Point Claude (or
any MCP client) at your cluster and ask about your Ray workloads in plain language.
Read-only today; a guarded write path is on the way (see below).

[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)
![Go](https://img.shields.io/badge/go-1.26.3-00ADD8)
![Status](https://img.shields.io/badge/status-v0.1.0%20preview-orange)

> **Status: v0.1.0 preview.** Today: `ray_capabilities`, the RayCluster read tools
> (`ray_cluster_list` / `ray_cluster_get` / `ray_cluster_events`), the full
> guarded RayCluster write path (`create` / `update` / `scale` / `delete`), and the
> read-only RayJob wedge (`ray_job_get` / `ray_job_logs` — CRD status fused with
> the live Ray dashboard/job API), over stdio. The remaining Ray job/service tools
> land next.

## Why ray-mcp

A generic Kubernetes MCP can `kubectl get` a RayCluster CRD and hand the agent raw
YAML. ray-mcp is built specifically for Ray, with the LLM as the consumer.

**What it does today (v0.1.0):**

- **Distilled status, not raw YAML.** `ray_cluster_get` returns a one-line health
  read — `"Ready; 1/1 workers ready"`, derived from the cluster's conditions — not a
  wall of `.status`. `ray_cluster_events` surfaces *why* a cluster is stuck,
  Warnings first — e.g. a pod's `FailedScheduling` event with message *"0/1 nodes
  available: insufficient nvidia.com/gpu"* — instead of leaving the agent to dig.
- **Read-only and token-bounded by default.** Safe to point at any cluster — no
  tool can mutate it. Output is compact and capped (small list rows, bounded event
  slices), because the consumer is an LLM with a finite context budget.
- **Guarded writes, opt-in by tier.** The full RayCluster write path —
  `create` / `update` / `scale` / `delete` — ships behind `--allow-mutations`
  (destructive ops additionally need `--allow-destructive`), via Server-Side Apply
  with dry-run, before/after diffs, and a stateless **confirm-fingerprint** two-step
  on destructive ops. The `ray-mcp/protected` annotation refuses deletion, and
  writes only ever go through the guarded CRD path — the unauthenticated Ray
  dashboard is never a write vector.

**On the roadmap (designed, not yet built):**

- **The wedge** — a read-only reach into Ray's dashboard/job API for live job
  status and logs, the runtime detail the CRDs don't hold (this is where the
  distillation above extends to jobs: *"why is my job pending?"*).
- **Ray job / service tools** — submit / get / logs / wait / list / delete for
  RayJob and RayService, layered on the guarded write path below.

## Install

```sh
go install github.com/risjai/ray-mcp/cmd/ray-mcp@latest   # or @v0.1.0 to pin
```

Requires **Go 1.26.3+**. This drops a `ray-mcp` binary in `$(go env GOPATH)/bin`
(or `$(go env GOBIN)`). No Go? Build from a clone instead — see
[docs/INSTALL.md](docs/INSTALL.md).

## Connect it to your agent

ray-mcp speaks MCP over stdio — your agent launches it as a subprocess. The binary
is a server; you don't run it directly (it'll just wait on stdin). Register it:

**Claude Code**
```sh
claude mcp add --scope user ray-mcp "$(go env GOPATH)/bin/ray-mcp" \
  -- --context <your-kube-context> --default-namespace <ns>
```

**Claude Desktop** — add to `claude_desktop_config.json`:
```json
{
  "mcpServers": {
    "ray-mcp": {
      "command": "/absolute/path/to/ray-mcp",
      "args": ["--context", "<your-kube-context>", "--default-namespace", "<ns>"]
    }
  }
}
```

Cursor and other MCP clients use the same shape (command + args). Full details,
flags, and a by-hand verification: [docs/INSTALL.md](docs/INSTALL.md).

## What you can do today

Four read-only tools (the three cluster tools carry `readOnlyHint`):

| Tool | Ask your agent… |
|------|-----------------|
| `ray_cluster_list` | "list the Ray clusters in namespace `team-a`" |
| `ray_cluster_get` | "is `ray-sample` healthy? how many workers are ready?" |
| `ray_cluster_events` | "why is `ray-sample` stuck? show recent events" |
| `ray_capabilities` | "what can this ray-mcp server do?" (needs no cluster) |

A real `ray_cluster_list` result against a live cluster looks like:

```json
{
  "clusters": [
    {"name": "ray-sample", "namespace": "default", "phase": "Ready",
     "ready": 1, "desired": 1, "ageSeconds": 122,
     "health": "Ready; 1/1 workers ready"}
  ],
  "count": 1, "moreAvailable": false
}
```

The server **boots even with no reachable cluster** — `ray_capabilities` always
works; the cluster tools dial lazily on first use and return a clean error (then
retry, no restart) if the kubeconfig can't reach a cluster.

## Try it end-to-end (no Ray experience needed)

Don't have a Ray cluster yet? **[docs/TRY-IT-WITH-CLAUDE-CODE.md](docs/TRY-IT-WITH-CLAUDE-CODE.md)**
takes you from zero to asking Claude Code about a live cluster: spin up a local
kind cluster, install KubeRay, create a sample Ray cluster, and connect ray-mcp.
Needs Docker + kubectl + Go + Claude Code; ~20–30 min; fully disposable.

## Learn more

- **[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)** — layers, ports, data flows, and
  diagrams (what's built vs. designed).
- **[docs/INSTALL.md](docs/INSTALL.md)** — install + every agent client + flag reference.
- **[docs/specs/ray-mcp-design.md](docs/specs/ray-mcp-design.md)** — the authoritative design.
- **[CONTRIBUTING.md](CONTRIBUTING.md)** — the five-tier test pyramid, the
  "adding a tool" checklist, and the project invariants. Read this before a PR.

## Roadmap

| Feature | Status |
|---------|--------|
| `ray_capabilities` — server info, bound context, tiers | ✅ Shipped (v0.1.0) |
| RayCluster read — `list` / `get` (distilled status) | ✅ Shipped (v0.1.0) |
| RayCluster events — `ray_cluster_events` (Warnings-first) | ✅ Shipped (v0.1.0) |
| RayCluster create — `ray_cluster_create` (unified apply pipeline, SSA, dry-run, diffs) | ✅ Shipped (`--allow-mutations`) |
| RayCluster writes — update / scale (SSA, dry-run, diffs, autoscaler-safe) | ✅ Shipped (`--allow-mutations`) |
| RayCluster delete — `ray_cluster_delete` (destructive tier, two-step confirm-fingerprint, `protected` guard) | ✅ Shipped (`--allow-destructive`) |
| The wedge — read-only Ray dashboard/job API reach (live status) | ✅ Shipped |
| RayJob read — `ray_job_get` / `ray_job_logs` (two-phase wedge: CRD + live dashboard) | ✅ Shipped |
| RayJob tools — submit / wait / list / delete | 📋 Planned |
| RayService tools — deploy / update / list / get / delete | 📋 Planned |
| Streamable HTTP transport + auth (static bearer / TokenReview) | 📋 Planned |
| Read-only RBAC floor — ServiceAccount + ClusterRole ([`deploy/rbac/`](deploy/rbac/)) | ✅ Shipped (read-only) |
| Helm chart + in-cluster Deployment | 📋 Planned |
| Prebuilt binaries + Homebrew (GoReleaser) | 📋 Planned |

✅ Shipped · 🚧 Next (Phase 2, in progress) · 📋 Planned

## Contributing & testing

PRs welcome. The fast test loop needs no Docker:

```sh
make test          # unit + dashboard + MCP tiers — fast, no Docker
make test-envtest  # KubeRay adapter against envtest (apiserver+etcd, no Docker)
make e2e           # real kind + KubeRay cluster (needs Docker + kind)
```

See **[CONTRIBUTING.md](CONTRIBUTING.md)** for the full workflow and what each tier
proves. Licensed under [Apache 2.0](LICENSE).
