# ray-mcp — Design Specification

**Date:** 2026-06-13
**Status:** Draft for review
**Repo:** `github.com/risjai/ray-mcp`

## 1. Summary

`ray-mcp` is a Model Context Protocol (MCP) server, written in Go, that lets an
AI agent manage Ray workloads running on Kubernetes via the
[KubeRay](https://github.com/ray-project/kuberay) operator. It exposes tools to
manage the full lifecycle of `RayCluster`, `RayJob`, and `RayService` resources,
plus runtime introspection (live job status, logs) sourced from Ray's own
dashboard API.

It is published as open-source artifacts (binary, Docker image, Helm chart).
The deployment model is **self-hosted, per-user**: each user runs their own
instance next to their own cluster, using their own Kubernetes credentials.
"Published" means the artifacts are public — not that we operate a shared
endpoint.

## 2. Goals & Non-Goals

### Goals
- Let an agent perform full lifecycle operations (read + write) on RayCluster,
  RayJob, and RayService through a small, well-typed set of MCP tools.
- Be Kubernetes-native: manage Ray via KubeRay CRDs using typed Go clients.
- Surface runtime detail KubeRay does not (live job status, logs) via Ray's
  dashboard/job API.
- Be safe by default for a tool that any autonomous agent might drive.
- Be trivial to "hook up": run locally against a kubeconfig (stdio) or deploy
  in-cluster as an authenticated HTTP service.

### Non-Goals (v1)
- No hosted multi-tenant service; no storage of other users' cluster credentials.
- No multi-cluster fan-out (one server instance binds to one Kubernetes context).
- No management of the KubeRay operator's own installation/lifecycle.
- No true real-time log streaming (v1 returns a bounded tail).
- No provisioning of the underlying Kubernetes cluster or node pools.

## 3. Key Decisions

| # | Decision | Choice |
|---|----------|--------|
| 1 | Deployment model | Self-hosted, per-user (binary/image/chart published) |
| 2 | Transports | Both stdio and streamable HTTP, selected by flag |
| 3 | Control plane | KubeRay CRDs primary; Ray dashboard/job API secondary |
| 4 | Operation surface | Full lifecycle read+write, guarded (safe by default) |
| 5 | Ray API reach | Auto port-forward via the k8s API (SPDY), on demand |
| 6 | Scope | Single cluster (one context), multi-namespace |
| 7 | HTTP auth | Bearer token; bind localhost by default; refuse no-token unless `--insecure` |
| 8 | K8s client | KubeRay typed client + sigs.k8s.io/controller-runtime |
| 9 | MCP SDK | Official `github.com/modelcontextprotocol/go-sdk` |
| 10 | Spec input | Curated typed params + `rawSpec` deep-merge escape hatch |
| 11 | Safety model | Layered: tier flags + dryRun + protected annotation + diffs |
| 12 | KubeRay version | Pin to KubeRay v1 API; declare supported range; report detected version |
| 13 | Logs | Bounded tail (last-N-lines / since-duration), not streaming |

## 4. Architecture

Layered hexagonal (ports & adapters). Data flows top→down; only adapters touch
the outside world. The domain layer imports no Kubernetes or HTTP packages —
it depends on Go interfaces — which makes it unit-testable with fakes.

```
┌──────────────────────────────────────────────────────────────┐
│ Transport (edge)         stdio  │  streamable HTTP (+ bearer)  │
├──────────────────────────────────────────────────────────────┤
│ MCP layer  (modelcontextprotocol/go-sdk)                       │
│   • tool registration + JSON schemas                           │
│   • arg decode/validate ⇄ domain DTOs                          │
│   • result/error formatting (text + structured content)        │
├──────────────────────────────────────────────────────────────┤
│ Domain / service layer   (no k8s/http imports)                 │
│   • ClusterService · JobService · ServiceService               │
│   • safety guards: mutation gate, destructive gate, dryRun,    │
│     protected-annotation, before/after diff                    │
│   • spec building: curated params → CRD spec (+ rawSpec merge) │
│   • orchestration (e.g. submit job → poll status via Ray API)  │
├──────────────────────────────────────────────────────────────┤
│ Adapters (ports)                                               │
│   ├─ KubeRayClient   (controller-runtime, typed CRDs)          │
│   ├─ RayAPIClient    (dashboard/job REST over a tunnel)        │
│   └─ PortForwarder   (k8s SPDY portforward to head svc:8265)   │
├──────────────────────────────────────────────────────────────┤
│ Kubernetes API server  +  Ray head dashboard (in-cluster)      │
└──────────────────────────────────────────────────────────────┘
```

### Boundaries
- **Domain depends on interfaces**, `KubeRayPort` and `RayAPIPort`, not on
  concrete clients. Fakes drive the bulk of the tests.
- **Transport and SDK are edges.** Switching stdio↔HTTP or swapping the MCP SDK
  never touches domain or adapters.
- **One instance = one Kubernetes context**, bound at startup; multi-namespace
  within that context.

### Package layout
```
cmd/ray-mcp/main.go              # flag parsing, wiring, transport selection
internal/config/                 # config struct, flag/env loading, validation
internal/mcp/                    # tool registration, schemas, arg↔DTO mapping
internal/domain/                 # services, guards, spec-building (pure-ish)
  cluster.go  job.go  service.go  guards.go  spec.go  diff.go  types.go
internal/adapters/kuberay/       # controller-runtime client impl
internal/adapters/rayapi/        # dashboard/job REST client
internal/adapters/portforward/   # SPDY tunnel manager
internal/observability/          # structured logging, optional metrics
```

## 5. Tool Surface

Tools are namespaced `ray_*`. Read tools are always registered. Write tools
register only when `--allow-mutations` is set; destructive tools additionally
require `--allow-destructive`. Disabled tools are not advertised to the agent.
Every tool takes an optional `namespace` arg, defaulting to the configured
default namespace.

### RayCluster
| Tool | Tier | Purpose |
|------|------|---------|
| `ray_cluster_list` | read | List RayClusters (+ ready/replicas/endpoints) |
| `ray_cluster_get` | read | Full status, conditions, head/worker detail |
| `ray_cluster_events` | read | Recent k8s events for the cluster's pods |
| `ray_cluster_create` | write | Create from curated params (+ `rawSpec`), `dryRun` |
| `ray_cluster_update` | write | Patch image/resources/replicas/autoscaling, `dryRun`, diff |
| `ray_cluster_scale` | write | Scale a worker group min/max/replicas, `dryRun`, diff |
| `ray_cluster_delete` | destructive | Delete (honors `protected`), `dryRun` |

### RayJob
| Tool | Tier | Purpose |
|------|------|---------|
| `ray_job_list` | read | List RayJobs (+ deployment/job status) |
| `ray_job_get` | read | Status, start/end times, message, dashboard job id |
| `ray_job_logs` | read | Bounded tail of job logs via Ray Job API (over tunnel) |
| `ray_job_submit` | write | entrypoint + runtimeEnv + cluster selector/spec, `dryRun` |
| `ray_job_stop` | write | Stop a running job |
| `ray_job_delete` | destructive | Delete the RayJob resource |

### RayService
| Tool | Tier | Purpose |
|------|------|---------|
| `ray_service_list` | read | List RayServices (+ serve status, healthy replicas) |
| `ray_service_get` | read | Serve app status, route prefix, conditions |
| `ray_service_deploy` | write | Create from serveConfig + cluster params (+ `rawSpec`), `dryRun` |
| `ray_service_update` | write | Update serveConfig (zero-downtime reconfig), `dryRun`, diff |
| `ray_service_delete` | destructive | Delete (honors `protected`) |

### Meta
| Tool | Tier | Purpose |
|------|------|---------|
| `ray_capabilities` | read | Server version, bound context, default ns, detected KubeRay version, enabled tiers |

### Curated create params (shared shape)
`name`, `namespace`, `rayVersion`, `image`,
`headResources{cpu,memory,gpu}`,
`workerGroups[]{name,replicas,min,max,resources}`,
`enableAutoscaling`, `labels`, `annotations`,
plus `rawSpec` (YAML/JSON, deep-merged last for full power).

## 6. Data Flow

### A) `ray_cluster_create` (write, CRD path)
1. MCP layer decodes args → `CreateClusterRequest` DTO; schema validation rejects
   bad input early.
2. `ClusterService.Create`: check mutation gate → build typed `RayCluster` from
   curated params → deep-merge `rawSpec` → validate.
3. If `dryRun`: server-side dry-run apply; return rendered spec + "would create";
   no mutation.
4. Else: `KubeRayClient.Create(ctx, obj)`; on success return name, namespace,
   initial status, applied spec summary.

### B) `ray_job_logs` (read, Ray API path)
1. `JobService.Logs`: resolve RayJob → its RayCluster → head service via KubeRay
   client.
2. `PortForwarder.Open(headSvc, 8265)` → ephemeral local port (ref-counted,
   shared by concurrent calls).
3. `RayAPIClient.JobLogs(jobID)` over the tunnel → bounded tail → return text.
4. `defer` tunnel close (ref-counted; last user closes it).

## 7. Safety Model

Layered defense-in-depth; never relies on the agent behaving well.

- **Tier gating at registration.** read always on; write needs `--allow-mutations`;
  destructive needs `--allow-destructive`. Disabled tools are not advertised.
- **`dryRun` arg** on every mutating tool → server-side dry-run / diff only,
  no mutation.
- **Protected annotation.** `ray-mcp/protected="true"` on a resource makes
  delete and destructive scale-down refuse with a clear message, regardless of
  flags.
- **Before/after diff** returned by every successful mutation (structured +
  human-readable).
- **RBAC is the floor.** App guards layer on top of whatever the kubeconfig/SA
  permits. We ship a least-privilege Role.

## 8. Configuration

Precedence: flags > environment > defaults.

| Flag | Env | Default | Purpose |
|------|-----|---------|---------|
| `--transport` | `RAY_MCP_TRANSPORT` | `stdio` | `stdio` or `http` |
| `--http-addr` | `RAY_MCP_HTTP_ADDR` | `127.0.0.1:8765` | HTTP listen address |
| `--auth-token` | `RAY_MCP_AUTH_TOKEN` | (none) | Bearer token for HTTP; required unless `--insecure` |
| `--insecure` | `RAY_MCP_INSECURE` | `false` | Allow HTTP with no token (dev only) |
| `--context` | `RAY_MCP_CONTEXT` | current context | Kubeconfig context to bind |
| `--kubeconfig` | `KUBECONFIG` | discovery / in-cluster SA | Credentials source |
| `--default-namespace` | `RAY_MCP_NAMESPACE` | `default` | Namespace when a tool omits one |
| `--allow-all-namespaces` | `RAY_MCP_ALL_NS` | `false` | Permit cluster-wide list |
| `--allow-mutations` | `RAY_MCP_ALLOW_MUTATIONS` | `false` | Register write tools |
| `--allow-destructive` | `RAY_MCP_ALLOW_DESTRUCTIVE` | `false` | Register destructive tools |
| `--log-level` | `RAY_MCP_LOG_LEVEL` | `info` | Structured log level |

HTTP transport binds `127.0.0.1` by default and refuses to start without an
auth token unless `--insecure` is passed.

## 9. Error Handling

- Adapters return typed errors: `NotFound`, `Forbidden`, `Conflict`,
  `RayAPIUnreachable`, `Timeout`.
- Domain maps them to MCP tool errors with actionable messages (e.g. `Forbidden`
  names the missing RBAC verb/resource). Raw k8s/Ray API errors are never
  leaked.
- All calls are `context.Context`-driven with deadlines.
- Port-forward failures degrade gracefully: CRD-derived status is still
  returned, annotated that live Ray detail was unavailable.

## 10. Testing Strategy

- **Domain layer (bulk of coverage):** pure unit tests with fake `KubeRayPort`/
  `RayAPIPort`. Covers guards, spec-building, rawSpec merge, diff, tier logic.
  No cluster required.
- **KubeRay adapter:** `envtest` (controller-runtime's API-server-in-a-box) with
  KubeRay CRDs installed.
- **Ray API adapter:** `httptest` server mimicking the dashboard/job API.
- **MCP layer:** in-memory transport from go-sdk; assert tool schemas, arg
  validation, and end-to-end tool calls against fakes.
- **Optional e2e:** kind + KubeRay smoke test behind a build tag / CI job.

## 11. Distribution

- Multi-arch static binaries via GoReleaser → GitHub Releases.
- Minimal distroless Docker image.
- Helm chart for in-cluster (HTTP) deployment, bundling a least-privilege
  Role/RoleBinding + ServiceAccount.
- README documents the stdio quickstart (point at kubeconfig, drop into an
  agent's MCP config) and the in-cluster HTTP deployment.
- CI: `golangci-lint` + unit + envtest on every PR.

## 12. Open Questions / Future Work

- Broader KubeRay version compatibility beyond the pinned v1 range.
- True streaming logs (vs. bounded tail) if/when the transport and tooling make
  it ergonomic.
- Optional OAuth 2.1 resource-server flow for HTTP (seam left in place).
- Multi-cluster support, if demand emerges.
