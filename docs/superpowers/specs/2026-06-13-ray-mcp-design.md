# ray-mcp — Design Specification

**Date:** 2026-06-13
**Status:** Draft for review
**Repo:** `github.com/risjai/ray-mcp`

## 1. Summary

`ray-mcp` is a Model Context Protocol (MCP) server, written in Go, that lets an
AI agent manage Ray workloads running on Kubernetes via the
[KubeRay](https://github.com/ray-project/kuberay) operator. It exposes tools to
manage the full lifecycle of `RayCluster`, `RayJob`, and `RayService` resources,
**and** — this is the point — to reach Ray's own dashboard/job API for the
runtime detail (live job status, logs, follow-to-completion) that the Kubernetes
CRDs do not expose.

It is a focused open-source project, self-hosted per-user: a user runs their own
instance next to their own cluster, using their own Kubernetes credentials.

## 2. Why this exists (the wedge)

**Null alternative.** A generic `apiVersion`+`kind` Kubernetes MCP server can
already create/get/delete arbitrary CRDs, including KubeRay's. So `ray-mcp` only
earns its existence where a generic tool **cannot** serve the agent, or serves
it so unreliably that it effectively can't. Everything in this spec is justified
against that bar.

### Hard moat — a generic K8s tool *structurally cannot* do this

These require talking to the Ray dashboard/job API on the head node (port 8265),
reached via an on-demand port-forward. The data does not live in the CRD, so no
amount of CRD access or prompt-engineering gets a generic tool there:

- **Tail job logs.** "Tail the last 200 lines of job `raysubmit_abc` on cluster
  `nightly-train`." Logs live in the Ray dashboard, not in `RayJob.status`.
- **Submit → know when it finishes.** "Submit this entrypoint and tell me when
  it actually succeeds/fails." This spans RayJob CRD creation **and** dashboard
  job-status polling — two control planes in one logical operation. (Mechanism is
  async-over-sync, not a multi-hour blocking call — see §7.A.)
- **Live runtime status.** Actor/task/resource detail surfaced by the dashboard
  that the CRD status never carries.

This cross-plane path (RayAPI client + port-forwarder) is the **core
differentiator**, not a secondary feature. The implementation must nail it.

### Soft moat — a generic tool *can*, but agents get it wrong

A generic tool can do these via raw spec surgery; the value here is that
`ray-mcp` makes them typed, reliable, and self-explaining — and deep nested
KubeRay specs are exactly where agents fail:

- **Typed worker-group autoscaling** (min/max/replicas per group) as first-class
  params, not hand-built spec paths.
- **RayService reconfig awareness.** KubeRay has *two distinct* update paths and
  an agent must not confuse them: editing `serveConfigV2` is an **in-place** update
  to Serve apps on the existing cluster, whereas editing the cluster config (e.g.
  `rayVersion`) triggers a **zero-downtime cluster swap** (KubeRay stands up a
  pending cluster, waits, switches head-service traffic, deletes the old one).
  `ray-mcp` knows which path a given change takes and reports it; a generic tool
  patching raw YAML does not.
- **Ray-tuned destructive guards** for operations whose danger is Ray-specific:
  scaling a worker group to zero, deleting a RayService that is serving traffic.

The hard moat is the reason to build; the soft moat is why it stays pleasant to
use. Distribution polish (§11) is justified by, and sequenced after, the wedge.

## 3. Goals & Non-Goals

### Goals
- Serve the **hard-moat** operations first: live job status, logs, and
  submit-then-follow-to-completion (async-over-sync, §7.A) across the CRD and
  dashboard APIs.
- Let an agent perform full lifecycle operations (read + write) on RayCluster,
  RayJob, and RayService through a small, well-typed set of MCP tools.
- Be Kubernetes-native: manage Ray via KubeRay CRDs using typed Go clients.
- Be safe by default for a tool that any autonomous agent might drive.
- Be trivial to "hook up": run locally against a kubeconfig (stdio) and drop
  into an agent's MCP config.
- **Sequence wedge → polish.** Wedge and its tests land before any distribution
  machinery (Helm, multi-arch, HTTP transport). Polish is earned, not upfront.

### Non-Goals (v1)
- **HTTP transport, bearer auth, and in-cluster deployment** — deferred (see
  §11). v1 is stdio-only. The transport seam is kept at the edge so HTTP can be
  added without a refactor.
- No hosted multi-tenant service; no storage of other users' cluster
  credentials.
- No multi-cluster fan-out (one server instance binds to one Kubernetes
  context).
- No management of the KubeRay operator's own installation/lifecycle.
- No true real-time log streaming (v1 returns a bounded tail).
- No provisioning of the underlying Kubernetes cluster or node pools.

## 4. Key Decisions

| # | Decision | Choice | v1? |
|---|----------|--------|-----|
| 1 | Deployment model | Self-hosted, per-user | yes (binary only) |
| 2 | Transport | stdio; HTTP seam kept at edge but deferred | stdio only |
| 3 | Control plane | KubeRay CRDs **and** Ray dashboard/job API as **co-core** (cross-plane path is the wedge) | yes |
| 4 | Operation surface | Full lifecycle read+write, guarded (safe by default) | yes |
| 5 | Ray API reach | Auto port-forward via the k8s API (SPDY), on demand — **core to the wedge** | yes |
| 6 | Scope | Single cluster (one context), multi-namespace | yes |
| 7 | HTTP auth | Bearer token; localhost default; refuse no-token unless `--insecure` | deferred (with HTTP) |
| 8 | K8s client | KubeRay typed API (`ray-operator/apis/ray/v1`) + clientset (`ray-operator/pkg/client/clientset/versioned`) via sigs.k8s.io/controller-runtime | yes |
| 9 | MCP SDK | Official `github.com/modelcontextprotocol/go-sdk` (**GA, v1.x**; stdio + streamable-HTTP transports) | yes |
| 10 | Spec input | Curated typed params (primary) + `rawSpec` deep-merge escape hatch, gated & documented as advanced/unvalidated; curated params win on conflict (R7) | yes |
| 11 | Safety model | Layered: tier flags + dryRun + protected annotation + diffs | yes |
| 12 | KubeRay version | Pin to KubeRay `ray.io/v1` API; declare supported range; detect **served CRD apiVersions** via discovery (operator semver is not exposed by the API) | yes |
| 13 | Logs | Bounded tail (last-N-lines / since-duration), not streaming | yes |

## 5. Architecture

Layered hexagonal (ports & adapters). Data flows top→down; only adapters touch
the outside world. The domain layer imports no Kubernetes or HTTP packages —
it depends on Go interfaces — which makes it unit-testable with fakes.

```
┌──────────────────────────────────────────────────────────────┐
│ Transport (edge)         stdio   [HTTP seam kept, not shipped] │
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
│   • orchestration: submit → bounded-wait/poll across both APIs │
├──────────────────────────────────────────────────────────────┤
│ Adapters (ports)                                               │
│   ├─ KubeRayClient   (controller-runtime, typed CRDs)          │
│   ├─ RayAPIClient    (dashboard/job REST over a tunnel) ★ wedge │
│   └─ PortForwarder   (k8s SPDY portforward to head svc:8265) ★ │
├──────────────────────────────────────────────────────────────┤
│ Kubernetes API server  +  Ray head dashboard (in-cluster)      │
└──────────────────────────────────────────────────────────────┘
★ = core to the wedge (§2); a generic K8s MCP cannot reach this.
```

### Boundaries
- **Domain depends on interfaces**, `KubeRayPort` and `RayAPIPort`, not on
  concrete clients. Fakes drive the bulk of the tests.
- **Transport and SDK are edges.** v1 ships stdio; the transport seam is kept so
  HTTP can be added later without touching domain or adapters.
- **One instance = one Kubernetes context**, bound at startup; multi-namespace
  within that context.
- **PortForwarder tunnels are per-call and ephemeral** (§7.A) — opened and closed
  within a single tool invocation. No long-lived or cross-call shared tunnels, so
  no tool call ever depends on a tunnel outliving it.
- **KubeRayClient mutates via partial patch (server-side-apply / strategic-merge),
  never full-object put** (§7.D) — so concurrent autoscaler/agent writes are not
  clobbered. Full-update paths retry once on `Conflict`.

### Package layout
```
cmd/ray-mcp/main.go              # flag parsing, wiring, transport selection
internal/config/                 # config struct, flag/env loading, validation
internal/mcp/                    # tool registration, schemas, arg↔DTO mapping
internal/domain/                 # services, guards, spec-building (pure-ish)
  cluster.go  job.go  service.go  guards.go  spec.go  diff.go  types.go
internal/adapters/kuberay/       # controller-runtime client impl
internal/adapters/rayapi/        # dashboard/job REST client    ★ wedge
internal/adapters/portforward/   # SPDY tunnel manager          ★ wedge
internal/observability/          # structured logging, optional metrics
```

## 6. Tool Surface

Tools are namespaced `ray_*`. Read tools are always registered. Write tools
register only when `--allow-mutations` is set; destructive tools additionally
require `--allow-destructive`. Disabled tools are not advertised to the agent.
Every tool takes an optional `namespace` arg, defaulting to the configured
default namespace. Tools marked ★ depend on the wedge (dashboard API + tunnel).

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
| `ray_job_get` | read ★ | Status incl. live dashboard job status, start/end, message |
| `ray_job_logs` | read ★ | Bounded tail of job logs via Ray Job API (over tunnel) |
| `ray_job_submit` | write ★ | entrypoint + runtimeEnv + cluster target (see below); optional bounded `wait` (then follow via `ray_job_get`); `dryRun` |
| `ray_job_stop` | write | Stop a running job |
| `ray_job_delete` | destructive | Delete the RayJob resource |

**Job identity.** The agent always refers to a job by its **RayJob k8s name**.
The Ray dashboard/job REST API is keyed by the **submission id** (the
`raysubmit_...` handle, used for status/logs/stop). KubeRay surfaces that handle
on the RayJob as **`status.jobId`** (this field *is* the submission id — there is
no separate `submissionId` field), alongside **`status.dashboardURL`**,
**`status.jobStatus`**, and **`status.jobDeploymentStatus`**. The service bridges
name → submission id + dashboard endpoint via those status fields. If status is
not yet populated (job not scheduled yet), wedge tools return a clear "job not yet
scheduled" message rather than a tunnel/connection error. The agent never has to
know the submission id exists.

**Cluster target (R8 — resolved: both modes, explicit and mutually exclusive).**
A RayJob either runs against an existing cluster or brings its own. `ray_job_submit`
takes exactly one of:
- `targetCluster: <name>` — run against an **existing** RayCluster (maps to the
  RayJob `spec.clusterSelector`); no cluster is created or deleted by the job.
- `clusterSpec: {curated params + rawSpec}` — maps to RayJob
  `spec.rayClusterSpec`; KubeRay creates an **ephemeral** cluster for the job and
  tears it down on completion via `spec.shutdownAfterJobFinishes=true`.

Supplying both, or neither, is a validation error. Because `clusterSpec` *creates
a cluster* via a "submit job" call, it is gated behind `--allow-mutations` like any
create, and the response states plainly that an ephemeral cluster will be / was
created.

### RayService
| Tool | Tier | Purpose |
|------|------|---------|
| `ray_service_list` | read | List RayServices (+ serve status, healthy replicas) |
| `ray_service_get` | read | Serve app status, route prefix, conditions |
| `ray_service_deploy` | write | Create from serveConfig + cluster params (+ `rawSpec`), `dryRun` |
| `ray_service_update` | write | Update `serveConfigV2` (in-place) or cluster config (zero-downtime swap); reports which path the change takes; `dryRun`, diff |
| `ray_service_delete` | destructive | Delete (honors `protected`; refuses if serving traffic unless forced) |

### Meta
| Tool | Tier | Purpose |
|------|------|---------|
| `ray_capabilities` | read | Server version, bound context, default ns, **served Ray CRD apiVersions** (via discovery) + supported range, enabled tiers |

### Curated create params (shared shape)
`name`, `namespace`, `rayVersion`, `image`,
`headResources{cpu,memory,gpu}`,
`workerGroups[]{name,replicas,min,max,resources}`,
`enableAutoscaling`, `labels`, `annotations`,
plus `rawSpec` (YAML/JSON, merged *underneath* the curated params — see below).

**`rawSpec` is the deliberate escape hatch, kept tight (R7).** The curated params
are the primary, wedge-aligned path; `rawSpec` exists only for fields we haven't
typed (tolerations, volumes, sidecars). It is documented as **advanced and
unvalidated passthrough**; on any key collision the **curated params win** (they
are merged last over `rawSpec`), so a typed field is never silently overridden by
a raw blob. It rides the existing `--allow-mutations` gate — no separate tier in
v1. Rationale for keeping it in v1: without an escape hatch, an agent that hits one
untyped field abandons the tool entirely; the cost is a documented advanced
surface, not a weakening of the typed default.

## 7. Data Flow

### A) `ray_job_submit` → completion (the wedge, cross-plane, async-over-sync)

MCP tool calls are request/response; a Ray job can run for hours. We therefore
**never** block a tool call (or hold a tunnel) for the lifetime of a job. Instead
we use the standard async-over-sync pattern: submit returns fast, a bounded wait
is best-effort, and the agent loops on `get` to follow long jobs.

1. MCP layer decodes args → `SubmitJobRequest` DTO; schema validation up front.
2. `JobService.Submit`: mutation gate → build typed `RayJob` (curated params
   overlaid on `rawSpec`, curated wins) → `KubeRayClient.Create`. Returns the
   RayJob **name** immediately.
3. If `wait=true` (default false): do a **bounded** wait capped by `waitTimeout`
   (default 120s). Within that window, open an ephemeral tunnel for *this call
   only* and poll dashboard job status.
4. Return one of: terminal status (succeeded/failed), or — if the cap is hit while
   still running — `{state: running, name, jobId, message: "still running; call
   ray_job_get to continue"}`. The agent follows long jobs by polling
   `ray_job_get`. Optional MCP progress notifications may be emitted during the
   bounded wait, but correctness never depends on the client handling them.

**Tunnel lifecycle:** tunnels are **per-call and ephemeral** — opened at the start
of a tool invocation, reused only within that one call's poll loop, closed on
return. There are no cross-call or multi-hour tunnels. On a mid-call SPDY failure
(e.g. head pod rescheduled), re-open once and retry; if still unreachable, return
gracefully per §10.

```mermaid
sequenceDiagram
    autonumber
    actor Agent
    box ray-mcp
        participant MCP as MCP Layer
        participant Svc as JobService
        participant PF as PortForwarder
    end
    participant K8s as Kubernetes<br>API
    participant Ray as Ray Dashboard<br>(head :8265)

    Agent->>+MCP: ray_job_submit(entrypoint, target, wait=true)
    MCP->>+Svc: SubmitJobRequest (validated)
    Note over Svc: mutation gate.<br>build typed RayJob<br>(curated over rawSpec)
    Svc->>+K8s: Create RayJob (ray.io/v1)
    K8s-->>-Svc: accepted (name)

    opt wait=true (bounded by waitTimeout, default 120s)
        Svc->>+K8s: read RayJob.status.jobId + dashboardURL
        K8s-->>-Svc: submission id + head svc
        Svc->>+PF: Open(headSvc, 8265)
        PF->>K8s: SPDY portforward
        PF-->>-Svc: ephemeral local port
        loop until terminal or waitTimeout
            Svc->>+Ray: GET /api/jobs/{submission_id}
            Ray-->>-Svc: jobStatus
        end
        PF->>PF: close tunnel (per-call)
    end

    alt terminal within cap
        Svc-->>MCP: status = SUCCEEDED / FAILED
    else still running at cap
        Svc-->>-MCP: state=running + "call ray_job_get to continue"
    end
    MCP-->>-Agent: bounded result (no multi-hour block)

    Note over Agent,Ray: For long jobs the agent polls ray_job_get.<br>Each call opens and closes its own tunnel.
```

### B) `ray_job_logs` (the wedge, dashboard API path)
1. `JobService.Logs`: resolve RayJob → submission id (`status.jobId`) + head
   service via KubeRay client.
2. `PortForwarder.Open(headSvc, 8265)` → ephemeral local port, scoped to this
   single call. (8265 is the dashboard / Job Submission REST API port.)
3. `RayAPIClient.JobLogs(submissionID)` → `GET /api/jobs/{submission_id}/logs`
   over the tunnel → bounded tail → text. (Status path: `GET /api/jobs/{id}`;
   submit: `POST /api/jobs/`; stop: `POST /api/jobs/{id}/stop`.)
4. `defer` tunnel close — opened and closed within this one call.

### C) `ray_cluster_create` (write, pure CRD path)
1. MCP layer decodes args → `CreateClusterRequest` DTO; validation up front.
2. `ClusterService.Create`: mutation gate → start from `rawSpec` (if any) →
   overlay the curated typed params on top (curated wins, §6) → validate.
3. If `dryRun`: server-side dry-run apply; return rendered spec + "would create";
   no mutation.
4. Else: `KubeRayClient.Create(ctx, obj)`; return name, namespace, initial status,
   applied spec summary.

### D) `ray_cluster_update` / `ray_cluster_scale` (write, concurrency-safe)

Worker replica counts on an autoscaling cluster are **live, contended fields** —
the Ray autoscaler writes `replicas` directly on the RayCluster CR (incrementing
on scale-up, and reducing it plus populating `workersToDelete` on scale-down), and
another agent might write too. A naive get→modify→put races and silently clobbers
those autoscaler writes (classic Kubernetes lost update; worst-case on an
autoscaling cluster). [verified: KubeRay autoscaling docs]

1. Mutation gate → resolve target by name.
2. Apply changes as a **partial patch, never a full-object put**:
   server-side-apply / strategic-merge-patch that carries *only* the fields being
   changed. `scale` patches the named worker group's
   `replicas`/`minReplicas`/`maxReplicas` only — it never rewrites the rest of the
   spec, so it cannot stomp autoscaler-owned fields elsewhere.
3. On `Conflict` (resourceVersion mismatch, only possible on paths that need a
   full update): retry-with-backoff once, then surface a clear conflict error.
4. Return a **field-level diff** (see §10 output contract), not the whole object.

## 8. Safety Model

Layered defense-in-depth; never relies on the agent behaving well.

- **Tier gating at registration.** read always on; write needs `--allow-mutations`;
  destructive needs `--allow-destructive`. Disabled tools are not advertised.
- **`dryRun` arg** on every mutating tool → server-side dry-run / diff only,
  no mutation.
- **Protected annotation.** `ray-mcp/protected="true"` on a resource makes delete
  and destructive scale-down refuse with a clear message, regardless of flags.
- **Ray-tuned destructive guards.** Scaling a worker group to zero and deleting a
  RayService that is actively serving traffic are treated as destructive and
  guarded accordingly (refuse-unless-forced + clear impact message).
- **Before/after diff** returned by every successful mutation (structured +
  human-readable).
- **RBAC is the floor.** App guards layer on top of whatever the kubeconfig/SA
  permits. We document a least-privilege Role.

## 9. Configuration

Precedence: flags > environment > defaults. (HTTP-related flags are listed for
the deferred transport but are not wired in v1.)

| Flag | Env | Default | Purpose | v1? |
|------|-----|---------|---------|-----|
| `--transport` | `RAY_MCP_TRANSPORT` | `stdio` | `stdio` (only value in v1) | yes |
| `--context` | `RAY_MCP_CONTEXT` | current context | Kubeconfig context to bind | yes |
| `--kubeconfig` | `KUBECONFIG` | discovery / in-cluster SA | Credentials source | yes |
| `--default-namespace` | `RAY_MCP_NAMESPACE` | `default` | Namespace when a tool omits one | yes |
| `--allow-all-namespaces` | `RAY_MCP_ALL_NS` | `false` | Permit cluster-wide list | yes |
| `--allow-mutations` | `RAY_MCP_ALLOW_MUTATIONS` | `false` | Register write tools | yes |
| `--allow-destructive` | `RAY_MCP_ALLOW_DESTRUCTIVE` | `false` | Register destructive tools | yes |
| `--log-level` | `RAY_MCP_LOG_LEVEL` | `info` | Structured log level | yes |
| `--http-addr` | `RAY_MCP_HTTP_ADDR` | `127.0.0.1:8765` | HTTP listen address | deferred |
| `--auth-token` | `RAY_MCP_AUTH_TOKEN` | (none) | Bearer token; required unless `--insecure` | deferred |
| `--insecure` | `RAY_MCP_INSECURE` | `false` | Allow HTTP with no token (dev only) | deferred |

## 10. Error Handling & Output Contract

**The consumer is an LLM with a finite context budget, not a human scrolling a
terminal.** Every payload is bounded by design; unbounded output is a context
bomb that degrades the agent.

**Output contract (token-bounded):**
- **Diffs are field-level and summarized:** changed paths with old→new *scalar*
  values. Large nested subtrees are summarized, not inlined — e.g.
  "`workerGroups[0].template` changed (3 fields)" rather than dumping the pod
  template. An opt-in `verbose` arg returns the full diff when truly needed.
- **List tools paginate / cap** (default ~50 items) and report "N more
  available" rather than returning unbounded lists.
- **Logs** are already a bounded tail (decision #13).
- **Errors and events are truncated** to a bounded, relevant slice (e.g. last N
  events / the admission message), never the raw firehose.

**Errors:**
- Adapters return typed errors: `NotFound`, `Forbidden`, `Conflict`,
  `RayAPIUnreachable`, `Timeout`.
- Domain maps them to MCP tool errors with actionable, bounded messages (e.g.
  `Forbidden` names the missing RBAC verb/resource). Raw k8s/Ray API errors are
  never leaked verbatim.
- All calls are `context.Context`-driven with deadlines.
- Port-forward failures degrade gracefully: CRD-derived status is still
  returned, annotated that live Ray detail (the wedge) was unavailable and why.

## 11. Testing Strategy

- **Domain layer (bulk of coverage):** pure unit tests with fake `KubeRayPort`/
  `RayAPIPort`. Covers guards, spec-building, rawSpec merge, diff, tier logic,
  and the submit→bounded-wait→follow orchestration (§7.A). No cluster required.
- **KubeRay adapter:** `envtest` (controller-runtime's API-server-in-a-box) with
  KubeRay CRDs installed.
- **Ray API adapter:** `httptest` server mimicking the dashboard/job API
  (status + logs) — directly exercises the wedge logic without a live head node.
- **MCP layer:** in-memory transport from go-sdk; assert tool schemas, arg
  validation, and end-to-end tool calls against fakes.
- **Optional e2e:** kind + KubeRay smoke test behind a build tag / CI job.

## 12. Distribution

Deliberately minimal for v1 — everything heavier is **deferred until a second
user or a proven wedge** (per §2/§3 sequencing):

**v1 ships:**
- A plain `go install` / `go build` binary.
- README: stdio quickstart (point at kubeconfig, drop into an agent's MCP
  config) plus the documented least-privilege RBAC Role. **The read-only default
  is called out loudly:** the headline goal is "an agent controls Ray," but
  mutations are OFF until `--allow-mutations` (and destructive ops until
  `--allow-destructive`). README leads with this so a first-time user doesn't read
  "my agent can't create anything" as a bug. `ray_capabilities` echoes the enabled
  tiers so the agent itself can see and explain the gate.
- CI: `golangci-lint` + unit + envtest on every PR.

**Deferred (add when earned):**
- HTTP transport + bearer auth (the in-cluster deployment story).
- Helm chart + RBAC/ServiceAccount bundle.
- GoReleaser multi-arch release pipeline.
- Distroless Docker image.
- OAuth 2.1 resource-server seam for HTTP.

## 13. Verified Technical Facts (research-backed, 2026-06-13)

Load-bearing API claims were fact-checked against official Ray/KubeRay docs and
the SDK repo before approval. Confirmed:

- **MCP Go SDK** `github.com/modelcontextprotocol/go-sdk` is official (with Google),
  **GA v1.x**, with `mcp.StdioTransport`, typed-schema `mcp.AddTool`, and
  `StreamableHTTPHandler`.
- **Ray dashboard / Job Submission REST API** on port **8265**; endpoints:
  `POST /api/jobs/` (submit), `GET /api/jobs/{id}` (status),
  `GET /api/jobs/{id}/logs` (logs), `POST /api/jobs/{id}/stop`. The `{id}` is the
  **submission id** (`raysubmit_...`).
- **`RayJob.status`** (`ray.io/v1`) exposes `jobId` (== submission id; no separate
  `submissionId` field), `dashboardURL`, `jobStatus`, `jobDeploymentStatus`.
- **RayJob targeting:** `spec.clusterSelector` (existing cluster) vs
  `spec.rayClusterSpec` (ephemeral) + `spec.shutdownAfterJobFinishes`.
  (`submissionMode` defaults to `K8sJobMode`.)
- **API group/version** `ray.io/v1` for RayCluster/RayJob/RayService; typed client
  at `ray-operator/apis/ray/v1` + `ray-operator/pkg/client/clientset/versioned`.
- **Autoscaler writes `replicas`** (and `workersToDelete`) on the RayCluster CR →
  the lost-update risk in §7.D is real. (No doc *recommendation* against manual
  edits is claimed — only the mechanism.)
- **RayService:** `serveConfigV2` change = **in-place** Serve update; cluster-config
  change = **zero-downtime cluster swap**. Two different paths (§2, §6).

## 14. Open Questions / Future Work

- Broader KubeRay version compatibility beyond the pinned v1 range.
- True streaming logs (vs. bounded tail) if/when the transport makes it
  ergonomic.
- The full deferred distribution set in §12, gated on demand.
- Multi-cluster support, if demand emerges.

## 15. Standing Commitments (eyes-open)

- Publishing as OSS (#1) + version-pin discipline (#12) is a **maintenance
  commitment**: tracking KubeRay releases, issue triage, supporting user configs
  we don't control. Accepted consciously; revisit if it outpaces value.
