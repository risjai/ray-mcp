# Implementation Plan: ray-mcp

**Date:** 2026-06-13
**Source of truth:** `docs/superpowers/specs/2026-06-13-ray-mcp-design.md` (Q1–Q15 decided; Q16 schema ratified, behavior open)
**Status:** Draft for human review — no code written yet

## Overview

`ray-mcp` is a Go MCP server that manages Ray on Kubernetes via KubeRay CRDs
(the guarded write path) and reaches Ray's dashboard/job API read-only for the
runtime detail CRDs don't expose (the wedge). This plan slices the build into
dependency-ordered, vertical tasks: each delivers one working tool end-to-end
(transport → MCP → domain → adapter → cluster), tested at every layer.

The guiding sequence is **walking skeleton → read path → apply pipeline → write
path → destructive tier → the wedge → jobs → services → HTTP/auth →
distribution**. Foundations that everything reuses (config, ports, the unified
apply pipeline, the wedge adapters) are built before the slices that depend on
them.

## Architecture Decisions (from the spec — context for implementers)

- **Layered hexagonal.** Domain imports no k8s/http; depends on `KubeRayPort`,
  `RayAPIPort`, `RayReachability` interfaces. Fakes drive the bulk of tests.
- **KubeRay access** = controller-runtime *client package* (uncached) + KubeRay
  Go types; all mutations via **Server-Side Apply** preceded by `DryRunAll`.
- **rawSpec wins** over curated params via RFC 7386 JSON Merge Patch; merged
  object stays **unstructured** (preserves newer-than-baseline fields).
- **Dashboard API is read-only by construction** (`RayAPIPort` has no write
  methods); reached via `RayReachability` (DirectDial in-cluster, pooled SPDY
  PortForward out-of-cluster).
- **Stateless server:** confirmations are content-derived fingerprints, no
  cross-call state.
- **stdio invariant:** logs/audit go to stderr/file, never stdout (stdout is the
  JSON-RPC wire).
- **MCP SDK:** official `modelcontextprotocol/go-sdk` v1.6.1 (verified).

## Dependency Graph

```
Task 1 scaffold ─┬─ Task 2 config ─────────────────────────────┐
                 ├─ Task 3 domain ports + types + fakes ─┐      │
                 │                                         │      │
                 └─ Task 4 WALKING SKELETON (ray_capabilities, stdio) ◄ needs 1,2,3
                                  │
   ┌──────────────────────────────┼───────────────────────────────┐
   │ Task 5 kuberay read + envtest │                               │
   │   └ Task 6 cluster list/get   │   Task 14 Reachability ┐      │
   │   └ Task 7 cluster events     │   Task 15 RayAPIClient ┤(wedge adapters,
   │                               │   Task 13 distill note ┘ parallel-safe)
   │ Task 8 APPLY PIPELINE ◄ reused by all create/update/deploy
   │   └ Task 9 cluster create     │           │
   │   └ Task 10 update/scale (SSA)│           └ Task 16 job get/logs (WEDGE)
   │       └ Task 11 fingerprint+protected guard
   │           └ Task 12 cluster delete (destructive)
   │                                            └ Task 17 job wait
   │ Task 18 job submit (apply pipeline + modes)
   │ Task 19 job list/delete (mode-aware)
   │ Task 20-22 RayService (reuse apply pipeline + wedge distill)
   │ Task 23-25 HTTP transport + auth + scope reconcile
   │ Task 26-28 RBAC/Helm + README + CI
```

Implementation order is bottom-up: scaffold → config/ports → skeleton → read →
apply pipeline → write → destructive → wedge → jobs → services → HTTP → dist.

---

## ⚠️ Open Questions — must be resolved at the gates below (from spec §14 + Review Round 2)

These are **not decided** in the spec. Each is mapped to the task it gates and a
recommended resolution point. The plan is sequenced so most can be answered
just-in-time, but three (C3, B2, B1/B3) should be settled before the phases they
touch.

| ID | Question | Gates | Lean (from spec) | Resolve before |
|----|----------|-------|------------------|----------------|
| **C3** | Curated params thin for GPU Ray (no `rayStartParams`/`tolerations`/`nodeSelector`) → `--allow-raw-spec=false` unusable for core GPU case | Task 8 (curated shape) | Grow curated params, or document the limit | **Task 8** |
| **B2** | Q5's unconditional `DryRunAll` may obsolete Q4's CRD-schema-read + its ClusterRole | Task 4 (capabilities field-set), Task 9, Task 26 (RBAC) | Demote schema-read to optional capability-reporting | **Task 9** |
| **B3** | "destructive" overloaded: registration-tier (`--allow-destructive`) vs runtime-confirm; does scale-to-zero need the flag? | Task 10, Task 12 | Separate the vocabulary | **Task 10** |
| **B1** | confirm-fingerprint with `resourceVersion` will livelock on busy autoscaling clusters | Task 11 | delete uses `hash(UID+op)`; reserve `resourceVersion` for scale/update | **Task 11** |
| **C4** | Status-distillation (the wedge crown jewel) is under-specified | Task 16, 17, 20 | Give it its own design note | **Task 13 (is this task)** |
| **Q16a** | `ray_job_delete` blast radius is mode-dependent (ephemeral cascade-deletes a whole cluster) | Task 19 | mode-aware: ephemeral→destructive+fingerprint, existing→plain write | **Task 19** |
| **Q16b** | `shutdownAfterJobFinishes` default (KubeRay default `false`) | Task 18 | default `true` + "pass false to keep for debugging" hint | **Task 18** |
| **D** | `ray_service_delete` "serving traffic" detection + missing `force` arg | Task 22 | add detection + `force` | **Task 22** |

---

## Task List

### Phase 0 — Foundation & Walking Skeleton

#### Task 1: Repo scaffold, module, lint, CI skeleton
**Description:** Establish the Go module, the package layout from spec §5, lint
config, and a green CI skeleton so every later task has a working baseline.

**Acceptance criteria:**
- [ ] `go.mod` at module path `github.com/risjai/ray-mcp`; empty packages per spec §5 layout compile.
- [ ] `golangci-lint run` passes on the skeleton; `Makefile` targets `build`/`test`/`lint`.
- [ ] GitHub Actions runs build + lint + test on PR (green on empty).

**Verification:** `go build ./...`; `golangci-lint run`; CI green.
**Dependencies:** None. **Scope:** S.
**Files:** `go.mod`, `Makefile`, `.golangci.yml`, `.github/workflows/ci.yml`, `internal/*/doc.go`.

#### Task 2: Config layer (pure parsing + static invariants)
**Description:** `internal/config` — flags/env with `flags > env > defaults`
precedence (spec §9), and the **boot invariants** that need no cluster: the
bind/auth rule (non-loopback ⇒ token, no `--insecure`) and tier flags.

**Acceptance criteria:**
- [ ] All §9 flags/env parse with correct precedence and defaults (incl. in-cluster default-namespace fallback ordering).
- [ ] Non-loopback `--http-addr` without `--auth-token`/`tokenreview` → refuse-to-boot error; loopback allowed tokenless.
- [ ] `--allow-mutations`/`--allow-destructive`/`--allow-raw-spec`/`--ray-access` validated.

**Verification:** `go test ./internal/config/...` (table-driven precedence + invariant cases).
**Dependencies:** Task 1. **Scope:** M.
**Files:** `internal/config/config.go`, `internal/config/config_test.go`.
**Note:** SelfSubjectAccessReview reconciliation (needs a cluster) is deferred to Task 25.

#### Task 3: Domain ports, types, error taxonomy, fakes
**Description:** Define the interfaces the domain depends on and the shared DTOs/
errors. This contract-first step unblocks parallel adapter/domain work and makes
the domain unit-testable with fakes.

**Acceptance criteria:**
- [ ] `KubeRayPort`, `RayAPIPort` (read-only — no write methods), `RayReachability` interfaces defined; `RayAPIPort` write-method absence is enforced by the interface.
- [ ] Error taxonomy types (`NotFound/Forbidden/Conflict/RayAPIUnreachable/Timeout`) and core DTOs.
- [ ] In-memory fakes implement every port and are used by a trivial passing test.

**Verification:** `go build ./...`; `go vet ./...`; fake-satisfies-interface test passes.
**Dependencies:** Task 1. **Scope:** M.
**Files:** `internal/domain/types.go`, `internal/domain/ports.go`, `internal/domain/errors.go`, `internal/domain/fakes_test.go`.

#### Task 4: Walking skeleton — `ray_capabilities` over stdio
**Description:** Thinnest complete vertical slice touching every layer: minimal
KubeRay adapter (context bind + server/served-API version), MCP registration via
go-sdk, stdio transport, wired in `main.go`. Reports version/context/default-ns/
enabled-tiers. **Defers** the CRD field-set/pruning report (B2-gated).

**Acceptance criteria:**
- [ ] Binary starts on stdio; an MCP client lists tools and calls `ray_capabilities`.
- [ ] Response uses go-sdk structured + text dual output; reflects the bound context and enabled tiers.
- [ ] Logging goes to stderr only (stdout stays clean JSON-RPC) — asserted.

**Verification:** `go build`; integration test via go-sdk `NewInMemoryTransports()`; manual stdio run against a kubeconfig.
**Dependencies:** Tasks 2, 3. **Scope:** M.
**Files:** `cmd/ray-mcp/main.go`, `internal/mcp/server.go`, `internal/mcp/capabilities.go`, `internal/adapters/kuberay/client.go`, `internal/transport/stdio.go`.

### ✅ Checkpoint A — Walking skeleton
- [ ] `go build ./...` clean; lint green.
- [ ] MCP client connects over stdio, calls `ray_capabilities`, gets structured response.
- [ ] Hexagonal seams are real (domain has no k8s/http imports — verified by `go list`/import check).
- [ ] **Human review before proceeding.**

---

### Phase 1 — RayCluster read path (token economy)

#### Task 5: KubeRay adapter read methods + envtest harness
**Description:** Implement List/Get RayCluster on the controller-runtime client;
stand up `envtest` with KubeRay CRDs as the integration substrate for all later
adapter tests.

**Acceptance criteria:**
- [ ] `envtest` boots with KubeRay CRDs installed; adapter List/Get round-trips a RayCluster.
- [ ] Adapter maps k8s errors to the domain error taxonomy (Task 3).

**Verification:** `go test -tags envtest ./internal/adapters/kuberay/...`.
**Dependencies:** Tasks 3, 4. **Scope:** M.
**Files:** `internal/adapters/kuberay/cluster.go`, `internal/adapters/kuberay/envtest_test.go`.

#### Task 6: `ray_cluster_list` + `ray_cluster_get` with token economy
**Description:** Two read tools end-to-end with the §10 output contract: `list`
returns tiny rows + pagination (k8s `continue` token, ~50 cap, never silent
truncation); `get` returns distilled status + a `verbose`/`raw` escape.

**Acceptance criteria:**
- [ ] `list` caps + paginates and reports "N of M, continue token X"; `get` distilled by default, full under `verbose`.
- [ ] Both registered with `readOnlyHint`; structured+text output.
- [ ] Unit tests (fake port) for verbosity tiers + pagination; envtest end-to-end.

**Verification:** `go test ./internal/domain/...`; `go test -tags envtest ./...`; MCP call.
**Dependencies:** Tasks 5, 6-prereqs. **Scope:** M.
**Files:** `internal/domain/cluster.go`, `internal/mcp/cluster.go`, `internal/domain/cluster_test.go`.

#### Task 7: `ray_cluster_events`
**Description:** Recent k8s events for a cluster's pods, token-bounded (last-N,
truncation marker).

**Acceptance criteria:**
- [ ] Returns bounded, relevant events (not the raw firehose); `readOnlyHint`.
- [ ] Unit + envtest coverage.

**Verification:** `go test -tags envtest ./...`.
**Dependencies:** Task 6. **Scope:** S.
**Files:** `internal/domain/cluster.go`, `internal/adapters/kuberay/events.go`, tests.

### ✅ Checkpoint B — RayCluster read path
- [ ] All RayCluster read tools work end-to-end, token-bounded.
- [ ] envtest suite green. **Human review.**

---

### 🚪 Decision Gate 1 — resolve **C3** and **B2** before Phase 2
- [ ] **C3:** grow curated params for GPU (`rayStartParams`/`tolerations`/`nodeSelector`) or document the `--allow-raw-spec=false` limit.
- [ ] **B2:** keep CRD-schema-read (and its ClusterRole) or demote to optional, relying on `DryRunAll` for pruning.

### Phase 2 — Unified apply pipeline + RayCluster write path

#### Task 8: Unified apply pipeline (`internal/domain/apply.go`)
**Description:** The reused core for every create/update/deploy: curated→typed→
JSON base, RFC 7386 merge (rawSpec wins, arrays replace wholesale), identity
guard, unstructured result, `DryRunAll`, SSA-apply, read-back field-level diff.
Pure-testable against the fake port.

**Acceptance criteria:**
- [ ] Merge tests prove rawSpec-wins, arrays-replace, identity-guard rejection, and newer-than-baseline field preservation.
- [ ] Diff summarization matches §10 (scalar changes inline, subtrees collapsed).
- [ ] `dryRun` path performs `DryRunAll` only; no mutation.

**Verification:** `go test ./internal/domain/...` (heavy unit coverage).
**Dependencies:** Task 3; Decision Gate 1 (C3). **Scope:** M (guard against L — keep merge/diff focused).
**Files:** `internal/domain/apply.go`, `internal/domain/diff.go`, `internal/domain/apply_test.go`.

#### Task 9: `ray_cluster_create` end-to-end (+ pruning detection)
**Description:** Wire the apply pipeline to a tool: mutation gate,
`dryRun` default false, pruning surfaced via `DryRunAll`.

**Acceptance criteria:**
- [ ] envtest: create succeeds; `dryRun=true` mutates nothing; an unknown field is reported as pruned.
- [ ] Tool is **absent** from the schema unless `--allow-mutations`.
- [ ] `idempotentHint` set appropriately; structured+text result with the diff.

**Verification:** `go test -tags envtest ./...`; MCP call with/without `--allow-mutations`.
**Dependencies:** Task 8; Decision Gate 1 (B2). **Scope:** M.
**Files:** `internal/mcp/cluster.go`, `internal/domain/cluster.go`, tests.

#### Task 10: `ray_cluster_update` + `ray_cluster_scale` (SSA, autoscaler-safe)
**Description:** SSA with our field manager sends only owned fields; the
autoscaler's `replicas` ownership is respected, not clobbered; `Conflict`
surfaced clearly; scale-to-zero flagged.

**Acceptance criteria:**
- [ ] envtest simulates autoscaler-owned `replicas`; our update does not clobber it.
- [ ] `Conflict` path returns an actionable error; retry-once only when the change is ours.
- [ ] Scale-to-zero is recognized as destructive (tier per B3).

**Verification:** `go test -tags envtest ./...` (field-ownership + conflict cases).
**Dependencies:** Task 9; Decision Gate 2 (B3). **Scope:** M.
**Files:** `internal/domain/cluster.go`, `internal/adapters/kuberay/apply.go`, tests.

### ✅ Checkpoint C — RayCluster write path (non-destructive)
- [ ] create/update/scale end-to-end; apply pipeline proven; pruning detected; SSA respects autoscaler. **Human review.**

---

### 🚪 Decision Gate 2 — resolve **B3** and **B1** before Phase 3
- [ ] **B3:** does scale-to-zero require `--allow-destructive`, or only the runtime confirm? Settle the tier-vs-confirm vocabulary.
- [ ] **B1:** delete fingerprint = `hash(UID+op)` (identity); reserve `resourceVersion` for scale/update to avoid autoscaler-churn livelock.

### Phase 3 — Destructive tier + stateless confirm

#### Task 11: Confirm-fingerprint + protected-annotation guard
**Description:** `guards.go` — stateless `confirm` fingerprint computed/verified by
recompute-from-live (stale → reject; free TOCTOU), and the self-gating `protected`
annotation (removal requires the fingerprint).

**Acceptance criteria:**
- [ ] Fingerprint match commits; mismatch/stale rejects; hash inputs follow B1's decision.
- [ ] Removing/altering `ray-mcp/protected` is refused without a valid fingerprint.
- [ ] Pure unit tests cover match/mismatch/stale/protected-removal.

**Verification:** `go test ./internal/domain/...`.
**Dependencies:** Task 10; Decision Gate 2 (B1). **Scope:** M.
**Files:** `internal/domain/guards.go`, `internal/domain/guards_test.go`.

#### Task 12: `ray_cluster_delete` (destructive, end-to-end)
**Description:** `--allow-destructive` gate, two-step confirm-fingerprint,
`protected` honored, `dryRun`.

**Acceptance criteria:**
- [ ] Tool absent without `--allow-destructive`; commit requires the fingerprint from a prior preview.
- [ ] `protected` resource refuses deletion; `destructiveHint` set.
- [ ] envtest two-call flow + protected-refusal.

**Verification:** `go test -tags envtest ./...`; MCP two-call flow.
**Dependencies:** Task 11. **Scope:** S/M.
**Files:** `internal/mcp/cluster.go`, `internal/domain/cluster.go`, tests.

### ✅ Checkpoint D — Full RayCluster lifecycle
- [ ] CRUD + scale + delete with all guards, end-to-end. **Human review.**

---

### Phase 4 — The wedge (cross-plane job runtime) ★ highest-value

#### Task 13: Status-distillation design note + pure `distill` module (resolves C4)
**Description:** Specify the wedge crown jewel before building it: inputs (CRD
status + pod events + dashboard status), distillation rules ("Pending:
unschedulable, no GPU nodes"), concrete worked examples, and RBAC needs. Land it
as a short design note **and** a pure `distill.go` with table-driven tests over
fixtures.

**Acceptance criteria:**
- [ ] Design note enumerates input sources and ≥6 concrete (input → distilled) examples covering pending/running/wedged/failed.
- [ ] `distill.go` is pure (no I/O) and passes table-driven tests over the fixtures.

**Verification:** note reviewed; `go test ./internal/domain/...`.
**Dependencies:** Task 3. **Scope:** M. **(Parallelizable with Tasks 5–12.)**
**Files:** `docs/superpowers/specs/2026-06-13-ray-mcp-status-distillation.md`, `internal/domain/distill.go`, `internal/domain/distill_test.go`.

#### Task 14: `RayReachability` adapter (DirectDial + pooled PortForward)
**Description:** Strategy selection (`auto`/`direct`/`port-forward`), in-cluster
detection, SPDY port-forward, pool-per-(ns,cluster) with idle reaper + re-dial.

**Acceptance criteria:**
- [ ] `auto` picks DirectDial in-cluster, PortForward otherwise; honors `--ray-access` override.
- [ ] Pooling reuses a warm endpoint; idle reaper closes it; dropped tunnel re-dials once.
- [ ] Unit tests with a fake dialer cover selection/pooling/reaping/re-dial.

**Verification:** `go test ./internal/adapters/reachability/...`.
**Dependencies:** Task 3. **Scope:** M. **(Parallelizable with Task 15.)**
**Files:** `internal/adapters/reachability/{strategy,portforward,pool}.go`, tests.

#### Task 15: `RayAPIClient` (read-only dashboard REST)
**Description:** `GET /api/jobs/{id}` (status) and `.../logs`; **no** submit/stop
methods; byte-bounded logs with truncation marker.

**Acceptance criteria:**
- [ ] httptest server exercises status + logs; byte ceiling + "N bytes omitted" marker enforced.
- [ ] Interface exposes only read methods (compile-time guarantee).

**Verification:** `go test ./internal/adapters/rayapi/...` (httptest).
**Dependencies:** Task 3. **Scope:** M. **(Parallelizable with Task 14.)**
**Files:** `internal/adapters/rayapi/client.go`, `internal/adapters/rayapi/client_test.go`.

#### Task 16: `ray_job_get` + `ray_job_logs` (the wedge, two-phase)
**Description:** Cross-plane reads: phase 1 poll CRD until `status.jobId`
populated, phase 2 dial dashboard; head endpoint read from status (not
DNS-templated, C2); distillation (Task 13); graceful degradation when dashboard
unreachable.

**Acceptance criteria:**
- [ ] Combined envtest (CRD) + httptest (dashboard): get/logs work once scheduled.
- [ ] Before `status.jobId` is set → "job not yet scheduled" (not a tunnel error).
- [ ] Dashboard-unreachable → CRD-derived status returned, annotated why.

**Verification:** `go test -tags envtest ./...` (combined harness).
**Dependencies:** Tasks 13, 14, 15, 5. **Scope:** M.
**Files:** `internal/domain/job.go`, `internal/mcp/job.go`, tests.

#### Task 17: `ray_job_wait` (bounded, two-phase)
**Description:** `waitSeconds ≤ 30`, `until=running|terminal` (default running);
phase-1 CRD then phase-2 dashboard; returns status + `reached`.

**Acceptance criteria:**
- [ ] Cap enforced (≤30s); returns promptly with `reached=false` if not reached.
- [ ] `until=running` answers "started vs stuck Pending" via phase 1 alone when jobId absent.

**Verification:** `go test ./...` + combined harness.
**Dependencies:** Task 16. **Scope:** S/M.
**Files:** `internal/domain/job.go`, `internal/mcp/job.go`, tests.

### ✅ Checkpoint E — The wedge works ★
- [ ] Cross-plane read path (the core differentiator) proven end-to-end on envtest + httptest.
- [ ] Distillation produces actionable status. **This is what justifies the project — review carefully with human.**

---

### 🚪 Decision Gate 3 — resolve **Q16b** (before Task 18) and **Q16a** (before Task 19)
- [ ] **Q16b:** `shutdownAfterJobFinishes` default (`true` vs KubeRay's `false`).
- [ ] **Q16a:** mode-aware `ray_job_delete` tiering (ephemeral cascade → destructive+fingerprint; existing → plain write).

### Phase 5 — RayJob write + list/delete

#### Task 18: `ray_job_submit` (non-blocking, two cluster-target modes)
**Description:** `existingCluster:<name>` XOR `clusterSpec:{...}` (both/neither →
error); ephemeral mode reuses the apply pipeline; returns immediately
(`{name, jobId-when-ready, initialStatus}`).

**Acceptance criteria:**
- [ ] envtest: both modes create the right RayJob shape; both/neither → validation error.
- [ ] Returns non-blocking; ephemeral applies `shutdownAfterJobFinishes` per Q16b with the documented hint.

**Verification:** `go test -tags envtest ./...`.
**Dependencies:** Tasks 8, 16; Decision Gate 3 (Q16b). **Scope:** M.
**Files:** `internal/domain/job.go`, `internal/mcp/job.go`, tests.

#### Task 19: `ray_job_list` + `ray_job_delete` (mode-aware)
**Description:** `list` token-bounded; `delete` tiered by mode per Q16a.

**Acceptance criteria:**
- [ ] `list` capped/paginated; `delete` of an ephemeral-cascade job follows the destructive path; existing-cluster job is a plain write.
- [ ] envtest covers both delete modes.

**Verification:** `go test -tags envtest ./...`.
**Dependencies:** Task 18; Decision Gate 3 (Q16a). **Scope:** M.
**Files:** `internal/domain/job.go`, `internal/mcp/job.go`, tests.

### ✅ Checkpoint F — Full RayJob lifecycle
- [ ] Submit/get/logs/wait/list/delete end-to-end; mode-aware delete correct. **Human review.**

---

### Phase 6 — RayService

#### Task 20: `ray_service_list` + `ray_service_get` (distilled rollout)
**Acceptance criteria:**
- [ ] `get` distills rollout phase + old/new serve health + cutover state; `list` token-bounded.
- [ ] Reuses Task 13 distillation patterns; envtest coverage.

**Verification:** `go test -tags envtest ./...`.
**Dependencies:** Tasks 13, 5. **Scope:** M.
**Files:** `internal/domain/service.go`, `internal/mcp/service.go`, tests.

#### Task 21: `ray_service_deploy` + `ray_service_update` (path-aware)
**Description:** Deploy via apply pipeline; update distinguishes in-place
`serveConfigV2` change from a zero-downtime cluster swap and **reports which**.

**Acceptance criteria:**
- [ ] envtest: serveConfigV2-only edit reported as in-place; cluster-config edit reported as zero-downtime swap.
- [ ] `dryRun` + diff supported.

**Verification:** `go test -tags envtest ./...`.
**Dependencies:** Tasks 8, 20. **Scope:** M.
**Files:** `internal/domain/service.go`, `internal/mcp/service.go`, tests.

#### Task 22: `ray_service_delete` (serving-traffic aware)
**Acceptance criteria:**
- [ ] Refuses deleting a service that is serving traffic unless `force`; confirm-fingerprint; `protected` honored.
- [ ] envtest covers serving + force paths.

**Verification:** `go test -tags envtest ./...`.
**Dependencies:** Task 21; Decision Gate 4 (D). **Scope:** M.
**Files:** `internal/domain/service.go`, `internal/mcp/service.go`, tests.
**🚪 Decision Gate 4 (D):** define "serving traffic" detection + add `force`.

### ✅ Checkpoint G — Full RayService lifecycle. **Human review.**

---

### Phase 7 — HTTP transport + auth

#### Task 23: Streamable HTTP transport + static bearer auth
**Acceptance criteria:**
- [ ] `--transport http` serves via go-sdk `NewStreamableHTTPHandler`; localhost default.
- [ ] Boot invariant enforced (non-loopback ⇒ token); same tools/behavior as stdio.
- [ ] HTTP integration test + boot-invariant test.

**Verification:** `go test ./internal/transport/...` (HTTP); both-transport MCP suite.
**Dependencies:** Task 4 (+ enough tools to be meaningful; safe after Phase 3). **Scope:** M.
**Files:** `internal/transport/http.go`, `internal/transport/auth.go`, tests.

#### Task 24: TokenReview auth mode + mutation audit log
**Acceptance criteria:**
- [ ] `--auth-mode tokenreview` validates caller SA tokens via the TokenReview API (faked in tests).
- [ ] Every mutating call is audit-logged (identity, tool, args summary, dryRun, outcome) to stderr/file — never stdout.

**Verification:** `go test ./...` (fake TokenReview; audit-log + stdout-clean assertions).
**Dependencies:** Task 23. **Scope:** M.
**Files:** `internal/transport/auth.go`, `internal/observability/audit.go`, tests.

#### Task 25: Scope reconciliation (SelfSubjectAccessReview at boot)
**Acceptance criteria:**
- [ ] `--allow-all-namespaces` without cluster-wide list → refuse-to-start (or loud-warn downgrade).
- [ ] `ray_capabilities` reports the actually-served namespaces (closes the Task 4 deferral).

**Verification:** `go test ./...` (fake SSAR).
**Dependencies:** Tasks 4, 24. **Scope:** S/M.
**Files:** `internal/config/reconcile.go`, `internal/mcp/capabilities.go`, tests.

### ✅ Checkpoint H — HTTP + auth + multi-tenancy-safe. **Human review.**

---

### Phase 8 — Distribution & hardening

#### Task 26: RBAC manifests + Helm chart
**Acceptance criteria:**
- [ ] Namespace `Role`+`RoleBinding` (templated per-ns) + opt-in cluster `ClusterRole` + CRD-read `ClusterRole` (per B2 outcome) + ServiceAccount.
- [ ] `helm lint` + `helm template` clean; manifests pass `kubeconform`.

**Verification:** `helm lint`; `helm template | kubeconform`.
**Dependencies:** Tasks 23–25; Decision Gate 1 (B2). **Scope:** M.
**Files:** `deploy/helm/**`, `deploy/rbac/*.yaml`.

#### Task 27: README + docs
**Acceptance criteria:**
- [ ] stdio quickstart leads; HTTP "shared team instance" section; RBAC + CI-tested KubeRay range; read-only default called out; agent loop (submit→wait→get→logs) documented.
- [ ] Apache-2.0 + KubeRay-native naming + donation-ready framing (§16).

**Verification:** doc review; link-check.
**Dependencies:** all tool tasks. **Scope:** M.
**Files:** `README.md`, `docs/**`.

#### Task 28: CI hardening (KubeRay version matrix)
**Acceptance criteria:**
- [ ] CI runs lint + unit + envtest across the CI-tested KubeRay version matrix on PR.
- [ ] (GoReleaser multi-arch + distroless remain deferred per §12 — tracked, not built.)

**Verification:** CI green across the matrix.
**Dependencies:** Task 1 + adapters. **Scope:** S/M.
**Files:** `.github/workflows/ci.yml`.

### ✅ Checkpoint I — v1 ready for review
- [ ] All acceptance criteria met; both transports; full lifecycle for all three CRDs; the wedge proven.
- [ ] All Decision Gates resolved and reflected in the spec.
- [ ] **Final human review before tagging v1.**

---

## Parallelization Opportunities
- **Parallel-safe after Task 4:** the wedge adapters (Tasks 13, 14, 15) can be built alongside the RayCluster read/write slices (Tasks 5–12) — disjoint packages.
- **Sequential (shared core):** Task 8 (apply pipeline) blocks all create/update/deploy; Task 11 (guards) blocks all destructive ops.
- **Define-contract-then-parallelize:** Task 3 ports are the contract; once landed, adapter and domain work proceed independently behind fakes.

## Risks and Mitigations
| Risk | Impact | Mitigation |
|------|--------|------------|
| 8 open questions unresolved at implementation | High | Decision Gates 1–4 force resolution before the gated phase; most are just-in-time. |
| Apply pipeline (Task 8) balloons past M | Med | Keep merge/diff focused; push curated-param breadth (C3) to a config decision, not pipeline code. |
| Wedge under-specified (C4) | High | Task 13 makes distillation a design-note-first, pure-tested module before any wedge tool. |
| envtest flakiness / KubeRay CRD version drift | Med | Pin CRD bundle to the compiled KubeRay version; CI matrix (Task 28). |
| go-sdk churn | Low | Quarantined behind `internal/mcp`; v1.6.1 features verified (spec §13). |
| SSA field-ownership vs autoscaler subtlety | Med | Task 10 explicitly tests autoscaler-owned `replicas`; B1 fixes the fingerprint-churn interaction. |

## Open Questions (need human input — see the gate table above)
C3, B2, B3, B1, Q16a, Q16b, D — each mapped to a Decision Gate. C4 is resolved by Task 13.
