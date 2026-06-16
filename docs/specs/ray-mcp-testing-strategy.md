# ray-mcp — Testing Strategy

**Date:** 2026-06-16
**Status:** Approved design — ready for implementation planning
**Extends:** `docs/specs/ray-mcp-design.md` §11 (Testing Strategy)
**Companions:** `tasks/plan.md`, `tasks/execution-plan.md`

This document operationalizes spec §11. It does not replace it — it adds the
runnable mechanics: a five-tier test pyramid, a per-task runnable test manifest
format, and a real-cluster (kind + KubeRay) e2e harness used as a pre-push gate.

## Why this exists

Spec §11 already names the right substrates (fakes, `envtest`, `httptest`,
go-sdk in-memory transport) and closes with *"Optional e2e: kind + KubeRay smoke
test behind a build tag / CI job."* Two gaps remain:

1. **The wedge cannot be proven by the hermetic tiers.** `envtest` is only
   `kube-apiserver` + `etcd` — it stores a `RayCluster` CR but runs **no KubeRay
   operator and no Ray pods**, and there is **no dashboard** behind it (that's why
   the spec fakes the dashboard with `httptest`). So the differentiator — submit a
   job → poll the *real* dashboard → stream *real* logs — is never exercised
   end-to-end without a real cluster.
2. **Per-task acceptance criteria are prose.** They need a runnable mapping
   (criterion → concrete test → how to run → expected → tier) so a task is
   provably done, not "looks done".

## 1. The five-tier test pyramid

Fastest → slowest. Tiers 1–4 run constantly (TDD loop + CI). Tier 5 is the
pre-push capstone.

| # | Tier | Substrate | Speed | When | Proves |
|---|------|-----------|-------|------|--------|
| 1 | **Unit** | fakes (`KubeRayPort` / `RayAPIPort` / fake dialer) | ms | every save, CI | domain logic: merge/diff, guards, distill, tier logic, pagination, namespace gate |
| 2 | **Adapter — KubeRay** | `envtest` (apiserver + etcd, KubeRay CRDs) | ~sec | `-tags envtest`, CI | SSA, `DryRunAll`, pruning prediction, k8s→domain error mapping. **CR storage only — no operator, no pods.** |
| 3 | **Adapter — Dashboard** | `httptest` | ms | every save, CI | dashboard REST client, byte-bounded logs, read-only guarantee (no submit/stop methods) |
| 4 | **MCP** | go-sdk in-memory transport | ms | every save, CI | tool schemas, arg validation, end-to-end tool calls — across **both stdio and HTTP** |
| 5 | **E2e ★** | **kind + real KubeRay operator + real Ray pods/dashboard** | ~min | `make test-e2e`, **pre-push** | the wedge for real: CR → operator reconciles → pods run → real dashboard → real logs |

**Key property:** tiers 2–4 are blind to the wedge end-to-end (no operator, faked
dashboard). Tier 5 is the only tier running a real reconciling cluster, so it is
the truth gate for the differentiator. It is the slowest and most environment-
dependent tier, so it is **not** in the per-save loop — it is a deliberate gate.

## 2. Per-task runnable test manifest

Every task carries a manifest: one row per acceptance criterion, mapping it to a
concrete test. Lives in `tasks/test-manifests/task-NN.md` (the `tasks/` tree is
git-ignored but always agent-readable).

Format:

| AC | Test (file::name) | Tier | How to run | Expected |
|----|-------------------|------|------------|----------|
| "list caps + paginates" | `internal/domain/cluster_test.go::TestListPagination` | unit | `go test ./internal/domain/...` | ≤50 rows + continue token; never silent truncation |
| "dryRun mutates nothing" | `internal/adapters/kuberay/apply_test.go::TestDryRunNoMutation` | envtest | `make test-envtest` | object count unchanged after call |

Rules:
- **Every acceptance criterion maps to ≥1 concrete test.** No orphan ACs.
- **Test-first.** The manifest is written as part of the task (failing test
  first, per spec §11 philosophy), not retrofitted.
- **Done = manifest all-green**, not "code exists".
- Manifests reference test tier so the right command runs them.

## 3. Real-cluster harness — kind + KubeRay

### Tooling decisions
- **Cluster tool: `kind`** (Kubernetes-in-Docker). Matches spec wording, used by
  KubeRay's own CI, cheap on Docker.
- **Build tags: `envtest` and `e2e` are separate tags.** The fast suite (tiers
  1, 3, 4) needs no tags and no Docker. Tier 2 is `-tags envtest`. Tier 5 is
  `-tags e2e`. This guarantees the per-save loop never requires Docker or a
  cluster.

### Make targets
- `make e2e-up` — `kind create cluster` + install KubeRay operator (Helm,
  pinned to the compiled KubeRay version) + wait-for-ready.
- `make test-e2e` — `go test -tags e2e ./...` against the running kind cluster
  (reads the kind kubeconfig).
- `make e2e-down` — `kind delete cluster`.
- `make e2e` — up → test → down in one shot.
- `make test-envtest` — `go test -tags envtest ./...` (tier 2, no kind).
- `make pre-push` — aggregate: unit + envtest + e2e. Run before pushing a
  cluster-touching task.

### Scaffold now, grow later
- A small task (**Task 4.5**, before Task 5) ships the make targets + **one
  trivial smoke e2e**: cluster boots, KubeRay CRDs register, the Task 4
  `ray_capabilities` skeleton connects to the **real** apiserver and reports the
  served KubeRay version.
- Each later cluster-touching task (5, 9, 16a, …) adds its own `-tags e2e` test
  as it lands. E2e coverage grows with the tools — no speculative fixtures.

### Pre-push discipline
- **Soft, not a hard hook.** Run `make pre-push` before pushing a
  cluster-touching task. Docs/WIP pushes are not blocked.
- A one-line reminder is added to the plan's Conventions.

### Version pinning (flaky-test killer)
- kind's K8s version, the KubeRay operator Helm chart, and the CRD bundle used
  by `envtest` **all pin to the same KubeRay version**, sourced from the CI
  matrix (Task 28). Keeps `envtest` (tier 2) and the real cluster (tier 5)
  behaviorally aligned.

## Open follow-ups (tracked, not blocking)
- **CI e2e job:** whether tier 5 also runs in GitHub Actions (kind-in-CI) or
  stays local-only is deferred to Task 28 (CI hardening). The pre-push gate
  stands regardless.
- **Node 20 action deprecation:** `actions/checkout@v4` / `actions/setup-go@v5`
  warn about Node 20 EOL — bump in Task 28.
