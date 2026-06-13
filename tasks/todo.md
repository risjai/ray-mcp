# ray-mcp — Task Checklist

Companion to `tasks/plan.md`. Source of truth: the design spec
(`docs/superpowers/specs/2026-06-13-ray-mcp-design.md`). Check tasks off as
acceptance criteria + verification pass.

**Conventions (see plan.md → Conventions):** Scope = effort+risk legend
(XS/S/M/L); test external behavior not internals; **one branch+PR per task**
(checkpoint review = the phase's merged PR diff + its verification); a 🚪 gate
**hard-blocks its first governed task** — if no human, proceed on the documented
lean and record it (don't silently guess, don't stall ungated work).

## Phase 0 — Foundation & Walking Skeleton
- [ ] **Task 1** — Repo scaffold, module, lint, CI skeleton (S)
- [ ] **Task 2** — Config layer: parsing + bind/auth boot invariants (M)
- [ ] **Task 3** — Domain ports, types, error taxonomy, fakes (M)
- [ ] **Task 4** — Walking skeleton: `ray_capabilities` over stdio (M) *(defers field-set report → re-homed to Task 9)*
- [ ] ✅ **Checkpoint A** — skeleton builds, stdio MCP call works, hexagonal seams real → human review

## Phase 1 — RayCluster read path
- [ ] **Task 5** — KubeRay adapter read methods + envtest harness (M)
- [ ] **Task 6** — `ray_cluster_list` + `ray_cluster_get` w/ token economy (M)
- [ ] **Task 7** — `ray_cluster_events` (S)
- [ ] ✅ **Checkpoint B** — read path end-to-end, token-bounded → human review

## 🚪 Decision Gate 1 (before Phase 2 / Task 8a)
- [ ] **C3** — grow curated params for GPU (`rayStartParams`/`tolerations`/`nodeSelector`) or document the `--allow-raw-spec=false` limit
- [ ] **B2** — keep CRD-schema-read + its ClusterRole, or demote to optional (DryRunAll covers pruning) — outcome shapes Task 9's field-set report + Task 26 RBAC

## Phase 2 — Apply pipeline + RayCluster write
- [ ] **Task 8a** — Merge + diff core, pure/no-cluster (RFC 7386 rawSpec-wins, identity guard, §10 diff) (M)
- [ ] **Task 8b** — Apply orchestration (DryRunAll → SSA → read-back diff) **+ mutation audit hook** (M)
- [ ] **Task 9** — `ray_cluster_create` end-to-end + pruning detection **+ delivers Task 4's deferred capabilities field-set** (M)
- [ ] 🚪 **Decision Gate 2a (before Task 10)** — **B3:** does scale-to-zero need `--allow-destructive`, or only runtime confirm?
- [ ] **Task 10** — `ray_cluster_update` + `ray_cluster_scale` (SSA, autoscaler-safe; **higher test risk — F13**) (M)
- [ ] ✅ **Checkpoint C** — non-destructive write path proven → human review

## 🚪 Decision Gate 2b (before Phase 3 / Task 11)
- [ ] **B1** — delete fingerprint = `hash(UID+op)`; reserve `resourceVersion` for scale/update (avoid autoscaler-churn livelock)

## Phase 3 — Destructive tier + stateless confirm
- [ ] **Task 11** — Confirm-fingerprint + protected-annotation guard (M)
- [ ] **Task 12** — `ray_cluster_delete` (destructive, end-to-end) (S/M)
- [ ] ✅ **Checkpoint D** — full RayCluster lifecycle → human review

## Phase 4 — The wedge ★ (highest value)
- [ ] **Task 13** — Status-distillation design note (**human-approved gate**) + pure `distill` module (resolves **C4**) (M) *(concurrent-agent-safe; critical-path — gates 16/17/20, schedule early)*
- [ ] **Task 14** — `RayReachability` adapter: DirectDial + pooled PortForward (M) *(concurrent-agent-safe)*
- [ ] **Task 15** — `RayAPIClient`: read-only dashboard REST (M) *(concurrent-agent-safe)*
- [ ] **Task 16a** — `ray_job_get` + `ray_job_logs`, two-phase happy path (M, **top schedule risk**)
- [ ] **Task 16b** — wedge graceful degradation (dashboard unreachable → annotated CRD status) (S/M)
- [ ] **Task 17** — `ray_job_wait` (bounded, two-phase) (S/M)
- [ ] ✅ **Checkpoint E** — the wedge works ★ → **go/no-go on the differentiator; careful human review**

## 🚪 Decision Gate 3 (Q16b before Task 18; Q16a before Task 19)
- [ ] **Q16b** — `shutdownAfterJobFinishes` default (`true` vs KubeRay's `false`)
- [ ] **Q16a** — mode-aware `ray_job_delete` (ephemeral cascade → destructive+fingerprint; existing → plain write)

## Phase 5 — RayJob write + list/delete
- [ ] **Task 18** — `ray_job_submit` (non-blocking, existingCluster XOR clusterSpec) (M)
- [ ] **Task 19** — `ray_job_list` + `ray_job_delete` (mode-aware) (M)
- [ ] ✅ **Checkpoint F** — full RayJob lifecycle → human review

## Phase 6 — RayService
- [ ] **Task 20** — `ray_service_list` + `ray_service_get` (distilled rollout) (M)
- [ ] **Task 21** — `ray_service_deploy` + `ray_service_update` (in-place vs zero-downtime swap, path-aware) (M)
- [ ] 🚪 **Decision Gate 4 (before Task 22)** — **D:** define "serving traffic" detection + add `force` arg
- [ ] **Task 22** — `ray_service_delete` (serving-traffic aware) (M)
- [ ] ✅ **Checkpoint G** — full RayService lifecycle → human review

## Phase 7 — HTTP transport + auth
- [ ] **Task 23** — Streamable HTTP transport + static bearer auth (boot invariant) (M)
- [ ] **Task 24** — TokenReview auth mode + feeds caller-identity into the (already-built) audit log (M)
- [ ] **Task 25** — Scope reconciliation (SelfSubjectAccessReview at boot; closes Task 4's *namespace* deferral) (S/M)
- [ ] ✅ **Checkpoint H** — HTTP + auth + multi-tenancy-safe → human review

## Phase 8 — Distribution & hardening
- [ ] **Task 26** — RBAC manifests + Helm chart (M)
- [ ] **Task 27** — README + docs (agent loop, RBAC, read-only default) (M)
- [ ] **Task 28** — CI hardening (KubeRay version matrix) (S/M)
- [ ] **Task 29** — Project versioning & release policy (semver, compat matrix, breaking-change) — or explicitly defer (S)
- [ ] ✅ **Checkpoint I** — v1 ready for review → final human review before tagging

---

### Open questions still needing a human decision (mapped to gates)
- **C3** (Gate 1), **B2** (Gate 1), **B3** (Gate 2a), **B1** (Gate 2b), **Q16b** (Gate 3), **Q16a** (Gate 3), **D** (Gate 4)
- **C4** resolved by Task 13. Each gate has an "if resolved against the lean → re-scope" note in plan.md's gate table.

### Deferred (post-v1, per spec §12) — tracked, not built
- GoReleaser multi-arch · distroless hardening · `ray_job_stop`/`spec.suspend` · OAuth 2.1 flow · multi-cluster
