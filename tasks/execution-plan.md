# ray-mcp — Execution Plan (research-grounded)

**Date:** 2026-06-16
**Status:** Task 1 complete & verified green. Ready to start Task 2 after pre-flight fixes.
**Companions:** `tasks/plan.md` (task definitions), `tasks/todo.md` (checklist).
**Source of truth:** `docs/specs/ray-mcp-design.md`.

This layer sits on top of `plan.md`/`todo.md`. It records what a 3-agent research
pass verified about the repo and the plan's external assumptions, the cheap
corrections to land *before* coding, and the concrete execution sequence with
per-task technical readiness notes. It does **not** redefine tasks — `plan.md`
remains the task spec.

---

## 1. Research verdict (what we checked)

**Current repo state (verified by running the toolchain):**
- Branch `task-1-scaffold`, HEAD `f69a285`. Working tree clean except untracked `docs/upstream/`.
- 8 Go files, all stubs: `cmd/ray-mcp/main.go` (empty `main`) + 7 `internal/*/doc.go`. All spec §5 packages present as stubs; no extra/missing packages.
- `go build ./...` ✅ · `go vet ./...` ✅ · `go test ./...` ✅ (no tests yet) · `go tool golangci-lint run ./...` ✅ **0 issues**, on Go 1.26.3.
- **Task 1 acceptance: MET** (all three ACs verified locally). One open item: confirm the GitHub Actions run is green on the branch (`gh run list`), and the runner has Go 1.26.3.
- `go.mod` has **zero direct requires** today — every dep is an indirect of the pinned golangci-lint tool. Runtime deps (MCP SDK, controller-runtime, KubeRay types, client-go) arrive in Tasks 4/5/14/15/23. Expect `go.mod` to grow sharply at Task 4.

**Plan ↔ spec consistency (verified):**
- All 19 tools in spec §6 map to a task; `ray_job_stop` correctly deferred. No task contradicts or overreaches the spec.
- **All 7 open decision gates are still genuinely open** (Q16a, Q16b, B1, B2, B3, C3, D; C4 resolved-by-building in Task 13). None was silently resolved by the grilling-decisions doc — the plan's gates are **not stale**.
- Two non-blocking wording nits: Task 21 deps should say **8b** (not "8"); plan.md:4's one-liner undersells the open-question count (the gate table below it is correct).
- Minor under-coverage to fold into existing tasks (no new tasks needed): `crdVersion` best-effort reporting → name it in Task 9; `--ray-dashboard-auth` passthrough → give it an AC in Task 15; `ray_capabilities` echoing the CI-tested KubeRay range → surface in Task 9/25 capabilities, not just Tasks 27/28.

**External technical assumptions (verified against pkg.go.dev / GitHub / source):**

| Assumption | Verdict | Note |
|---|---|---|
| MCP go-sdk **v1.6.1** (`NewInMemoryTransports`, `NewStreamableHTTPHandler`, stdio, structured+text) | ✅ VERIFIED | One naming nit (stdio constructor) — see §2. Latest release 2026-05-22. |
| KubeRay types `ray-operator/apis/ray/v1` (RayCluster/RayJob/RayService) | ✅ VERIFIED | KubeRay **v1.6.1** latest (2026-04-23). |
| controller-runtime client + SSA + `DryRunAll` | ✅ VERIFIED | **v0.24.1** latest. Caveats in §3 (Task 8a). |
| envtest + KubeRay CRDs | ✅ VERIFIED | Binary-assets gotcha — see §2 (Task 5/28). |
| "RFC 7386" JSON Merge Patch | ⚠️ **WRONG RFC #** | Correct is **RFC 7396**. `evanphx/json-patch/v5` (v5.9.11) provides it. |
| SPDY PortForward | ⚠️ NEEDS-ATTENTION | Modern default is **SPDY-over-WebSocket with bare-SPDY fallback**. client-go ≥ v0.34. |

None of these changes the dependency graph, task sizing, or the decision gates.

---

## 2. Pre-flight fixes — land BEFORE Task 2 (cheap, doc-only)

These are small, verified, and prevent an agent from following a dead path or
shipping a wrong citation in a "become-the-default" project. Recommend a single
small commit on the current branch (or a tiny `task-1.1-docs-fixups` branch).

1. **Fix the 3 broken spec paths** (`docs/superpowers/specs/2026-06-13-…` → `docs/specs/…`):
   - `tasks/todo.md:4` → `docs/specs/ray-mcp-design.md`
   - `tasks/plan.md:4` → `docs/specs/ray-mcp-design.md`
   - `tasks/plan.md:384` (Task 13 output file) → `docs/specs/ray-mcp-status-distillation.md`
2. **`RFC 7386` → `RFC 7396`** (JSON Merge Patch) — 10 occurrences:
   `tasks/todo.md:31`, `tasks/plan.md:40,259`, `docs/specs/ray-mcp-grilling-decisions.md:174`, `docs/specs/ray-mcp-design.md:141,167,316,389,446,634`.
3. **Plan wording nits** (optional, while you're in there): Task 21 deps "Tasks 8, **20**" → "Tasks **8b**, 20"; refresh plan.md:4's one-liner to acknowledge B1/B2/B3/C3/C4/D as open.
4. **Decide `docs/upstream/`**: commit it, gitignore it, or land it explicitly — it's currently untracked.
5. **Add a `.gitignore`** (no Go ignore present) before test/coverage/`bin/` artifacts appear in later tasks.

> These are *documentation/repo-hygiene* corrections. The substantive technical
> notes from the API verification (items in §3) get folded into the relevant
> task's PR as they come up — no need to rewrite plan.md now.

---

## 3. Per-task technical readiness notes (bake these in when you reach the task)

Verified specifics to save the implementing agent a round of trial-and-error:

- **Task 4 (skeleton / MCP):** stdio transport is `&mcp.StdioTransport{}` *directly* — there is **no `NewStdioTransport`**. `NewInMemoryTransports()` (tests) and `NewStreamableHTTPHandler` (Task 23) are correct as named. `CallToolResult` has both `Content []Content` and `StructuredContent any` — the spec's "structured + text dual output" matches the SDK 1:1.
- **Task 5 (kuberay read + envtest):** envtest does **not** bundle etcd/kube-apiserver. Pick a binary-asset strategy now — recommend **`setup-envtest`** + a `make envtest-assets` target (deterministic, cacheable in CI) over `DownloadBinaryAssets: true`. CI (Task 28) fails the first envtest run without this decided. CRD bundle for `CRDDirectoryPaths`: `ray-operator/config/crd/bases/ray.io_{rayclusters,rayjobs,rayservices}.yaml`, vendored/submoduled from the matching KubeRay tag (pin to the compiled KubeRay version).
- **Task 8a (merge/diff core):** the merged object must be **JSON-built from rawSpec+curated**, never round-tripped from a typed object into `Unstructured` (zero-value vs unset becomes ambiguous and breaks SSA field ownership). Add the **`null`-deletes-key** RFC 7396 semantic to the ACs alongside arrays-replace-wholesale — `evanphx/json-patch/v5` implements both; assert both.
- **Task 8b (apply orchestration):** controller-runtime — `client.Apply` exists since v0.22.0; equivalently `client.Patch(ctx, obj, client.Apply, client.FieldOwner("ray-mcp"), client.ForceOwnership)`. **Pick one and standardize.** `FieldManager` is `+required`; wrap once via `client.WithFieldOwner(c, "ray-mcp")`. Apply is always strict-validated (free), so `WithFieldValidation` is moot for the apply path.
- **Task 14 (reachability):** default to **WebSocket-tunneled SPDY with bare-SPDY fallback** (client-go's `portforward.NewFallbackDialer(SPDYOverWebsocket, spdy, httpstream.IsUpgradeFailure)` — what `kubectl port-forward` does), not SPDY-only. Apiservers ≥ 1.30 / Konnectivity proxies increasingly drop bare SPDY. Widen the fake dialer to exercise both the fallback and the re-dial-once path. One extra constructor, same M scope.
- **FYI (not in v1 scope):** KubeRay v1.5.1 added AuthOptions/AuthMode; v1.6.0 added RayCronJob. Possible future capabilities only.

---

## 4. Execution sequence

The 1→29 numbering in `plan.md` is one valid topological order. Recommended
execution, honoring the per-task `Dependencies` (the authoritative order):

**Convention:** one branch + PR per task (`task-NN-slug`), rebased on the phase
branch. A Checkpoint = review the phase's merged diff + run its verification.
A 🚪 gate hard-blocks its first governed task; if no human, proceed on the
documented lean and record it in the PR + spec (don't silently guess, don't
stall ungated work).

### Phase 0 — Foundation (Task 1 ✅ done)
1. **Pre-flight fixes** (§2) → small docs commit.
2. **Task 2** (config: parsing + bind/auth boot invariant) — depends on 1.
3. **Task 3** (domain ports, types, error taxonomy, fakes) — depends on 1; **unblocks the wedge track**.
4. **Task 4** (walking skeleton `ray_capabilities` over stdio) — depends on 2,3.
5. **🚦 Checkpoint A** (human): builds, stdio MCP call works, hexagonal seams real (`go list` import check).

### After Task 3 — two tracks (parallel if you have capacity)
- **Cluster track (one executor, sequential — shares `internal/mcp/*`, `internal/domain/*`):**
  Task 5 → 6 → 7 → **Checkpoint B** → 🚪**Gate 1 (C3, B2)** → 8a → 8b → 9 → 🚪**Gate 2a (B3)** → 10 → **Checkpoint C** → 🚪**Gate 2b (B1)** → 11 → 12 → **Checkpoint D**.
- **Wedge track (concurrent-agent-safe — disjoint packages, use git worktrees):**
  **Task 13** (distill design note — *human-approved gate*, schedule EARLY; gates 16/17/20), **Task 14** (reachability), **Task 15** (rayapi). These three touch `internal/domain/distill.go`, `internal/adapters/reachability/`, `internal/adapters/rayapi/` respectively — truly parallelizable.

### Convergence — the wedge ★
- **Task 16a** (`ray_job_get`/`ray_job_logs` two-phase happy path) — depends on {13,14,15,5}; **top schedule risk**.
- **Task 16b** (graceful degradation), **Task 17** (`ray_job_wait`).
- **🚦 Checkpoint E** (careful human review): **go/no-go on the differentiator.**

### Remaining phases (single executor, reorder-safe within phase)
- 🚪**Gate 3 (Q16b before 18, Q16a before 19)** → **Task 18** (job submit) → **Task 19** (job list/delete) → **Checkpoint F**.
- **Task 20** (service list/get) → **Task 21** (deploy/update) → 🚪**Gate 4 (D)** → **Task 22** (service delete) → **Checkpoint G**.
- **Task 23** (HTTP + static bearer) → **Task 24** (TokenReview + caller-identity into audit) → **Task 25** (scope reconcile) → **Checkpoint H**.
- **Task 26** (RBAC + Helm) → **Task 27** (README/docs) → **Task 28** (CI KubeRay matrix) → **Task 29** (versioning/release policy) → **Checkpoint I** (final human review before v1 tag).

### Critical-path callouts
- **Task 13 is front-loadable but gates 16/17/20** — start it early on the wedge track; if it slips, the wedge slips.
- **Task 8a/8b** is the most-reused, correctness-critical module — blocks all create/update/deploy.
- **Task 11 (guards)** blocks all destructive ops.
- **Task 16** is make-or-break; Checkpoint E is the project's go/no-go.

---

## 5. Decision gates needing a human (block the first governed task)

| Gate | Question | Lean | Blocks |
|---|---|---|---|
| **Gate 1 — C3** | curated params thin for GPU (no rayStartParams/tolerations/nodeSelector) | grow params, or document the `--allow-raw-spec=false` limit | Task 8a |
| **Gate 1 — B2** | unconditional `DryRunAll` may obsolete CRD-schema-read + its ClusterRole | demote schema-read to optional | Task 9, Task 26 |
| **Gate 2a — B3** | does scale-to-zero need `--allow-destructive`, or only runtime confirm? | separate the vocabulary | Task 10 |
| **Gate 2b — B1** | confirm-fingerprint w/ `resourceVersion` livelocks on autoscaling clusters | delete = `hash(UID+op)`; reserve `resourceVersion` for scale/update | Task 11 |
| **Gate 3 — Q16b** | `shutdownAfterJobFinishes` default | `true` + "pass false to keep for debugging" hint | Task 18 |
| **Gate 3 — Q16a** | mode-aware `ray_job_delete` (ephemeral cascade) | ephemeral→destructive+fingerprint, existing→plain write | Task 19 |
| **Gate 4 — D** | RayService "serving traffic" detection + `force` arg | add detection + `force` | Task 22 |

C4 (distillation under-specification) is resolved by *building* Task 13's design
note, not a yes/no gate. Each gate has an "if resolved against the lean →
re-scope" note in `plan.md`'s gate table that bounds a reversal.

---

## 6. Immediate next actions

1. Land the §2 pre-flight fixes (docs-only; ~1 small commit).
2. (Optional) Confirm CI is green on `task-1-scaffold` (`gh run list`) and decide the `docs/upstream/` + `.gitignore` hygiene items.
3. Start **Task 2** (config). In parallel, if running a team, kick off **Task 3** (it unblocks the wedge track), then front-load **Task 13**'s design note since it's a human-approved gate on the whole wedge.
