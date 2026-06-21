# Status Distillation — Design Note (closes C4)

**Status:** Draft · **Closes:** grilling-log **C4** ("status-distillation is
under-specified — needs its own design note: inputs, RBAC, concrete examples").
**Grounds in:** Q11 (long-running ops / distillation is the differentiation),
design §7.A (the wedge), §10 (token economy + error taxonomy), §6 (tool surface).
**Precedes:** the architecture review's Candidate 1 (name the distillation seam
*before* RayJob lands, while exactly one implementation exists).

---

## 1. What distillation is, and why it's the moat

A generic Kubernetes MCP hands the agent raw `.status` YAML. ray-mcp hands it one
agent-actionable line: **"Pending: unschedulable, no GPU nodes"** instead of a
condition array the agent must re-derive every call (Q11 §3). That translation —
*typed CRD status (± live Ray API status) → a distilled, bounded, agent-readable
view* — is the soft-moat crown jewel. It is the reason the read tools exist.

Today it lives, correctly and deeply, as unexported helpers in
`internal/adapters/kuberay/cluster.go` (`clusterPhase`, `clusterHealth`,
`healthDetail`, `condReason`, `clusterAge`, `dashboardURL`). It is right *for
RayCluster*. The risk C4 names is that when `RayJob` and `RayService` land, the
natural move is a fresh `job.go`/`service.go` that re-grows the same shape by
hand, spreading the moat policy across three files with no single review surface.

This note specifies the distillation **contract** (inputs → output, per kind) so
that the seam introduced for Candidate 1 is shaped by the real cross-kind
variation, not guessed from the single RayCluster case.

---

## 2. The distilled output (the shared vocabulary)

Every kind distills to the same conceptual shape — already partly present in
`internal/domain/types.go` (`ClusterSummary`/`JobSummary`/`ServiceSummary` all
carry `Phase`/`*Status` + a 1-line `Health`). This note makes the **wedged
signal** first-class, because "is it progressing or stuck, and why" is the part
that is the differentiation and the part most likely to drift per kind.

| Field | Meaning | Bound |
|---|---|---|
| **Phase** | human lifecycle phase derived from the condition ladder (not raw `.status.state`) | one short enum-ish string |
| **Ready / Desired** | progress counters (workers ready, serve replicas healthy) | ints; never re-summed from spec — operator-computed |
| **Wedged** | bool: is this stuck rather than progressing? | — |
| **WedgeReason** | when wedged, the one actionable cause (+ the triggering event) | ≤ ~1 line; e.g. `"unschedulable: insufficient nvidia.com/gpu"` |
| **Health** | the composed 1-line glance: `phase; counts; reason?` | ≤ ~1 line (current `clusterHealth` shape) |

`Health` is a *composition* of the others, not a fourth independent source of
truth. The full object stays behind `verbose`/`raw` (§10); distillation never
replaces the escape hatch, it is the default that saves the agent's tokens.

> **Note — `Wedged`/`WedgeReason` are new.** Today RayCluster folds "stuck" into
> the `Health` string via `healthDetail`. Promoting it to a typed field is the
> one *additive* domain change this note recommends, and only because all three
> kinds need it and Q11 calls it out by name. It is not required to ship
> RayCluster; it is required to keep the moat coherent across kinds.

---

## 3. Inputs, per kind (the C4 "inputs" gap)

Distillation is **not uniform across kinds** — this is the core finding, and the
reason a single shared function would be wrong. The shared part is the *health-line
composition and the condition-reason extraction*; the *sources* and the *wedge
heuristics* differ.

### 3.1 RayCluster — single source: the CRD (built, today)
- **Source:** `RayCluster.status` only. No Ray dashboard API involved.
- **Phase:** condition-TYPE ladder (`RayClusterSuspended` → `Suspending` →
  `ReplicaFailure` → `HeadPodReady`+`Provisioned` → `Provisioning`), falling back
  to the deprecated `.status.state`, then `Unknown`. (Switches on type, not reason
  strings — reasons shift between minor releases. See `clusterPhase`.)
- **Counters:** `status.readyWorkerReplicas` / `status.desiredWorkerReplicas`
  (worker-only; head excluded — operator-computed, not re-summed).
- **Wedge heuristic:** `ReplicaFailure=True`, or `Provisioned` present-but-not-True
  for too long → wedged; reason from the condition message, enriched by the most
  relevant pod event (e.g. `FailedScheduling`).

### 3.2 RayJob — TWO sources: CRD **then** live Ray API (the two-phase wedge read)
This is the case that breaks any "one status struct in" assumption.
- **Source 1 — CRD:** `status.jobDeploymentStatus` (lifecycle), `status.jobId`,
  `status.rayClusterName`, `status.dashboardURL` (already modeled in
  `domain.JobDetail`).
- **Source 2 — Ray dashboard API:** `GET /api/jobs/{jobId}` → `domain.RayJobStatus`
  (PENDING/RUNNING/SUCCEEDED/FAILED/STOPPED + message), but **only after**
  `status.jobId` is populated.
- **Two-phase rule (design §7.A):** while `status.jobId` is empty → return
  *"job not yet scheduled"* from the CRD alone, **no dial**. Once present → dial
  the dashboard and distill the live status.
- **Graceful degrade (§10):** dashboard unreachable → return the CRD-derived view
  annotated *"live Ray detail unavailable: <why>"* (the `RayAPIUnreachableError`
  reason), never a hard failure.
- **Wedge heuristic:** `jobId` empty for too long, or Ray status `PENDING` with an
  unschedulable head/worker pod → `"Pending: unschedulable, no GPU nodes"`.

### 3.3 RayService — single source: the CRD, rollout-shaped
- **Source:** `RayService.status` — serve status, rollout/cutover phase, healthy
  serve replicas (modeled in `domain.ServiceDetail.RolloutPhase` + `ServiceSummary`).
- **Phase:** rollout phase (pending → rolling out → cutover → running) + old/new
  serve health.
- **Wedge heuristic:** new serve app `UNHEALTHY` past a threshold, or rollout
  stalled mid-cutover → wedged with the failing application/deployment named.

**Implication for the seam:** the *health-line composer* and *condition-reason
extractor* are genuinely shared (kind-agnostic string work); the *source plumbing*
and *wedge predicate* are per-kind. The seam must let the per-kind half vary
without re-growing the shared half.

---

## 4. RBAC (the C4 "RBAC" gap)

Distillation reads only — it never widens the write surface. What it *consumes*,
and therefore what the served-namespace `Role` (or `ClusterRole` in cluster mode,
Q12) must grant:

| Read | Verbs | Resources |
|---|---|---|
| Cluster/Job/Service phase + counters | `get`, `list`, `watch` | `rayclusters`, `rayjobs`, `rayservices` (`ray.io` group) |
| Status subresource (where served separately) | `get` | `…/status` |
| Wedge-reason event enrichment (`ray_*_events`, the pod-event half of the heuristic) | `get`, `list` | `events` (core), `pods` (to map a cluster→its pods) |
| RayJob live status (the wedge) | — | **no K8s RBAC** — it is an HTTP call to the dashboard, gated by *reachability*, not RBAC |

Two boundaries worth stating so they are not conflated:
- **The dashboard dial needs no RBAC verb**; it needs network *reachability*
  (DirectDial in-cluster vs SPDY PortForward out-of-cluster, Q6). PortForward
  itself needs `pods/portforward` `create` — but that belongs to the reachability
  adapter's RBAC, not distillation's.
- **Forbidden during distillation degrades, it does not crash.** A missing
  `events`/`pods` grant must drop the wedge-reason enrichment to "reason
  unavailable (RBAC)" and still return phase+counters — the same graceful-degrade
  discipline as an unreachable dashboard. It must never turn a `get` into a raw
  `Forbidden` leak (now centralized in `mapDomainError`, Candidate 2).

Boot-time scope reconciliation (`--allow-all-namespaces` vs the SA's actual
cluster-wide list grant) is **Q12's SelfSubjectAccessReview**, not this note —
distillation assumes the scope is already validated.

---

## 5. Concrete examples (the C4 "concrete examples" gap)

Each row: raw status in → distilled `Health` (+ `Wedged`) out. These double as
golden-test fixtures for the seam.

**RayCluster — healthy:**
- in: `HeadPodReady=True`, `Provisioned=True`, ready/desired = 2/2
- out: `Ready; 2/2 workers ready` · `Wedged=false`

**RayCluster — wedged on GPU:**
- in: `Provisioned=False (reason=…)`, ready/desired = 0/2, pod event
  `FailedScheduling: 0/3 nodes insufficient nvidia.com/gpu`
- out: `Provisioning; 0/2 workers ready; unschedulable: insufficient nvidia.com/gpu`
  · `Wedged=true`, `WedgeReason="unschedulable: insufficient nvidia.com/gpu"`

**RayJob — not yet scheduled (phase 1, no dial):**
- in: `status.jobId=""`, `jobDeploymentStatus=Initializing`
- out: `job not yet scheduled` · `Wedged=false` (no dashboard call made)

**RayJob — running (phase 2, dialed):**
- in: `jobId=raysubmit_abc`, dashboard `GET /api/jobs/abc` → `RUNNING`
- out: `Running` · `Wedged=false`

**RayJob — dashboard unreachable (graceful degrade):**
- in: `jobId=raysubmit_abc`, dial → `RayAPIUnreachableError("connection refused")`
- out: `<CRD jobDeploymentStatus>; live Ray detail unavailable: connection refused`
  · `Wedged=false` (not knowable; degraded, not failed)

**RayService — rollout stuck:**
- in: new serve app `UNHEALTHY`, rollout mid-cutover past threshold
- out: `RollingOut; new serve UNHEALTHY: <app>` · `Wedged=true`

---

## 6. The seam (the architectural half — Candidate 1)

Goal: **one navigable home** for "how ray-mcp turns Ray status into an
agent-actionable line," so a reviewer reads one module, not three; and the RayJob
task *reuses* the shared half instead of re-typing it.

Constraint that fixes the shape: the typed-status reading (KubeRay condition
constants, `RayJobStatus`) **must stay in the adapter** — the domain imports no
Kubernetes packages (verified: `internal/domain` is stdlib-only). So the split is:

- **Domain (`internal/domain`):** owns the *vocabulary*, not the extraction.
  - Promote `Wedged bool` + `WedgeReason string` onto the summary types (§2).
  - A pure, kind-agnostic **health-line composer** — the I/O-free `strings.Join`
    of `phase; counts; reason?` plus the `condReason` reason-formatting. This is
    the genuinely reusable core; it imports nothing.
- **Adapter (`internal/adapters/kuberay/distill.go`, new):** the shared
  condition-ladder + reason-extraction helpers currently inline in `cluster.go`,
  moved to one file, plus one thin per-kind extractor (`clusterDistill`,
  `jobDistill`, `serviceDistill`) that reads that kind's typed status and calls
  the domain composer. The per-kind *wedge predicate* lives with its extractor.

```
domain:        Distilled vocabulary (Phase·Ready/Desired·Wedged·WedgeReason·Health)
               + pure HealthLine composer  ──────────────┐  (stdlib only)
                                                          │ called by
kuberay/distill.go:  shared condition-ladder · condReason · ageGuard
                          ▲              ▲              ▲
                     clusterDistill  jobDistill   serviceDistill
                     (CRD only)   (CRD→dashboard,  (CRD, rollout)
                                   two-phase+degrade)
```

**Deletion test:** delete `clusterHealth`/`healthDetail`/`condReason` today and
the logic reappears in every caller and every future kind → they earn their keep.
The seam's job is to make jobs/services *reuse* them, not re-grow them.

### Discipline guardrails (so this stays honest, not speculative)
- The repo's CLAUDE.md forbids abstractions for single-use code. The shared
  composer is multi-use **the moment RayJob lands**; this note recommends naming
  it then, or now-as-part-of-C4 *only* because the deletion test passes and C4
  explicitly asks for it. The risk of naming at n=1 is designing the abstraction
  against one shape — §3 exists precisely to enumerate all three shapes first, so
  the composer is shaped by real variation.
- Do **not** build a kind registry / dispatch table / "distillation DSL." Three
  named extractors calling one composer is the whole design. One extractor + a
  hypothetical interface is the anti-pattern to avoid.
- The two-phase RayJob orchestration (CRD → dashboard, graceful degrade) is
  **fake-testable today** against `RayAPIPort`/`RayReachability` fakes, before any
  tunnel code exists (review Candidate 3, testing-strategy §11). Prove the domain
  wedge logic there.

---

## 7. Open questions to resolve when RayJob lands
- **Wedge thresholds:** "too long" for `jobId` empty / `Provisioned` not True —
  fixed duration, or surfaced as-is and left to the agent? (Lean: surface the age,
  no magic timer in v1.)
- **Where the pod-event enrichment runs:** inside `distill`, or composed by the
  service from a separate `Events` read? (Lean: service composes — keeps `distill`
  a pure status→line function and the event read independently testable.)
- **`Wedged` for RayService cutover** vs. a richer rollout sub-state — does one
  bool suffice, or does rollout need its own small enum? (Defer to the RayService
  task; do not pre-model.)
