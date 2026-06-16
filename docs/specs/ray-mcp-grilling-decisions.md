# ray-mcp — Grilling Decision Log

**Date started:** 2026-06-13
**Companion to:** `2026-06-13-ray-mcp-design.md`
**Purpose:** Capture every question raised during the design grilling and the
decision taken, so they can be reviewed in one place and folded back into the
design spec.

**Status legend:** ✅ decided · 🔄 in progress · ⏳ not yet reached

---

## Research grounding (done before Q1)

Ran a deep-research pass (17 primary sources, 3-vote adversarial verification,
23/25 claims confirmed) on: *does a KubeRay-aware MCP server already exist, and
why not?*

**Findings:**
- **No official** Ray-project or Anyscale MCP server for KubeRay CRD lifecycle
  exists; **no dominant community one** either.
- **Anyscale deliberately chose a non-MCP path** for agent control — first-party
  tooling is "Agent Skills" (`SKILL.md` folders, GA ~Apr 2026). In Anyscale
  docs, MCP appears only as *a workload you host on Ray Serve*, never as a Ray
  control plane. The most-resourced potential incumbent walked the other way.
- **Existing Ray MCP servers hit the wrong layer:** rayatouille (Go, read-only,
  11 dashboard tools), ray-mcp-server / killerapp (Python, write-capable but via
  Ray *core* API `ray.init()`/`JobSubmissionClient`, not CRDs), ray-agent
  (manifest gen). All individual, near-zero stars, some archived/deleted.
- **Generic K8s MCP servers already CRUD Ray CRDs** via `apiVersion`+`kind`
  escape hatches — `containers/kubernetes-mcp-server` (Go, ~1k★) and
  `Flux159/mcp-server-kubernetes` (TS, ~1.4k★) — but with **zero Ray-aware
  semantics.** This is the main thing a new project must beat.
- **`apiserversdk`** (KubeRay v1.4.0+, alpha, beta targeted v1.6) is an
  importable Go **reverse-proxy** library exposing CRUD over all three CRDs.
  *Correction to the research framing:* it is a **server-side proxy**, not a
  client library, so it does not replace a typed CRD client.

**Why it doesn't exist yet:** MCP is new; Anyscale's commercial incentives point
at the managed platform, not an OSS control plane; the community took the easy
dashboard-API path; generic K8s MCP servers suppress the urgency. → The gap is
real and largely uncontested. **Decision: build it.**

---

## Q1 — Purpose & wedge ✅

**Question:** What is this *for*, and what is the wedge, given a ~1k★ generic Go
K8s MCP server already drives Ray CRDs?

**Options:** (a) serious "become the default OSS KubeRay MCP server"; (b)
personal/internal tool; (c) portfolio/learning exercise.

**Recommendation:** (a), with the wedge written in as a first-class section:
Ray-aware semantics a generic `apiVersion`+`kind` tool can't give an agent —
typed worker-group autoscaling, RayService zero-downtime rollout, job
submit→poll-to-terminal orchestration, live status/logs from the dashboard API,
and safety tiers tuned to Ray destructive ops.

**DECISION:** ✅ **(a) — become the default OSS KubeRay MCP server.** Highest
ambition; raises the bar on adoption, not just code quality. Wedge as above.

---

## Q2 — Survival posture & maintenance commitment ✅

**Question:** How does "default" survive a first-party (ray-project) entrant, and
how aligned with ray-project should the project be?

**Options:** (A) independent land-grab (speed + registries + momentum); (B)
upstream-alignment / donation play (clean-room Apache-2.0, KubeRay-native,
court ray-project, keep donation door open); blend.

**Recommendation:** (B) with (A)'s low-cost tactics layered on; be honest about
the ongoing maintenance "default" demands.

**DECISION:** ✅ **(B) + (A)'s low-cost tactics** — upstream-alignment /
donation play as the primary posture, with the cheap land-grab tactics (MCP
registry listings, discoverability) layered on as a hedge. User commits to
**long-term maintenance**; expects **contributors to join over time.** Implies:
Apache-2.0, KubeRay-native types/concepts/cadence, build so it reads like
something ray-project could adopt; also ship registry/discoverability tactics
since they cost little and hedge if alignment stalls.

**Coupled correction:** `apiserversdk` is a server-side reverse proxy, not a
client lib → the typed-CRD-client decision (#8) stands regardless. Its relevance
is strategic (the vector a first-party MCP server would likely use; a possible
deployment/alignment surface).

---

## Q3 — Client foundation (what "KubeRay-native" compiles against) ✅

**Question:** Decision #8 says "KubeRay typed client + controller-runtime." Pin
the three hidden sub-choices: which part of controller-runtime, which CRD type
source, and the create/update/dryRun mechanism.

**Recommendation & DECISION:** ✅
1. **controller-runtime *client package* (`pkg/client.New()`), NOT the
   manager/framework** — an uncached, direct-to-API-server typed client. We are
   a client, not a controller; no cache/reconcile/leader-election/webhooks.
2. **Depend on the KubeRay Go module** (`github.com/ray-project/kuberay/
   ray-operator/apis/ray/v1`) for typed CRDs + scheme. **This module pin IS the
   supported-version baseline** (feeds #12): typed fields = ceiling of curated
   params; `rawSpec` covers anything newer. This dependency is the literal
   embodiment of the wedge — the generic K8s MCP server can't do this.
3. **Server-Side Apply (`client.Apply`) + `client.DryRunAll`** for
   create/update/dryRun — clean fit for #11's server-side dry-run; field-manager
   + conflict handling for free.
4. **`apiserversdk` stays OUT of the client path** (it's a server-side proxy).
   Noted only as a future HTTP-deployment/alignment seam (alpha; beta ~v1.6).
5. **Action:** reword spec decision #8 to "controller-runtime client package" so
   nobody wires up a manager.

**Considered & rejected:** KubeRay's generated clientset (client-go style) —
leaner/more vanilla, but worse SSA + unstructured `rawSpec` merge ergonomics.

---

## Q4 — Version baseline, compatibility contract, runtime detection ✅

**Context / footgun:** With structural-schema CRDs, the K8s API server
**silently prunes fields the *installed CRD schema* doesn't know** — no error.
So "compatibility" is governed by the installed CRD schema, not a version string;
a newer field set against an older CRD vanishes and the agent thinks it worked.

**Sub-decisions & DECISIONS:**
1. **API group:** ✅ **`ray.io/v1` only. No `v1alpha1`** (legacy; supporting it
   dilutes the typed wedge for ~no adoption).
2. **Compile target:** ✅ **Latest stable GA KubeRay at first commit (v1.5.x as
   of 2026-06), bump deliberately per KubeRay release** as part of the Q2
   maintenance cadence. Not auto-latest (unstable for users), not ancient
   (defeats wedge). Module pin = ceiling of curated params; `rawSpec` covers
   anything newer.
3. **Runtime detection:** ✅ **Read the RayCluster CRD** (user accepted more
   privilege; it *strengthens* the wedge — pre-apply pruning prediction is
   Ray-aware safety the generic K8s MCP server can't do). Diverges from the
   original "exclude CRD read" lean. Accepted **with 3 refinements:**
   - **(3a) Tightly scoped privilege:** small ClusterRole, `get
     customresourcedefinitions` with **`resourceNames:
     [rayclusters.ray.io, rayjobs.ray.io, rayservices.ray.io]`** — not blanket
     CRD read. `resourceNames` works for `get`-by-name. README least-privilege
     story survives ("+ read 3 named Ray CRDs cluster-scoped").
   - **(3b) Honest reporting:** CRD read gives **schema truth** (valid fields →
     reliable pruning prediction — the real prize) but only **best-effort
     version** (report `app.kubernetes.io/version` label if installer set it,
     else served API version). `ray_capabilities` never fabricates a precise
     version it couldn't verify.
   - **(3c) Use-if-present, not required:** Helm ships the ClusterRole by
     default, but degrade gracefully if SA lacks it → `crdVersion: "unknown
     (insufficient RBAC)"`, fall back to read-back diff.

**Resulting layers:** pre-apply pruning prediction (CRD schema) **+** post-apply
ground-truth read-back diff (Q3) — complementary, both cheap, neither mandatory.

**Architecture impact:** shipped RBAC = **namespace Role + small
ClusterRole/Binding**; Helm chart ships both. README declares a **CI-tested
range** (e.g., "tested against KubeRay v1.3–v1.5"), not a runtime-derived promise.

---

## Q5 — `rawSpec` merge semantics + escape-hatch security boundary ✅

**Landmine:** "deep-merge" (#10) is under-specified, and the naive "build typed →
merge → unmarshal to typed → apply" pipeline **silently drops every field newer
than the compiled baseline**, destroying Q4's "rawSpec covers anything newer"
promise. Merge mechanics and the escape-hatch security boundary are the same
decision.

**(a) Unified apply pipeline — DECISION ✅** (reused by every
create/update/deploy tool; unifies Q3 SSA + Q4 pruning + #10 merge + #11 diff):
1. Curated params → typed `RayCluster` → marshal to JSON (**base**).
2. `rawSpec` (YAML|JSON) → JSON.
3. **RFC 7396 JSON Merge Patch**, rawSpec over base (rawSpec wins). **Arrays
   replace wholesale**, documented loudly ("set `workerGroups` in rawSpec → you
   own the whole list"). Predictability > merge-by-key cleverness.
4. **Identity guard:** merged result changing `name`/`namespace` away from
   tool-arg identity → **error**, not silent ignore.
5. Keep result **unstructured** (preserves newer-than-baseline fields — the
   wedge). Validation is **server-side**, not a client typed round-trip.
6. **Always `DryRunAll`** the unstructured object → API server validates vs the
   *installed* CRD schema → reveals pruning (Q4) → diff intent-vs-result.
7. If not dryRun: **SSA-apply** unstructured → read back → diff (#11). A typed
   unmarshal MAY run for diagnostic warnings only; must NOT drop applied fields.

**(b) Security boundary — DECISION ✅ bounded by RBAC, no in-server field
policy.** `rawSpec` reaches full PodTemplateSpec (privileged, hostPath,
hostNetwork, serviceAccountName, arbitrary images). Decision: **do NOT reinvent
PodSecurity in the server.** Self-hosted, per-user, bounded by the user's own
RBAC (#7 "RBAC is the floor"); pod-spec policy is the job of PodSecurity
admission / Kyverno / Gatekeeper.
- Ship a **`--allow-raw-spec` flag (default `true`)**; `false` drops the
  `rawSpec` arg from the schema entirely (curated-params-only hard mode for
  autonomous agents).
- Document the boundary: "rawSpec is a full escape hatch bounded only by RBAC +
  admission control; for autonomous agents use restrictive RBAC and/or
  PodSecurity admission, or `--allow-raw-spec=false`."

**Considered & rejected for v1:** built-in denylist of scary fields
(hostPath/privileged/hostNetwork/serviceAccountName). RBAC + admission is the
real floor; a built-in denylist would be a worse copy of PodSecurity admission.

---

## Q6 — Ray dashboard reachability + read-only boundary ✅

**Bug found:** #5 says "auto port-forward" as a constant, but reachability is
**deployment-dependent**: stdio/out-of-cluster needs SPDY port-forward;
in-cluster HTTP reaches head-svc directly via cluster DNS (port-forward is
pointless overhead + extra `pods/portforward` RBAC). Data-flow B was written for
the stdio case only.

**Unstated reality:** Ray dashboard / Job API has **no built-in auth** (basis of
"ShadowRay" RCE attacks — open Job API = unauthenticated RCE). The tunnel
terminates at an RCE-capable endpoint.

**DECISIONS ✅:**
1. **Reachability = strategy, auto-detected.** `RayReachability` port w/ two
   adapters: **DirectDial** (in-cluster) + **PortForward** (out-of-cluster).
   `--ray-access=auto|direct|port-forward`, `auto` detects in-cluster config.
   Rewrite data-flow B to branch. In-cluster needs no `pods/portforward` RBAC.
2. **Dashboard/Job API is strictly READ-ONLY** — live status + `ray_job_logs`
   only. Enforced **by construction**: `RayAPIPort` has **no write methods**.
   Every mutation goes through the guarded CRD path. The unauthenticated
   dashboard is never a write vector through this tool.
3. **Tunnel lifecycle:** one tunnel per `(namespace, cluster)` + idle-timeout
   reaper (not per-call ref-count/deferred-close). Re-dials on next use if
   dropped. #9 graceful degradation stays: reachability failure → CRD-derived
   status annotated "live Ray detail unavailable."
4. **Dashboard-auth seam, off by default** — optional token/header passthrough
   for orgs that front :8265 with an auth proxy.

**Tool-surface change:** ✅ **`ray_job_stop` DROPPED from v1** (deferred). It was
the only Ray-side write candidate; CRD `spec.suspend` semantics (heavier — can
tear down cluster) deferred rather than implemented now. **v1 RayJob tools:
`ray_job_list`/`get`/`logs` (read) + `ray_job_submit` (write) +
`ray_job_delete` (destructive).** Confirms zero Ray-side write surface in v1.
(`ray_job_submit` is still a write tool but creates a RayJob *CRD* → on the
guarded CRD path, consistent with the invariant.)

---

## Q7 — Transport scope: is in-cluster HTTP a v1 thing? ✅

**Separation:** transport *plumbing* is cheap (go-sdk gives stdio + streamable
HTTP free); the in-cluster HTTP *deployment* is expensive (bearer auth, insecure
footgun, TLS, Helm, NetworkPolicy, bigger threat model). **stdio is the
wedge-delivery vehicle** — every local MCP client speaks it; drop-in binary +
kubeconfig = adoption path to "default." HTTP = smaller "shared team instance"
scenario (non-goals already rule out hosted multi-tenant).

**DECISION ✅:** **Both transports in v1. stdio = primary, must-be-flawless**
(README quickstart leads with it; registry listings point at it; most test
coverage). **HTTP = secondary but its security is designed IN v1, not bolted on**
(a half-designed authenticated endpoint is worse than none). Docs honest about
"start here" (stdio) vs "shared team instance" (HTTP).

**Considered & rejected:** defer HTTP+Helm to v1.1. Guts the Helm chart's reason
to exist, contradicts goal #5, and the cheap part is the plumbing — the costly
part (auth/hardening) we're designing anyway, so deferral buys little.

---

## Q8 — HTTP auth model ✅

**Problems with #7 as written:** (1) `127.0.0.1` default contradicts HTTP's only
purpose (in-cluster shared instance — loopback is unreachable by other pods);
(2) `--insecure` (no-token) is a loaded gun on a network-reachable cluster-driver
+ autonomous agent; (3) a static bearer token has no identity/rotation/audit.
**Deeper point:** the token only gates *talking to the server*; the server then
acts with its **own SA's RBAC**, so every caller gets the union of their restraint
and the SA's power → **RBAC on the server SA is the real privilege boundary**
(consistent with #7); the token must be strong + non-optional on any non-loopback
bind.

**DECISIONS ✅:**
- **Kill `--insecure` no-token mode.** Invariant: **non-loopback bind ⇒ token
  mandatory, no bypass flag.** Process refuses to boot otherwise, with an
  explanatory error. Only tokenless case = loopback bind (OS gates it).
- **Default bind stays `127.0.0.1`**; binding non-loopback without a token (or
  TokenReview) = refuse to start.
- **Two token modes, both built in v1:** (default) **static bearer** via
  `--auth-token`/env — "shared secret, rotation is your job, actions
  attributable to the server SA"; (opt-in) **Kubernetes TokenReview** — callers
  present their own SA token, validated via TokenReview API → real caller
  identity, rotatable, idiomatic (the realized form of the spec's "OAuth 2.1
  future-work" seam; cheaper than OAuth for in-cluster).
- **v1 default = static bearer; TokenReview = built but opt-in.** Rationale:
  stdio (primary adoption path) has no HTTP auth at all, so v1 shouldn't gate on
  the complex mode — but TokenReview ships so serious in-cluster operators have
  the idiomatic option and the future-work seam is real, not vapor.
- **TLS:** push termination to ingress/mesh, document it; don't build TLS into
  the binary for v1.
- **Audit-log every mutating call:** caller identity (token fingerprint / SA
  username), tool, args summary, dryRun flag, outcome. Not polish — it's how you
  answer "what did the agent do."

---

## Q9 — Mutation interaction model (dryRun default + confirm + MCP annotations) ✅

**Gap:** spec never states `dryRun`'s default — but that default *is* the safety
posture. Also spec ignores **MCP tool annotations** (`readOnlyHint`/
`destructiveHint`/`idempotentHint`) which compliant clients use to auto-prompt
the human — free protocol-layer "safe by default."

**Models weighed:** (A) dryRun=false kubectl-like (weakest in-moment guard);
(B) dryRun=true preview-first ("safe by default" but **silent-non-action**
failure — agent sees "would create," believes done, never commits); (C)
confirm-token (diff + server token, 2nd call commits — robust, complex, server
holds pending state).

**DECISION ✅ — layered model, not one knob:**
1. **Tag ALL tools with MCP annotations** (regardless of other choices):
   `readOnlyHint:true` on read tools; `destructiveHint:true` on
   delete/destructive-scale; `idempotentHint:true` on SSA update/scale. Free
   client-side confirmation.
2. **`dryRun` defaults `false` for writes** — meaningful because of the layers
   around it. B's silent-non-action is genuinely dangerous (misleads autonomous
   agent); no K8s tooling defaults to preview (adoption intuition). Agent can
   pass `dryRun=true` to preview; Q5 pipeline runs server-side dry-run + diff
   internally on every apply anyway.
3. **Confirm-token (C) for the destructive tier ONLY** — `ray_cluster_delete`,
   `ray_service_delete`, destructive scale-downs return diff + token; commit
   requires echoing token. Cost justified only for irreversible/traffic-affecting
   ops. Plain writes (create/update/submit) don't need it.

**Considered & rejected:** dryRun=true everywhere — *feels* safer but
silent-non-action actively misleads an autonomous agent; worse than confirm-token
on the ops that actually matter.

---

## Q10 — `protected` annotation: real guard or theater? ✅

**Logic hole:** as written (`ray-mcp/protected=true` refuses delete "regardless
of flags") it's theater for the named threat — (1) the agent can **remove the
annotation** via `ray_cluster_update`/`rawSpec` then delete (two plausible
steps = real confused-agent failure mode); (2) it's enforced in *our server*,
not the cluster, so `kubectl` / generic K8s MCP / a 2nd instance ignore it.

**DECISION ✅ — keep it, close the loop, be honest:**
- **(a) Gate its own removal:** removing/changing `ray-mcp/protected` via any
  patch this tool issues is refused unless the **destructive confirm-token (Q9)**
  is presented. Turns the 2-step bypass into a deliberate confirmed act —
  without this, the guard is decorative.
- **(b) Document honestly:** a **guardrail against fat-finger / confused-agent
  deletion through this tool**, NOT a security control. "Defense against
  mistakes, not adversaries. Real protection is RBAC." Matches #7.
- **Noted, not done in v1:** `metadata.finalizers` would block deletion at the
  API-server level (works vs kubectl too) but risks wedging resources into stuck
  states — too easy a footgun for v1.

**Considered & rejected:** drop it entirely (rely on tier+confirm-token+RBAC).
Cheap once confirm-token machinery exists; "don't accidentally delete prod
RayService" is genuinely useful for an agent-driven tool, provided it's not
oversold as security.

---

## Q11 — Long-running ops (submit job / deploy service) ✅

**Tension:** MCP calls are synchronous req/response; Ray jobs run minutes–hours.
**Trap:** block-and-poll server-side → hits MCP client request timeout (~tens of
s) on real workloads → call dies, job keeps running, agent thinks it failed
(worst outcome — looks like orchestration, breaks on the long jobs that matter).

**Options:** (A) fire-and-return non-blocking; (B) block-and-poll (breaks on real
jobs); (C) MCP progress notifications (still timeout-bound, uneven client
support); (D) hybrid — non-blocking + bounded `wait` helper.

**DECISION ✅ — (D):**
1. **All long-running mutations non-blocking by default.** `ray_job_submit` →
   `{name, jobId-when-available, initialStatus}` immediately; `ray_service_deploy`
   → immediate rollout-pending status. Never block to completion.
2. **Bounded wait, capped well under client timeouts (~120s).**
   `ray_job_wait(name, waitSeconds≤120, until=running|terminal)` → current status
   + "reached" bool. Default `until=running` ("did it start or is it stuck
   Pending?"); waiting for terminal on a real job is pointless.
3. **Wedge = status distillation, NOT server-side polling.** `ray_job_get` /
   `ray_service_get` return *distilled, agent-actionable* status: phase,
   progressing-vs-wedged (e.g. "Pending: unschedulable, no GPU nodes" + pod
   event), terminal exit + logs pointer; RayService rollout phase + old/new serve
   health + cutover state. Generic K8s MCP hands raw `.status` YAML; we hand "stuck
   because no GPU nodes." **That distillation is the differentiation.**
4. **Document the agent loop** in README (submit → wait(running) → poll get →
   logs).

**Considered & rejected:** (B) block-to-completion — intuitive but a trap; times
out on real jobs, #1 way the tool would feel broken.

**Tool-surface change:** ✅ adds **`ray_job_wait`** (read-ish, bounded).

> **Correction (Review Round 2):** the wait cap logged here as ~120s was
> **inconsistent with this same question's "tens of seconds" client-timeout
> premise** (A1). Superseded → **≤30s** (well under a typical ~60s client
> timeout). The spec reflects 30s.

---

## Q12 — Namespace model vs. least-privilege RBAC ✅

**Tension:** #6 "multi-namespace" vs. #7/Q4 "namespace-scoped Role." A
Role+RoleBinding = one namespace; multi-ns = either N RoleBindings (explicit,
least-priv, manual) or a ClusterRole (broad, kills the least-priv story).
`--allow-all-namespaces` implies cluster-wide list → **requires** ClusterRole, so
the flag silently demands broad RBAC and disagrees with the shipped namespaced
Role. Also: in-cluster default ns should be the **pod's own namespace**, not the
literal `"default"`.

**DECISION ✅ — explicit two-mode, RBAC-honest:**
1. **Namespaced mode (default, the least-priv story):** operate in a configured
   namespace set; Helm ships **Role+RoleBinding per namespace** (templated list,
   default = release namespace). `ray_capabilities` reports served namespaces;
   tool call outside the set → clear "not configured for namespace X" *before*
   hitting the API.
2. **Cluster mode (opt-in):** `--allow-all-namespaces` → **ClusterRole+
   ClusterRoleBinding** (shipped but OFF by default). Only this enables
   cluster-wide list / arbitrary-ns access.
3. **Flag and RBAC must agree, fail loud:** startup **SelfSubjectAccessReview**
   determines actual scope; if `--allow-all-namespaces` set but SA lacks
   cluster-wide list → refuse to start (or loud-warn downgrade), not confusing
   per-call `Forbidden`s. Self-reported `ray_capabilities` matches reality
   (same honesty principle as Q4/Q8).
4. **Default namespace = pod's own namespace in-cluster** (SA-token ns file),
   fall back to `"default"` only for stdio/kubeconfig. `--default-namespace`
   overrides both.

**Considered & rejected:** always ship a ClusterRole and treat
`--allow-all-namespaces` as a pure app-level gate atop always-broad RBAC —
simpler Helm, but makes the least-privilege claim false by default (bad for
posture-B credibility). Chose real two-mode RBAC despite more templating.

---

## Q13 — Token economy of tool results (MCP-specific constraint) ✅

**Gap:** spec describes *what* tools return but ignores that every result is
consumed by an LLM against a context window — an MCP constraint absent from
CLI/REST. Failure modes in the current surface: `list` dumping full status for
40 clusters; `get` returning raw verbose CRD `.status`; "bounded tail" logs
bounded by lines not bytes (10K-line stack trace ≈ unbounded); huge
PodTemplateSpec diffs. Connects to Q11 — distillation is both intelligent *and*
compact.

**DECISION ✅ — token economy as a cross-cutting rule; aggressive defaults +
explicit escape hatches** (agent can ask for more; can't un-spend forced tokens):
1. **Two-tier verbosity:** `list` → tiny row (name, phase, ready replicas, age,
   1-line health), never full status; `get` → distilled view (Q11), not raw
   `.status`; **`verbose`/`raw` arg on `get`** (default off) for the full object.
2. **Every list paginates + caps:** hard default cap (~50) + `limit`/`continue`
   (reuse k8s `continue` tokens). "Showing 50 of 213, continue token X" —
   **never silently truncate** (CLAUDE.md "surface confusion").
3. **`ray_job_logs` byte/token-bounded**, not just lines: `tailLines` + hard byte
   ceiling (~10–20KB) + "truncated, N bytes omitted" marker; default to tail.
4. **MCP structured + text dual output** (go-sdk): lead with compact
   `structuredContent`; short human `text`; don't pretty-print giant YAML.
5. **Diffs summarized by default** ("3 fields changed: replicas 2→5, image X→Y,
   +annotation Z"); full structural diff behind `verbose`.

**No counter-position taken** — only knob was default aggressiveness; chose
aggressive.

---

## Q14 — Confirm mechanism: stateful token vs. stateless fingerprint ✅

**Self-correction:** Q9/Q10's confirm-token introduced **server-side pending
state with TTL** into an architecture that assumes a **stateless** server (added
by the grilling, not the original spec). Problems: in-memory map breaks across
HTTP replicas / rolling upgrades; needs TTL + single-use reaper; extra round-trip
some agents fumble.

**Insight:** a **content-derived fingerprint** gives the same guarantee
statelessly — destructive/protected-removal call returns diff +
`fingerprint = hash(resourceUID + resourceVersion + operation)`; agent commits
with `confirm=<fingerprint>`; server **recomputes from the live object and
compares** — no stored state. Because `resourceVersion` is included, a resource
changed between preview and commit → fingerprint mismatch → **stale confirm
rejected** (free TOCTOU / optimistic-concurrency protection; equivalent to a
`resourceVersion` delete precondition).

**DECISION ✅ — stateless content-derived fingerprint** for destructive ops (Q9)
+ protected-annotation removal (Q10). Server stays stateless → HTTP deployment
trivially horizontally scalable; gains stale-write rejection. **Add §4 invariant:
"server holds no cross-call state; confirmations are content-derived."**

**Considered & rejected:** server-issued random token — only advantage is
*guaranteeing the preview happened* (fingerprint is predictable, could be
computed without viewing the diff). Rejected: "force the LLM to look" is
unenforceable anyway; statelessness + TOCTOU are concrete wins over a soft
guarantee.

---

## Q15 — MCP SDK: official go-sdk vs. mark3labs/mcp-go ✅

**Tradeoff:** official `modelcontextprotocol/go-sdk` (canonical, tracks protocol
features first, but younger / possible pre-1.0 churn) vs. `mark3labs/mcp-go`
(community incumbent, more battle-tested, but could lag/diverge from canonical).
**Posture-B framing:** a donate-upstream project on the *official* SDK is far
more blessable — same "native/canonical foundation" logic as Q3's KubeRay types.

**DECISION ✅ — keep official `go-sdk` (#9 stands), justified by upstream
alignment**, with two accepted consequences:
1. **Pin exact version + budget for churn**; SDK fully quarantined behind the
   `internal/mcp/` edge (hexagonal arch already does this) → an SDK swap/upgrade
   touches only that package.
2. **Feature-existence dependency (OPEN, resolve at implementation start):**
   confirm the SDK today supports Q9 tool annotations
   (`destructiveHint`/`readOnlyHint`/`idempotentHint`), Q13 `structuredContent`
   dual output, and the §10 in-memory transport. If any are missing it changes
   either the SDK choice or those decisions. Reasoned from a Jan-2026 cutoff —
   not yet verified. Low-risk to defer due to edge isolation.

**Considered & rejected:** mark3labs/mcp-go — more mature now, but community
foundation weakens the upstream-donation story; maturity gap is quarantined to
one package anyway.

---

## Q16 — `ray_job_submit` ephemeral vs. existing cluster 🔄 SCHEMA RATIFIED, 2 SUB-QS OPEN

*(Grilling stopped here originally. Review Round 2 ratified the submit **schema**;
two behavioral sub-questions remain open — see the updated status summary and
spec §14 Q16a/Q16b.)*

**The fork:** KubeRay RayJob runs in two modes — **ephemeral**
(`spec.rayClusterSpec`: job creates its own RayCluster and, if
`shutdownAfterJobFinishes=true`, tears it down) vs. **existing-cluster**
(`spec.clusterSelector`: submits to a running RayCluster, never touches its
lifecycle). The spec's "cluster selector/spec" hides this.

**Why it matters:** (1) **`ray_job_delete` blast radius is mode-dependent** —
deleting an ephemeral job can cascade-delete a whole RayCluster (+ every other
job/actor on it); deleting an existing-cluster job just removes the record. Q9/Q14
currently treat `ray_job_delete` as one uniform thing. (2)
`shutdownAfterJobFinishes` is an agent footgun: default-off → orphaned
GPU-burning clusters (cost surprise); default-on → cluster torn down before
post-mortem (lost debugging). (3) Mode interacts with namespace/cluster
resolution.

**Recommendation (pending user decision):**
- (a) Explicit `mode` / one-of `existingCluster:<name>` XOR `clusterSpec:{curated
  + rawSpec}`; both-or-neither → error. Ephemeral reuses the **Q5 pipeline**
  (a job's `rayClusterSpec` *is* a cluster spec). **`ray_job_delete` becomes
  mode-aware**: ephemeral-with-cascade → destructive tier (confirm-fingerprint);
  existing-cluster → plain write. (Ray-aware safety the generic K8s MCP can't do;
  extends Q9/Q14.)
- (b) `shutdownAfterJobFinishes` default — lean **`true`** (prevent silent
  GPU-cost leak; surface "pass false to keep for debugging" in result). Flip to
  `false` if primary users are interactive debuggers.

---

## Status summary (updated Review Round 2, 2026-06-13)

**Q1–Q15 deltas are now FOLDED INTO the design spec.** The main spec
(`2026-06-13-ray-mcp-design.md`) was rewritten to incorporate every decided
question, then re-reviewed. This log is the *decision history*; the spec is the
*current source of truth*. Where they differ, the spec wins (the Q11 wait-cap
correction above is an example).

**Decided + folded into spec (✅):** Q1 purpose/wedge · Q2 posture+maintenance ·
Q3 client foundation · Q4 version baseline/detection · Q5 rawSpec merge + security
boundary · Q6 Ray reachability + read-only dashboard · Q7 transport scope · Q8
HTTP auth · Q9 mutation interaction · Q10 protected annotation · Q11 long-running
ops (wait cap corrected 120s→30s) · Q12 namespace model · Q13 token economy · Q14
stateless confirm fingerprint · Q15 MCP SDK.

**Q15 open item — CLOSED ✅ (verified 2026-06-13).** Official go-sdk confirmed
**GA v1.6.1 (2026-05-22)**; `ToolAnnotations{ReadOnlyHint, DestructiveHint,
IdempotentHint}`, `CallToolResult.StructuredContent`, `StdioTransport`,
`NewStreamableHTTPHandler`, typed `AddTool[In,Out]`, `NewInMemoryTransports()` all
present by exact name. No SDK risk. (Repo is "official, in collaboration with
Google" — not Anthropic-maintained; don't mislabel.)

**Q16 — schema RATIFIED 🔄, two sub-questions OPEN (need sign-off before impl):**
- **Q16a** `ray_job_delete` mode-aware tiering (ephemeral-cascade → destructive +
  confirm-fingerprint vs. existing-cluster → plain write). Couples to **B3**.
- **Q16b** `shutdownAfterJobFinishes` default (KubeRay default `false`; lean `true`).

**New findings from Review Round 2 (verification + structural pass):**
- **Verification (primary sources):** §13 facts hold. Codegen-critical: RayJob
  `jobId` is the only submission-id field (**no `submissionId`**) and **also
  exists as `spec.jobId`** — folded into spec §13. Autoscaler field path
  `spec.workerGroupSpecs[].scaleStrategy.workersToDelete` confirmed.
- **Fixed in spec this round:** A1 wait-cap 120→30s; A2 Q16 resolved/​open
  contradiction (schema ratified in §6, sub-qs scoped in §14); A3 dangling
  "R1/R8" refs removed; B4 **stdout/stderr stdio-logging invariant** added (§5);
  C1 two-phase wait (CRD→dashboard) made explicit in §7.A + mermaid; C2
  head-endpoint resolved from status not DNS-templating (§7.B).
- **OPEN decision branches surfaced this round (not yet ratified — recommend a
  short pass):**
  - **B1** confirm-fingerprint vs. autoscaler churn — `resourceVersion` in the
    hash will livelock confirm on busy autoscaling clusters; lean: delete uses
    `hash(UID+op)` (identity), reserve `resourceVersion` for scale/update.
  - **B2** Q5's unconditional `DryRunAll` partly obsoletes Q4's CRD-schema-read +
    its ClusterRole — reconsider whether that extra privilege earns its keep or
    demotes to optional capability-reporting.
  - **B3** "destructive" overloaded for *registration tier* vs. *runtime confirm*;
    scale-to-zero is `--allow-mutations`-gated but confirm-only — decide if it
    needs `--allow-destructive`. Couples to Q16a.
  - **C3** curated params thin for GPU Ray (no `rayStartParams`/`tolerations`/
    `nodeSelector`) → `--allow-raw-spec=false` hard mode unusable in the core GPU
    case; grow curated params or document the limit.
  - **C4** status-distillation (the wedge crown jewel) is under-specified — needs
    its own design note (inputs, RBAC, concrete examples).

**Not yet grilled (candidate future questions):**
- RayService zero-downtime **update** semantics — what the tool exposes, how
  rollout progress is distilled (pairs with Q11).
- `ray_cluster_scale` vs. `ray_cluster_update` overlap (fold or keep both?).
- Project's own versioning/release strategy (semver, KubeRay-compat matrix,
  breaking-change policy) — distinct from Q4's KubeRay baseline.
- Observability specifics (metrics, structured-log schema, audit-log correlation).
- Error-taxonomy completeness; how SSA field-manager `Conflict` surfaces.
- `ray_service_delete` "serving traffic" detection + missing `force` arg (D).

