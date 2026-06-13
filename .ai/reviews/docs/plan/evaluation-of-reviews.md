# Evaluation: do the two plan reviews make sense?

**Subject:** `.ai/reviews/docs/plan/doc-review.md` + `doc-feedback.md` (external reviews of `tasks/plan.md`)
**Method:** the ~40 raw comments were deduped into 22 canonical findings; each was verified twice — a grounded verdict that had to quote the actual plan, then an independent adversarial challenge that tried to flip it (44 agent checks total). All 22 verdicts held at high confidence.
**Date:** 2026-06-13

## Verdict: yes, the reviews make sense — they are substantively right but severity-inflated

- **0 of 22 findings were INVALID.** Every issue the reviewers raise points at something real in `tasks/plan.md`. Nothing was fabricated.
- **8 fully VALID, 14 PARTIALLY_VALID.** The partials are cases where the *observation* is real but the *claim overstates it* (alleges more than the text supports, or names consequences the plan already partly mitigates).
- **17 of 22 severities are overstated; 0 understated.** This is the headline: the reviews are well-calibrated on *what* is wrong and miscalibrated on *how bad*. Five of six `[HIGH]` flags are really MED-or-LOW doc-hygiene issues; only one HIGH (F14, audit-log timing) survives as genuinely structural.
- **Fix quality:** 7 sound, 10 partially-sound, 5 offered no fix. The suggested fixes are mostly reasonable but several inherit the same over-scoping as the finding.

So: trust the reviews as a **punch-list of real gaps**, but **re-triage the severities** — most are cheap doc edits, not the execution-blocking defects the `[HIGH]` markers imply.

## The findings that genuinely matter (act on these)

| ID | Issue | Verdict | Real severity | Action |
|----|-------|---------|---------------|--------|
| **F14** | Audit-log every mutation is a safety requirement, but it's only built at Task 24 (Phase 7) while mutations start at Task 9 (Phase 2) — and it's stdio-relevant, not just HTTP | **VALID** | **The one real HIGH** | Pull the audit-log hook into the apply pipeline (Task 8) / first mutation (Task 9), not Phase 7. |
| **F3** | Task 4 *defers* the `ray_capabilities` CRD field-set/pruning report; no later task re-homes it (Task 25 only closes the *namespace* deferral) — confirmed: grep finds no task delivering `crdVersion`/field-set | **VALID** | MED | Add the deferred field-set report to an explicit task (likely alongside Task 9 or Task 25). |
| **F8** | Project's own versioning/release strategy (semver, KubeRay-compat matrix, breaking-change policy) is in neither the plan nor the deferred list, despite the "become the default" ambition | **VALID** (partial) | MED | Add a Phase 8 task or consciously defer it in writing. |
| **F6** | No test *philosophy* stated ("test external behavior, not implementation details") despite test-heavy ACs | **VALID** | LOW-MED | One line pointing at spec §11's behavioral-testing stance. |
| **F5** | Checkpoints say "human review" but the execution/VCS workflow (branch/PR per task or phase; what review inspects) is never defined | **VALID** (partial) | MED | State the review unit once, up front. |
| **F4** | Decision-Gate model assumes a human is available mid-build; no fallback (block / proceed-on-lean / skip) if not — load-bearing for autonomous execution | **VALID** (partial) | MED | Add one line: gates hard-block, or proceed-on-documented-lean. |
| **F7** | No re-scope path if a gate resolves *against* its lean (esp. B1: keeping `resourceVersion` changes Task 11 materially) | **PARTIALLY_VALID** | LOW-MED | One "if rejected → re-scope to X" line per high-impact gate. |
| **F9** | `Scope: S/M/L` used on all 28 tasks, never defined (no legend) | **VALID** | LOW (reviewer got severity right) | Add a one-line key. |
| **F15** | Task 6 `Dependencies: Tasks 5, 6-prereqs` — "6-prereqs" undefined + self-referential | **VALID** | LOW (typo) | Fix to "Tasks 3, 5". |
| **F17** | Decision Gate 4 is an inline footnote inside Task 22's body; every other gate is a standalone section before its phase | **VALID** | LOW | Promote to a standalone gate (todo.md already does this). |
| **F18** | Task 13 design-note "reviewed" — by whom? It resolves C4 and feeds the wedge but is the lone open item with no human gate of its own | **VALID** | MED | Name the approver / add a human gate on the note. |

## The findings the reviewers OVERSTATED (real kernel, inflated claim)

- **F1 / F2 (both `[HIGH]`) — the biggest miscalibration.** The reviews allege the plan's "four views of dependencies disagree on B2, B3, **and Task 13**," and that gates "land too late." Verified reality: **only the B3 gate is genuinely misplaced** (the gate table + Task 10's own `Dependencies` line say "resolve before Task 10," but the Decision Gate 2 block is physically placed *after* Task 10, labelled "before Phase 3"). The **B2** allegation is wrong (B2's "resolve before" is Task 9, not Task 4; Task 4 *defers* the field-set, so Gate 1 before Phase 2 protects it) and the **Task 13** "role/numbering disagreement" is **flatly wrong** — graph, gate table, heading, and todo.md all agree. So two `[HIGH]`s collapse to a single one-line gate-relabel. **Fix:** move/relabel Decision Gate 2 to "before Task 10."
- **F10 (`[HIGH]`) — wedge sizing.** Real point: Task 16 is the highest-integration task (CRD + reachability + dashboard + distillation + two-phase poll + degradation) yet sized "M" like a CRUD slice. But "M" is defensible *because* the hard parts are factored out into Tasks 13/14/15 (the adapters + the distill module) that Task 16 only wires together. Worth re-sizing or decomposing, not a blocker.
- **F20 / F21 — wedge-first vs foundations-first.** Real gap: the plan never *weighs* a wedge-first ordering or lists "differentiator validated late" as a risk. But the reviewers overstate "delays the wedge / before ANY wedge work": the wedge **adapters (13/14/15) are explicitly parallel-safe from Task 4**, and Task 16 depends on `{13,14,15,5}` — *not* on the full RayCluster lifecycle (Tasks 8–12). So the differentiator is reachable at Checkpoint E without finishing CRUD. Add a sentence acknowledging the tradeoff; the order itself is sound.
- **F11, F12, F13, F16, F22** — all PARTIALLY_VALID, all overstated. Each has a true kernel (Task 13 is critical-path yet labelled "parallel/side"; Task 8 apply-pipeline is the correctness keystone and could split merge/diff out; Task 10's envtest autoscaler-fake is fiddlier than its M peers; "parallel-safe" doesn't distinguish reorder-for-one-agent vs concurrent-agents-need-worktrees; parallelism read as afterthought) — but each names a consequence the plan already partly addresses.

## What this says about acting on the reviews

1. **Do the cheap edits now** — they're real and trivial: fix the `6-prereqs` typo (F15), add the S/M/L legend (F9), promote Gate 4 to a standalone section (F17), relabel Gate 2 to "before Task 10" (the only true F1/F2 defect).
2. **Close the two real gaps** before execution: re-home the deferred capabilities field-set (F3) and pull the audit-log hook earlier (F14 — the one finding whose HIGH is deserved).
3. **Add the missing meta-statements** (each one line): test philosophy (F6), execution/VCS review unit (F5), decision-gate fallback (F4), gate-rejection re-scope (F7), and a wedge-first-tradeoff acknowledgement (F20). Decide versioning policy in or out (F8).
4. **Don't over-react** to the `[HIGH]` markers on F1/F2/F10/F20/F21 — they read as alarming but are, on verification, low-to-medium doc-hygiene and "say-the-tradeoff-out-loud" items, not execution-blockers.

**Bottom line for the user:** the reviews are worth taking seriously — they're accurate and caught real omissions — but read them with the severities re-graded. The plan is sound in shape; it needs ~10 small edits and 2 genuine structural fixes (audit-log timing, deferred-field-set re-homing), not a rework.
