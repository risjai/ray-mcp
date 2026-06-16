# AGENTS.md — ray-mcp

A Go MCP server for managing Ray on Kubernetes via KubeRay CRDs (the guarded
write path) plus a read-only reach into Ray's dashboard/job API for runtime
detail CRDs don't expose (the "wedge" — the project's differentiator).

This file is a **pointer + guardrail**, not a duplicate of the planning docs.
When they conflict, the **spec wins**.

## Read these first (source of truth, in order)
- `docs/specs/ray-mcp-design.md` — the design spec. Authoritative.
- `docs/specs/ray-mcp-grilling-decisions.md` — decisions log (Q1–Q16, B/C/D branches).
- `tasks/plan.md` — the 29-task implementation plan (per-task ACs + verify command).
- `tasks/execution-plan.md` — research-grounded execution order, decision gates, per-task readiness notes.
- `tasks/todo.md` — the task checklist / current status.

## Load-bearing invariants (easy to violate, real consequences)
- **stdio invariant** — under stdio transport, **stdout IS the JSON-RPC wire**. All
  logs/audit go to **stderr or a file, never stdout** (spec §5). A stray `fmt.Println`
  corrupts the protocol.
- **Hexagonal imports** — `internal/domain` imports **no Kubernetes or HTTP** packages;
  it depends only on the port interfaces (`KubeRayPort`, `RayAPIPort`, `RayReachability`).
  Verified by an import check at Checkpoint A (spec §5).
- **Dashboard is read-only by construction** — `RayAPIPort` has no submit/stop methods (spec §6, Q6).
- **All mutations via Server-Side Apply preceded by `DryRunAll`**; rawSpec wins over curated
  params via RFC 7396 JSON Merge Patch; merged object stays unstructured (spec §7).
- **Stateless server** — confirmations are content-derived fingerprints, no cross-call state (spec §8).

## Workflow conventions
- **One branch + PR per task** (`task-NN-short-slug`). Never commit straight to `main`.
- **Decision gates (🚪)** hard-block their first governed task. If no human, proceed on the
  documented "lean", record it in the PR + spec — don't silently guess, don't stall ungated work.
- **Commit/push only when asked.** Note: the repo's `.github/workflows/ci.yml` means pushes
  need a token with `workflow` scope.

## Build / test
- Go module `github.com/risjai/ray-mcp` (Go 1.26.3). Targets: `make build | test | lint | vet | tidy`.
- **Test external behavior through public interfaces, not internals** (spec §11).
  Substrate per layer: fakes (domain) · `envtest` (KubeRay adapter) · `httptest` (dashboard) ·
  go-sdk in-memory transport (MCP). A real-cluster e2e tier is being designed — see the testing
  strategy doc once it lands.
- A task is "done" only when its behavioral ACs are proven green, not when code exists.
