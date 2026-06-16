# Contributing to ray-mcp

Thanks for contributing! ray-mcp is a Go MCP server that manages Ray on
Kubernetes via KubeRay CRDs (the guarded write path) plus a read-only reach into
Ray's dashboard/job API (the "wedge"). This guide covers **how to test your
change before you raise a PR** — please read the Testing section before adding a
new tool.

## Prerequisites

- **Go 1.26.3** (see `go.mod`).
- For the optional real-cluster tests: **Docker** and **kind**
  (`brew install kind` on macOS), plus `helm` and `kubectl`.

The fast test loop needs **none** of the cluster tooling — only Go.

## Project layout (hexagonal)

```
cmd/ray-mcp/            # entrypoint: flag parsing, wiring, transport selection
internal/config/        # flag/env config + boot invariants
internal/domain/        # ports (interfaces), DTOs, errors — NO k8s/http imports
internal/mcp/           # tool registration, schemas, arg<->DTO mapping (go-sdk)
internal/adapters/
  kuberay/              # controller-runtime client (SSA + DryRunAll)
  rayapi/               # dashboard/job REST client — READ-ONLY by construction
  reachability/         # DirectDial + PortForward strategies
internal/transport/     # stdio (and later HTTP)
test/e2e/               # real-cluster (kind + KubeRay) smoke tests, -tags e2e
hack/                   # version pins
```

### Invariants you must not break

1. **`internal/domain` imports no Kubernetes or HTTP packages.** It depends only
   on the port interfaces. Enforced in CI by an import check.
2. **stdout is the JSON-RPC wire under stdio.** All logs/diagnostics go to
   **stderr or a file, never stdout** — a stray `fmt.Println` corrupts the
   protocol. There's a test that asserts this.
3. **The Ray dashboard API is read-only by construction.** `RayAPIPort` has no
   submit/stop methods. Every mutation goes through the guarded CRD path.
4. **The shipped binary must not link controller-runtime / client-go / k8s.io**
   from test-only code. Those deps exist in `go.mod` for the envtest tier but
   must stay reachable only from `_test.go` files. CI checks this.

## The five-tier test pyramid

| # | Tier | Substrate | Needs Docker? | Build tag | Run with |
|---|------|-----------|---------------|-----------|----------|
| 1 | **Unit** | in-memory fakes | no | none | `make test` |
| 3 | **Dashboard** | `httptest` | no | none | `make test` |
| 4 | **MCP** | go-sdk in-memory transport | no | none | `make test` |
| 2 | **KubeRay adapter** | `envtest` (apiserver + etcd, no operator) | **no** | `envtest` | `make test-envtest` |
| 5 | **E2e** | **kind + real KubeRay operator + pods** | **yes** | `e2e` | `make e2e` |

Tiers 1/3/4 run on every save and in CI with plain `go test ./...` — no Docker,
no downloaded binaries. Tier 2 needs the envtest binaries (auto-downloaded by
`make test-envtest`) but **no Docker**. Tier 5 is the only tier with a real
reconciling cluster — it's the pre-push capstone, not part of the per-save loop.

**Why tier 5 matters:** tiers 2–4 are blind to the wedge end-to-end. `envtest`
stores a `RayCluster` CR but runs no operator and no pods, and the dashboard is
faked. Only kind + real KubeRay proves "submit a job → poll the real dashboard →
stream real logs". See `docs/specs/ray-mcp-testing-strategy.md` for the full
rationale.

## Test commands

```sh
make test          # tiers 1/3/4 — fast, no Docker, no binaries. Run constantly.
make test-envtest  # tier 2 — downloads envtest binaries + KubeRay CRDs, no Docker.
make e2e           # tier 5 — kind up -> test -> down. Needs Docker + kind.
make e2e-up        # just stand up the kind cluster + KubeRay operator.
make test-e2e      # run -tags e2e tests against an already-running cluster.
make e2e-down      # tear the kind cluster down.
make pre-push      # all runnable tiers: test + test-envtest + e2e.
make lint vet build
```

Versions for both envtest and e2e are pinned in `hack/kuberay-version.env` so the
two tiers stay behaviorally aligned. Bump them there, in one place.

## Adding a new tool — the checklist

Every tool is `ray_*` and goes through the layers. Before you raise the PR:

1. **Write the test first** (TDD — see `docs/specs/ray-mcp-testing-strategy.md`).
   Map every acceptance criterion to a concrete test; a tool is "done" when its
   behavioral tests are green, not when the code exists.
2. **Pick the right tier(s)** for what you're proving:
   - Pure logic (guards, merge/diff, distillation, pagination) → **unit** (fakes).
   - Anything touching the KubeRay CRD API (create/update/scale/delete, SSA,
     DryRunAll, pruning) → **envtest** (`-tags envtest`), plus unit for the logic.
   - Dashboard/job REST behavior → **httptest** (no tag).
   - Tool schema + arg validation + the end-to-end call → **MCP** (in-memory
     transport, no tag).
   - A cluster-touching tool also gets a small **e2e** test (`-tags e2e`) added to
     `test/e2e/` — proving it against a real operator. E2e coverage grows one
     tool at a time; don't write speculative fixtures.
3. **Respect the tiers' build tags** so the fast loop stays Docker-free: put
   envtest tests behind `//go:build envtest` and e2e tests behind `//go:build e2e`.
4. **Run the gate that matches your change:**
   - Docs / pure-Go change → `make test lint vet`.
   - CRD-touching change → also `make test-envtest`.
   - A new cluster-touching tool → `make pre-push` (includes e2e) before pushing.

## Raising a PR

1. **Branch off the latest default branch** (`master`): one branch + PR per
   logical change.
2. Run the test gate for your change (above). CI runs tiers 1/3/4 + lint + vet +
   the invariant checks on every PR.
3. **Update docs that your change affects** — the README if you change
   user-facing surface (flags, tools, install/run), and add/extend the relevant
   tests. Don't leave the README describing a state that no longer exists.
4. Keep the diff surgical: touch only what the change needs.

CI must be green before merge. If a check fails, fix the underlying cause —
never make CI pass by skipping or weakening a test.
