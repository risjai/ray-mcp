# ray-mcp RBAC — the read-only security floor

This directory is ray-mcp's **deployment contract for in-cluster RBAC**: a
ServiceAccount plus a read-only ClusterRole and binding. It is the *floor* an
autonomous agent runs under.

## Why this is the security boundary, not a convenience

In-cluster, ray-mcp runs under a pod ServiceAccount, and the agent driving it
inherits that ServiceAccount's RBAC. So **the ServiceAccount's RBAC is the entire
security envelope** for an autonomous caller — every caller gets the union of
their own restraint and the ServiceAccount's power (spec Q12; grilling #7, "RBAC
is the floor"). Tighten the floor and you tighten what *any* agent can do, no
matter how it is prompted.

ray-mcp v0.1.0 is **read-only**, so this floor is read-only. We ship it now,
before the write tools exist, so the boundary is correct-by-default when the
dangerous tools land (Tasks 8–12) instead of being retrofitted under pressure.

## Apply it

```sh
kubectl apply -k deploy/rbac/
```

This installs:

| Kind | Name | Purpose |
|------|------|---------|
| `ServiceAccount` | `ray-mcp` | the identity ray-mcp runs as |
| `ClusterRole` | `ray-mcp-readonly` | the read-only grant |
| `ClusterRoleBinding` | `ray-mcp-readonly` | binds the grant to the SA cluster-wide |

The manifests default to the `default` namespace. To install elsewhere, set
`namespace:` in `kustomization.yaml` (it rewrites the ServiceAccount namespace and
the ClusterRoleBinding subject namespace together), or edit `serviceaccount.yaml`
+ `clusterrolebinding.yaml` to match.

## Exactly what is granted, and why

Every rule maps to a real API call ray-mcp makes. Nothing else is granted.

| apiGroups | resources | verbs | Needed by |
|-----------|-----------|-------|-----------|
| `ray.io` | `rayclusters`, `rayjobs`, `rayservices` | `get`, `list`, `watch` | `ray_cluster_list` (List), `ray_cluster_get` (Get). `rayjobs`/`rayservices` are included **proactively** — see note. |
| `""` (core) | `pods`, `events` | `get`, `list` | `ray_cluster_events` lists the cluster's pods (by the `ray.io/cluster` label) and the namespace's events to filter them. |

**Read-only by construction:** the only verbs anywhere are `get`/`list`/`watch`.
There is no write verb to find. `rbac_test.go` (a fast unit test, no Docker) fails
the build if anyone ever adds one.

### Two judgment calls

- **`rayjobs`/`rayservices` are included though no tool calls them yet.** They are
  read-only KubeRay CRs and the read tools for them are on the roadmap
  (`ray_job_*` / `ray_service_*` reads). Grouping the three read CRDs now means the
  floor will not need editing when those tools land. The cost is nil: only read
  verbs on read-only resources. To keep it strictly to what is exercised today,
  drop those two resources and leave `rayclusters`.
- **`watch` on the CRDs but not on core.** ray-mcp uses an *uncached* client and
  watches nothing today, so neither grant is exercised. `watch` rounds out the
  standard read triple on the CRDs (harmless, future-proof); core stays tighter
  (`get`/`list` only) precisely because nothing watches pods/events.

### Deliberately NOT granted

`create`/`update`/`patch`/`delete` on anything, `pods/exec`, `pods/portforward`,
`secrets`, `nodes`, and any `*` wildcard. The wedge's port-forward (Task 14) will
use a **direct in-cluster dial** to the head service, which needs no
`pods/portforward` RBAC; if a future out-of-cluster mode needs it, it will be
added with that task, scoped and reviewed.

## ClusterRole vs. Role (cluster-wide vs. single-namespace)

This ships a **ClusterRole** + **ClusterRoleBinding** because ray-mcp supports
`--allow-all-namespaces`, and listing RayClusters across all namespaces requires a
cluster-scoped read.

To confine ray-mcp to **one namespace**, do *not* edit the ClusterRole — keep it
and bind it with a namespaced **RoleBinding** instead of the ClusterRoleBinding. A
RoleBinding that references a ClusterRole grants those verbs only within the
RoleBinding's own namespace:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: ray-mcp-readonly
  namespace: <the one namespace ray-mcp may read>
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: ray-mcp-readonly
subjects:
  - kind: ServiceAccount
    name: ray-mcp
    namespace: <the one namespace ray-mcp may read>
```

With that binding, run ray-mcp without `--allow-all-namespaces` (scoped to its own
namespace) and a cross-namespace list will be correctly Forbidden.

## Attaching the ServiceAccount to a caller

**Today (stdio, works now).** ray-mcp v0.1.0 speaks MCP over stdio and is launched
as a subprocess by the agent. When that agent pod itself runs in the cluster, set
its pod's `spec.serviceAccountName: ray-mcp` so the subprocess inherits the
projected token / in-cluster config and ray-mcp reads under this floor:

```yaml
spec:
  serviceAccountName: ray-mcp
  containers:
    - name: agent
      # ... launches `ray-mcp` over stdio ...
```

**Future (a ray-mcp Service over HTTP).** When ray-mcp is deployed as its own
networked Service (HTTP transport — not in v0.1.0), that Deployment sets
`serviceAccountName: ray-mcp` and the same floor applies. The HTTP transport and
its Deployment manifest are deferred; this RBAC is the part that is correct to
ship now.

## What is deferred

This is **read-only**. Write verbs arrive with the write tools (Tasks 8–12) and the
schema-read decision (gate B2) — not before. This floor is the slice of the
planned RBAC task (Task 26) pulled forward so the boundary is right from v0.1.0.
The Helm chart, the HTTP-transport Deployment, and any write/exec/portforward
grants are all out of scope here.

## Trust: the floor is *proven*, not asserted

- `deploy/rbac/rbac_test.go` (fast, no Docker) parses `clusterrole.yaml` and fails
  if any verb is outside `{get, list, watch}` — a cheap always-on guard against an
  accidental write verb.
- `test/e2e/rbac_test.go` (`-tags e2e`, needs kind) applies *these* manifests to a
  real cluster, then uses `SubjectAccessReview` to assert the reads are ALLOWED and
  writes / `secrets` / `pods/exec` are DENIED for the `ray-mcp` ServiceAccount —
  turning "we wrote a Role" into "the floor holds, both ways".
