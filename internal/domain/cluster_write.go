package domain

import (
	"context"
	"fmt"
	"strings"
)

// The RayCluster write path (spec §7.C, the create half). This composes the two
// halves already built: the curated→base construction (ClusterBaseBuilder, an
// adapter port — step 1 needs the KubeRay Go types the domain must not import),
// the pure RFC 7396 merge + identity guard (Merge, Task 8a, steps 3-4), and the
// cluster-touching apply orchestration (ApplyService, Task 8b, steps 6-7:
// DryRunAll → SSA → read-back diff → audit). ClusterWriteService is the thin glue
// that runs them in order for ray_cluster_create; ray_cluster_update/scale (Task
// 10) reuse the same ApplyService through their own entry points.
//
// It imports no Kubernetes/HTTP packages: ClusterBaseBuilder is an interface the
// KubeRay adapter satisfies, and the spec crosses the boundary as MergedSpec.

// ResourceQuantities is the curated per-node resource shape (spec §6:
// headResources{cpu,memory,gpu}, and the same under each worker group). Each is a
// Kubernetes quantity string ("2", "500m", "4Gi", "1") or empty to omit. The
// adapter maps them to the container's resource requests/limits; gpu maps to the
// nvidia.com/gpu extended resource. Anything richer (tolerations, nodeSelector,
// multiple containers) is the rawSpec escape hatch's job (Gate 1 C3: curated
// params stay thin).
type ResourceQuantities struct {
	CPU    string
	Memory string
	GPU    string
}

// WorkerGroupParams is one curated worker group (spec §6:
// workerGroups[]{name,replicas,min,max,resources}). Replicas is the desired count;
// Min/MaxReplicas bound the autoscaler when EnableAutoscaling is set. The adapter
// guarantees the KubeRay invariant maxReplicas >= replicas >= 0 by clamping
// MaxReplicas up when a caller leaves it at zero.
type WorkerGroupParams struct {
	Name        string
	Replicas    int32
	MinReplicas int32
	MaxReplicas int32
	Resources   ResourceQuantities
}

// ClusterCreateParams is the decoded ray_cluster_create argument set: the thin
// curated shape (spec §6) plus the rawSpec escape hatch and the dryRun flag.
// Namespace is resolved by the service (default fallback) before the base is
// built; RawSpec is merged OVER the curated base (rawSpec wins, Task 8a). It is
// the input to BuildClusterBase (curated half only) and to Merge (with RawSpec).
type ClusterCreateParams struct {
	Namespace         string
	Name              string
	RayVersion        string
	Image             string
	HeadResources     ResourceQuantities
	WorkerGroups      []WorkerGroupParams
	EnableAutoscaling bool
	Labels            map[string]string
	Annotations       map[string]string

	RawSpec MergedSpec // the escape hatch, merged over the curated base; nil for curated-only.
	DryRun  bool
}

// ClusterBaseBuilder turns the curated half of ClusterCreateParams into the base
// unstructured RayCluster (spec §7.C step 1: curated params → typed KubeRay object
// → JSON map). It is an adapter port because that construction needs the compiled
// KubeRay Go types, which the domain must not import. The returned base carries
// metadata.name/namespace equal to the params identity so Merge's identity guard
// passes for a curated-only (or identity-preserving rawSpec) apply. It must NOT
// apply RawSpec — that is Merge's job, kept separate so the rawSpec-wins +
// identity-guard semantics live in one tested place.
type ClusterBaseBuilder interface {
	BuildClusterBase(p ClusterCreateParams) (MergedSpec, error)
}

// ClusterWriteService is the orchestration layer for the RayCluster write tools.
// It owns the cross-cutting write policy the MCP layer must not duplicate: the
// default-namespace fallback, the curated→base→merge→apply sequence, and the
// audit-carrying ApplyService. It imports no Kubernetes or HTTP packages — only
// the ClusterBaseBuilder port, the pure Merge, and the ApplyService.
type ClusterWriteService struct {
	base             ClusterBaseBuilder
	apply            *ApplyService
	defaultNamespace string
}

// NewClusterWriteService builds the service over the base builder, the apply
// pipeline (Task 8b), and the default namespace (injected as a plain string so the
// domain stays config/k8s-free).
func NewClusterWriteService(base ClusterBaseBuilder, apply *ApplyService, defaultNamespace string) *ClusterWriteService {
	return &ClusterWriteService{base: base, apply: apply, defaultNamespace: defaultNamespace}
}

// Create runs the create half of the unified apply pipeline for one RayCluster:
//
//  1. resolve the namespace (default fallback) and fix the identity;
//  2. build the curated base (ClusterBaseBuilder, step 1);
//  3. merge rawSpec over the base, rawSpec wins, identity-guarded (Merge, steps
//     3-4);
//  4. hand the merged spec to the ApplyService (steps 6-7: DryRunAll → maybe SSA
//     → read-back diff → audit).
//
// The dryRun flag flows to the ApplyService, which always DryRunAll-validates and
// only commits when dryRun is false. A merge identity violation (a rawSpec that
// retargets name/namespace) is returned before any cluster call.
func (s *ClusterWriteService) Create(ctx context.Context, p ClusterCreateParams) (ApplyResult, error) {
	p.Namespace = s.resolveNamespace(p.Namespace)
	if p.Name == "" {
		return ApplyResult{}, fmt.Errorf("name is required")
	}

	base, err := s.base.BuildClusterBase(p)
	if err != nil {
		return ApplyResult{}, err
	}

	merged, err := Merge(base, p.RawSpec, Identity{Namespace: p.Namespace, Name: p.Name})
	if err != nil {
		return ApplyResult{}, err
	}

	return s.apply.Apply(ctx, ApplyRequest{
		Kind:        KindRayCluster,
		Namespace:   p.Namespace,
		Name:        p.Name,
		Spec:        merged,
		DryRun:      p.DryRun,
		Tool:        "ray_cluster_create",
		ArgsSummary: createArgsSummary(p),
	})
}

// resolveNamespace applies the default-namespace fallback (mirrors the read
// service so both halves share one namespace policy).
func (s *ClusterWriteService) resolveNamespace(ns string) string {
	if ns == "" {
		return s.defaultNamespace
	}
	return ns
}

// DefaultNamespace returns the namespace used when a create omits one, so the MCP
// layer can echo the real target namespace in its result.
func (s *ClusterWriteService) DefaultNamespace() string {
	return s.defaultNamespace
}

// createArgsSummary builds the short, bounded audit summary for a create (spec §8,
// Q8: a summary, never the full spec). It names the identity-defining knobs only —
// image, worker-group count, autoscaling, and whether a rawSpec was supplied — so
// the audit log answers "what was attempted" without echoing the whole object.
func createArgsSummary(p ClusterCreateParams) string {
	parts := []string{fmt.Sprintf("name=%s", p.Name)}
	if p.Image != "" {
		parts = append(parts, fmt.Sprintf("image=%s", p.Image))
	}
	parts = append(parts, fmt.Sprintf("workerGroups=%d", len(p.WorkerGroups)))
	if p.EnableAutoscaling {
		parts = append(parts, "autoscaling=true")
	}
	if len(p.RawSpec) > 0 {
		parts = append(parts, "rawSpec=true")
	}
	return strings.Join(parts, " ")
}
