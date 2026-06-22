package domain

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// The RayCluster update/scale half of the write path (spec §7.C, Task 10). Unlike
// create (which builds a fresh full object from curated params), update and scale
// are READ-MODIFY-APPLY-FULL: they read the live object as a plain map, overlay the
// requested change, and re-apply the WHOLE object as the ray-mcp field manager.
//
// Why full, never partial: under Server-Side Apply a partial apply by the SAME
// field manager PRUNES every field that manager previously owned but did not
// resend (a create owned the whole spec, so a partial update would delete the
// worker groups, rayVersion, etc.). Re-applying the full object keeps ray-mcp a
// single coherent owner. (Verified against k8s SSA semantics + KubeRay v1.6.1.)
//
// The autoscaler caveat (envtest-verified against KubeRay v1.6.1): spec.
// workerGroupSpecs is an ATOMIC SSA list (the CRD declares no
// x-kubernetes-list-type), so an apply must resend each worker group WHOLE — there
// is no granular per-field ownership of replicas. Ray's in-tree autoscaler writes
// replicas via JSON Patch (an Update-type manager), and because the list is atomic
// a ray-mcp apply that resends it DOES conflict (409) with that write. So:
//   - we NEVER strip or rewrite replicas: update re-asserts the live value it just
//     read (whatever the autoscaler last set); omitting it would reset the field to
//     its zero default on the atomic list. scale changes replicas only when the
//     caller explicitly sets it, and refuses an explicit replicas under autoscaling.
//   - on a conflict we retry once with force from a FRESH read (see
//     applyWithConflictRetry). This is best-effort, not race-free: an autoscaler
//     write that interleaves the retry's read→apply gap can still be lost (the
//     forced apply re-asserts the value read just before it). Acceptable for an
//     interactive tool — the autoscaler re-corrects on its next reconcile.
// min/max are the autoscaler's bounds, which ray-mcp owns and may set freely.
//
// It imports no Kubernetes/HTTP packages: it reads via ClusterGetter (the live
// object as a MergedSpec map), modifies maps directly (no typed round-trip, so the
// wedge holds), merges rawSpec via the pure Merge, and applies via ApplyService.

// ClusterGetter is the narrow read slice update/scale need: fetch the live object
// to read-modify-apply. The KubeRay adapter and the full ClusterReader both
// satisfy it. ClusterDetail.Raw carries the full unstructured object.
type ClusterGetter interface {
	GetCluster(ctx context.Context, namespace, name string) (ClusterDetail, error)
}

// ClusterUpdateParams is the decoded ray_cluster_update argument set: thin curated
// deltas (only the set fields are applied), the rawSpec escape hatch, and dryRun.
// Pointer/empty-default fields distinguish "not provided" from "set to zero":
// Image/RayVersion empty = unchanged; EnableAutoscaling nil = unchanged. Worker
// counts and per-node resources are NOT here — replica counts are ray_cluster_scale's
// job, and resource/structural changes go through rawSpec (curated params stay
// thin, Gate 1 C3).
type ClusterUpdateParams struct {
	Namespace         string
	Name              string
	Image             string // new image for head + all worker containers; "" = unchanged.
	RayVersion        string // new spec.rayVersion; "" = unchanged.
	EnableAutoscaling *bool  // toggle spec.enableInTreeAutoscaling; nil = unchanged.
	Labels            map[string]string
	Annotations       map[string]string

	RawSpec MergedSpec // merged OVER the live object (rawSpec wins); nil for curated-only.
	DryRun  bool
}

// hasChange reports whether the update carries at least one field to change, so an
// empty update is rejected rather than re-applying an unchanged object (which only
// churns SSA ownership).
func (p ClusterUpdateParams) hasChange() bool {
	return p.Image != "" || p.RayVersion != "" || p.EnableAutoscaling != nil ||
		len(p.Labels) > 0 || len(p.Annotations) > 0 || len(p.RawSpec) > 0
}

// ClusterScaleParams is the decoded ray_cluster_scale argument set: the target
// worker group and the bound(s) to change. Each replica field is a pointer so
// "not provided" is distinct from "set to 0" (the scale-to-zero teardown).
// MinReplicas/MaxReplicas are the autoscaler bounds (ray-mcp-owned, always safe);
// Replicas is the desired count (refused under autoscaling — the autoscaler owns
// the live count). AllowDestructive gates the scale-to-zero path (B3).
type ClusterScaleParams struct {
	Namespace        string
	Name             string
	WorkerGroup      string
	Replicas         *int32
	MinReplicas      *int32
	MaxReplicas      *int32
	AllowDestructive bool // the --allow-destructive tier; required for scale-to-zero (B3).
	DryRun           bool
}

// Update applies thin curated deltas to a live RayCluster via read-modify-apply-full
// (spec §7.C, Task 10). Each attempt reads the live object, overlays the set fields
// (image onto head + worker containers, rayVersion, autoscaling toggle, labels/
// annotations), merges rawSpec over the result (rawSpec wins, identity-guarded),
// and hands the FULL object to the ApplyService (DryRunAll → SSA → diff → audit).
//
// It NEVER alters worker replicas: spec.workerGroupSpecs is an ATOMIC SSA list, so
// the apply must resend each worker group whole, and the replicas value it resends
// is the one just read — i.e. whatever the autoscaler last set. Resending the live
// value re-asserts (never clobbers) it; omitting it would reset the field to its
// zero default on the atomic list. An empty update is rejected before the read.
func (s *ClusterWriteService) Update(ctx context.Context, p ClusterUpdateParams) (ApplyResult, error) {
	p.Namespace = s.resolveNamespace(p.Namespace)
	if p.Name == "" {
		return ApplyResult{}, fmt.Errorf("name is required")
	}
	if !p.hasChange() {
		return ApplyResult{}, fmt.Errorf("ray_cluster_update needs at least one field to change (image, rayVersion, enableAutoscaling, labels, annotations, or rawSpec)")
	}

	build := func() (ApplyRequest, error) {
		obj, spec, err := s.readForWrite(ctx, p.Namespace, p.Name)
		if err != nil {
			return ApplyRequest{}, err
		}
		if p.RayVersion != "" {
			spec["rayVersion"] = p.RayVersion
		}
		if p.Image != "" {
			setClusterImage(spec, p.Image)
		}
		if p.EnableAutoscaling != nil {
			spec["enableInTreeAutoscaling"] = *p.EnableAutoscaling
		}
		applyMetaStringMap(obj, "labels", p.Labels)
		applyMetaStringMap(obj, "annotations", p.Annotations)

		merged, err := Merge(obj, p.RawSpec, Identity{Namespace: p.Namespace, Name: p.Name})
		if err != nil {
			return ApplyRequest{}, err
		}
		return ApplyRequest{
			Kind: KindRayCluster, Namespace: p.Namespace, Name: p.Name, Spec: merged,
			DryRun: p.DryRun, Tool: "ray_cluster_update", ArgsSummary: updateArgsSummary(p),
		}, nil
	}
	return s.applyWithConflictRetry(ctx, build)
}

// Scale changes a worker group's min/max/replicas via read-modify-apply-full. Each
// attempt reads the live object, locates the named worker group, applies the
// provided bounds, enforces the autoscaler-safety + min<=max + scale-to-zero-
// destructive policies, and applies the FULL object. The replicas refusal and the
// min<=max guard are client-side because the server-side DryRunAll does NOT enforce
// them in KubeRay v1.6.1 (those rules live in the operator reconcile, not an
// admission webhook). Bounds left unset keep their live value (atomic list).
func (s *ClusterWriteService) Scale(ctx context.Context, p ClusterScaleParams) (ApplyResult, error) {
	p.Namespace = s.resolveNamespace(p.Namespace)
	if p.Name == "" {
		return ApplyResult{}, fmt.Errorf("name is required")
	}
	if p.WorkerGroup == "" {
		return ApplyResult{}, fmt.Errorf("workerGroup is required (the worker group to scale)")
	}

	build := func() (ApplyRequest, error) {
		obj, spec, err := s.readForWrite(ctx, p.Namespace, p.Name)
		if err != nil {
			return ApplyRequest{}, err
		}
		wg, err := findWorkerGroup(spec, p.WorkerGroup)
		if err != nil {
			return ApplyRequest{}, err
		}

		if p.Replicas != nil && specIsAutoscaling(spec) {
			return ApplyRequest{}, fmt.Errorf(
				"cluster %q autoscales; cannot set replicas directly — set minReplicas/maxReplicas instead (the autoscaler owns the live replica count)", p.Name)
		}
		// Scale-to-zero is a teardown (removes all worker pods for the group): gate it
		// behind the destructive tier (B3). A non-zero scale stays a plain write.
		if p.Replicas != nil && *p.Replicas == 0 && !p.AllowDestructive {
			return ApplyRequest{}, fmt.Errorf(
				"scaling worker group %q to zero tears down all its workers and requires --allow-destructive", p.WorkerGroup)
		}

		if p.Replicas != nil {
			wg["replicas"] = int64(*p.Replicas)
		}
		if p.MinReplicas != nil {
			wg["minReplicas"] = int64(*p.MinReplicas)
		}
		if p.MaxReplicas != nil {
			wg["maxReplicas"] = int64(*p.MaxReplicas)
		}
		// Client-side bounds guard: v1.6.1's RayCluster validating webhook is
		// ENABLE_WEBHOOKS-gated and OFF by default, so on a standard install
		// DryRunAll does not reach it and won't reject an invalid min/max/replicas
		// combination (the rule otherwise lives only in the operator reconcile). We
		// enforce min<=max and replicas<=max ourselves — mirroring the create path's
		// maxReplicas clamp, which treats max>=replicas as load-bearing.
		if err := validateWorkerBounds(wg, p.WorkerGroup); err != nil {
			return ApplyRequest{}, err
		}
		return ApplyRequest{
			Kind: KindRayCluster, Namespace: p.Namespace, Name: p.Name, Spec: obj,
			DryRun: p.DryRun, Tool: "ray_cluster_scale", ArgsSummary: scaleArgsSummary(p),
		}, nil
	}
	return s.applyWithConflictRetry(ctx, build)
}

// applyWithConflictRetry runs a read-modify-apply attempt and, on a genuine SSA
// field-ownership conflict, retries ONCE with force (spec §7.D step 3: "retry once
// only when the change is ours to make"). The retry rebuilds from a FRESH read, so
// the forced apply re-asserts the current live value of any contended field — the
// autoscaler's worker replicas on the atomic workerGroupSpecs list — rather than
// reviving the stale value that lost the first race. A dry-run is never forced (it
// mutates nothing, so it cannot conflict). build returns the request to apply; a
// nil-spec / validation error from build short-circuits without an apply.
func (s *ClusterWriteService) applyWithConflictRetry(ctx context.Context, build func() (ApplyRequest, error)) (ApplyResult, error) {
	req, err := build()
	if err != nil {
		return ApplyResult{}, err
	}
	res, err := s.apply.Apply(ctx, req)
	var conflict *ConflictError
	if err != nil && !req.DryRun && errors.As(err, &conflict) {
		// Re-read and re-apply once with force. The fresh read re-asserts the live
		// contended value (e.g. autoscaler replicas); force takes ownership of the
		// atomic list element another manager grabbed.
		retry, buildErr := build()
		if buildErr != nil {
			return ApplyResult{}, buildErr
		}
		retry.Force = true
		return s.apply.Apply(ctx, retry) //nolint:wrapcheck // domain errors carry their own bounded message.
	}
	return res, err //nolint:wrapcheck // domain errors carry their own bounded message.
}

// readForWrite fetches the live cluster and returns a clean apply intent: the full
// object stripped of server-authored noise (status, metadata.managedFields/
// resourceVersion/uid/creationTimestamp/generation) so the SSA body carries only
// the declarative intent ray-mcp owns. It returns the object map and its spec
// submap (created if absent) for the caller to modify in place.
func (s *ClusterWriteService) readForWrite(ctx context.Context, namespace, name string) (MergedSpec, map[string]any, error) {
	detail, err := s.get.GetCluster(ctx, namespace, name)
	if err != nil {
		return nil, nil, err
	}
	obj := cloneMap(detail.Raw)
	if obj == nil {
		obj = map[string]any{}
	}
	delete(obj, "status")
	obj["apiVersion"] = "ray.io/v1"
	obj["kind"] = string(KindRayCluster)

	meta, _ := obj["metadata"].(map[string]any)
	if meta == nil {
		meta = map[string]any{}
		obj["metadata"] = meta
	}
	for _, k := range []string{"managedFields", "resourceVersion", "uid", "creationTimestamp", "generation", "selfLink"} {
		delete(meta, k)
	}
	// Pin the identity authoritatively (SSA needs name/namespace; Merge re-guards it).
	meta["name"] = name
	meta["namespace"] = namespace

	spec, _ := obj["spec"].(map[string]any)
	if spec == nil {
		spec = map[string]any{}
		obj["spec"] = spec
	}
	stripWorkersToDelete(spec)
	return obj, spec, nil
}

// stripWorkersToDelete removes spec.workerGroupSpecs[].scaleStrategy.workersToDelete
// from every worker group before the apply. That field is transient command state
// the Ray autoscaler/KubeRay author and the operator consumes — never declarative
// intent ray-mcp owns. KubeRay v1.6.1 deletes the named pods unconditionally
// ("regardless of the value of Replicas") and then clears the slice IN MEMORY ONLY
// (no persisting Update in the reconcile path), so an already-actioned list can
// linger on the live spec. If a read-modify-apply re-asserted it as ray-mcp-owned
// intent, the operator would honor it again on the next reconcile and could delete
// a pod whose name recurred after a scale-up. ray-mcp never authors workersToDelete,
// so we drop it (like status) and let the autoscaler keep ownership of the field. An
// emptied scaleStrategy map is removed so the apply body carries no empty husk.
func stripWorkersToDelete(spec map[string]any) {
	groups, _ := spec["workerGroupSpecs"].([]any)
	for _, g := range groups {
		wg, ok := g.(map[string]any)
		if !ok {
			continue
		}
		ss, ok := wg["scaleStrategy"].(map[string]any)
		if !ok {
			continue
		}
		delete(ss, "workersToDelete")
		if len(ss) == 0 {
			delete(wg, "scaleStrategy")
		}
	}
}

// specIsAutoscaling reports whether spec.enableInTreeAutoscaling is true.
func specIsAutoscaling(spec map[string]any) bool {
	v, _ := spec["enableInTreeAutoscaling"].(bool)
	return v
}

// findWorkerGroup returns the worker group map with the given groupName, or an
// error naming the missing group (scale never creates a group).
func findWorkerGroup(spec map[string]any, name string) (map[string]any, error) {
	groups, _ := spec["workerGroupSpecs"].([]any)
	for _, g := range groups {
		wg, ok := g.(map[string]any)
		if ok && wg["groupName"] == name {
			return wg, nil
		}
	}
	return nil, fmt.Errorf("worker group %q not found on the cluster (scale does not create groups; use update/rawSpec to add one)", name)
}

// validateWorkerBounds enforces non-negativity, minReplicas <= maxReplicas, and
// replicas <= maxReplicas on the EFFECTIVE bounds of a worker group (whatever is
// set on the map after the caller's overlay, falling back to the live values for
// unset bounds). The server-side DryRunAll does not catch these on a standard
// install — v1.6.1's RayCluster validating webhook is ENABLE_WEBHOOKS-gated and off
// by default, so the rule otherwise lives only in the operator reconcile. The
// replicas<=max check mirrors the create path's maxReplicas clamp, which treats
// max>=replicas as a load-bearing KubeRay invariant.
func validateWorkerBounds(wg map[string]any, name string) error {
	minR, hasMin := intFromAny(wg["minReplicas"])
	maxR, hasMax := intFromAny(wg["maxReplicas"])
	replicas, hasReplicas := intFromAny(wg["replicas"])
	if hasMin && minR < 0 {
		return fmt.Errorf("worker group %q: minReplicas must be >= 0", name)
	}
	if hasMax && maxR < 0 {
		return fmt.Errorf("worker group %q: maxReplicas must be >= 0", name)
	}
	if hasMin && hasMax && minR > maxR {
		return fmt.Errorf("worker group %q: minReplicas %d is greater than maxReplicas %d (KubeRay rejects this)", name, minR, maxR)
	}
	if hasReplicas && hasMax && replicas > maxR {
		return fmt.Errorf("worker group %q: replicas %d exceeds maxReplicas %d (raise maxReplicas, or lower replicas)", name, replicas, maxR)
	}
	return nil
}

// intFromAny reads an int from the JSON-number shapes a worker group map carries
// (int64 from the live read, int/int32 if set by us). Reports whether a number was
// present.
func intFromAny(v any) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int:
		return int64(n), true
	case int32:
		return int64(n), true
	case float64:
		return int64(n), true
	default:
		return 0, false
	}
}

// setClusterImage overwrites the image of the FIRST container of the head group and
// of every worker group (the Ray container is index 0 — KubeRay injects the start
// command there). Containers beyond index 0 (sidecars) are untouched.
func setClusterImage(spec map[string]any, image string) {
	if head, ok := spec["headGroupSpec"].(map[string]any); ok {
		setFirstContainerImage(head, image)
	}
	groups, _ := spec["workerGroupSpecs"].([]any)
	for _, g := range groups {
		if wg, ok := g.(map[string]any); ok {
			setFirstContainerImage(wg, image)
		}
	}
}

// setFirstContainerImage sets group.template.spec.containers[0].image, creating the
// nested maps only when they already lead to a container (it never fabricates a
// container that the live object lacks).
func setFirstContainerImage(group map[string]any, image string) {
	tmpl, ok := group["template"].(map[string]any)
	if !ok {
		return
	}
	podSpec, ok := tmpl["spec"].(map[string]any)
	if !ok {
		return
	}
	containers, ok := podSpec["containers"].([]any)
	if !ok || len(containers) == 0 {
		return
	}
	if c0, ok := containers[0].(map[string]any); ok {
		c0["image"] = image
	}
}

// applyMetaStringMap merges a string map into metadata[key] (labels/annotations),
// creating the sub-object when needed. An empty/nil input is a no-op (no change).
func applyMetaStringMap(obj MergedSpec, key string, kv map[string]string) {
	if len(kv) == 0 {
		return
	}
	meta, _ := obj["metadata"].(map[string]any)
	if meta == nil {
		meta = map[string]any{}
		obj["metadata"] = meta
	}
	dst, _ := meta[key].(map[string]any)
	if dst == nil {
		dst = map[string]any{}
		meta[key] = dst
	}
	for k, v := range kv {
		dst[k] = v
	}
}

// updateArgsSummary builds the bounded audit summary for an update (spec §8, Q8):
// the changed knobs only, never the full object.
func updateArgsSummary(p ClusterUpdateParams) string {
	parts := []string{fmt.Sprintf("name=%s", p.Name)}
	if p.Image != "" {
		parts = append(parts, fmt.Sprintf("image=%s", p.Image))
	}
	if p.RayVersion != "" {
		parts = append(parts, fmt.Sprintf("rayVersion=%s", p.RayVersion))
	}
	if p.EnableAutoscaling != nil {
		parts = append(parts, fmt.Sprintf("autoscaling=%t", *p.EnableAutoscaling))
	}
	if len(p.Labels) > 0 {
		parts = append(parts, "labels")
	}
	if len(p.Annotations) > 0 {
		parts = append(parts, "annotations")
	}
	if len(p.RawSpec) > 0 {
		parts = append(parts, "rawSpec=true")
	}
	return strings.Join(parts, " ")
}

// scaleArgsSummary builds the bounded audit summary for a scale.
func scaleArgsSummary(p ClusterScaleParams) string {
	parts := []string{fmt.Sprintf("name=%s", p.Name), fmt.Sprintf("group=%s", p.WorkerGroup)}
	if p.Replicas != nil {
		parts = append(parts, fmt.Sprintf("replicas=%d", *p.Replicas))
	}
	if p.MinReplicas != nil {
		parts = append(parts, fmt.Sprintf("min=%d", *p.MinReplicas))
	}
	if p.MaxReplicas != nil {
		parts = append(parts, fmt.Sprintf("max=%d", *p.MaxReplicas))
	}
	return strings.Join(parts, " ")
}
