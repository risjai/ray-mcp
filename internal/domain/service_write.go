package domain

import (
	"context"
	"fmt"
	"reflect"
	"strings"
)

// The RayService write path (Task 21): ray_service_deploy + ray_service_update.
// It mirrors the RayCluster write path (cluster_write.go / cluster_update.go):
// Deploy builds a fresh object from curated params, Update reads-modifies-applies
// the live object. The headline feature is the FAITHFUL PREDICTOR classifier
// (classifyServiceUpdate): it replicates KubeRay v1.6.1's routing to predict
// whether a given update triggers an in-place serve reconfiguration, a
// zero-downtime cluster swap, or neither (replicas-only).
//
// It imports no Kubernetes or HTTP packages: ServiceBaseBuilder is an interface
// the KubeRay adapter satisfies, and the spec crosses the boundary as MergedSpec.

// ServiceDeployParams is the decoded ray_service_deploy argument set: identity,
// the thin curated cluster shape (reuses the same knobs as ClusterCreateParams for
// the embedded rayClusterConfig), serveConfigV2, plus rawSpec and dryRun. Namespace
// is resolved by the service (default fallback) before the base is built.
type ServiceDeployParams struct {
	Namespace         string
	Name              string
	ServeConfigV2     string
	RayVersion        string
	Image             string
	HeadResources     ResourceQuantities
	WorkerGroups      []WorkerGroupParams
	EnableAutoscaling bool
	Labels            map[string]string
	Annotations       map[string]string

	RawSpec MergedSpec // merged over the curated base; nil for curated-only.
	DryRun  bool
}

// ServiceUpdateParams is the decoded ray_service_update argument set: thin curated
// deltas on a LIVE RayService, plus rawSpec and dryRun. Only set fields change.
// The classifier predicts which operator path the change triggers, surfaced in the
// result. Mirrors ClusterUpdateParams.
type ServiceUpdateParams struct {
	Namespace         string
	Name              string
	ServeConfigV2     *string // new spec.serveConfigV2; nil = unchanged.
	Image             string  // new image for head + worker containers; "" = unchanged.
	RayVersion        string  // new rayClusterConfig.rayVersion; "" = unchanged.
	EnableAutoscaling *bool   // toggle enableInTreeAutoscaling; nil = unchanged.
	Labels            map[string]string
	Annotations       map[string]string

	RawSpec MergedSpec // merged OVER the live object (rawSpec wins); nil for curated-only.
	DryRun  bool
}

// hasChange reports whether the update carries at least one field to change.
func (p ServiceUpdateParams) hasChange() bool {
	return p.ServeConfigV2 != nil || p.Image != "" || p.RayVersion != "" ||
		p.EnableAutoscaling != nil || len(p.Labels) > 0 || len(p.Annotations) > 0 ||
		len(p.RawSpec) > 0
}

// ServiceBaseBuilder turns the curated half of ServiceDeployParams into the base
// unstructured RayService (typed KubeRay object → JSON map). It is an adapter port
// because that construction needs the compiled KubeRay Go types the domain must not
// import. It must NOT apply RawSpec — that is Merge's job.
type ServiceBaseBuilder interface {
	BuildServiceBase(p ServiceDeployParams) (MergedSpec, error)
}

// ServiceGetter is the narrow read slice the update path needs: fetch the live
// RayService for read-modify-apply. The KubeRay adapter satisfies it via GetService.
type ServiceGetter interface {
	GetService(ctx context.Context, namespace, name string) (ServiceDetail, error)
}

// ServiceWriteService is the orchestration layer for the RayService write tools.
// It owns the cross-cutting write policy: default-namespace fallback, the
// curated→base→merge→apply sequence (deploy), the read-modify-apply-full +
// classifier (update), the destructive delete pipeline (protected → serving guard
// → confirm), and the audit-carrying ApplyService.
type ServiceWriteService struct {
	base             ServiceBaseBuilder
	get              ServiceGetter
	del              Deleter
	apply            *ApplyService
	defaultNamespace string
}

// NewServiceWriteService builds the service over the base builder (deploy), the
// live-object reader (update read-modify-apply + delete guards), the deleter
// (destructive tier), the apply pipeline, and the default namespace.
func NewServiceWriteService(base ServiceBaseBuilder, get ServiceGetter, del Deleter, apply *ApplyService, defaultNamespace string) *ServiceWriteService {
	return &ServiceWriteService{base: base, get: get, del: del, apply: apply, defaultNamespace: defaultNamespace}
}

// DefaultNamespace returns the namespace used when a deploy omits one.
func (s *ServiceWriteService) DefaultNamespace() string {
	return s.defaultNamespace
}

// ServiceDeployResult is the deploy outcome: mirrors ApplyResult + identity.
type ServiceDeployResult struct {
	ApplyResult
	Name      string
	Namespace string
}

// ServiceUpdateResult is the update outcome: the apply diff plus the predicted
// operator path (the headline feature).
type ServiceUpdateResult struct {
	ApplyResult
	Name          string
	Namespace     string
	PredictedPath string // "in-place" | "zero-downtime-swap" | "scale (no swap)" | hedged variant.
}

// Deploy runs the create-style apply for one RayService: resolve namespace, build
// curated base, merge rawSpec, apply.
func (s *ServiceWriteService) Deploy(ctx context.Context, p ServiceDeployParams) (ServiceDeployResult, error) {
	p.Namespace = s.resolveNamespace(p.Namespace)
	if p.Name == "" {
		return ServiceDeployResult{}, fmt.Errorf("name is required")
	}

	base, err := s.base.BuildServiceBase(p)
	if err != nil {
		return ServiceDeployResult{}, err
	}

	merged, err := Merge(base, p.RawSpec, Identity{Namespace: p.Namespace, Name: p.Name})
	if err != nil {
		return ServiceDeployResult{}, err
	}

	res, err := s.apply.Apply(ctx, ApplyRequest{
		Kind:        KindRayService,
		Namespace:   p.Namespace,
		Name:        p.Name,
		Spec:        merged,
		DryRun:      p.DryRun,
		Tool:        "ray_service_deploy",
		ArgsSummary: deployArgsSummary(p),
	})
	if err != nil {
		return ServiceDeployResult{}, err
	}

	return ServiceDeployResult{ApplyResult: res, Name: p.Name, Namespace: p.Namespace}, nil
}

// Update applies thin curated deltas to a live RayService via read-modify-apply-full,
// classifies the change (the faithful predictor), and returns the predicted path
// alongside the field-level diff. An empty update is rejected before the read.
func (s *ServiceWriteService) Update(ctx context.Context, p ServiceUpdateParams) (ServiceUpdateResult, error) {
	p.Namespace = s.resolveNamespace(p.Namespace)
	if p.Name == "" {
		return ServiceUpdateResult{}, fmt.Errorf("name is required")
	}
	if !p.hasChange() {
		return ServiceUpdateResult{}, fmt.Errorf("ray_service_update needs at least one field to change (serveConfigV2, image, rayVersion, enableAutoscaling, labels, annotations, or rawSpec)")
	}

	// Read-modify-apply, applied ONCE (no conflict-retry loop, unlike the RayCluster
	// scale/update path). A RayService's rayClusterConfig is a desired-state TEMPLATE
	// that only the user/this tool writes; the autoscaler contends the child RayCluster
	// the operator spawns from it, not the RayService spec — so there is no concurrent
	// writer to lose to, and SSA already serializes against a stale resourceVersion.
	obj, spec, err := s.readForWrite(ctx, p.Namespace, p.Name)
	if err != nil {
		return ServiceUpdateResult{}, err
	}

	// Snapshot the LIVE spec before mutations (for the classifier).
	liveSpec := cloneMap(spec)

	// Apply curated deltas.
	rcc := rayClusterConfig(spec)
	if p.RayVersion != "" {
		rcc["rayVersion"] = p.RayVersion
	}
	if p.Image != "" {
		setClusterImage(rcc, p.Image)
	}
	if p.EnableAutoscaling != nil {
		rcc["enableInTreeAutoscaling"] = *p.EnableAutoscaling
	}
	if p.ServeConfigV2 != nil {
		spec["serveConfigV2"] = *p.ServeConfigV2
	}
	applyMetaStringMap(obj, "labels", p.Labels)
	applyMetaStringMap(obj, "annotations", p.Annotations)

	merged, err := Merge(obj, p.RawSpec, Identity{Namespace: p.Namespace, Name: p.Name})
	if err != nil {
		return ServiceUpdateResult{}, err
	}

	// Classify: compare the LIVE spec vs the SUBMITTED (merged) spec.
	submittedSpec, _ := merged["spec"].(map[string]any)
	predicted := classifyServiceUpdate(liveSpec, submittedSpec)

	res, err := s.apply.Apply(ctx, ApplyRequest{
		Kind: KindRayService, Namespace: p.Namespace, Name: p.Name, Spec: merged,
		DryRun: p.DryRun, Tool: "ray_service_update", ArgsSummary: serviceUpdateArgsSummary(p),
	})
	if err != nil {
		return ServiceUpdateResult{}, err
	}

	return ServiceUpdateResult{
		ApplyResult:   res,
		Name:          p.Name,
		Namespace:     p.Namespace,
		PredictedPath: predicted,
	}, nil
}

// ServiceDeleteParams is the decoded ray_service_delete argument set: the target
// identity, the confirm fingerprint (empty for preview, echoed back to commit),
// the Force override for the serving-traffic guard, and dryRun. Deleting a
// RayService always cascades to its owned RayCluster(s), so the tool registers
// under the destructive tier; Force additionally overrides the refuse-when-serving
// guard (Decision Gate 4). Mirrors ClusterDeleteParams.
type ServiceDeleteParams struct {
	Namespace string
	Name      string
	Confirm   string
	Force     bool // override the serving-traffic guard (Gate 4); confirm is still required.
	DryRun    bool
}

// Delete runs the destructive delete pipeline for one RayService: protected-guarded,
// serving-traffic guarded (Gate 4), confirm-fingerprint gated, audit-carrying.
// Deleting a RayService cascades to its owned RayCluster(s) and every actor/serve
// replica on them, so the tool is destructive-tier (registered only under
// --allow-destructive). The order mirrors ClusterWriteService.Delete:
//
//  1. Resolve namespace + require name.
//  2. Read the live object (GetService) — NotFound propagates.
//  3. Protected check FIRST (a protected service never yields a fingerprint).
//  4. Serving-traffic guard: if the service appears to be serving traffic
//     (Gate 4: NumServeEndpoints>0 OR an upgrade/rollback is in flight) and Force
//     is not set, refuse — BEFORE minting a fingerprint, so a serving service does
//     not yield a working confirm until the caller opts in with force.
//  5. RequireConfirm gate — empty confirm returns a preview (ConfirmRequiredError
//     carrying the fingerprint); matching confirm proceeds; wrong/stale confirm
//     returns ConfirmMismatchError.
//  6. On matching confirm: call the Deleter, emit one audit record, return.
func (s *ServiceWriteService) Delete(ctx context.Context, p ServiceDeleteParams) error {
	p.Namespace = s.resolveNamespace(p.Namespace)
	if p.Name == "" {
		return fmt.Errorf("name is required")
	}

	detail, err := s.get.GetService(ctx, p.Namespace, p.Name)
	if err != nil {
		return err
	}

	if IsProtected(detail.Raw) {
		return fmt.Errorf("deletion refused: RayService %q is protected (ray-mcp/protected=%q); remove the annotation via ray_service_update first", p.Name, protectedValue)
	}

	if !p.Force {
		if reason := serviceServingReason(detail.Raw); reason != "" {
			return &ServingRefusedError{Name: p.Name, Reason: reason}
		}
	}

	if err := RequireConfirm(detail.Raw, OpDelete, p.Confirm); err != nil {
		return err
	}

	delErr := s.del.Delete(ctx, KindRayService, p.Namespace, p.Name, p.DryRun)
	s.apply.RecordDestructive(ctx, "ray_service_delete", KindRayService, p.Namespace, p.Name, serviceDeleteArgsSummary(p), p.DryRun, delErr)
	return delErr
}

// serviceServingReason reports why a RayService appears to be serving traffic, or
// "" if it does not (Decision Gate 4, verified vs KubeRay v1.6.1). It reads the
// live object's .status directly (detail.Raw carries the full status), staying
// k8s-free. The signals, in order:
//
//   - status.numServeEndpoints > 0 — the operator's own count of READY serve pods
//     behind the serve Service (EndpointSlice-derived). The primary, most reliable
//     signal; errs toward 0 when pods go un-ready, so gating on >0 is conservative.
//   - an UpgradeInProgress or RollbackInProgress condition is True — a zero-downtime
//     swap/rollback is mid-flight, during which the (old) active cluster is still
//     serving even while Ready may be False, and deleting tears down BOTH clusters.
//
// It deliberately does NOT gate on the Ready condition alone: Ready is defined as
// numServeEndpoints>0 in v1.6.1, so it is False during a rollback while the old
// cluster still serves — gating on it would green-light deleting a live service.
// The CRD carries no request/connection metric, so this detects "serving-CAPABLE",
// not live load — the honest claim the refusal message must make.
func serviceServingReason(live MergedSpec) string {
	status, _ := live["status"].(map[string]any)
	if status == nil {
		return ""
	}
	if n, ok := numberField(status["numServeEndpoints"]); ok && n > 0 {
		return fmt.Sprintf("%d ready serve endpoint(s)", int64(n))
	}
	if statusConditionTrue(status, "UpgradeInProgress") {
		return "a zero-downtime upgrade is in progress (both clusters exist)"
	}
	if statusConditionTrue(status, "RollbackInProgress") {
		return "a rollback is in progress (the active cluster is still serving)"
	}
	return ""
}

// numberField reads a JSON number that unstructured conversion may represent as
// int64, float64, or int32, returning it as float64. The bool reports whether the
// value was a recognized numeric type.
func numberField(v any) (float64, bool) {
	switch n := v.(type) {
	case int64:
		return float64(n), true
	case float64:
		return n, true
	case int32:
		return float64(n), true
	case int:
		return float64(n), true
	default:
		return 0, false
	}
}

// statusConditionTrue reports whether status.conditions[] contains an entry of the
// given type with status "True". conditions is a []any of map[string]any once the
// object has crossed the boundary as an unstructured map.
func statusConditionTrue(status map[string]any, condType string) bool {
	conds, _ := status["conditions"].([]any)
	for _, c := range conds {
		cond, ok := c.(map[string]any)
		if !ok {
			continue
		}
		if t, _ := cond["type"].(string); t != condType {
			continue
		}
		st, _ := cond["status"].(string)
		return st == "True"
	}
	return false
}

// serviceDeleteArgsSummary builds the short, bounded audit summary for a service
// delete (spec §8, Q8: a summary, never the full spec — and never the fingerprint).
func serviceDeleteArgsSummary(p ServiceDeleteParams) string {
	return fmt.Sprintf("name=%s force=%t dryRun=%t", p.Name, p.Force, p.DryRun)
}

// classifyServiceUpdate replicates KubeRay v1.6.1's shouldPrepareNewCluster routing
// to predict the update path a submitted change triggers:
//   - serve-config-only change → "in-place"
//   - cluster-spec change (after zeroing excluded fields) → "zero-downtime-swap"
//     (with a hedge for upgradeStrategy.type == None/unset)
//   - replicas/min/max-only → "scale (no swap)"
//
// Both specs are the full spec subtree (spec.{serveConfigV2, rayClusterConfig, ...}).
// The comparison is STRUCTURAL EQUALITY of the zeroed (normalized) rayClusterConfig
// maps. It is pure and imports no k8s packages.
func classifyServiceUpdate(live, submitted map[string]any) string {
	if live == nil || submitted == nil {
		return "unknown"
	}

	// 1. Serve-config change?
	liveServe, _ := live["serveConfigV2"].(string)
	submittedServe, _ := submitted["serveConfigV2"].(string)
	serveChanged := liveServe != submittedServe

	// 2. Cluster-spec change? Compare the rayClusterConfig after zeroing excluded fields.
	liveRCC, _ := live["rayClusterConfig"].(map[string]any)
	submittedRCC, _ := submitted["rayClusterConfig"].(map[string]any)

	normalizedLive := normalizeForHash(liveRCC)
	normalizedSubmitted := normalizeForHash(submittedRCC)
	clusterChanged := !reflect.DeepEqual(normalizedLive, normalizedSubmitted)

	if clusterChanged {
		// The operator's PARTIAL-match tier (isClusterSpecHashEqual partial=true):
		// if the only difference is one or more worker groups APPENDED at the end
		// (every existing group's hash-affecting fields unchanged), KubeRay updates
		// the EXISTING cluster in place — adding the new workers — and does NOT swap.
		// shouldPrepareNewCluster returns false on this path BEFORE it ever consults
		// zero-downtime upgrade, so an append is in-place even when swap is disabled.
		if partialHashEqual(liveRCC, submittedRCC) {
			return "in-place cluster update (worker group added, no swap)"
		}
		// A genuine cluster-spec change (existing group's template/image/rayVersion,
		// a removed/reordered group, head changes, ...) dominates serve changes (the
		// new cluster carries the new serve config anyway) and triggers a swap IFF
		// zero-downtime upgrade is enabled. upgradeStrategy.type is read from the
		// SUBMITTED RayService spec (it lives on the spec, not inside rayClusterConfig).
		return classifyClusterChange(submitted)
	}

	if serveChanged {
		return "in-place"
	}

	// Neither serve nor cluster hash changed: could be replicas-only, or a no-op.
	if replicasOrExcludedChanged(liveRCC, submittedRCC) {
		return "scale (no swap)"
	}
	return "no change detected"
}

// partialHashEqual replicates KubeRay v1.6.1's "added worker groups" branch of
// shouldPrepareNewRayCluster: when the submitted rayClusterConfig has MORE worker
// groups than the live one, the operator truncates the submitted worker groups back
// to the live count and re-compares the hash. If the truncated spec matches the live
// spec, the ONLY difference is appended worker groups, which the operator applies to
// the EXISTING cluster in place (updateRayClusterInstance) rather than preparing a
// new one. Returns false unless the submitted spec strictly appends (more groups,
// every pre-existing group's hash-affecting fields unchanged).
func partialHashEqual(liveRCC, submittedRCC map[string]any) bool {
	liveGroups, _ := liveRCC["workerGroupSpecs"].([]any)
	submittedGroups, _ := submittedRCC["workerGroupSpecs"].([]any)
	if len(submittedGroups) <= len(liveGroups) {
		return false
	}
	// Truncate the submitted worker groups to the live count (a shallow copy so the
	// caller's map is untouched; normalizeForHash deep-clones before mutating).
	truncated := make(map[string]any, len(submittedRCC))
	for k, v := range submittedRCC {
		truncated[k] = v
	}
	truncated["workerGroupSpecs"] = submittedGroups[:len(liveGroups)]
	return reflect.DeepEqual(normalizeForHash(liveRCC), normalizeForHash(truncated))
}

// classifyClusterChange determines the swap prediction based on upgradeStrategy.type.
// spec is the full RayService spec (containing upgradeStrategy at the top level).
func classifyClusterChange(spec map[string]any) string {
	strategyType := upgradeStrategyType(spec)
	switch strings.ToLower(strategyType) {
	case "none":
		return "zero-downtime-swap, OR no-op if the operator has ENABLE_ZERO_DOWNTIME=false (upgradeStrategy.type=None disables swap; the tool cannot see the operator env)"
	case "":
		// Unset: the operator env ENABLE_ZERO_DOWNTIME decides; default is enabled
		// (v1.6.1), but the tool cannot confirm.
		return "zero-downtime-swap (predicted; operator default enables it, but upgradeStrategy.type is unset — if ENABLE_ZERO_DOWNTIME=false in the operator env, this change is a no-op)"
	default:
		// NewCluster / NewClusterWithIncrementalUpgrade → enabled.
		return "zero-downtime-swap"
	}
}

// upgradeStrategyType reads spec.upgradeStrategy.type from the RayService spec map.
func upgradeStrategyType(spec map[string]any) string {
	us, _ := spec["upgradeStrategy"].(map[string]any)
	if us == nil {
		return ""
	}
	t, _ := us["type"].(string)
	return t
}

// normalizeForHash replicates KubeRay v1.6.1's GenerateHashWithoutReplicasAndWorkersToDelete:
// it deep-clones the rayClusterConfig and nils the excluded fields so only the
// "hash-affecting" fields remain. The excluded fields (per worker group) are:
// replicas, minReplicas, maxReplicas, scaleStrategy.workersToDelete,
// template.spec.tolerations, template.spec.schedulingGates;
// head: template.spec.tolerations, template.spec.schedulingGates;
// top-level: upgradeStrategy (on the PARENT spec, but we zero it if present here too).
//
// The result is suitable for reflect.DeepEqual comparison.
func normalizeForHash(rcc map[string]any) map[string]any {
	if rcc == nil {
		return map[string]any{}
	}
	norm := cloneMap(rcc)

	// Zero excluded fields on head group.
	if head, ok := norm["headGroupSpec"].(map[string]any); ok {
		zeroPodExclusions(head)
	}

	// Zero excluded fields per worker group.
	groups, _ := norm["workerGroupSpecs"].([]any)
	for _, g := range groups {
		wg, ok := g.(map[string]any)
		if !ok {
			continue
		}
		delete(wg, "replicas")
		delete(wg, "minReplicas")
		delete(wg, "maxReplicas")
		// scaleStrategy.workersToDelete
		if ss, ok := wg["scaleStrategy"].(map[string]any); ok {
			delete(ss, "workersToDelete")
			if len(ss) == 0 {
				delete(wg, "scaleStrategy")
			}
		}
		zeroPodExclusions(wg)
	}

	// Top-level upgradeStrategy is excluded from the hash. It lives on the parent
	// RayService spec, not inside rayClusterConfig, but if it is somehow present
	// here (e.g. via rawSpec), zero it for safety.
	delete(norm, "upgradeStrategy")

	return norm
}

// zeroPodExclusions zeroes template.spec.tolerations and template.spec.schedulingGates
// on a group spec (head or worker).
func zeroPodExclusions(group map[string]any) {
	tmpl, ok := group["template"].(map[string]any)
	if !ok {
		return
	}
	podSpec, ok := tmpl["spec"].(map[string]any)
	if !ok {
		return
	}
	delete(podSpec, "tolerations")
	delete(podSpec, "schedulingGates")
}

// replicasOrExcludedChanged reports whether the live and submitted rayClusterConfig
// differ ONLY in the excluded fields (replicas, min, max, tolerations, etc.).
func replicasOrExcludedChanged(liveRCC, submittedRCC map[string]any) bool {
	// If the two full maps are already equal → no change at all.
	if reflect.DeepEqual(liveRCC, submittedRCC) {
		return false
	}
	// The normalized versions are equal (checked before calling this), so the
	// difference must be in the excluded fields only.
	return true
}

// readForWrite fetches the live RayService and returns a clean apply intent:
// the full object stripped of server-authored noise (status, managedFields, etc.).
func (s *ServiceWriteService) readForWrite(ctx context.Context, namespace, name string) (MergedSpec, map[string]any, error) {
	detail, err := s.get.GetService(ctx, namespace, name)
	if err != nil {
		return nil, nil, err
	}
	obj := cloneMap(detail.Raw)
	if obj == nil {
		obj = map[string]any{}
	}
	delete(obj, "status")
	obj["apiVersion"] = "ray.io/v1"
	obj["kind"] = string(KindRayService)

	meta, _ := obj["metadata"].(map[string]any)
	if meta == nil {
		meta = map[string]any{}
		obj["metadata"] = meta
	}
	for _, k := range []string{"managedFields", "resourceVersion", "uid", "creationTimestamp", "generation", "selfLink"} {
		delete(meta, k)
	}
	meta["name"] = name
	meta["namespace"] = namespace

	spec, _ := obj["spec"].(map[string]any)
	if spec == nil {
		spec = map[string]any{}
		obj["spec"] = spec
	}
	return obj, spec, nil
}

// rayClusterConfig returns (or creates) spec["rayClusterConfig"] as a map, for
// mutation by the update deltas. This is the embedded cluster spec on a RayService
// (JSON key "rayClusterConfig", Go field RayClusterSpec — the documented trap).
func rayClusterConfig(spec map[string]any) map[string]any {
	rcc, _ := spec["rayClusterConfig"].(map[string]any)
	if rcc == nil {
		rcc = map[string]any{}
		spec["rayClusterConfig"] = rcc
	}
	return rcc
}

func (s *ServiceWriteService) resolveNamespace(ns string) string {
	if ns == "" {
		return s.defaultNamespace
	}
	return ns
}

// deployArgsSummary builds the short audit summary for a deploy.
func deployArgsSummary(p ServiceDeployParams) string {
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

// serviceUpdateArgsSummary builds the bounded audit summary for a service update.
func serviceUpdateArgsSummary(p ServiceUpdateParams) string {
	parts := []string{fmt.Sprintf("name=%s", p.Name)}
	if p.ServeConfigV2 != nil {
		parts = append(parts, "serveConfigV2")
	}
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
