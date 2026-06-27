package domain

import (
	"context"
	"fmt"
	"strings"
)

// The RayJob write path (spec §7, Task 18): the non-blocking ray_job_submit. It
// reuses the same unified apply pipeline the RayCluster writes use — the curated
// base (JobBaseBuilder, an adapter port that needs the KubeRay Go types the domain
// must not import), the pure RFC 7396 Merge + identity guard, and the
// cluster-touching ApplyService (DryRunAll → SSA → read-back diff → audit). The
// service is the thin glue that resolves write policy and runs them in order.
//
// A RayJob runs against EXACTLY ONE cluster target (spec §6, Q16, ratified
// two-mode schema):
//   - ExistingCluster: run against an already-existing RayCluster
//     (→ spec.clusterSelector["ray.io/cluster"]); no cluster is created or deleted.
//   - ClusterSpec: an ephemeral cluster KubeRay creates for the job
//     (→ spec.rayClusterSpec), torn down on completion when
//     shutdownAfterJobFinishes is set.
//
// Supplying both, or neither, is a validation error (stricter than KubeRay, which
// silently ignores rayClusterSpec when both are set — surfacing that as an error
// is the safer ray-mcp choice; spec §6).

// ClusterSubmitSpec is the curated ephemeral-cluster shape for ray_job_submit's
// clusterSpec mode. It is the same curated cluster knobs as ClusterCreateParams
// (minus identity, which the RayJob owns and the embedded cluster must not carry)
// — the adapter maps it to the RayJob's spec.rayClusterSpec. The rawSpec escape
// hatch is the submit tool's single top-level RawSpec (merged over the whole
// RayJob), so a power-user tweaks the embedded cluster via
// rawSpec:{spec:{rayClusterSpec:{...}}} rather than a second nested escape hatch.
type ClusterSubmitSpec struct {
	RayVersion        string
	Image             string
	HeadResources     ResourceQuantities
	WorkerGroups      []WorkerGroupParams
	EnableAutoscaling bool
}

// JobSubmitParams is the decoded ray_job_submit argument set: the identity +
// entrypoint, exactly one cluster target, the optional runtime env / metadata,
// the ephemeral shutdown knob (Q16b), and the shared rawSpec/dryRun. Namespace is
// resolved by the service (default fallback) before the base is built; RawSpec is
// merged OVER the curated base (rawSpec wins).
//
// ShutdownAfterJobFinishes is a *bool so the service can tell "unset" (default to
// true for ephemeral mode, Q16b) from an explicit false ("keep the cluster for
// debugging"). It is only valid in ClusterSpec mode — setting it with
// ExistingCluster is refused, because KubeRay would never tear down a cluster the
// job does not own and honoring it would be a silent no-op surprise.
type JobSubmitParams struct {
	Namespace  string
	Name       string
	Entrypoint string

	ExistingCluster string             // mode A: spec.clusterSelector["ray.io/cluster"].
	ClusterSpec     *ClusterSubmitSpec // mode B: spec.rayClusterSpec (ephemeral).

	RuntimeEnvYAML           string
	Metadata                 map[string]string
	ShutdownAfterJobFinishes *bool // ephemeral-only; nil → default true (Q16b).

	RawSpec MergedSpec // the escape hatch, merged over the curated base; nil for curated-only.
	DryRun  bool
}

// JobBaseBuilder turns the curated half of JobSubmitParams into the base
// unstructured RayJob (spec §7.C step 1: curated params → typed KubeRay object →
// JSON map). It is an adapter port because that construction needs the compiled
// KubeRay Go types the domain must not import. The returned base carries
// metadata.name/namespace equal to the params identity so Merge's identity guard
// passes. It must NOT apply RawSpec — that is Merge's job. By the time the builder
// is called the service has already resolved the mode and the shutdown default, so
// the builder reads ShutdownAfterJobFinishes as a concrete (non-nil) decision.
type JobBaseBuilder interface {
	BuildJobBase(p JobSubmitParams) (MergedSpec, error)
}

// JobGetter is the narrow read slice the delete pipeline needs: fetch the live
// RayJob to read its mode (ephemeral vs existing-cluster), protected annotation,
// and identity for the confirm fingerprint. It mirrors ClusterGetter; the KubeRay
// adapter and the full JobReader both satisfy it. JobDetail.Raw carries the full
// unstructured object the guards read.
type JobGetter interface {
	GetJob(ctx context.Context, namespace, name string) (JobDetail, error)
}

// JobDeleteParams is the decoded ray_job_delete argument set: the target identity,
// the confirm fingerprint (empty for preview, echoed back for commit — used only
// in the ephemeral mode), the AllowDestructive tier flag (gates the ephemeral
// cascade, B3/Q16a), and the dryRun flag.
type JobDeleteParams struct {
	Namespace        string
	Name             string
	Confirm          string
	AllowDestructive bool // the --allow-destructive tier; required for an ephemeral cascade (Q16a).
	DryRun           bool
}

// JobWriteService is the orchestration layer for the RayJob write tools. It owns
// the cross-cutting write policy the MCP layer must not duplicate: the
// default-namespace fallback, the mode XOR + shutdown-default resolution, the
// curated→base→merge→apply sequence, and the audit-carrying ApplyService. It
// imports no Kubernetes or HTTP packages — only the JobBaseBuilder port, the pure
// Merge, and the ApplyService.
type JobWriteService struct {
	base             JobBaseBuilder
	get              JobGetter
	del              Deleter
	apply            *ApplyService
	defaultNamespace string
}

// NewJobWriteService builds the service over the base builder (submit), the
// live-object reader + deleter (mode-aware delete, Q16a), the apply pipeline (Task
// 8b), and the default namespace (injected as a plain string so the domain stays
// config/k8s-free).
func NewJobWriteService(base JobBaseBuilder, get JobGetter, del Deleter, apply *ApplyService, defaultNamespace string) *JobWriteService {
	return &JobWriteService{base: base, get: get, del: del, apply: apply, defaultNamespace: defaultNamespace}
}

// DefaultNamespace returns the namespace used when a submit omits one, so the MCP
// layer can echo the real target namespace in its result.
func (s *JobWriteService) DefaultNamespace() string {
	return s.defaultNamespace
}

// JobSubmitResult is the non-blocking submit outcome: the identity, whether this
// was a dry-run and whether it was the ephemeral-cluster mode, plus the
// just-submitted view read back from the server (spec §7.A, Q11:
// {name, jobId-when-ready, initialStatus}). JobID/DeploymentStatus are read from
// the server's read-back status — likely empty/New right after submit (the
// controller has not reconciled yet), so the agent follows with ray_job_get /
// ray_job_wait. The field-level Diff mirrors the create result (spec §10).
type JobSubmitResult struct {
	Name             string
	Namespace        string
	DryRun           bool
	Ephemeral        bool
	JobID            string // status.jobId once the controller sets it (often empty at submit).
	DeploymentStatus string // status.jobDeploymentStatus (empty == New).
	Diff             DiffResult
}

// Submit runs the non-blocking submit through the unified apply pipeline:
//
//  1. resolve the namespace (default fallback) and validate identity + entrypoint
//     + the exactly-one-cluster-target XOR + the ephemeral-only shutdown rule;
//  2. resolve the ephemeral shutdown default (Q16b) onto the params;
//  3. build the curated RayJob base (JobBaseBuilder, step 1);
//  4. merge rawSpec over the base, rawSpec wins, identity-guarded (Merge);
//  5. hand the merged spec to the ApplyService (DryRunAll → maybe SSA → read-back
//     diff → audit), keyed by KindRayJob.
//
// It returns immediately after the apply read-back — it does NOT wait for the job
// to schedule or run (a Ray job runs minutes-to-hours; blocking would time out on
// exactly the long jobs that matter — spec §7.A). A validation failure is returned
// before any cluster call.
func (s *JobWriteService) Submit(ctx context.Context, p JobSubmitParams) (JobSubmitResult, error) {
	p.Namespace = s.resolveNamespace(p.Namespace)
	if err := s.validate(&p); err != nil {
		return JobSubmitResult{}, err
	}

	base, err := s.base.BuildJobBase(p)
	if err != nil {
		return JobSubmitResult{}, err
	}

	merged, err := Merge(base, p.RawSpec, Identity{Namespace: p.Namespace, Name: p.Name})
	if err != nil {
		return JobSubmitResult{}, err
	}

	res, err := s.apply.Apply(ctx, ApplyRequest{
		Kind:        KindRayJob,
		Namespace:   p.Namespace,
		Name:        p.Name,
		Spec:        merged,
		DryRun:      p.DryRun,
		Tool:        "ray_job_submit",
		ArgsSummary: submitArgsSummary(p),
	})
	if err != nil {
		return JobSubmitResult{}, err
	}

	jobID, _ := nestedString(res.Object, "status", "jobId")
	deployStatus, _ := nestedString(res.Object, "status", "jobDeploymentStatus")
	return JobSubmitResult{
		Name:             p.Name,
		Namespace:        p.Namespace,
		DryRun:           res.DryRun,
		Ephemeral:        p.ClusterSpec != nil,
		JobID:            jobID,
		DeploymentStatus: deployStatus,
		Diff:             res.Diff,
	}, nil
}

// Delete runs the mode-aware delete pipeline for one RayJob (Q16a). The blast
// radius is mode-dependent, so the tiering is too:
//
//   - EPHEMERAL (owns its cluster via spec.rayClusterSpec): deleting it cascades
//     to the whole RayCluster and every actor/job on it — a destructive teardown.
//     Gated behind the destructive tier (AllowDestructive) AND a confirm
//     fingerprint (preview → commit), exactly like ray_cluster_delete.
//   - EXISTING-CLUSTER (spec.clusterSelector): deleting it only removes the RayJob
//     record; the targeted cluster is untouched. A plain write — no tier, no
//     confirm.
//
// The order mirrors ClusterWriteService.Delete: resolve ns → require name →
// GetJob (NotFound propagates) → protected check BEFORE any tier/confirm (a
// protected job never yields a fingerprint, regardless of mode) → mode branch.
// For the ephemeral branch the tier gate precedes the confirm gate, so a job that
// would cascade never mints a fingerprint until the operator has opted into the
// destructive tier. Both branches end at the shared Deleter + one audit record.
func (s *JobWriteService) Delete(ctx context.Context, p JobDeleteParams) error {
	p.Namespace = s.resolveNamespace(p.Namespace)
	if p.Name == "" {
		return fmt.Errorf("name is required")
	}

	detail, err := s.get.GetJob(ctx, p.Namespace, p.Name)
	if err != nil {
		return err
	}

	if IsProtected(detail.Raw) {
		return fmt.Errorf("deletion refused: RayJob %q is protected (ray-mcp/protected=%q); remove the annotation via rawSpec/update first", p.Name, protectedValue)
	}

	if jobIsEphemeral(detail.Raw) {
		// Ephemeral cascade: destructive tier first, then confirm-fingerprint.
		if !p.AllowDestructive {
			return fmt.Errorf(
				"deleting RayJob %q cascade-deletes its ephemeral RayCluster (and every actor/job on it) and requires --allow-destructive", p.Name)
		}
		if err := RequireConfirm(detail.Raw, OpDelete, p.Confirm); err != nil {
			return err
		}
	}

	delErr := s.del.Delete(ctx, KindRayJob, p.Namespace, p.Name, p.DryRun)
	s.apply.RecordDestructive(ctx, "ray_job_delete", KindRayJob, p.Namespace, p.Name, jobDeleteArgsSummary(p), p.DryRun, delErr)
	return delErr
}

// jobIsEphemeral classifies a live RayJob's cluster mode from its spec: ephemeral
// iff it owns a cluster (spec.rayClusterSpec present) AND does not target an
// existing one (spec.clusterSelector empty). clusterSelector WINS when both are
// set — KubeRay treats a both-set RayJob as existing-cluster (it ignores
// rayClusterSpec), so an attached job is never mis-tiered as a cascade. This
// reads detail.Raw directly (no typed round-trip) so a wedge-era field newer than
// the compiled KubeRay baseline cannot perturb the decision.
func jobIsEphemeral(live MergedSpec) bool {
	spec, _ := live["spec"].(map[string]any)
	if spec == nil {
		return false
	}
	if sel, ok := spec["clusterSelector"].(map[string]any); ok && len(sel) > 0 {
		return false
	}
	rcs, ok := spec["rayClusterSpec"].(map[string]any)
	return ok && rcs != nil
}

// jobDeleteArgsSummary builds the short, bounded audit summary for a job delete
// (spec §8, Q8: a summary, never the full spec — and never the fingerprint).
// Mirrors deleteArgsSummary for clusters.
func jobDeleteArgsSummary(p JobDeleteParams) string {
	return fmt.Sprintf("name=%s dryRun=%t", p.Name, p.DryRun)
}

// validate enforces the submit invariants before any cluster call, and resolves
// the ephemeral shutdown default onto p (Q16b) once the mode is known. The order
// is identity → entrypoint → exactly-one-target → ephemeral-only-shutdown so the
// most fundamental error surfaces first.
func (s *JobWriteService) validate(p *JobSubmitParams) error {
	if p.Name == "" {
		return fmt.Errorf("name is required")
	}
	if p.Entrypoint == "" {
		// KubeRay does not reject an empty entrypoint (the apply succeeds and the
		// driver fails opaquely later), so ray-mcp must.
		return fmt.Errorf("entrypoint is required")
	}

	existing := p.ExistingCluster != ""
	ephemeral := p.ClusterSpec != nil
	if existing == ephemeral {
		return fmt.Errorf("exactly one of existingCluster or clusterSpec must be set (got %s)", bothOrNeither(existing))
	}

	if existing {
		if p.ShutdownAfterJobFinishes != nil {
			return fmt.Errorf("shutdownAfterJobFinishes applies only to clusterSpec (ephemeral) mode; existingCluster jobs never delete the cluster they target")
		}
		return nil
	}

	// Ephemeral mode: default shutdown to true (Q16b — diverges from KubeRay's
	// false for a safer cost posture); an explicit value is preserved.
	if p.ShutdownAfterJobFinishes == nil {
		enabled := true
		p.ShutdownAfterJobFinishes = &enabled
	}
	return nil
}

// bothOrNeither renders which side of the XOR failed for the validation message.
func bothOrNeither(existing bool) string {
	if existing {
		return "both"
	}
	return "neither"
}

// resolveNamespace applies the default-namespace fallback (mirrors the read and
// cluster-write services so all paths share one namespace policy).
func (s *JobWriteService) resolveNamespace(ns string) string {
	if ns == "" {
		return s.defaultNamespace
	}
	return ns
}

// submitArgsSummary builds the short, bounded audit summary for a submit (spec §8,
// Q8: a summary, never the full spec). It names the mode + whether a rawSpec was
// supplied — enough to answer "what was attempted" without echoing the object.
func submitArgsSummary(p JobSubmitParams) string {
	parts := []string{fmt.Sprintf("name=%s", p.Name)}
	if p.ExistingCluster != "" {
		parts = append(parts, fmt.Sprintf("existingCluster=%s", p.ExistingCluster))
	} else {
		parts = append(parts, "ephemeral=true")
		if p.ShutdownAfterJobFinishes != nil {
			parts = append(parts, fmt.Sprintf("shutdownAfterJobFinishes=%t", *p.ShutdownAfterJobFinishes))
		}
	}
	if len(p.RawSpec) > 0 {
		parts = append(parts, "rawSpec=true")
	}
	return strings.Join(parts, " ")
}
