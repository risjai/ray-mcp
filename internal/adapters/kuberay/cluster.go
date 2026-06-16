package kuberay

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/risjai/ray-mcp/internal/domain"
)

// defaultListLimit is the per-page cap applied when ListOptions.Limit is 0
// (spec §10: every list paginates + caps at ~50 so a single call can never
// dump an unbounded set into the agent's context).
const defaultListLimit = 50

// dashboardPort is the Ray head dashboard / Job Submission REST API port. It is
// only used to synthesize a DashboardURL when the head service name is known;
// the RayCluster status carries no dashboard URL of its own (that is RayJob).
const dashboardPort = 8265

// ListClusters returns a page of compact RayCluster summaries plus the k8s
// continue token for the next page. It honors ListOptions verbatim: namespace
// scope (or all namespaces), a hard page cap (defaulting to defaultListLimit),
// and the opaque continue token. The continue token surfaces unchanged so the
// domain can report "N shown, continue token X" rather than silently truncating
// (spec §10).
func (c *Client) ListClusters(ctx context.Context, namespace string, opts domain.ListOptions) (domain.ClusterList, error) {
	limit := opts.Limit
	if limit <= 0 {
		limit = defaultListLimit
	}

	listOpts := []client.ListOption{client.Limit(limit)}
	if opts.AllNamespaces {
		listOpts = append(listOpts, client.InNamespace(""))
	} else {
		listOpts = append(listOpts, client.InNamespace(namespace))
	}
	if opts.Continue != "" {
		listOpts = append(listOpts, client.Continue(opts.Continue))
	}

	var rcl rayv1.RayClusterList
	if err := c.k8s.List(ctx, &rcl, listOpts...); err != nil {
		return domain.ClusterList{}, mapK8sError(err, "list", domain.KindRayCluster, namespace, "")
	}

	items := make([]domain.ClusterSummary, 0, len(rcl.Items))
	for i := range rcl.Items {
		items = append(items, toClusterSummary(&rcl.Items[i]))
	}

	return domain.ClusterList{
		Items:    items,
		Continue: rcl.Continue,
	}, nil
}

// GetCluster returns the distilled detail for one RayCluster, including the full
// object under Raw for the verbose/raw escape hatch.
func (c *Client) GetCluster(ctx context.Context, namespace, name string) (domain.ClusterDetail, error) {
	var rc rayv1.RayCluster
	if err := c.k8s.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &rc); err != nil {
		return domain.ClusterDetail{}, mapK8sError(err, "get", domain.KindRayCluster, namespace, name)
	}

	detail, err := toClusterDetail(&rc)
	if err != nil {
		return domain.ClusterDetail{}, err
	}
	return detail, nil
}

// toClusterSummary maps a typed RayCluster to the compact list row. The status
// fields it reads are operator-computed (worker-only ready/desired counts; the
// head is excluded) — they are NOT re-summed from the spec.
func toClusterSummary(rc *rayv1.RayCluster) domain.ClusterSummary {
	return domain.ClusterSummary{
		Name:            rc.Name,
		Namespace:       rc.Namespace,
		Phase:           clusterPhase(rc),
		ReadyReplicas:   rc.Status.ReadyWorkerReplicas,
		DesiredReplicas: rc.Status.DesiredWorkerReplicas,
		Age:             clusterAge(rc),
		Health:          clusterHealth(rc),
	}
}

// toClusterDetail extends the summary with the head service name, a synthesized
// dashboard URL (empty until the head service is up), and the full object as a
// plain map under Raw. Raw is produced via the unstructured converter so the
// distilled view never round-trips a typed object back into Unstructured for the
// guarded write path (that ambiguity is an SSA hazard reserved for Apply).
func toClusterDetail(rc *rayv1.RayCluster) (domain.ClusterDetail, error) {
	// The typed client leaves TypeMeta empty on a Get/List decode, so set the
	// GVK explicitly before converting; otherwise Raw would lack apiVersion/kind
	// and the verbose/raw escape hatch would hand back a headless object.
	rc.SetGroupVersionKind(rayv1.GroupVersion.WithKind("RayCluster"))

	raw, err := runtime.DefaultUnstructuredConverter.ToUnstructured(rc)
	if err != nil {
		return domain.ClusterDetail{}, fmt.Errorf("convert RayCluster %q to unstructured: %w", rc.Name, err)
	}

	return domain.ClusterDetail{
		ClusterSummary:  toClusterSummary(rc),
		HeadServiceName: rc.Status.Head.ServiceName,
		DashboardURL:    dashboardURL(rc),
		Raw:             raw,
	}, nil
}

// clusterPhase derives a human phase from the RayCluster conditions, which are
// authoritative in v1.6.1 (Status.State is deprecated). It switches on the
// condition TYPE, not reason strings (reason constants may shift between minor
// releases). The ladder mirrors the operator's own state machine; it falls back
// to the deprecated State only when no conditions are present (e.g. an older
// operator), and to "Unknown" when even that is empty.
func clusterPhase(rc *rayv1.RayCluster) string {
	conds := rc.Status.Conditions
	state := deprecatedState(rc)

	switch {
	case meta.IsStatusConditionTrue(conds, string(rayv1.RayClusterSuspended)) || state == rayv1.Suspended:
		return "Suspended"
	case meta.IsStatusConditionTrue(conds, string(rayv1.RayClusterSuspending)):
		return "Suspending"
	case meta.IsStatusConditionTrue(conds, string(rayv1.RayClusterReplicaFailure)):
		return "Failed"
	case meta.IsStatusConditionTrue(conds, string(rayv1.HeadPodReady)) &&
		meta.IsStatusConditionTrue(conds, string(rayv1.RayClusterProvisioned)):
		return "Ready"
	}

	// Provisioned condition present but not yet True: still coming up.
	if meta.FindStatusCondition(conds, string(rayv1.RayClusterProvisioned)) != nil {
		return "Provisioning"
	}

	// No conditions reported: fall back to the deprecated State, else Unknown.
	if state != "" {
		return string(state)
	}
	return "Unknown"
}

// deprecatedState reads rc.Status.State, the field KubeRay deprecated in favour
// of Conditions in v1.6.1. We read it deliberately as the last-resort fallback
// in the phase ladder (for an older operator that populates State but not
// Conditions), so the deprecation warning is suppressed here, in one place, with
// this rationale — not scattered across the call sites.
//
//nolint:staticcheck // SA1019: intentional deprecated-field fallback, see doc comment.
func deprecatedState(rc *rayv1.RayCluster) rayv1.ClusterState {
	return rc.Status.State
}

// clusterAge reports the time since creation, guarding the zero CreationTimestamp
// (an object built in-memory before the apiserver stamps it) so we never report a
// nonsensical multi-decade age.
func clusterAge(rc *rayv1.RayCluster) time.Duration {
	if rc.CreationTimestamp.IsZero() {
		return 0
	}
	return time.Since(rc.CreationTimestamp.Time)
}

// clusterHealth composes a single-line health summary from the phase, the
// worker-ready/desired counts, and the most relevant condition's
// reason/message (falling back to the deprecated Status.Reason). It is a glance,
// not the full status.
func clusterHealth(rc *rayv1.RayCluster) string {
	phase := clusterPhase(rc)
	parts := []string{
		phase,
		fmt.Sprintf("%d/%d workers ready", rc.Status.ReadyWorkerReplicas, rc.Status.DesiredWorkerReplicas),
	}

	if detail := healthDetail(rc); detail != "" {
		parts = append(parts, detail)
	}

	return strings.Join(parts, "; ")
}

// healthDetail extracts the most relevant reason/message for the health line: a
// ReplicaFailure condition when present, else the not-yet-True Provisioned
// condition, else the deprecated Status.Reason. Returns empty when there is
// nothing actionable to add.
func healthDetail(rc *rayv1.RayCluster) string {
	conds := rc.Status.Conditions

	if cond := meta.FindStatusCondition(conds, string(rayv1.RayClusterReplicaFailure)); cond != nil && cond.Status == metav1.ConditionTrue {
		return condReason(cond)
	}
	if cond := meta.FindStatusCondition(conds, string(rayv1.RayClusterProvisioned)); cond != nil && cond.Status != metav1.ConditionTrue {
		return condReason(cond)
	}
	return rc.Status.Reason
}

// condReason renders a condition's reason/message compactly for the health line.
func condReason(cond *metav1.Condition) string {
	switch {
	case cond.Reason != "" && cond.Message != "":
		return fmt.Sprintf("%s: %s", cond.Reason, cond.Message)
	case cond.Message != "":
		return cond.Message
	default:
		return cond.Reason
	}
}

// dashboardURL synthesizes the in-cluster head dashboard URL from the head
// service name, or returns empty when the head service is not yet up. The
// RayCluster status carries no dashboard URL of its own, so this is a derived
// convenience — never string-templated from a guess (spec C2): if the operator
// has not populated Head.ServiceName we return empty rather than fabricate one.
func dashboardURL(rc *rayv1.RayCluster) string {
	svc := rc.Status.Head.ServiceName
	if svc == "" {
		return ""
	}
	return fmt.Sprintf("http://%s.%s.svc:%d", svc, rc.Namespace, dashboardPort)
}

// mapK8sError maps a controller-runtime/client-go error to the domain error
// taxonomy so callers branch on typed errors, never on string matching. The
// original error is preserved via %w on the fallthrough so context is not lost.
func mapK8sError(err error, verb string, kind domain.Kind, namespace, name string) error {
	switch {
	case apierrors.IsNotFound(err):
		return &domain.NotFoundError{Kind: kind, Namespace: namespace, Name: name}
	case apierrors.IsForbidden(err):
		return &domain.ForbiddenError{Verb: verb, Resource: resourceFor(kind), Namespace: namespace}
	case apierrors.IsConflict(err):
		return &domain.ConflictError{Kind: kind, Namespace: namespace, Name: name, Detail: err.Error()}
	case errors.Is(err, context.DeadlineExceeded) || apierrors.IsTimeout(err) || apierrors.IsServerTimeout(err):
		return &domain.TimeoutError{Op: fmt.Sprintf("%s %s", verb, kind)}
	default:
		return fmt.Errorf("%s %s %q in namespace %q: %w", verb, kind, name, namespace, err)
	}
}

// resourceFor maps a Kind to its lowercase plural resource name for RBAC error
// messages (the verb/resource pair the operator must grant).
func resourceFor(kind domain.Kind) string {
	switch kind {
	case domain.KindRayCluster:
		return "rayclusters"
	case domain.KindRayJob:
		return "rayjobs"
	case domain.KindRayService:
		return "rayservices"
	default:
		return strings.ToLower(string(kind)) + "s"
	}
}
