package kuberay

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/risjai/ray-mcp/internal/domain"
)

// ListServices returns a page of compact RayService summaries plus the k8s
// continue token for the next page. It mirrors ListClusters/ListJobs: namespace
// scope (or all namespaces), a hard page cap (defaulting to defaultListLimit),
// and the opaque continue token surfaced unchanged so the domain reports "N
// shown, continue token X" rather than silently truncating (spec §10). Each row
// carries the distilled serve status, healthy serve endpoints and a one-line
// health summary via toServiceSummary.
func (c *Client) ListServices(ctx context.Context, namespace string, opts domain.ListOptions) (domain.ServiceList, error) {
	k8s, err := c.ensureClient()
	if err != nil {
		return domain.ServiceList{}, err
	}

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

	var rsl rayv1.RayServiceList
	if err := k8s.List(ctx, &rsl, listOpts...); err != nil {
		return domain.ServiceList{}, mapK8sError(err, "list", domain.KindRayService, namespace, "")
	}

	items := make([]domain.ServiceSummary, 0, len(rsl.Items))
	for i := range rsl.Items {
		items = append(items, toServiceSummary(&rsl.Items[i]))
	}

	return domain.ServiceList{
		Items:    items,
		Continue: rsl.Continue,
	}, nil
}

// GetService returns the distilled rollout view for one RayService (spec §6: the
// rollout phase + serve health, not raw .status), with the full object under Raw
// for the verbose/raw escape hatch.
func (c *Client) GetService(ctx context.Context, namespace, name string) (domain.ServiceDetail, error) {
	k8s, err := c.ensureClient()
	if err != nil {
		return domain.ServiceDetail{}, err
	}

	var rs rayv1.RayService
	if err := k8s.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &rs); err != nil {
		return domain.ServiceDetail{}, mapK8sError(err, "get", domain.KindRayService, namespace, name)
	}

	return toServiceDetail(&rs)
}

// toServiceSummary maps a typed RayService to the compact list row. ServiceStatus
// is the distilled serve status (derived from the Ready condition, not the raw
// deprecated field); HealthyReplicas is the operator-computed NumServeEndpoints
// (unique ready serve pods/endpoints — NOT re-summed from the Serve config).
func toServiceSummary(rs *rayv1.RayService) domain.ServiceSummary {
	return domain.ServiceSummary{
		Name:            rs.Name,
		Namespace:       rs.Namespace,
		ServiceStatus:   serviceServeStatus(rs),
		HealthyReplicas: rs.Status.NumServeEndpoints,
		Age:             serviceAge(rs),
		Health:          serviceHealth(rs),
	}
}

// toServiceDetail extends the summary with the rollout phase and the full object
// as a plain map under Raw. As with toClusterDetail/toJobDetail the GVK is set
// before conversion (the typed client leaves TypeMeta empty on a Get/List decode)
// so Raw carries apiVersion/kind for the verbose escape hatch.
func toServiceDetail(rs *rayv1.RayService) (domain.ServiceDetail, error) {
	rs.SetGroupVersionKind(rayv1.GroupVersion.WithKind("RayService"))

	raw, err := runtime.DefaultUnstructuredConverter.ToUnstructured(rs)
	if err != nil {
		return domain.ServiceDetail{}, fmt.Errorf("convert RayService %q to unstructured: %w", rs.Name, err)
	}

	return domain.ServiceDetail{
		ServiceSummary: toServiceSummary(rs),
		RolloutPhase:   serviceRolloutPhase(rs),
		Raw:            raw,
	}, nil
}

// serviceServeStatus derives the serve-status column from the Ready
// (RayServiceReady) condition — True→"Running", False→the not-ready reason —
// falling back to the deprecated Status.ServiceStatus (older operator that set it
// but not Conditions), then "Unknown". It never returns blank. The operator sets
// ServiceStatus directly from the Ready condition (v1.6.1), but that field
// collapses every not-ready state to "" — so the condition, not the field, is the
// primary source.
func serviceServeStatus(rs *rayv1.RayService) string {
	if cond := meta.FindStatusCondition(rs.Status.Conditions, string(rayv1.RayServiceReady)); cond != nil {
		if cond.Status == metav1.ConditionTrue {
			return "Running"
		}
		if cond.Reason != "" {
			return cond.Reason
		}
	}
	if deprecatedServiceStatus(rs) == rayv1.Running {
		return "Running"
	}
	return "Unknown"
}

// serviceRolloutPhase derives the rollout phase from the RayService conditions,
// switching on the condition TYPE/STATUS (not reason strings, which may shift
// between minor releases) in priority order, verified against the operator's own
// calculateConditions (v1.6.1):
//
//   - RollbackInProgress True → "RollingBack" (highest priority; both clusters
//     exist during a rollback, so it must outrank UpgradeInProgress).
//   - UpgradeInProgress True → "RollingOut" (the authoritative zero-downtime-swap
//     signal: set only when BOTH active and pending cluster names are non-empty —
//     so initial creation, which has only a pending cluster, is NOT mis-read here).
//   - Ready True → "Running".
//   - Ready False with an Initializing[Timeout] reason (or no conditions) →
//     "Initializing".
//   - else → "Unknown".
//
// Under the default NewCluster strategy cutover is an instantaneous promotion
// (ActiveServiceStatus = PendingServiceStatus), so there is no distinct typed
// "Cutover" phase to emit — the design's four-state ladder maps onto these
// observable states.
func serviceRolloutPhase(rs *rayv1.RayService) string {
	conds := rs.Status.Conditions

	switch {
	case meta.IsStatusConditionTrue(conds, string(rayv1.RollbackInProgress)):
		return "RollingBack"
	case meta.IsStatusConditionTrue(conds, string(rayv1.UpgradeInProgress)):
		return "RollingOut"
	case meta.IsStatusConditionTrue(conds, string(rayv1.RayServiceReady)):
		return "Running"
	}

	if ready := meta.FindStatusCondition(conds, string(rayv1.RayServiceReady)); ready != nil {
		switch ready.Reason {
		case string(rayv1.RayServiceInitializing), string(rayv1.RayServiceInitializingTimeout):
			return "Initializing"
		}
	} else if len(conds) == 0 && deprecatedServiceStatus(rs) == rayv1.Running {
		// No conditions at all: an older operator may have populated only the
		// deprecated ServiceStatus.
		return "Running"
	}

	return "Unknown"
}

// serviceHealth composes the one-line health summary from the rollout phase, the
// serve-endpoint count, and the most relevant serve detail. It is a glance, not
// the full status; the kind-agnostic join is the shared domain composer (status
// distillation design note §6).
func serviceHealth(rs *rayv1.RayService) string {
	return domain.HealthLine(
		serviceRolloutPhase(rs),
		fmt.Sprintf("%d serve endpoints", rs.Status.NumServeEndpoints),
		serviceHealthDetail(rs),
	)
}

// serviceHealthDetail extracts the most relevant serve detail for the health
// line. During a rollout (UpgradeInProgress/RollbackInProgress) the in-flight
// (pending) cluster's unhealthy apps surface as "new serve UNHEALTHY: <apps>" —
// the wedge signal, named WITHOUT a time threshold (we surface the fact, we do not
// claim "wedged"). Outside a rollout the active cluster's unhealthy apps surface
// as "serve UNHEALTHY: <apps>". When no app is named unhealthy but the service is
// not ready, the Ready condition's reason/message surfaces so a ValidationFailed
// (or initializing) service stays actionable from the one-line health.
func serviceHealthDetail(rs *rayv1.RayService) string {
	conds := rs.Status.Conditions
	rollingOut := meta.IsStatusConditionTrue(conds, string(rayv1.UpgradeInProgress)) ||
		meta.IsStatusConditionTrue(conds, string(rayv1.RollbackInProgress))

	if rollingOut {
		if apps := unhealthyApps(rs.Status.PendingServiceStatus.Applications); len(apps) > 0 {
			return "new serve UNHEALTHY: " + strings.Join(apps, ", ")
		}
	} else if apps := unhealthyApps(rs.Status.ActiveServiceStatus.Applications); len(apps) > 0 {
		return "serve UNHEALTHY: " + strings.Join(apps, ", ")
	}

	if ready := meta.FindStatusCondition(conds, string(rayv1.RayServiceReady)); ready != nil && ready.Status != metav1.ConditionTrue {
		return domain.ConditionReason(ready.Reason, ready.Message)
	}
	return ""
}

// unhealthyApps returns the sorted names of Serve applications in a terminal-bad
// state (UNHEALTHY or DEPLOY_FAILED). DEPLOYING/NOT_STARTED/DELETING are transient
// and deliberately not named. Sorted for a deterministic health line.
func unhealthyApps(apps map[string]rayv1.AppStatus) []string {
	var names []string
	for name, app := range apps {
		if app.Status == rayv1.ApplicationStatusEnum.UNHEALTHY || app.Status == rayv1.ApplicationStatusEnum.DEPLOY_FAILED {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}

// serviceAge reports the time since creation, guarding a zero CreationTimestamp
// (an object built in-memory before the apiserver stamps it) — same guard as
// clusterAge/jobAge.
func serviceAge(rs *rayv1.RayService) time.Duration {
	if rs.CreationTimestamp.IsZero() {
		return 0
	}
	return time.Since(rs.CreationTimestamp.Time)
}

// deprecatedServiceStatus reads rs.Status.ServiceStatus, the field KubeRay
// deprecated in favour of Conditions (v1.6.1). We read it deliberately as the
// last-resort fallback (for an older operator that populates ServiceStatus but
// not Conditions), so the deprecation warning is suppressed here, in one place,
// with this rationale — mirroring deprecatedState for RayCluster.
//
//nolint:staticcheck // SA1019: intentional deprecated-field fallback, see doc comment.
func deprecatedServiceStatus(rs *rayv1.RayService) rayv1.ServiceStatus {
	return rs.Status.ServiceStatus
}
