package kuberay

import (
	"context"
	"fmt"
	"time"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/risjai/ray-mcp/internal/domain"
)

// ListJobs returns a page of compact RayJob summaries plus the k8s continue token
// for the next page. It mirrors ListClusters: namespace scope (or all
// namespaces), a hard page cap (defaulting to defaultListLimit), and the opaque
// continue token surfaced unchanged so the domain reports "N shown, continue
// token X" rather than silently truncating (spec §10). Each row carries both the
// Ray-side job status and the CRD deployment status via toJobSummary.
func (c *Client) ListJobs(ctx context.Context, namespace string, opts domain.ListOptions) (domain.JobList, error) {
	k8s, err := c.ensureClient()
	if err != nil {
		return domain.JobList{}, err
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

	var rjl rayv1.RayJobList
	if err := k8s.List(ctx, &rjl, listOpts...); err != nil {
		return domain.JobList{}, mapK8sError(err, "list", domain.KindRayJob, namespace, "")
	}

	items := make([]domain.JobSummary, 0, len(rjl.Items))
	for i := range rjl.Items {
		items = append(items, toJobSummary(&rjl.Items[i]))
	}

	return domain.JobList{
		Items:    items,
		Continue: rjl.Continue,
	}, nil
}

// GetJob returns the distilled detail for one RayJob — phase 1 of the two-phase
// wedge read (Task 16a). It maps the operator-written status identity fields that
// bridge the k8s name to the Ray submission id and dashboard endpoint
// (status.jobId, dashboardURL, rayClusterName), which the domain JobService uses
// to decide whether the job is scheduled and, if so, where to dial. The full
// object is carried under Raw for the verbose/raw escape hatch.
func (c *Client) GetJob(ctx context.Context, namespace, name string) (domain.JobDetail, error) {
	k8s, err := c.ensureClient()
	if err != nil {
		return domain.JobDetail{}, err
	}

	var rj rayv1.RayJob
	if err := k8s.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &rj); err != nil {
		return domain.JobDetail{}, mapK8sError(err, "get", domain.KindRayJob, namespace, name)
	}

	return toJobDetail(&rj)
}

// toJobSummary maps a typed RayJob to the compact list row. It carries BOTH the
// Ray-side phase (status.jobStatus) and the CRD lifecycle
// (status.jobDeploymentStatus) — the two status fields ray_job_list surfaces side
// by side (spec §10).
func toJobSummary(rj *rayv1.RayJob) domain.JobSummary {
	return domain.JobSummary{
		Name:                rj.Name,
		Namespace:           rj.Namespace,
		JobStatus:           string(rj.Status.JobStatus),
		JobDeploymentStatus: string(rj.Status.JobDeploymentStatus),
		Age:                 jobAge(rj),
		Health:              jobHealth(rj),
	}
}

// toJobDetail extends the summary with the identity fields that bridge k8s name
// → Ray submission id + dashboard endpoint, plus the full object under Raw.
// Mirrors toClusterDetail: the GVK is set explicitly before the unstructured
// conversion (the typed client leaves TypeMeta empty on a Get decode) so Raw
// carries apiVersion/kind for the verbose escape hatch.
func toJobDetail(rj *rayv1.RayJob) (domain.JobDetail, error) {
	rj.SetGroupVersionKind(rayv1.GroupVersion.WithKind("RayJob"))

	raw, err := runtime.DefaultUnstructuredConverter.ToUnstructured(rj)
	if err != nil {
		return domain.JobDetail{}, fmt.Errorf("convert RayJob %q to unstructured: %w", rj.Name, err)
	}

	return domain.JobDetail{
		JobSummary:     toJobSummary(rj),
		JobID:          rj.Status.JobId,
		DashboardURL:   rj.Status.DashboardURL,
		RayClusterName: rj.Status.RayClusterName,
		Raw:            raw,
	}, nil
}

// jobAge reports the time since creation, guarding a zero CreationTimestamp (an
// object built in-memory before the apiserver stamps it) so we never report a
// nonsensical multi-decade age — same guard as clusterAge.
func jobAge(rj *rayv1.RayJob) time.Duration {
	if rj.CreationTimestamp.IsZero() {
		return 0
	}
	return time.Since(rj.CreationTimestamp.Time)
}

// jobHealth composes a single-line health summary from the CRD lifecycle
// (jobDeploymentStatus — the operator's view), the Ray-side phase (jobStatus),
// and the status message/reason when present. It is a glance, not the full
// status; the kind-agnostic join is the shared domain composer (status
// distillation design note §6). The CRD lifecycle leads because it answers the
// operator's first question — "has this been scheduled / is it provisioning" —
// before the Ray driver phase is meaningful.
func jobHealth(rj *rayv1.RayJob) string {
	return domain.HealthLine(
		deploymentStatusOrUnknown(rj),
		rayPhaseSegment(rj),
		domain.ConditionReason(string(rj.Status.Reason), rj.Status.Message),
	)
}

// deploymentStatusOrUnknown renders the CRD lifecycle phase, falling back to
// "Unknown" for the empty (New) status so the health line never starts blank.
func deploymentStatusOrUnknown(rj *rayv1.RayJob) string {
	if rj.Status.JobDeploymentStatus == "" {
		return "Unknown"
	}
	return string(rj.Status.JobDeploymentStatus)
}

// rayPhaseSegment renders the Ray driver phase as a labeled segment, or empty
// when the driver has not reported one yet (pre-scheduling) so HealthLine omits it.
func rayPhaseSegment(rj *rayv1.RayJob) string {
	if rj.Status.JobStatus == "" {
		return ""
	}
	return fmt.Sprintf("job %s", rj.Status.JobStatus)
}
