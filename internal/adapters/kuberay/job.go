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

// toJobSummary maps a typed RayJob to the compact list row. JobStatus is the
// Ray-side phase (status.jobStatus), distinct from the CRD lifecycle
// (status.jobDeploymentStatus) carried on the detail.
func toJobSummary(rj *rayv1.RayJob) domain.JobSummary {
	return domain.JobSummary{
		Name:      rj.Name,
		Namespace: rj.Namespace,
		JobStatus: string(rj.Status.JobStatus),
		Age:       jobAge(rj),
		Health:    jobHealth(rj),
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
		JobSummary:          toJobSummary(rj),
		JobID:               rj.Status.JobId,
		DashboardURL:        rj.Status.DashboardURL,
		JobDeploymentStatus: string(rj.Status.JobDeploymentStatus),
		RayClusterName:      rj.Status.RayClusterName,
		Raw:                 raw,
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
