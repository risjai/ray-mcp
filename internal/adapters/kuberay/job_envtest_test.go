//go:build envtest

// Tier 2 (envtest) coverage for the KubeRay adapter's RayJob read path — the
// phase-1 CRD read of the two-phase wedge (Task 16a). It boots the same real
// apiserver+etcd+CRD harness as envtest_test.go and exercises GetJob's
// status→JobDetail mapping end-to-end: the identity fields that bridge the k8s
// name to the Ray submission id + dashboard endpoint (status.jobId,
// dashboardURL, rayClusterName, jobStatus, jobDeploymentStatus).
//
// As with the cluster tests, NO operator runs in envtest, so .status is written
// directly via the /status subresource to drive the mapping deterministically.
package kuberay

import (
	"context"
	"errors"
	"testing"
	"time"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/risjai/ray-mcp/internal/domain"
)

// newRayJob builds a minimal valid RayJob. The CRD requires spec.entrypoint and
// an embedded rayClusterSpec (head + worker groups), so the spec must satisfy
// both to pass envtest's schema validation on create.
func newRayJob(namespace, name string) *rayv1.RayJob {
	headContainer := corev1.Container{Name: "ray-head", Image: "rayproject/ray:2.9.0"}
	workerContainer := corev1.Container{Name: "ray-worker", Image: "rayproject/ray:2.9.0"}

	return &rayv1.RayJob{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: rayv1.RayJobSpec{
			Entrypoint: "python -c 'import ray; ray.init()'",
			RayClusterSpec: &rayv1.RayClusterSpec{
				HeadGroupSpec: rayv1.HeadGroupSpec{
					RayStartParams: map[string]string{},
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{Containers: []corev1.Container{headContainer}},
					},
				},
				WorkerGroupSpecs: []rayv1.WorkerGroupSpec{{
					GroupName:      "workers",
					Replicas:       ptr[int32](1),
					MinReplicas:    ptr[int32](0),
					MaxReplicas:    ptr[int32](5),
					RayStartParams: map[string]string{},
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{Containers: []corev1.Container{workerContainer}},
					},
				}},
			},
		},
	}
}

// TestGetJobMapsScheduledStatus creates a RayJob, writes a scheduled status
// (jobId + dashboardURL + rayClusterName + Running) via the /status subresource,
// and asserts GetJob maps the identity fields the wedge dials on.
func TestGetJobMapsScheduledStatus(t *testing.T) {
	adapter, k8s := startAdapter(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const (
		namespace    = "default"
		name         = "scheduled-job"
		jobID        = "raysubmit_abc123"
		clusterName  = "scheduled-job-raycluster-xyz"
		dashboardURL = "http://scheduled-job-head-svc.default.svc:8265"
	)

	rj := newRayJob(namespace, name)
	if err := k8s.Create(ctx, rj); err != nil {
		t.Fatalf("create RayJob: %v", err)
	}

	rj.Status = rayv1.RayJobStatus{
		JobId:               jobID,
		RayClusterName:      clusterName,
		DashboardURL:        dashboardURL,
		JobStatus:           rayv1.JobStatusRunning,
		JobDeploymentStatus: rayv1.JobDeploymentStatusRunning,
		Message:             "Job is running",
	}
	if err := k8s.Status().Update(ctx, rj); err != nil {
		t.Fatalf("status subresource update: %v", err)
	}

	detail, err := adapter.GetJob(ctx, namespace, name)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}

	if detail.Name != name {
		t.Errorf("Name = %q, want %q", detail.Name, name)
	}
	if detail.Namespace != namespace {
		t.Errorf("Namespace = %q, want %q", detail.Namespace, namespace)
	}
	if detail.JobID != jobID {
		t.Errorf("JobID = %q, want %q", detail.JobID, jobID)
	}
	if detail.RayClusterName != clusterName {
		t.Errorf("RayClusterName = %q, want %q", detail.RayClusterName, clusterName)
	}
	if detail.DashboardURL != dashboardURL {
		t.Errorf("DashboardURL = %q, want %q", detail.DashboardURL, dashboardURL)
	}
	if detail.JobStatus != "RUNNING" {
		t.Errorf("JobStatus = %q, want RUNNING", detail.JobStatus)
	}
	if detail.JobDeploymentStatus != "Running" {
		t.Errorf("JobDeploymentStatus = %q, want Running", detail.JobDeploymentStatus)
	}
	if detail.Age <= 0 {
		t.Errorf("Age = %v, want > 0", detail.Age)
	}
	if detail.Raw == nil {
		t.Error("Raw is nil, want the full object map")
	} else if kind, _ := detail.Raw["kind"].(string); kind != "RayJob" {
		t.Errorf("Raw[kind] = %q, want RayJob", kind)
	}
}

// TestGetJobNotYetScheduled asserts a freshly-created RayJob whose status carries
// neither jobId nor dashboardURL maps to empty identity fields — the domain gate
// reads this as "not yet scheduled" and never dials.
func TestGetJobNotYetScheduled(t *testing.T) {
	adapter, k8s := startAdapter(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const (
		namespace = "default"
		name      = "pending-job"
	)

	rj := newRayJob(namespace, name)
	if err := k8s.Create(ctx, rj); err != nil {
		t.Fatalf("create RayJob: %v", err)
	}
	// Initializing: jobId/rayClusterName may be set early, but dashboardURL is not
	// (the cluster is not Ready yet).
	rj.Status = rayv1.RayJobStatus{
		JobDeploymentStatus: rayv1.JobDeploymentStatusInitializing,
	}
	if err := k8s.Status().Update(ctx, rj); err != nil {
		t.Fatalf("status subresource update: %v", err)
	}

	detail, err := adapter.GetJob(ctx, namespace, name)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}

	if detail.JobID != "" {
		t.Errorf("JobID = %q, want empty (not yet scheduled)", detail.JobID)
	}
	if detail.DashboardURL != "" {
		t.Errorf("DashboardURL = %q, want empty (not yet scheduled)", detail.DashboardURL)
	}
	if detail.JobDeploymentStatus != "Initializing" {
		t.Errorf("JobDeploymentStatus = %q, want Initializing", detail.JobDeploymentStatus)
	}
}

// TestGetJobNotFound asserts a missing job name maps to *domain.NotFoundError
// with the RayJob kind.
func TestGetJobNotFound(t *testing.T) {
	adapter, _ := startAdapter(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	_, err := adapter.GetJob(ctx, "default", "no-such-job")
	var nf *domain.NotFoundError
	if !errors.As(err, &nf) {
		t.Fatalf("error = %T (%v), want *domain.NotFoundError", err, err)
	}
	if nf.Kind != domain.KindRayJob {
		t.Errorf("NotFoundError.Kind = %q, want %q", nf.Kind, domain.KindRayJob)
	}
	if nf.Name != "no-such-job" {
		t.Errorf("NotFoundError.Name = %q, want %q", nf.Name, "no-such-job")
	}
}
