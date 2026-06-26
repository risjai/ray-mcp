//go:build envtest

// Combined CRD + dashboard coverage for the two-phase "wedge" read (Task 16a,
// Checkpoint E — the project's core differentiator). This is the AC's combined
// harness: a REAL RayJob CRD read (envtest apiserver) fused with a REAL Ray
// dashboard read (httptest) through the production domain.JobService and the
// production kuberay + rayapi adapters.
//
// The reachability resolver is the one stubbed seam: envtest runs no kubelet, so
// there is no real head pod to port-forward or service to dial. Reachability is
// the injectable strategy precisely so the endpoint can be pointed at the
// httptest dashboard here; BOTH reads it sits between (phase-1 CRD, phase-2 HTTP)
// are the real adapters.
package kuberay

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"

	"github.com/risjai/ray-mcp/internal/adapters/rayapi"
	"github.com/risjai/ray-mcp/internal/config"
	"github.com/risjai/ray-mcp/internal/domain"
)

// fixedEndpoint is a RayReachability that returns one canned endpoint regardless
// of cluster — standing in for the head-pod resolution envtest cannot perform.
type fixedEndpoint struct{ baseURL string }

func (f fixedEndpoint) Endpoint(_ context.Context, _, _ string, _ int) (domain.Endpoint, error) {
	return domain.Endpoint{BaseURL: f.baseURL}, nil
}

// TestWedgeTwoPhaseGetAndLogs is the end-to-end proof: phase 1 reads a scheduled
// RayJob from the real apiserver; phase 2 dials the real rayapi client against an
// httptest dashboard; the JobService fuses them into a distilled, live view.
func TestWedgeTwoPhaseGetAndLogs(t *testing.T) {
	adapter, k8s := startAdapter(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const (
		namespace = "default"
		name      = "wedge-job"
		jobID     = "raysubmit_wedge1"
		cluster   = "wedge-job-raycluster"
		logBody   = "epoch 1/3\nepoch 2/3\nepoch 3/3\n"
	)

	// httptest dashboard: status RUNNING + a small log buffer, keyed by jobID.
	dashboard := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/jobs/" + jobID:
			writeJSON(t, w, `{"submission_id":%q,"status":"RUNNING","message":"training","start_time":1700000000000}`, jobID)
		case "/api/jobs/" + jobID + "/logs":
			writeJSON(t, w, `{"logs":%q}`, logBody)
		default:
			t.Errorf("unexpected dashboard path %q", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer dashboard.Close()

	// Phase-1 fixture: a scheduled RayJob (jobId + dashboardURL both set → the gate
	// opens). The dashboardURL value is the CRD's own; phase 2 dials the endpoint
	// reachability returns, which here is the httptest server.
	rj := newRayJob(namespace, name)
	if err := k8s.Create(ctx, rj); err != nil {
		t.Fatalf("create RayJob: %v", err)
	}
	rj.Status = rayv1.RayJobStatus{
		JobId:               jobID,
		RayClusterName:      cluster,
		DashboardURL:        "http://" + name + "-head-svc.default.svc:8265",
		JobStatus:           rayv1.JobStatusRunning,
		JobDeploymentStatus: rayv1.JobDeploymentStatusRunning,
	}
	if err := k8s.Status().Update(ctx, rj); err != nil {
		t.Fatalf("status subresource update: %v", err)
	}

	// Production wiring: real kuberay adapter (phase 1), stubbed reachability
	// pointing at the httptest dashboard, real rayapi client (phase 2).
	api := rayapi.NewClient(&config.Config{})
	svc := domain.NewJobService(adapter, fixedEndpoint{baseURL: dashboard.URL}, api, namespace)

	// ray_job_get: the live dashboard status must come through fused with identity.
	got, err := svc.Get(ctx, domain.JobGetRequest{Name: name})
	if err != nil {
		t.Fatalf("JobService.Get: %v", err)
	}
	if !got.Scheduled {
		t.Fatalf("Scheduled = false, want true for a job past the dial gate")
	}
	if got.Live == nil || got.Live.Status != "RUNNING" {
		t.Fatalf("Live = %+v, want the dashboard's RUNNING status", got.Live)
	}
	if got.Live.Message != "training" {
		t.Errorf("Live.Message = %q, want training (from the dashboard)", got.Live.Message)
	}
	if got.Detail.JobID != jobID {
		t.Errorf("Detail.JobID = %q, want %q (from the CRD)", got.Detail.JobID, jobID)
	}
	if got.Live.StartedAt.IsZero() {
		t.Errorf("Live.StartedAt is zero, want the dashboard's start_time")
	}

	// ray_job_logs: the bounded tail must come from the same dashboard.
	logs, err := svc.Logs(ctx, domain.JobLogsRequest{Name: name})
	if err != nil {
		t.Fatalf("JobService.Logs: %v", err)
	}
	if !logs.Scheduled || logs.Logs == nil {
		t.Fatalf("logs Scheduled=%v Logs=%v, want a dialed tail", logs.Scheduled, logs.Logs)
	}
	if logs.Logs.Text != logBody {
		t.Errorf("logs.Text = %q, want %q", logs.Logs.Text, logBody)
	}
}

// TestWedgeWaitReachesRunning proves ray_job_wait (Task 17) end-to-end through
// the combined harness: phase 1 reads a scheduled RayJob from the real apiserver,
// phase 2 dials the real rayapi client against an httptest dashboard reporting
// RUNNING, and the bounded wait returns reached=true with the live status — no
// real sleeping, since the condition is met on the first poll.
func TestWedgeWaitReachesRunning(t *testing.T) {
	adapter, k8s := startAdapter(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const (
		namespace = "default"
		name      = "wedge-wait"
		jobID     = "raysubmit_wait1"
		cluster   = "wedge-wait-raycluster"
	)

	dashboard := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/jobs/"+jobID {
			writeJSON(t, w, `{"submission_id":%q,"status":"RUNNING","message":"training"}`, jobID)
			return
		}
		t.Errorf("unexpected dashboard path %q", r.URL.Path)
		w.WriteHeader(http.StatusNotFound)
	}))
	defer dashboard.Close()

	rj := newRayJob(namespace, name)
	if err := k8s.Create(ctx, rj); err != nil {
		t.Fatalf("create RayJob: %v", err)
	}
	rj.Status = rayv1.RayJobStatus{
		JobId:               jobID,
		RayClusterName:      cluster,
		DashboardURL:        "http://" + name + "-head-svc.default.svc:8265",
		JobStatus:           rayv1.JobStatusRunning,
		JobDeploymentStatus: rayv1.JobDeploymentStatusRunning,
	}
	if err := k8s.Status().Update(ctx, rj); err != nil {
		t.Fatalf("status subresource update: %v", err)
	}

	api := rayapi.NewClient(&config.Config{})
	svc := domain.NewJobService(adapter, fixedEndpoint{baseURL: dashboard.URL}, api, namespace)

	got, err := svc.Wait(ctx, domain.JobWaitRequest{Name: name, Until: "running", WaitSeconds: 30})
	if err != nil {
		t.Fatalf("JobService.Wait: %v", err)
	}
	if !got.Reached {
		t.Errorf("Reached = false, want true for a RUNNING job under until=running")
	}
	if got.Live == nil || got.Live.Status != "RUNNING" {
		t.Fatalf("Live = %+v, want the dashboard's RUNNING status", got.Live)
	}
}

// TestWedgeNotScheduledNoDial proves the AC's negative path against the real
// apiserver: a RayJob whose status has not reached the dial gate returns
// "not scheduled" WITHOUT any dashboard dial. A dashboard that fails the test if
// hit proves phase 2 never ran.
func TestWedgeNotScheduledNoDial(t *testing.T) {
	adapter, k8s := startAdapter(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const (
		namespace = "default"
		name      = "wedge-pending"
	)

	dashboard := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("dashboard dialed for a not-scheduled job: %s", r.URL.Path)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer dashboard.Close()

	rj := newRayJob(namespace, name)
	if err := k8s.Create(ctx, rj); err != nil {
		t.Fatalf("create RayJob: %v", err)
	}
	// Initializing, no dashboardURL → gate stays closed.
	rj.Status = rayv1.RayJobStatus{JobDeploymentStatus: rayv1.JobDeploymentStatusInitializing}
	if err := k8s.Status().Update(ctx, rj); err != nil {
		t.Fatalf("status subresource update: %v", err)
	}

	api := rayapi.NewClient(&config.Config{})
	svc := domain.NewJobService(adapter, fixedEndpoint{baseURL: dashboard.URL}, api, namespace)

	got, err := svc.Get(ctx, domain.JobGetRequest{Name: name})
	if err != nil {
		t.Fatalf("JobService.Get: %v (a not-scheduled job must not error)", err)
	}
	if got.Scheduled {
		t.Errorf("Scheduled = true, want false (no dashboardURL yet)")
	}
	if got.Live != nil {
		t.Errorf("Live = %+v, want nil (no dial before scheduling)", got.Live)
	}
	if got.Detail.JobDeploymentStatus != "Initializing" {
		t.Errorf("Detail.JobDeploymentStatus = %q, want Initializing", got.Detail.JobDeploymentStatus)
	}
}

// TestWedgeDegradesWhenDashboardUnreachable proves the AC's graceful-degrade path
// end-to-end against the real apiserver AND the real rayapi client (Task 16b): a
// SCHEDULED RayJob whose dashboard cannot be dialed returns the CRD-derived view
// annotated "degraded" with a bounded reason — never a hard failure. The
// dashboard is a closed httptest server (its URL captured, then shut down), so
// the real rayapi client gets a genuine connection-refused that the JobService
// must absorb into Degraded rather than propagate.
func TestWedgeDegradesWhenDashboardUnreachable(t *testing.T) {
	adapter, k8s := startAdapter(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const (
		namespace = "default"
		name      = "wedge-degraded"
		jobID     = "raysubmit_degraded1"
		cluster   = "wedge-degraded-raycluster"
	)

	// Stand up then immediately close a dashboard so its address is real but
	// refuses connections — a genuine dial failure for the real rayapi client.
	dashboard := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	deadURL := dashboard.URL
	dashboard.Close()

	rj := newRayJob(namespace, name)
	if err := k8s.Create(ctx, rj); err != nil {
		t.Fatalf("create RayJob: %v", err)
	}
	// Scheduled (jobId + dashboardURL set) → the gate opens and phase 2 dials.
	rj.Status = rayv1.RayJobStatus{
		JobId:               jobID,
		RayClusterName:      cluster,
		DashboardURL:        "http://" + name + "-head-svc.default.svc:8265",
		JobStatus:           rayv1.JobStatusRunning,
		JobDeploymentStatus: rayv1.JobDeploymentStatusRunning,
	}
	if err := k8s.Status().Update(ctx, rj); err != nil {
		t.Fatalf("status subresource update: %v", err)
	}

	api := rayapi.NewClient(&config.Config{})
	svc := domain.NewJobService(adapter, fixedEndpoint{baseURL: deadURL}, api, namespace)

	got, err := svc.Get(ctx, domain.JobGetRequest{Name: name})
	if err != nil {
		t.Fatalf("JobService.Get returned a hard error for an unreachable dashboard (want graceful degrade): %v", err)
	}
	if !got.Scheduled {
		t.Errorf("Scheduled = false, want true (the job is scheduled; only the live dial failed)")
	}
	if !got.Degraded {
		t.Errorf("Degraded = false, want true")
	}
	if got.Live != nil {
		t.Errorf("Live = %+v, want nil when degraded", got.Live)
	}
	if got.DegradeReason == "" {
		t.Errorf("DegradeReason is empty, want the bounded dial failure cause")
	}
	if got.Detail.JobDeploymentStatus != "Running" {
		t.Errorf("Detail.JobDeploymentStatus = %q, want Running (CRD-derived view preserved)", got.Detail.JobDeploymentStatus)
	}

	// Logs degrade the same way.
	logs, err := svc.Logs(ctx, domain.JobLogsRequest{Name: name})
	if err != nil {
		t.Fatalf("JobService.Logs hard error for unreachable dashboard: %v", err)
	}
	if !logs.Scheduled || !logs.Degraded || logs.Logs != nil {
		t.Errorf("logs Scheduled=%v Degraded=%v Logs=%v, want true/true/nil", logs.Scheduled, logs.Degraded, logs.Logs)
	}
}

// writeJSON writes a dashboard JSON body, failing the test on a write error
// (errcheck) — the combined harness's analogue of rayapi's writeBody helper.
func writeJSON(t *testing.T, w http.ResponseWriter, format string, args ...any) {
	t.Helper()
	if _, err := fmt.Fprintf(w, format, args...); err != nil {
		t.Errorf("write dashboard body: %v", err)
	}
}
