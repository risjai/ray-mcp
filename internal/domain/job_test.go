package domain

import (
	"context"
	"errors"
	"testing"
)

// newJobFake seeds a fakeKubeRay with the given job details keyed by
// namespace/name.
func newJobFake(details ...JobDetail) *fakeKubeRay {
	jobs := make(map[string]JobDetail, len(details))
	for _, d := range details {
		jobs[key(d.Namespace, d.Name)] = d
	}
	return &fakeKubeRay{jobs: jobs}
}

// scheduledJob is a JobDetail past the dial gate: both status.jobId and
// status.dashboardURL are populated (the operator has provisioned the cluster
// and the dashboard is up).
func scheduledJob(namespace, name string) JobDetail {
	return JobDetail{
		JobSummary: JobSummary{
			Name:      name,
			Namespace: namespace,
			JobStatus: "RUNNING",
		},
		JobID:               "raysubmit_abc123",
		DashboardURL:        "http://" + name + "-head-svc." + namespace + ".svc:8265",
		JobDeploymentStatus: "Running",
		RayClusterName:      name + "-cluster",
	}
}

// TestJobGetScheduledReturnsLiveStatus is the happy path: a scheduled job dials
// the dashboard via reachability and returns the live Ray status.
func TestJobGetScheduledReturnsLiveStatus(t *testing.T) {
	t.Parallel()

	job := scheduledJob("default", "demo")
	kube := newJobFake(job)
	api := &fakeRayAPI{status: map[string]RayJobStatus{
		"raysubmit_abc123": {JobID: "raysubmit_abc123", Status: "RUNNING", Message: "Job is running"},
	}}
	reach := &fakeReachability{endpoint: Endpoint{BaseURL: "http://127.0.0.1:34567"}}

	svc := NewJobService(kube, reach, api, "default")

	res, err := svc.Get(context.Background(), JobGetRequest{Name: "demo"})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !res.Scheduled {
		t.Errorf("Scheduled = false, want true for a job with dashboardURL+jobId set")
	}
	if res.Live == nil {
		t.Fatalf("Live = nil, want the dialed dashboard status")
	}
	if res.Live.Status != "RUNNING" {
		t.Errorf("Live.Status = %q, want RUNNING", res.Live.Status)
	}
	if res.Detail.JobID != "raysubmit_abc123" {
		t.Errorf("Detail.JobID = %q, want raysubmit_abc123", res.Detail.JobID)
	}
}

// TestJobGetPassesClusterAndPortToReachability asserts phase 2 resolves the head
// endpoint from status.rayClusterName (C2: read from status, not DNS-templated)
// on the dashboard port — never the job name or the CRD dashboardURL.
func TestJobGetPassesClusterAndPortToReachability(t *testing.T) {
	t.Parallel()

	job := scheduledJob("ray-system", "trainer")
	kube := newJobFake(job)
	api := &fakeRayAPI{status: map[string]RayJobStatus{
		"raysubmit_abc123": {Status: "RUNNING"},
	}}
	reach := &recordingReachability{endpoint: Endpoint{BaseURL: "http://127.0.0.1:1"}}

	svc := NewJobService(kube, reach, api, "default")

	if _, err := svc.Get(context.Background(), JobGetRequest{Namespace: "ray-system", Name: "trainer"}); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if reach.gotCluster != "trainer-cluster" {
		t.Errorf("reachability cluster = %q, want trainer-cluster (status.rayClusterName)", reach.gotCluster)
	}
	if reach.gotNamespace != "ray-system" {
		t.Errorf("reachability namespace = %q, want ray-system", reach.gotNamespace)
	}
	if reach.gotPort != dashboardPort {
		t.Errorf("reachability port = %d, want %d (dashboard port)", reach.gotPort, dashboardPort)
	}
}

// TestJobGetNotScheduledNoJobID asserts a job whose status.jobId is not yet set
// is reported as not scheduled WITHOUT dialing the dashboard (a nil api would
// panic if dialed) — the AC's "job not yet scheduled, not a connection error".
func TestJobGetNotScheduledNoJobID(t *testing.T) {
	t.Parallel()

	job := JobDetail{
		JobSummary:          JobSummary{Name: "pending", Namespace: "default"},
		DashboardURL:        "http://pending-head:8265", // present, but jobId is not.
		JobDeploymentStatus: "Initializing",
	}
	kube := newJobFake(job)

	// nil api + nil reach: a dial attempt would panic, proving the gate short-circuits.
	svc := NewJobService(kube, nil, nil, "default")

	res, err := svc.Get(context.Background(), JobGetRequest{Name: "pending"})
	if err != nil {
		t.Fatalf("Get: %v (a not-yet-scheduled job must not surface an error)", err)
	}
	if res.Scheduled {
		t.Errorf("Scheduled = true, want false when status.jobId is empty")
	}
	if res.Live != nil {
		t.Errorf("Live = %+v, want nil (no dashboard dial before scheduling)", res.Live)
	}
	if res.Detail.JobDeploymentStatus != "Initializing" {
		t.Errorf("Detail.JobDeploymentStatus = %q, want Initializing (CRD detail still returned)", res.Detail.JobDeploymentStatus)
	}
}

// TestJobGetNotScheduledNoDashboardURL asserts that jobId alone (without
// dashboardURL) is still "not scheduled": the gate requires BOTH, since the
// dashboard URL is only populated once the cluster is Ready.
func TestJobGetNotScheduledNoDashboardURL(t *testing.T) {
	t.Parallel()

	job := JobDetail{
		JobSummary:          JobSummary{Name: "early", Namespace: "default"},
		JobID:               "raysubmit_early", // set early, before the cluster is Ready.
		JobDeploymentStatus: "Initializing",
	}
	kube := newJobFake(job)
	svc := NewJobService(kube, nil, nil, "default")

	res, err := svc.Get(context.Background(), JobGetRequest{Name: "early"})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if res.Scheduled {
		t.Errorf("Scheduled = true, want false when dashboardURL is empty (gate requires both)")
	}
}

// TestJobGetDefaultsNamespace asserts an omitted namespace falls back to the
// service default for the phase-1 CRD read.
func TestJobGetDefaultsNamespace(t *testing.T) {
	t.Parallel()

	job := scheduledJob("ray-system", "demo")
	kube := newJobFake(job)
	api := &fakeRayAPI{status: map[string]RayJobStatus{"raysubmit_abc123": {Status: "RUNNING"}}}
	reach := &fakeReachability{endpoint: Endpoint{BaseURL: "http://127.0.0.1:1"}}
	svc := NewJobService(kube, reach, api, "ray-system")

	res, err := svc.Get(context.Background(), JobGetRequest{Name: "demo"})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if res.Detail.Name != "demo" {
		t.Errorf("Detail.Name = %q, want demo (resolved via default namespace)", res.Detail.Name)
	}
}

// TestJobGetNotFoundPropagates asserts a missing job surfaces the typed
// NotFoundError unchanged from the port.
func TestJobGetNotFoundPropagates(t *testing.T) {
	t.Parallel()

	svc := NewJobService(newJobFake(), nil, nil, "default")

	_, err := svc.Get(context.Background(), JobGetRequest{Name: "ghost"})
	var nf *NotFoundError
	if !errors.As(err, &nf) {
		t.Fatalf("error = %T (%v), want *NotFoundError", err, err)
	}
	if nf.Kind != KindRayJob {
		t.Errorf("NotFoundError.Kind = %q, want %q", nf.Kind, KindRayJob)
	}
}

// erroringReachability fails every resolution with a fixed error — the seam for
// the graceful-degrade tests (Task 16b): a scheduled job whose head endpoint
// cannot be resolved.
type erroringReachability struct{ err error }

func (e erroringReachability) Endpoint(_ context.Context, _, _ string, _ int) (Endpoint, error) {
	return Endpoint{}, e.err
}

// erroringRayAPI resolves the endpoint fine but fails the dashboard dial — the
// seam for a reachable head whose dashboard is down (Task 16b degrade).
type erroringRayAPI struct{ err error }

func (e erroringRayAPI) JobStatus(_ context.Context, _ Endpoint, _ string) (RayJobStatus, error) {
	return RayJobStatus{}, e.err
}

func (e erroringRayAPI) JobLogs(_ context.Context, _ Endpoint, _ string, _ LogOptions) (RayJobLogs, error) {
	return RayJobLogs{}, e.err
}

// TestJobGetDegradesWhenEndpointUnreachable asserts a scheduled job whose head
// endpoint cannot be resolved degrades gracefully (spec §3.2/§5): Scheduled
// stays true, Live is nil, Degraded is set with the bounded reason, and NO error
// is returned — the agent gets the CRD-derived view, not a connection error.
func TestJobGetDegradesWhenEndpointUnreachable(t *testing.T) {
	t.Parallel()

	job := scheduledJob("default", "demo")
	kube := newJobFake(job)
	reach := erroringReachability{err: &RayAPIUnreachableError{Endpoint: "default/demo-cluster", Reason: "connection refused"}}
	svc := NewJobService(kube, reach, nil, "default") // nil api: the dial must never be reached.

	res, err := svc.Get(context.Background(), JobGetRequest{Name: "demo"})
	if err != nil {
		t.Fatalf("Get degraded path returned a hard error: %v", err)
	}
	if !res.Scheduled {
		t.Errorf("Scheduled = false, want true (the job IS scheduled; only the live dial failed)")
	}
	if !res.Degraded {
		t.Errorf("Degraded = false, want true")
	}
	if res.Live != nil {
		t.Errorf("Live = %+v, want nil (no live status when degraded)", res.Live)
	}
	if res.DegradeReason != "connection refused" {
		t.Errorf("DegradeReason = %q, want %q", res.DegradeReason, "connection refused")
	}
	if res.Detail.JobDeploymentStatus != "Running" {
		t.Errorf("Detail.JobDeploymentStatus = %q, want Running (CRD-derived view preserved)", res.Detail.JobDeploymentStatus)
	}
}

// TestJobGetDegradesWhenDashboardDown asserts the degrade also covers a reachable
// head whose dashboard dial fails (the endpoint resolves, the HTTP call does not).
func TestJobGetDegradesWhenDashboardDown(t *testing.T) {
	t.Parallel()

	job := scheduledJob("default", "demo")
	kube := newJobFake(job)
	reach := &fakeReachability{endpoint: Endpoint{BaseURL: "http://127.0.0.1:1"}}
	api := erroringRayAPI{err: &RayAPIUnreachableError{Endpoint: "http://127.0.0.1:1", Reason: "dashboard returned HTTP 503"}}
	svc := NewJobService(kube, reach, api, "default")

	res, err := svc.Get(context.Background(), JobGetRequest{Name: "demo"})
	if err != nil {
		t.Fatalf("Get degraded path returned a hard error: %v", err)
	}
	if !res.Scheduled || !res.Degraded || res.Live != nil {
		t.Fatalf("got Scheduled=%v Degraded=%v Live=%v, want true/true/nil", res.Scheduled, res.Degraded, res.Live)
	}
	if res.DegradeReason != "dashboard returned HTTP 503" {
		t.Errorf("DegradeReason = %q, want %q", res.DegradeReason, "dashboard returned HTTP 503")
	}
}

// TestJobGetDegradesOnTimeout asserts a dial timeout also degrades (a timed-out
// dial is morally unreachable — "never a hard failure", spec §10).
func TestJobGetDegradesOnTimeout(t *testing.T) {
	t.Parallel()

	job := scheduledJob("default", "demo")
	kube := newJobFake(job)
	reach := &fakeReachability{endpoint: Endpoint{BaseURL: "http://127.0.0.1:1"}}
	api := erroringRayAPI{err: &TimeoutError{Op: "JobStatus"}}
	svc := NewJobService(kube, reach, api, "default")

	res, err := svc.Get(context.Background(), JobGetRequest{Name: "demo"})
	if err != nil {
		t.Fatalf("Get on timeout returned a hard error: %v", err)
	}
	if !res.Degraded || res.DegradeReason == "" {
		t.Errorf("got Degraded=%v DegradeReason=%q, want degraded with a reason", res.Degraded, res.DegradeReason)
	}
}

// TestJobGetPropagatesDashboardNotFound asserts a dashboard 404 (reachable, but
// the submission id is unknown) is NOT a degrade — it propagates as NotFound so
// the agent learns the dashboard does not know the job. Degrade is for
// unreachability, not for a reachable "no such job".
func TestJobGetPropagatesDashboardNotFound(t *testing.T) {
	t.Parallel()

	job := scheduledJob("default", "demo")
	kube := newJobFake(job)
	reach := &fakeReachability{endpoint: Endpoint{BaseURL: "http://127.0.0.1:1"}}
	api := erroringRayAPI{err: &NotFoundError{Kind: KindRayJob, Name: "raysubmit_abc123"}}
	svc := NewJobService(kube, reach, api, "default")

	_, err := svc.Get(context.Background(), JobGetRequest{Name: "demo"})
	var nf *NotFoundError
	if !errors.As(err, &nf) {
		t.Fatalf("error = %T (%v), want *NotFoundError to propagate (not degrade)", err, err)
	}
}

// TestJobLogsDegradesWhenDashboardUnreachable asserts logs degrade the same way:
// a scheduled job whose dashboard is unreachable reports Scheduled=true,
// Degraded with reason, no logs, no hard error.
func TestJobLogsDegradesWhenDashboardUnreachable(t *testing.T) {
	t.Parallel()

	job := scheduledJob("default", "demo")
	kube := newJobFake(job)
	reach := erroringReachability{err: &RayAPIUnreachableError{Endpoint: "default/demo-cluster", Reason: "connection refused"}}
	svc := NewJobService(kube, reach, nil, "default")

	res, err := svc.Logs(context.Background(), JobLogsRequest{Name: "demo"})
	if err != nil {
		t.Fatalf("Logs degraded path returned a hard error: %v", err)
	}
	if !res.Scheduled || !res.Degraded {
		t.Errorf("got Scheduled=%v Degraded=%v, want true/true", res.Scheduled, res.Degraded)
	}
	if res.Logs != nil {
		t.Errorf("Logs = %+v, want nil when degraded", res.Logs)
	}
	if res.DegradeReason != "connection refused" {
		t.Errorf("DegradeReason = %q, want %q", res.DegradeReason, "connection refused")
	}
}

// TestJobGetVerboseGate asserts Raw is stripped by default and carried only when
// Verbose is requested (spec §10 distilled-by-default), mirroring ClusterService.
func TestJobGetVerboseGate(t *testing.T) {
	t.Parallel()

	job := scheduledJob("default", "demo")
	job.Raw = MergedSpec{"kind": "RayJob"}
	kube := newJobFake(job)
	api := &fakeRayAPI{status: map[string]RayJobStatus{"raysubmit_abc123": {Status: "RUNNING"}}}
	reach := &fakeReachability{endpoint: Endpoint{BaseURL: "http://127.0.0.1:1"}}
	svc := NewJobService(kube, reach, api, "default")

	distilled, err := svc.Get(context.Background(), JobGetRequest{Name: "demo"})
	if err != nil {
		t.Fatalf("Get (distilled): %v", err)
	}
	if distilled.Detail.Raw != nil {
		t.Errorf("distilled Raw = %+v, want nil", distilled.Detail.Raw)
	}

	verbose, err := svc.Get(context.Background(), JobGetRequest{Name: "demo", Verbose: true})
	if err != nil {
		t.Fatalf("Get (verbose): %v", err)
	}
	if verbose.Detail.Raw == nil {
		t.Errorf("verbose Raw = nil, want the full object")
	}
}

// TestJobLogsScheduledReturnsLogs asserts a scheduled job's logs are fetched and
// the bounding signal surfaces.
func TestJobLogsScheduledReturnsLogs(t *testing.T) {
	t.Parallel()

	job := scheduledJob("default", "demo")
	kube := newJobFake(job)
	api := &fakeRayAPI{logs: map[string]RayJobLogs{
		"raysubmit_abc123": {Text: "log tail", Truncated: true, BytesOmitted: 4096},
	}}
	reach := &fakeReachability{endpoint: Endpoint{BaseURL: "http://127.0.0.1:1"}}
	svc := NewJobService(kube, reach, api, "default")

	res, err := svc.Logs(context.Background(), JobLogsRequest{Name: "demo"})
	if err != nil {
		t.Fatalf("Logs: %v", err)
	}
	if !res.Scheduled {
		t.Errorf("Scheduled = false, want true")
	}
	if res.Logs == nil {
		t.Fatalf("Logs = nil, want the dialed log tail")
	}
	if res.Logs.Text != "log tail" || !res.Logs.Truncated {
		t.Errorf("Logs = %+v, want the bounded tail", res.Logs)
	}
}

// TestJobLogsNotScheduledNoLogs asserts logs on a not-yet-scheduled job report
// not-scheduled without dialing (no logs exist before the job runs).
func TestJobLogsNotScheduledNoLogs(t *testing.T) {
	t.Parallel()

	job := JobDetail{
		JobSummary:          JobSummary{Name: "pending", Namespace: "default"},
		JobDeploymentStatus: "Initializing",
	}
	kube := newJobFake(job)
	svc := NewJobService(kube, nil, nil, "default")

	res, err := svc.Logs(context.Background(), JobLogsRequest{Name: "pending"})
	if err != nil {
		t.Fatalf("Logs: %v", err)
	}
	if res.Scheduled {
		t.Errorf("Scheduled = true, want false")
	}
	if res.Logs != nil {
		t.Errorf("Logs = %+v, want nil before scheduling", res.Logs)
	}
}

// TestJobLogsPassesOptions asserts the tail/byte options pass through to the port.
func TestJobLogsPassesOptions(t *testing.T) {
	t.Parallel()

	job := scheduledJob("default", "demo")
	kube := newJobFake(job)
	api := &recordingRayAPI{logs: RayJobLogs{Text: "x"}}
	reach := &fakeReachability{endpoint: Endpoint{BaseURL: "http://127.0.0.1:1"}}
	svc := NewJobService(kube, reach, api, "default")

	if _, err := svc.Logs(context.Background(), JobLogsRequest{Name: "demo", TailLines: 42, MaxBytes: 8192}); err != nil {
		t.Fatalf("Logs: %v", err)
	}
	if api.gotOpts.TailLines != 42 || api.gotOpts.MaxBytes != 8192 {
		t.Errorf("port LogOptions = %+v, want {TailLines:42 MaxBytes:8192}", api.gotOpts)
	}
}

// recordingReachability captures the args Endpoint was called with.
type recordingReachability struct {
	endpoint     Endpoint
	gotNamespace string
	gotCluster   string
	gotPort      int
}

func (r *recordingReachability) Endpoint(_ context.Context, namespace, cluster string, port int) (Endpoint, error) {
	r.gotNamespace, r.gotCluster, r.gotPort = namespace, cluster, port
	return r.endpoint, nil
}

// recordingRayAPI captures the LogOptions JobLogs was called with.
type recordingRayAPI struct {
	logs    RayJobLogs
	gotOpts LogOptions
}

func (r *recordingRayAPI) JobStatus(_ context.Context, _ Endpoint, jobID string) (RayJobStatus, error) {
	return RayJobStatus{JobID: jobID, Status: "RUNNING"}, nil
}

func (r *recordingRayAPI) JobLogs(_ context.Context, _ Endpoint, _ string, opts LogOptions) (RayJobLogs, error) {
	r.gotOpts = opts
	return r.logs, nil
}
