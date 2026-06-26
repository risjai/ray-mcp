package mcp_test

import (
	"context"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/risjai/ray-mcp/internal/config"
	"github.com/risjai/ray-mcp/internal/domain"
	mcpserver "github.com/risjai/ray-mcp/internal/mcp"
)

// fakeJobReader implements domain.JobReader for the MCP-layer job tests. Tests
// seed jobs keyed by namespace/name directly.
type fakeJobReader struct {
	jobs map[string]domain.JobDetail
}

func (f *fakeJobReader) GetJob(_ context.Context, namespace, name string) (domain.JobDetail, error) {
	j, ok := f.jobs[namespace+"/"+name]
	if !ok {
		return domain.JobDetail{}, &domain.NotFoundError{Kind: domain.KindRayJob, Namespace: namespace, Name: name}
	}
	return j, nil
}

// fakeReach returns a canned endpoint for any cluster.
type fakeReach struct{ endpoint domain.Endpoint }

func (f fakeReach) Endpoint(_ context.Context, _, _ string, _ int) (domain.Endpoint, error) {
	return f.endpoint, nil
}

// fakeRayAPI returns canned live status / logs keyed by submission id.
type fakeRayAPI struct {
	status map[string]domain.RayJobStatus
	logs   map[string]domain.RayJobLogs
}

func (f fakeRayAPI) JobStatus(_ context.Context, _ domain.Endpoint, jobID string) (domain.RayJobStatus, error) {
	return f.status[jobID], nil
}

func (f fakeRayAPI) JobLogs(_ context.Context, _ domain.Endpoint, jobID string, _ domain.LogOptions) (domain.RayJobLogs, error) {
	return f.logs[jobID], nil
}

// connectJobs wires a server whose wedge backend is the given collaborators and
// returns an in-memory client session.
func connectJobs(t *testing.T, cfg *config.Config, wedge mcpserver.WedgeBackend) *mcp.ClientSession {
	t.Helper()
	ctx := context.Background()

	server := mcpserver.NewServer(cfg,
		fakeSource{contextName: "ctx", defaultNamespace: cfg.DefaultNamespace},
		&fakeKubeRay{}, &fakeKubeRay{}, wedge, domain.NopAuditSink{})
	serverT, clientT := mcp.NewInMemoryTransports()
	if _, err := server.Connect(ctx, serverT, nil); err != nil {
		t.Fatalf("server.Connect: %v", err)
	}
	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "v0"}, nil)
	session, err := client.Connect(ctx, clientT, nil)
	if err != nil {
		t.Fatalf("client.Connect: %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })
	return session
}

// scheduledJobDetail is a JobDetail past the dial gate (jobId + dashboardURL set).
func scheduledJobDetail(namespace, name string) domain.JobDetail {
	return domain.JobDetail{
		JobSummary: domain.JobSummary{
			Name: name, Namespace: namespace, JobStatus: "RUNNING", Health: "Running; job RUNNING",
		},
		JobID:               "raysubmit_xyz",
		DashboardURL:        "http://" + name + "-head.svc:8265",
		JobDeploymentStatus: "Running",
		RayClusterName:      name + "-cluster",
	}
}

func wedgeFor(jobs map[string]domain.JobDetail, api fakeRayAPI) mcpserver.WedgeBackend {
	return mcpserver.WedgeBackend{
		Jobs:  &fakeJobReader{jobs: jobs},
		Reach: fakeReach{endpoint: domain.Endpoint{BaseURL: "http://127.0.0.1:30000"}},
		API:   api,
	}
}

// TestListToolsShowsJobToolsWithReadOnlyHint asserts both job tools are
// registered and annotated read-only when a wedge backend is wired.
func TestListToolsShowsJobToolsWithReadOnlyHint(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "default"}
	session := connectJobs(t, cfg, wedgeFor(map[string]domain.JobDetail{}, fakeRayAPI{}))

	res, err := session.ListTools(context.Background(), &mcp.ListToolsParams{})
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	byName := map[string]*mcp.Tool{}
	for _, tool := range res.Tools {
		byName[tool.Name] = tool
	}
	for _, name := range []string{"ray_job_get", "ray_job_logs"} {
		tool, ok := byName[name]
		if !ok {
			t.Fatalf("%s not registered; got %v", name, res.Tools)
		}
		if tool.Annotations == nil || !tool.Annotations.ReadOnlyHint {
			t.Errorf("%s missing readOnlyHint annotation: %+v", name, tool.Annotations)
		}
	}
}

// TestJobToolsAbsentWithoutWedge asserts the job tools are NOT registered when no
// wedge backend is wired (zero WedgeBackend) — the gate that lets cluster-only
// servers omit them.
func TestJobToolsAbsentWithoutWedge(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "default"}
	session := connectJobs(t, cfg, mcpserver.WedgeBackend{})

	res, err := session.ListTools(context.Background(), &mcp.ListToolsParams{})
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	for _, tool := range res.Tools {
		if tool.Name == "ray_job_get" || tool.Name == "ray_job_logs" {
			t.Errorf("%s registered without a wedge backend", tool.Name)
		}
	}
}

// TestJobGetScheduledFusesLiveStatus asserts ray_job_get on a scheduled job
// returns the live dashboard status fused with the CRD identity.
func TestJobGetScheduledFusesLiveStatus(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "demo"}
	jobs := map[string]domain.JobDetail{"demo/trainer": scheduledJobDetail("demo", "trainer")}
	api := fakeRayAPI{status: map[string]domain.RayJobStatus{
		"raysubmit_xyz": {JobID: "raysubmit_xyz", Status: "SUCCEEDED", Message: "done"},
	}}
	session := connectJobs(t, cfg, wedgeFor(jobs, api))

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "ray_job_get",
		Arguments: map[string]any{"name": "trainer"},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if res.IsError {
		t.Fatalf("tool reported error: %+v", res)
	}
	sc := res.StructuredContent.(map[string]any)
	if sc["scheduled"] != true {
		t.Errorf("scheduled = %v, want true", sc["scheduled"])
	}
	if sc["jobStatus"] != "SUCCEEDED" {
		t.Errorf("jobStatus = %v, want SUCCEEDED (live dashboard status)", sc["jobStatus"])
	}
	if sc["jobId"] != "raysubmit_xyz" {
		t.Errorf("jobId = %v, want raysubmit_xyz", sc["jobId"])
	}
	if _, present := sc["raw"]; present {
		t.Errorf("raw present in distilled get: %v", sc["raw"])
	}
	if textContent(res) == "" {
		t.Error("no text summary in result content")
	}
}

// TestJobGetNotScheduledReportsClean asserts a not-yet-scheduled job returns a
// clean scheduled=false result (the AC: not a tunnel/connection error).
func TestJobGetNotScheduledReportsClean(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "demo"}
	jobs := map[string]domain.JobDetail{"demo/pending": {
		JobSummary:          domain.JobSummary{Name: "pending", Namespace: "demo"},
		JobDeploymentStatus: "Initializing",
	}}
	// nil-ish API: the dashboard must NOT be dialed for a not-scheduled job.
	session := connectJobs(t, cfg, wedgeFor(jobs, fakeRayAPI{}))

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "ray_job_get",
		Arguments: map[string]any{"name": "pending"},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if res.IsError {
		t.Fatalf("tool reported error for a not-scheduled job (want clean result): %+v", res)
	}
	sc := res.StructuredContent.(map[string]any)
	if sc["scheduled"] != false {
		t.Errorf("scheduled = %v, want false", sc["scheduled"])
	}
	if sc["deploymentStatus"] != "Initializing" {
		t.Errorf("deploymentStatus = %v, want Initializing", sc["deploymentStatus"])
	}
}

// TestJobGetMissingNameErrors asserts an empty name is a validation error.
func TestJobGetMissingNameErrors(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "demo"}
	session := connectJobs(t, cfg, wedgeFor(map[string]domain.JobDetail{}, fakeRayAPI{}))

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "ray_job_get",
		Arguments: map[string]any{},
	})
	if err != nil {
		t.Fatalf("CallTool transport error: %v", err)
	}
	if !res.IsError {
		t.Fatalf("empty name did not produce a tool error: %+v", res)
	}
}

// TestJobGetNotFoundMapsCleanError asserts a missing job surfaces a clean,
// bounded NotFound tool error.
func TestJobGetNotFoundMapsCleanError(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "demo"}
	session := connectJobs(t, cfg, wedgeFor(map[string]domain.JobDetail{}, fakeRayAPI{}))

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "ray_job_get",
		Arguments: map[string]any{"name": "ghost"},
	})
	if err != nil {
		t.Fatalf("CallTool transport error: %v", err)
	}
	if !res.IsError {
		t.Fatalf("missing job did not produce a tool error: %+v", res)
	}
}

// TestJobLogsScheduledReturnsTail asserts ray_job_logs returns the bounded tail
// and truncation signal for a scheduled job.
func TestJobLogsScheduledReturnsTail(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "demo"}
	jobs := map[string]domain.JobDetail{"demo/trainer": scheduledJobDetail("demo", "trainer")}
	api := fakeRayAPI{logs: map[string]domain.RayJobLogs{
		"raysubmit_xyz": {Text: "the tail", Truncated: true, BytesOmitted: 2048},
	}}
	session := connectJobs(t, cfg, wedgeFor(jobs, api))

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "ray_job_logs",
		Arguments: map[string]any{"name": "trainer", "tailLines": 50},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if res.IsError {
		t.Fatalf("tool reported error: %+v", res)
	}
	sc := res.StructuredContent.(map[string]any)
	if sc["scheduled"] != true {
		t.Errorf("scheduled = %v, want true", sc["scheduled"])
	}
	if sc["logs"] != "the tail" {
		t.Errorf("logs = %v, want 'the tail'", sc["logs"])
	}
	if sc["truncated"] != true {
		t.Errorf("truncated = %v, want true", sc["truncated"])
	}
}

// unreachableReach fails endpoint resolution with a typed unreachable error —
// the seam for the graceful-degrade MCP tests (Task 16b).
type unreachableReach struct{ reason string }

func (u unreachableReach) Endpoint(_ context.Context, namespace, cluster string, _ int) (domain.Endpoint, error) {
	return domain.Endpoint{}, &domain.RayAPIUnreachableError{Endpoint: namespace + "/" + cluster, Reason: u.reason}
}

// degradedWedge wires a scheduled job whose dashboard is unreachable.
func degradedWedge(jobs map[string]domain.JobDetail, reason string) mcpserver.WedgeBackend {
	return mcpserver.WedgeBackend{
		Jobs:  &fakeJobReader{jobs: jobs},
		Reach: unreachableReach{reason: reason},
		API:   fakeRayAPI{},
	}
}

// TestJobGetDegradesGracefullyWhenDashboardUnreachable asserts the AC's degrade
// path at the tool edge: a scheduled job whose dashboard is unreachable returns a
// clean (non-error) result with scheduled=true, degraded=true, the bounded
// reason, and the CRD-derived lifecycle — never a tool error.
func TestJobGetDegradesGracefullyWhenDashboardUnreachable(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "demo"}
	jobs := map[string]domain.JobDetail{"demo/trainer": scheduledJobDetail("demo", "trainer")}
	session := connectJobs(t, cfg, degradedWedge(jobs, "connection refused"))

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "ray_job_get",
		Arguments: map[string]any{"name": "trainer"},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if res.IsError {
		t.Fatalf("tool reported a hard error for an unreachable dashboard (want graceful degrade): %+v", res)
	}
	sc := res.StructuredContent.(map[string]any)
	if sc["scheduled"] != true {
		t.Errorf("scheduled = %v, want true (the job is scheduled; only the live dial failed)", sc["scheduled"])
	}
	if sc["degraded"] != true {
		t.Errorf("degraded = %v, want true", sc["degraded"])
	}
	if sc["deploymentStatus"] != "Running" {
		t.Errorf("deploymentStatus = %v, want Running (CRD-derived view preserved)", sc["deploymentStatus"])
	}
	if reason, _ := sc["degradeReason"].(string); reason != "connection refused" {
		t.Errorf("degradeReason = %v, want 'connection refused'", sc["degradeReason"])
	}
	if txt := textContent(res); !strings.Contains(txt, "live Ray detail unavailable") || !strings.Contains(txt, "connection refused") {
		t.Errorf("summary = %q, want the 'live Ray detail unavailable: connection refused' degrade line", txt)
	}
}

// TestJobLogsDegradesGracefullyWhenDashboardUnreachable asserts logs degrade the
// same way: scheduled=true, degraded=true, no logs, clean (non-error) result.
func TestJobLogsDegradesGracefullyWhenDashboardUnreachable(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "demo"}
	jobs := map[string]domain.JobDetail{"demo/trainer": scheduledJobDetail("demo", "trainer")}
	session := connectJobs(t, cfg, degradedWedge(jobs, "connection refused"))

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "ray_job_logs",
		Arguments: map[string]any{"name": "trainer"},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if res.IsError {
		t.Fatalf("tool reported a hard error for unreachable logs (want graceful degrade): %+v", res)
	}
	sc := res.StructuredContent.(map[string]any)
	if sc["scheduled"] != true || sc["degraded"] != true {
		t.Errorf("scheduled=%v degraded=%v, want true/true", sc["scheduled"], sc["degraded"])
	}
	if sc["logs"] != "" {
		t.Errorf("logs = %v, want empty when degraded", sc["logs"])
	}
	if txt := textContent(res); !strings.Contains(txt, "live Ray detail unavailable") {
		t.Errorf("summary = %q, want the degrade line", txt)
	}
}

// TestListToolsShowsJobWaitWithReadOnlyHint asserts ray_job_wait is registered
// and annotated read-only alongside the other job tools.
func TestListToolsShowsJobWaitWithReadOnlyHint(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "default"}
	session := connectJobs(t, cfg, wedgeFor(map[string]domain.JobDetail{}, fakeRayAPI{}))

	res, err := session.ListTools(context.Background(), &mcp.ListToolsParams{})
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	var tool *mcp.Tool
	for _, tl := range res.Tools {
		if tl.Name == "ray_job_wait" {
			tool = tl
		}
	}
	if tool == nil {
		t.Fatalf("ray_job_wait not registered; got %v", res.Tools)
	}
	if tool.Annotations == nil || !tool.Annotations.ReadOnlyHint {
		t.Errorf("ray_job_wait missing readOnlyHint annotation: %+v", tool.Annotations)
	}
}

// TestJobWaitReachedRunning asserts ray_job_wait returns reached=true with the
// live status for a RUNNING job under the default until=running. WaitSeconds=0
// keeps the call to a single poll (no real sleeping in the MCP-wired service).
func TestJobWaitReachedRunning(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "demo"}
	jobs := map[string]domain.JobDetail{"demo/trainer": scheduledJobDetail("demo", "trainer")}
	api := fakeRayAPI{status: map[string]domain.RayJobStatus{
		"raysubmit_xyz": {JobID: "raysubmit_xyz", Status: "RUNNING"},
	}}
	session := connectJobs(t, cfg, wedgeFor(jobs, api))

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "ray_job_wait",
		Arguments: map[string]any{"name": "trainer", "waitSeconds": 0},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if res.IsError {
		t.Fatalf("tool reported error: %+v", res)
	}
	sc := res.StructuredContent.(map[string]any)
	if sc["reached"] != true {
		t.Errorf("reached = %v, want true", sc["reached"])
	}
	if sc["until"] != "running" {
		t.Errorf("until = %v, want running (default)", sc["until"])
	}
	if sc["jobStatus"] != "RUNNING" {
		t.Errorf("jobStatus = %v, want RUNNING (live)", sc["jobStatus"])
	}
	if textContent(res) == "" {
		t.Error("no text summary in result content")
	}
}

// TestJobWaitNotReachedPending asserts a single-poll wait on a PENDING job
// returns a clean reached=false (the honest "not yet" answer, not an error).
func TestJobWaitNotReachedPending(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "demo"}
	jobs := map[string]domain.JobDetail{"demo/trainer": scheduledJobDetail("demo", "trainer")}
	api := fakeRayAPI{status: map[string]domain.RayJobStatus{
		"raysubmit_xyz": {JobID: "raysubmit_xyz", Status: "PENDING"},
	}}
	session := connectJobs(t, cfg, wedgeFor(jobs, api))

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "ray_job_wait",
		Arguments: map[string]any{"name": "trainer", "until": "running", "waitSeconds": 0},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if res.IsError {
		t.Fatalf("tool reported error for a not-yet-running job (want clean reached=false): %+v", res)
	}
	sc := res.StructuredContent.(map[string]any)
	if sc["reached"] != false {
		t.Errorf("reached = %v, want false", sc["reached"])
	}
}

// TestJobWaitPreSchedulingFailureReadsTerminal asserts a job that terminally
// fails BEFORE it is ever scheduled (jobDeploymentStatus=Failed, no jobId) is
// reported as reached with a terminal summary derived from the CRD lifecycle —
// not the contradictory "not yet scheduled" wording (Live is nil on this path,
// so the summary must read succeeded-vs-failed off jobDeploymentStatus).
func TestJobWaitPreSchedulingFailureReadsTerminal(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "demo"}
	jobs := map[string]domain.JobDetail{"demo/doomed": {
		JobSummary:          domain.JobSummary{Name: "doomed", Namespace: "demo"},
		JobDeploymentStatus: "Failed", // terminal on the CRD; never scheduled.
	}}
	// nil-ish API: a pre-scheduling-failed job must NOT be dialed.
	session := connectJobs(t, cfg, wedgeFor(jobs, fakeRayAPI{}))

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "ray_job_wait",
		Arguments: map[string]any{"name": "doomed", "until": "terminal", "waitSeconds": 0},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if res.IsError {
		t.Fatalf("tool reported error for a terminally-failed job (want clean reached=true): %+v", res)
	}
	sc := res.StructuredContent.(map[string]any)
	if sc["reached"] != true {
		t.Errorf("reached = %v, want true (Failed is terminal)", sc["reached"])
	}
	if sc["deploymentStatus"] != "Failed" {
		t.Errorf("deploymentStatus = %v, want Failed", sc["deploymentStatus"])
	}
	txt := textContent(res)
	if strings.Contains(txt, "not yet scheduled") {
		t.Errorf("summary = %q, must not call a terminally-failed job 'not yet scheduled'", txt)
	}
	if !strings.Contains(txt, "Failed") {
		t.Errorf("summary = %q, want the terminal Failed lifecycle surfaced", txt)
	}
}

// TestJobWaitMissingNameErrors asserts an empty name is a validation error.
func TestJobWaitMissingNameErrors(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "demo"}
	session := connectJobs(t, cfg, wedgeFor(map[string]domain.JobDetail{}, fakeRayAPI{}))

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "ray_job_wait",
		Arguments: map[string]any{"waitSeconds": 0},
	})
	if err != nil {
		t.Fatalf("CallTool transport error: %v", err)
	}
	if !res.IsError {
		t.Fatalf("empty name did not produce a tool error: %+v", res)
	}
}

// TestJobWaitInvalidUntilErrors asserts an unrecognized until value is rejected
// at the edge rather than silently treated as the default.
func TestJobWaitInvalidUntilErrors(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "demo"}
	jobs := map[string]domain.JobDetail{"demo/trainer": scheduledJobDetail("demo", "trainer")}
	session := connectJobs(t, cfg, wedgeFor(jobs, fakeRayAPI{}))

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "ray_job_wait",
		Arguments: map[string]any{"name": "trainer", "until": "forever", "waitSeconds": 0},
	})
	if err != nil {
		t.Fatalf("CallTool transport error: %v", err)
	}
	if !res.IsError {
		t.Fatalf("invalid until did not produce a tool error: %+v", res)
	}
}

// TestJobLogsNotScheduledNoLogs asserts logs on a not-yet-scheduled job report
// scheduled=false with no logs (not an error).
func TestJobLogsNotScheduledNoLogs(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "demo"}
	jobs := map[string]domain.JobDetail{"demo/pending": {
		JobSummary:          domain.JobSummary{Name: "pending", Namespace: "demo"},
		JobDeploymentStatus: "Initializing",
	}}
	session := connectJobs(t, cfg, wedgeFor(jobs, fakeRayAPI{}))

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "ray_job_logs",
		Arguments: map[string]any{"name": "pending"},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if res.IsError {
		t.Fatalf("tool reported error for not-scheduled logs: %+v", res)
	}
	sc := res.StructuredContent.(map[string]any)
	if sc["scheduled"] != false {
		t.Errorf("scheduled = %v, want false", sc["scheduled"])
	}
	if sc["logs"] != "" {
		t.Errorf("logs = %v, want empty", sc["logs"])
	}
}
