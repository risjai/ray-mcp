package domain

import "context"

// dashboardPort is the Ray head dashboard / Job Submission REST API port. The
// reachability resolver maps the cluster's head to a usable endpoint on this
// port; the domain owns the constant so the wedge dials a fixed, well-known port
// (8265) rather than threading it through config.
const dashboardPort = 8265

// JobReader is the narrow read slice of KubeRayPort the RayJob read tools need —
// the phase-1 CRD read of the two-phase wedge. JobService depends on this (not
// the full port) so callers wire only what these tools use, mirroring
// ClusterReader. The KubeRay adapter satisfies it with GetJob; the full
// KubeRayPort satisfies it too.
type JobReader interface {
	GetJob(ctx context.Context, namespace, name string) (JobDetail, error)
}

// JobService orchestrates the two-phase cross-plane read for the RayJob read
// tools (ray_job_get / ray_job_logs) — the "wedge" (spec §5/§7.B). Phase 1 reads
// the RayJob CRD via the KubeRay port; phase 2, only once the job is scheduled,
// resolves the head endpoint via RayReachability and dials the Ray dashboard via
// the read-only RayAPIPort. It imports no Kubernetes or HTTP packages — only the
// ports and DTOs — so it is unit-testable with fakes.
//
// The service owns the cross-cutting read policy the MCP layer must not
// duplicate: the default-namespace fallback, the "is it scheduled yet" gate
// (which decides whether phase 2 runs at all), and the verbose/distilled Raw
// gate (spec §10).
type JobService struct {
	kube             JobReader
	reach            RayReachability
	api              RayAPIPort
	defaultNamespace string
}

// NewJobService builds the service. The default namespace is injected as a plain
// string (not the config struct) so the domain stays free of any config or
// Kubernetes import. reach/api may be nil only when the caller guarantees no
// scheduled job will be dialed (the not-scheduled unit tests rely on this).
func NewJobService(kube JobReader, reach RayReachability, api RayAPIPort, defaultNamespace string) *JobService {
	return &JobService{kube: kube, reach: reach, api: api, defaultNamespace: defaultNamespace}
}

// JobGetRequest is the decoded ray_job_get argument set. Name is required
// (validated at the MCP edge); Namespace defaults to the service's default;
// Verbose toggles the full-object escape hatch.
type JobGetRequest struct {
	Namespace string
	Name      string
	Verbose   bool
}

// JobGetResult carries the phase-1 CRD detail plus, when the job is scheduled,
// the live phase-2 dashboard status. Scheduled distinguishes "job not yet
// scheduled" (Live nil, no dial attempted — the AC's clean signal) from a
// running job whose dashboard answered. Raw on Detail is stripped unless Verbose.
type JobGetResult struct {
	Detail    JobDetail
	Scheduled bool          // true iff phase 2 ran (status.jobId + dashboardURL both set).
	Live      *RayJobStatus // the dialed dashboard status; nil when not scheduled.
	Verbose   bool
}

// Get performs the two-phase read. Phase 1 reads the RayJob CRD. If the job is
// not yet scheduled (the dial gate below), it returns the CRD detail with
// Scheduled=false and no dial — surfacing "not yet scheduled" rather than a
// tunnel/connection error. Otherwise phase 2 resolves the head endpoint from
// status.rayClusterName (C2: read from status, never DNS-templated) and dials
// GET /api/jobs/{jobId}. A dial/unreachable error propagates here (Task 16b adds
// graceful degradation to CRD-derived status).
func (s *JobService) Get(ctx context.Context, req JobGetRequest) (JobGetResult, error) {
	namespace := s.resolveNamespace(req.Namespace)

	detail, err := s.kube.GetJob(ctx, namespace, req.Name)
	if err != nil {
		return JobGetResult{}, err
	}

	if !scheduled(detail) {
		return JobGetResult{Detail: gateRaw(detail, req.Verbose), Scheduled: false, Verbose: req.Verbose}, nil
	}

	endpoint, err := s.reach.Endpoint(ctx, namespace, detail.RayClusterName, dashboardPort)
	if err != nil {
		return JobGetResult{}, err
	}

	live, err := s.api.JobStatus(ctx, endpoint, detail.JobID)
	if err != nil {
		return JobGetResult{}, err
	}

	return JobGetResult{Detail: gateRaw(detail, req.Verbose), Scheduled: true, Live: &live, Verbose: req.Verbose}, nil
}

// JobLogsRequest is the decoded ray_job_logs argument set. Name is required;
// Namespace defaults to the service's default; TailLines/MaxBytes bound the tail
// (0 means adapter default — spec §10).
type JobLogsRequest struct {
	Namespace string
	Name      string
	TailLines int
	MaxBytes  int
}

// JobLogsResult carries the phase-1 CRD detail plus, when scheduled, the bounded
// log tail. As with Get, Scheduled distinguishes "no logs yet" from a dialed tail.
type JobLogsResult struct {
	Detail    JobDetail
	Scheduled bool
	Logs      *RayJobLogs // the bounded tail; nil when not scheduled.
}

// Logs performs the same two-phase resolution as Get, dialing
// GET /api/jobs/{jobId}/logs once the job is scheduled. Before scheduling there
// are no logs to fetch, so it returns Scheduled=false without dialing.
func (s *JobService) Logs(ctx context.Context, req JobLogsRequest) (JobLogsResult, error) {
	namespace := s.resolveNamespace(req.Namespace)

	detail, err := s.kube.GetJob(ctx, namespace, req.Name)
	if err != nil {
		return JobLogsResult{}, err
	}

	if !scheduled(detail) {
		return JobLogsResult{Detail: detail, Scheduled: false}, nil
	}

	endpoint, err := s.reach.Endpoint(ctx, namespace, detail.RayClusterName, dashboardPort)
	if err != nil {
		return JobLogsResult{}, err
	}

	logs, err := s.api.JobLogs(ctx, endpoint, detail.JobID, LogOptions{TailLines: req.TailLines, MaxBytes: req.MaxBytes})
	if err != nil {
		return JobLogsResult{}, err
	}

	return JobLogsResult{Detail: detail, Scheduled: true, Logs: &logs}, nil
}

// scheduled reports whether the RayJob has progressed far enough to dial the
// dashboard. KubeRay sets status.jobId AND status.rayClusterName early (New →
// Initializing), but status.dashboardURL only AFTER the RayCluster is Ready and
// the head URL is fetched — so requiring BOTH jobId and dashboardURL signals the
// head is provisioned and worth dialing, not just that a submission id was
// reserved (verified vs KubeRay v1.6.1 rayjob_controller). This is reachable-
// intent, not a guarantee: the dashboard HTTP server can still briefly answer
// ErrAgain just after dashboardURL is set, which degrades to a typed unreachable
// error (Task 16b adds CRD-derived fallback for that window). Dialing on jobId
// alone, though, would hit a not-yet-provisioned head and surface a connection
// error where the honest answer is "not yet scheduled".
func scheduled(detail JobDetail) bool {
	return detail.JobID != "" && detail.DashboardURL != ""
}

// gateRaw strips the full object unless verbose was requested (spec §10:
// distilled by default). Done here in the domain — not the MCP layer — so the
// policy lives in one place, mirroring ClusterService.Get.
func gateRaw(detail JobDetail, verbose bool) JobDetail {
	if !verbose {
		detail.Raw = nil
	}
	return detail
}

// resolveNamespace applies the default-namespace fallback for the phase-1 read.
func (s *JobService) resolveNamespace(ns string) string {
	if ns == "" {
		return s.defaultNamespace
	}
	return ns
}
