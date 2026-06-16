package domain

import "time"

// Kind enumerates the three KubeRay CRD kinds the guarded write path manages.
// It parameterizes KubeRayPort.Apply/Delete/Events and the list/get methods so
// one apply/delete path serves create/update/scale/deploy (spec §7.C).
type Kind string

const (
	// KindRayCluster is the RayCluster CRD (ray.io/v1).
	KindRayCluster Kind = "RayCluster"
	// KindRayJob is the RayJob CRD (ray.io/v1).
	KindRayJob Kind = "RayJob"
	// KindRayService is the RayService CRD (ray.io/v1).
	KindRayService Kind = "RayService"
)

// MergedSpec is the unstructured CRD spec crossing the KubeRayPort.Apply
// boundary. It is a plain Go map by design: the domain layer imports no
// Kubernetes packages, so the merged object never becomes an
// unstructured.Unstructured here — the KubeRay adapter converts map↔unstructured
// at the edge (spec §7.C, hexagonal invariant). The map preserves fields newer
// than the compiled KubeRay baseline (the wedge).
type MergedSpec map[string]any

// ClusterSummary is the compact list row for a RayCluster (spec §10: list rows
// are tiny — name, phase, ready replicas, age, 1-line health — never full
// status).
type ClusterSummary struct {
	Name            string
	Namespace       string
	Phase           string
	ReadyReplicas   int32
	DesiredReplicas int32
	Age             time.Duration
	Health          string // 1-line health summary.
}

// ClusterDetail is the distilled get view for a RayCluster (spec §10: distilled
// by default, full object only under verbose/raw). Raw carries the full
// unstructured object for the verbose/raw escape hatch.
type ClusterDetail struct {
	ClusterSummary

	HeadServiceName string
	DashboardURL    string
	Raw             MergedSpec // full object, surfaced only under verbose/raw.
}

// JobSummary is the compact list row for a RayJob.
type JobSummary struct {
	Name      string
	Namespace string
	JobStatus string // status.jobStatus (Ray-side phase).
	Age       time.Duration
	Health    string // 1-line health summary.
}

// JobDetail is the distilled get view for a RayJob. It bridges the k8s name to
// the Ray submission id and dashboard endpoint via the spec-pinned status fields
// (spec §6 "Job identity", §13). These let the wedge dial GET /api/jobs/{jobId}.
type JobDetail struct {
	JobSummary

	JobID               string // status.jobId — the submission id (raysubmit_...).
	DashboardURL        string // status.dashboardURL.
	JobDeploymentStatus string // status.jobDeploymentStatus (CRD-side lifecycle).
	RayClusterName      string // status.rayClusterName — head service resolution.
	Raw                 MergedSpec
}

// ServiceSummary is the compact list row for a RayService.
type ServiceSummary struct {
	Name            string
	Namespace       string
	ServiceStatus   string // serve status (e.g. Running/UNHEALTHY).
	HealthyReplicas int32
	Age             time.Duration
	Health          string // 1-line health summary.
}

// ServiceDetail is the distilled get view for a RayService: rollout status, not
// raw .status (spec §6).
type ServiceDetail struct {
	ServiceSummary

	RolloutPhase string // rollout/cutover phase.
	Raw          MergedSpec
}

// RayJobStatus is the live status returned by RayAPIPort from
// GET /api/jobs/{jobId}. It carries enough to distill an agent-actionable
// status (spec §7.A, Q11).
type RayJobStatus struct {
	JobID     string // the submission id queried.
	Status    string // Ray job status (PENDING/RUNNING/SUCCEEDED/FAILED/STOPPED).
	Message   string // bounded status message, e.g. failure reason.
	StartedAt time.Time
	EndedAt   time.Time
}

// LogOptions bounds a logs fetch (spec §10: logs are byte/token-bounded, not
// just line-bounded — a tailLines cap alone is insufficient).
type LogOptions struct {
	TailLines int // last-N lines; 0 means adapter default.
	MaxBytes  int // hard byte ceiling (~10-20KB); 0 means adapter default.
}

// RayJobLogs is the bounded log tail returned by RayAPIPort from
// GET /api/jobs/{jobId}/logs.
type RayJobLogs struct {
	Text         string // the bounded tail text.
	Truncated    bool   // true if the byte/line ceiling clipped the output.
	BytesOmitted int    // bytes dropped when Truncated, for the "N bytes omitted" marker.
}

// Event is a bounded k8s event for a resource (spec §10: events are truncated to
// a relevant slice, never the raw firehose).
type Event struct {
	Type     string // Normal/Warning.
	Reason   string
	Message  string
	Count    int32
	LastSeen time.Time
}

// ListOptions carries token-economy pagination for list reads (spec §10: every
// list paginates + caps, reusing the k8s continue token; never silently
// truncate).
type ListOptions struct {
	Limit         int    // hard cap per page; 0 means adapter default (~50).
	Continue      string // k8s continue token from a prior page; empty for the first.
	AllNamespaces bool   // list across all served namespaces.
}

// ClusterList is a page of cluster summaries plus the continue token for the
// next page (empty when exhausted). Pagination is explicit so the domain can
// report "N of M, continue token X" rather than silently truncating.
type ClusterList struct {
	Items    []ClusterSummary
	Continue string
}

// JobList is a page of job summaries plus the continue token.
type JobList struct {
	Items    []JobSummary
	Continue string
}

// ServiceList is a page of service summaries plus the continue token.
type ServiceList struct {
	Items    []ServiceSummary
	Continue string
}

// Endpoint is a usable base URL for the head dashboard / Job Submission REST API
// (port 8265), returned by RayReachability. For v1 the contract is a base URL
// string: DirectDial returns the in-cluster service URL; PortForward returns the
// local tunnel URL. Pooling/teardown of port-forward tunnels is owned by the
// PortForward adapter (idle-reaped), not exposed across this port — so no Close
// handle is needed here.
type Endpoint struct {
	BaseURL string // e.g. "http://<head-svc>:8265" or "http://127.0.0.1:<local>".
}
