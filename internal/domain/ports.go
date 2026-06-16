package domain

import "context"

// The three port interfaces the domain depends on (spec §5). The domain imports
// no Kubernetes or HTTP packages; it depends only on these interfaces, so it is
// unit-testable with fakes. Every method takes context.Context first (spec §10:
// all calls are deadline-driven). Adapters implement these in later tasks.

// KubeRayPort is the guarded CRD path: read + write for RayCluster, RayJob and
// RayService via the controller-runtime client (uncached). Writes go through a
// single SSA-based Apply parameterized by Kind (spec §7.C); reads honor the
// token-economy two-tier shape (compact list rows, distilled get — spec §10).
type KubeRayPort interface {
	// ListClusters returns a page of compact RayCluster summaries and a continue
	// token for the next page (empty when exhausted).
	ListClusters(ctx context.Context, namespace string, opts ListOptions) (ClusterList, error)
	// GetCluster returns the distilled detail for one RayCluster (Raw populated
	// for the verbose/raw escape hatch).
	GetCluster(ctx context.Context, namespace, name string) (ClusterDetail, error)

	// ListJobs returns a page of compact RayJob summaries and a continue token.
	ListJobs(ctx context.Context, namespace string, opts ListOptions) (JobList, error)
	// GetJob returns the distilled detail for one RayJob, including the status
	// fields that bridge k8s name → Ray submission id + dashboard endpoint.
	GetJob(ctx context.Context, namespace, name string) (JobDetail, error)

	// ListServices returns a page of compact RayService summaries and a continue token.
	ListServices(ctx context.Context, namespace string, opts ListOptions) (ServiceList, error)
	// GetService returns the distilled detail for one RayService.
	GetService(ctx context.Context, namespace, name string) (ServiceDetail, error)

	// Apply is the unified write path for create/update/scale/deploy: it applies
	// the merged unstructured spec for the given Kind via Server-Side Apply with
	// the "ray-mcp" field manager. When dryRun is true it maps to DryRunAll and
	// the API server validates against the installed CRD schema without
	// persisting. Returns the applied (or dry-run) object as a plain map.
	Apply(ctx context.Context, kind Kind, namespace, name string, spec MergedSpec, dryRun bool) (MergedSpec, error)

	// Delete removes a resource (the destructive tier). When dryRun is true it
	// validates the delete without persisting.
	Delete(ctx context.Context, kind Kind, namespace, name string, dryRun bool) error

	// Events returns recent, bounded k8s events for a resource (for
	// ray_cluster_events — spec §10: a relevant slice, never the raw firehose).
	Events(ctx context.Context, kind Kind, namespace, name string, limit int) ([]Event, error)
}

// RayAPIPort is the Ray dashboard / Job Submission REST API — the wedge.
//
// READ-ONLY BY CONSTRUCTION (spec §5, §7.B, Q6). This interface has NO
// submit/stop/delete methods, and that absence IS the contract: the Ray
// dashboard is unauthenticated by default (the "ShadowRay" RCE surface), so
// ray-mcp consumes it read-only and NEVER exposes it as a write vector. Every
// mutation goes through the guarded CRD path (KubeRayPort), not here. Do not add
// a write method to this interface.
//
// Keyed by the Ray submission id (RayJob status.jobId), resolved from the CRD by
// the domain before dialing.
type RayAPIPort interface {
	// JobStatus fetches live job status — GET /api/jobs/{jobID}.
	JobStatus(ctx context.Context, endpoint Endpoint, jobID string) (RayJobStatus, error)
	// JobLogs fetches a bounded log tail — GET /api/jobs/{jobID}/logs — bounded
	// by opts (tailLines + a hard byte ceiling, spec §10).
	JobLogs(ctx context.Context, endpoint Endpoint, jobID string, opts LogOptions) (RayJobLogs, error)
}

// RayReachability resolves a usable endpoint for a cluster's head dashboard
// (spec §5, §7). It is a strategy: DirectDial (in-cluster DNS, no port-forward
// RBAC) and PortForward (pooled SPDY, out-of-cluster) implement it in later
// tasks. The port only declares the resolution method; pooling/teardown is an
// adapter-internal concern.
type RayReachability interface {
	// Endpoint returns a usable base URL for the named cluster's head dashboard
	// on the given port (8265 for the Job Submission REST API).
	Endpoint(ctx context.Context, namespace, cluster string, port int) (Endpoint, error)
}
