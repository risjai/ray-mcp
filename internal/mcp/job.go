package mcp

import (
	"context"
	"errors"
	"fmt"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/risjai/ray-mcp/internal/domain"
)

// jobGetInput is the ray_job_get argument object. Name is required; namespace
// defaults to the server default; verbose toggles the full-object escape hatch.
type jobGetInput struct {
	Name      string `json:"name"                jsonschema:"the RayJob name (required)"`
	Namespace string `json:"namespace,omitempty" jsonschema:"namespace of the RayJob; defaults to the server default namespace"`
	Verbose   bool   `json:"verbose,omitempty"   jsonschema:"include the full unstructured object under raw; default is the distilled view"`
}

// JobGetOutput is the structured ray_job_get result: the cross-plane fused view
// (spec §5/§7.B). It always carries the CRD-side identity + lifecycle; when the
// job is scheduled it also carries the LIVE Ray status dialed from the dashboard.
// Scheduled distinguishes "not yet scheduled" (no dashboard dial) from a running
// job, so an agent never reads a connection error as a job failure.
type JobGetOutput struct {
	Name             string `json:"name"`
	Namespace        string `json:"namespace"`
	Scheduled        bool   `json:"scheduled" jsonschema:"true once status.jobId+dashboardURL are set; false means not yet scheduled"`
	Degraded         bool   `json:"degraded" jsonschema:"true when the job is scheduled but the live Ray dashboard was unreachable, so this is the CRD-derived view only (live jobStatus/message/timestamps absent)"`
	DegradeReason    string `json:"degradeReason,omitempty" jsonschema:"when degraded, the bounded reason the live Ray detail was unavailable"`
	JobID            string `json:"jobId,omitempty" jsonschema:"the Ray submission id (status.jobId); empty before scheduling"`
	RayClusterName   string `json:"rayClusterName,omitempty" jsonschema:"the RayCluster backing this job (status.rayClusterName)"`
	DeploymentStatus string `json:"deploymentStatus" jsonschema:"the CRD lifecycle phase (status.jobDeploymentStatus): Initializing/Running/Complete/Failed/..."`
	JobStatus        string `json:"jobStatus,omitempty" jsonschema:"the Ray driver phase: live from the dashboard when scheduled, else the CRD's cached status.jobStatus"`
	Message          string `json:"message,omitempty" jsonschema:"a bounded status/failure message when present"`
	Health           string `json:"health" jsonschema:"1-line distilled health summary"`
	AgeSeconds       int64  `json:"ageSeconds"`
	DashboardURL     string `json:"dashboardURL,omitempty"`
	StartedAtUnix    int64  `json:"startedAtUnix,omitempty" jsonschema:"job start time (epoch seconds) from the live dashboard status; omitted when not started"`
	EndedAtUnix      int64  `json:"endedAtUnix,omitempty" jsonschema:"job end time (epoch seconds) from the live dashboard status; omitted when not ended"`

	Raw map[string]any `json:"raw,omitempty" jsonschema:"the full unstructured object; present only when verbose=true"`
}

// jobLogsInput is the ray_job_logs argument object. Name is required; the bounds
// default at the adapter (spec §10: logs are byte- AND line-bounded).
type jobLogsInput struct {
	Name      string `json:"name"                jsonschema:"the RayJob name (required)"`
	Namespace string `json:"namespace,omitempty" jsonschema:"namespace of the RayJob; defaults to the server default namespace"`
	TailLines int    `json:"tailLines,omitempty" jsonschema:"keep only the last N lines; 0 uses the server default (~200)"`
	MaxBytes  int    `json:"maxBytes,omitempty"  jsonschema:"hard byte ceiling on the returned tail; 0 uses the server default (~16KB)"`
}

// JobLogsOutput is the structured ray_job_logs result: the bounded log tail plus
// the honest truncation signal. Scheduled=false means there are no logs yet (the
// job has not been scheduled to a cluster), NOT an error.
type JobLogsOutput struct {
	Name          string `json:"name"`
	Namespace     string `json:"namespace"`
	Scheduled     bool   `json:"scheduled" jsonschema:"false means the job is not yet scheduled, so no logs exist yet"`
	Degraded      bool   `json:"degraded" jsonschema:"true when the job is scheduled but the Ray dashboard was unreachable, so no logs could be fetched (not an error)"`
	DegradeReason string `json:"degradeReason,omitempty" jsonschema:"when degraded, the bounded reason the logs were unavailable"`
	Logs          string `json:"logs" jsonschema:"the bounded log tail (most recent output)"`
	Truncated     bool   `json:"truncated" jsonschema:"true if the line/byte ceiling clipped earlier output"`
	BytesOmitted  int    `json:"bytesOmitted,omitempty" jsonschema:"a floor on how many bytes of the full buffer were dropped"`
}

// addJobTools registers ray_job_get and ray_job_logs against the domain
// JobService. Both are read-only; the handlers do validation + mapping only,
// with the two-phase orchestration policy living in the service.
func addJobTools(server *mcp.Server, svc *domain.JobService) {
	mcp.AddTool(server, &mcp.Tool{
		Name:        "ray_job_get",
		Description: "Get a RayJob's distilled, cross-plane status: the KubeRay CRD lifecycle fused with the LIVE Ray job status dialed from the dashboard once the job is scheduled. Before scheduling it reports 'not yet scheduled' (never a connection error). Pass verbose=true for the full unstructured object. Read-only.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in jobGetInput) (*mcp.CallToolResult, JobGetOutput, error) {
		if in.Name == "" {
			return nil, JobGetOutput{}, errors.New("name is required")
		}

		res, err := svc.Get(ctx, domain.JobGetRequest{
			Namespace: in.Namespace,
			Name:      in.Name,
			Verbose:   in.Verbose,
		})
		if err != nil {
			return nil, JobGetOutput{}, mapDomainError(err) //nolint:wrapcheck // mapped to a clean, bounded tool error.
		}

		out := toJobGetOutput(res)
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: jobGetSummary(out)}},
		}, out, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "ray_job_logs",
		Description: "Fetch a bounded tail of a RayJob's driver logs from the Ray dashboard (most recent output, line- and byte-capped — never the full buffer). Returns scheduled=false with no logs when the job has not been scheduled yet. Read-only.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in jobLogsInput) (*mcp.CallToolResult, JobLogsOutput, error) {
		if in.Name == "" {
			return nil, JobLogsOutput{}, errors.New("name is required")
		}

		res, err := svc.Logs(ctx, domain.JobLogsRequest{
			Namespace: in.Namespace,
			Name:      in.Name,
			TailLines: in.TailLines,
			MaxBytes:  in.MaxBytes,
		})
		if err != nil {
			return nil, JobLogsOutput{}, mapDomainError(err) //nolint:wrapcheck // mapped to a clean, bounded tool error.
		}

		out := toJobLogsOutput(res)
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: jobLogsSummary(out)}},
		}, out, nil
	})
}

// toJobGetOutput maps the domain get result to the structured output. When the
// job is scheduled the LIVE dashboard status is authoritative for the Ray driver
// phase + message + timestamps; otherwise the CRD's cached fields stand in.
func toJobGetOutput(res domain.JobGetResult) JobGetOutput {
	d := res.Detail
	out := JobGetOutput{
		Name:             d.Name,
		Namespace:        d.Namespace,
		Scheduled:        res.Scheduled,
		Degraded:         res.Degraded,
		DegradeReason:    res.DegradeReason,
		JobID:            d.JobID,
		RayClusterName:   d.RayClusterName,
		DeploymentStatus: d.JobDeploymentStatus,
		JobStatus:        d.JobStatus, // CRD-cached phase; overridden by live below.
		Health:           d.Health,
		AgeSeconds:       int64(d.Age.Seconds()),
		DashboardURL:     d.DashboardURL,
	}

	if res.Scheduled && res.Live != nil {
		out.JobStatus = res.Live.Status
		out.Message = res.Live.Message
		if !res.Live.StartedAt.IsZero() {
			out.StartedAtUnix = res.Live.StartedAt.Unix()
		}
		if !res.Live.EndedAt.IsZero() {
			out.EndedAtUnix = res.Live.EndedAt.Unix()
		}
	}

	if res.Verbose && d.Raw != nil {
		out.Raw = d.Raw
	}
	return out
}

// toJobLogsOutput maps the domain logs result to the structured output. Before
// scheduling there is no tail, so Logs is empty and Scheduled is false.
func toJobLogsOutput(res domain.JobLogsResult) JobLogsOutput {
	out := JobLogsOutput{
		Name:          res.Detail.Name,
		Namespace:     res.Detail.Namespace,
		Scheduled:     res.Scheduled,
		Degraded:      res.Degraded,
		DegradeReason: res.DegradeReason,
	}
	if res.Logs != nil {
		out.Logs = res.Logs.Text
		out.Truncated = res.Logs.Truncated
		out.BytesOmitted = res.Logs.BytesOmitted
	}
	return out
}

// jobGetSummary renders the one-line human-readable text content. A not-yet-
// scheduled job says so via the CRD lifecycle (deploymentStatus) rather than
// surfacing an empty Ray status as if the job had run.
func jobGetSummary(out JobGetOutput) string {
	if !out.Scheduled {
		return fmt.Sprintf(
			"RayJob %q in namespace %q: not yet scheduled (%s)",
			out.Name, out.Namespace, deploymentOrUnknown(out.DeploymentStatus),
		)
	}
	if out.Degraded {
		// Graceful degrade (spec §3.2/§5): the job is scheduled but the live Ray
		// dashboard was unreachable, so report the CRD lifecycle and why the live
		// detail is missing — never a connection error.
		return fmt.Sprintf(
			"RayJob %q in namespace %q: %s; live Ray detail unavailable: %s",
			out.Name, out.Namespace, deploymentOrUnknown(out.DeploymentStatus), out.DegradeReason,
		)
	}
	return fmt.Sprintf(
		"RayJob %q in namespace %q: %s",
		out.Name, out.Namespace, out.Health,
	)
}

// jobLogsSummary renders the one-line text content for a logs fetch, honest that
// a not-yet-scheduled job has no logs (rather than implying an empty log buffer).
func jobLogsSummary(out JobLogsOutput) string {
	if !out.Scheduled {
		return fmt.Sprintf("RayJob %q in namespace %q has no logs yet (not scheduled)", out.Name, out.Namespace)
	}
	if out.Degraded {
		return fmt.Sprintf(
			"RayJob %q in namespace %q: logs unavailable — live Ray detail unavailable: %s",
			out.Name, out.Namespace, out.DegradeReason,
		)
	}
	if out.Truncated {
		return fmt.Sprintf(
			"RayJob %q logs (tail; ~%d earlier bytes omitted)",
			out.Name, out.BytesOmitted,
		)
	}
	return fmt.Sprintf("RayJob %q logs (full buffer)", out.Name)
}

// deploymentOrUnknown renders the CRD lifecycle phase, mapping the empty (New)
// status to "Unknown" so the not-scheduled message never trails off blank.
func deploymentOrUnknown(s string) string {
	if s == "" {
		return "Unknown"
	}
	return s
}
