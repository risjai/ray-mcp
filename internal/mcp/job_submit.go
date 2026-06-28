package mcp

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/risjai/ray-mcp/internal/domain"
)

// clusterSubmitInput is the curated ephemeral-cluster shape for ray_job_submit's
// clusterSpec mode (spec §6, Q16 mode B). It mirrors the standalone create's
// curated knobs minus identity (the RayJob owns the job identity; KubeRay
// generates the embedded cluster's name). A power-user tweaks the embedded cluster
// via the submit tool's single top-level rawSpec (rawSpec:{spec:{rayClusterSpec}}),
// so there is no second nested escape hatch here.
type clusterSubmitInput struct {
	RayVersion        string             `json:"rayVersion,omitempty"        jsonschema:"the Ray version for the ephemeral cluster, e.g. \"2.9.0\""`
	Image             string             `json:"image,omitempty"             jsonschema:"the Ray container image for head and workers"`
	HeadResources     resourcesInput     `json:"headResources,omitempty"     jsonschema:"resource quantities for the head node"`
	WorkerGroups      []workerGroupInput `json:"workerGroups,omitempty"      jsonschema:"the worker groups for the ephemeral cluster"`
	EnableAutoscaling bool               `json:"enableAutoscaling,omitempty" jsonschema:"enable the Ray in-tree autoscaler (honors per-group min/max)"`
}

// jobSubmitInput is the ray_job_submit argument object: identity + entrypoint,
// EXACTLY ONE cluster target (existingCluster XOR clusterSpec), optional runtime
// env / metadata, the ephemeral shutdown knob (Q16b), plus the shared rawSpec
// escape hatch and dryRun flag. rawSpec is removed from the advertised schema when
// --allow-raw-spec=false (see jobSubmitInputSchema); dryRun maps to the pipeline's
// always-on DryRunAll short-circuit (server-side validation, no mutation).
type jobSubmitInput struct {
	Name       string `json:"name"                 jsonschema:"the RayJob name (required)"`
	Namespace  string `json:"namespace,omitempty"  jsonschema:"target namespace; defaults to the server default namespace"`
	Entrypoint string `json:"entrypoint"           jsonschema:"the shell command the Ray job runs, e.g. \"python main.py\" (required)"`

	ExistingCluster string              `json:"existingCluster,omitempty" jsonschema:"run against an already-running RayCluster by name (mode A); mutually exclusive with clusterSpec"`
	ClusterSpec     *clusterSubmitInput `json:"clusterSpec,omitempty"     jsonschema:"create an ephemeral cluster for this job (mode B); mutually exclusive with existingCluster"`

	RuntimeEnvYAML           string            `json:"runtimeEnvYAML,omitempty"           jsonschema:"the Ray runtime environment as a YAML document (pip, env_vars, working_dir, ...)"`
	Metadata                 map[string]string `json:"metadata,omitempty"                 jsonschema:"arbitrary job metadata passed to Ray"`
	ShutdownAfterJobFinishes *bool             `json:"shutdownAfterJobFinishes,omitempty" jsonschema:"ephemeral mode only: tear down the cluster when the job finishes (default true; pass false to keep it for debugging). Rejected with existingCluster"`

	RawSpec map[string]any `json:"rawSpec,omitempty" jsonschema:"power-user escape hatch: a partial RayJob object merged OVER the curated params (rawSpec wins, arrays replace wholesale). Removed from this schema when --allow-raw-spec=false"`
	DryRun  bool           `json:"dryRun,omitempty"  jsonschema:"validate against the live CRD schema (server-side dry-run) without submitting anything"`
}

// JobSubmitOutput is the structured ray_job_submit result: the non-blocking submit
// outcome (spec §7.A, Q11). It echoes identity, whether this was a dry-run and
// whether it was the ephemeral-cluster mode, the just-submitted Ray status read
// back from the server (jobId/deploymentStatus are usually empty/New right after
// submit — the controller has not reconciled yet, so the agent follows with
// ray_job_get / ray_job_wait), and the field-level diff of intent-vs-server-result.
type JobSubmitOutput struct {
	Name             string              `json:"name"`
	Namespace        string              `json:"namespace"`
	DryRun           bool                `json:"dryRun"            jsonschema:"true if nothing was persisted (a server-side validation only)"`
	Ephemeral        bool                `json:"ephemeral"         jsonschema:"true if this submitted an ephemeral-cluster job (clusterSpec mode)"`
	JobID            string              `json:"jobId,omitempty"   jsonschema:"the Ray submission id (status.jobId); usually empty right after submit, before the controller reconciles"`
	DeploymentStatus string              `json:"deploymentStatus"  jsonschema:"the CRD lifecycle phase (status.jobDeploymentStatus); empty means New (not yet reconciled)"`
	FieldCount       int                 `json:"fieldCount"        jsonschema:"the number of fields the server set or changed relative to the submitted intent"`
	Diff             []fieldChangeOutput `json:"diff"              jsonschema:"the field-level diff of the submitted intent vs the server's view (server defaults surface here)"`
}

// addJobWriteTools registers the mutating RayJob tools against the domain
// JobWriteService. It is called by NewServer ONLY when --allow-mutations is set, so
// an unmutated server never advertises ray_job_submit (spec §6). allowRawSpec gates
// whether the rawSpec arg appears in the advertised schema: when false the
// power-user escape hatch is removed entirely (Gate 1 C3 hard mode), with a
// defense-in-depth runtime reject for clients that ignore the pruned schema.
func addJobWriteTools(server *mcp.Server, svc *domain.JobWriteService, allowRawSpec, allowDestructive bool) {
	mcp.AddTool(server, &mcp.Tool{
		Name:        "ray_job_submit",
		Description: "Submit a RayJob (non-blocking) against EXACTLY ONE cluster target: existingCluster (run on a running RayCluster) XOR clusterSpec (create an ephemeral cluster for the job, torn down on finish by default — pass shutdownAfterJobFinishes=false to keep it). Supplying both or neither is an error. Always server-side validated first; pass dryRun=true to validate without submitting. Returns immediately with the field-level diff and the just-submitted status (jobId/deploymentStatus are usually empty until the controller reconciles — follow with ray_job_get/ray_job_wait). Requires --allow-mutations.",
		// CreateResource is not idempotent: a second submit of the same name is an
		// already-exists error, not a no-op (mirrors ray_cluster_create).
		Annotations: &mcp.ToolAnnotations{IdempotentHint: false},
		InputSchema: jobSubmitInputSchema(allowRawSpec),
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in jobSubmitInput) (*mcp.CallToolResult, JobSubmitOutput, error) {
		if in.Name == "" {
			return nil, JobSubmitOutput{}, errors.New("name is required")
		}
		// Defense in depth: even if a client ignores the advertised schema and sends
		// rawSpec under --allow-raw-spec=false, the hard-mode contract must hold —
		// reject rather than silently honor it.
		if !allowRawSpec && len(in.RawSpec) > 0 {
			return nil, JobSubmitOutput{}, errors.New("rawSpec is disabled (--allow-raw-spec=false)")
		}

		res, err := svc.Submit(ctx, toJobSubmitParams(in))
		if err != nil {
			return nil, JobSubmitOutput{}, mapDomainError(err) //nolint:wrapcheck // mapped to a clean, bounded tool error.
		}

		out := toJobSubmitOutput(res)
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: jobSubmitSummary(out)}},
		}, out, nil
	})

	// ray_job_delete registers alongside submit (under --allow-mutations): an
	// existing-cluster delete is a plain write. The ephemeral cascade is gated at
	// runtime by allowDestructive (Q16a), not at registration.
	addJobDeleteTool(server, svc, allowDestructive)
}

// jobSubmitInputSchema returns the advertised input schema for ray_job_submit.
// When allowRawSpec is true it is nil, so the SDK reflects the full struct schema
// (rawSpec included). When false it reflects the struct and DELETES the rawSpec
// property, so the escape hatch is absent from the schema the agent sees (spec §6).
// Reflection failure falls back to nil (full schema): the handler's
// defense-in-depth check still rejects a rawSpec, so the contract holds regardless.
func jobSubmitInputSchema(allowRawSpec bool) any {
	if allowRawSpec {
		return nil
	}
	schema, err := jsonschema.For[jobSubmitInput](nil)
	if err != nil {
		return nil
	}
	delete(schema.Properties, "rawSpec")
	return schema
}

// toJobSubmitParams maps the decoded MCP input to the domain submit params.
// Namespace is left as-given (empty allowed); the service applies the default
// fallback. The mode XOR + shutdown default are enforced by the service, not here.
func toJobSubmitParams(in jobSubmitInput) domain.JobSubmitParams {
	return domain.JobSubmitParams{
		Namespace:                in.Namespace,
		Name:                     in.Name,
		Entrypoint:               in.Entrypoint,
		ExistingCluster:          in.ExistingCluster,
		ClusterSpec:              toClusterSubmitSpec(in.ClusterSpec),
		RuntimeEnvYAML:           in.RuntimeEnvYAML,
		Metadata:                 in.Metadata,
		ShutdownAfterJobFinishes: in.ShutdownAfterJobFinishes,
		RawSpec:                  domain.MergedSpec(in.RawSpec),
		DryRun:                   in.DryRun,
	}
}

// toClusterSubmitSpec maps the curated ephemeral-cluster input to the domain spec,
// or nil when clusterSpec was omitted (existing-cluster mode).
func toClusterSubmitSpec(in *clusterSubmitInput) *domain.ClusterSubmitSpec {
	if in == nil {
		return nil
	}
	groups := make([]domain.WorkerGroupParams, 0, len(in.WorkerGroups))
	for _, wg := range in.WorkerGroups {
		groups = append(groups, domain.WorkerGroupParams{
			Name:        wg.Name,
			Replicas:    wg.Replicas,
			MinReplicas: wg.MinReplicas,
			MaxReplicas: wg.MaxReplicas,
			Resources:   toResourceQuantities(wg.Resources),
		})
	}
	return &domain.ClusterSubmitSpec{
		RayVersion:        in.RayVersion,
		Image:             in.Image,
		HeadResources:     toResourceQuantities(in.HeadResources),
		WorkerGroups:      groups,
		EnableAutoscaling: in.EnableAutoscaling,
	}
}

// toJobSubmitOutput maps the domain submit result to the structured output,
// projecting the bounded diff and its headline field count.
func toJobSubmitOutput(res domain.JobSubmitResult) JobSubmitOutput {
	changes := make([]fieldChangeOutput, 0, len(res.Diff.Changes))
	for _, c := range res.Diff.Changes {
		changes = append(changes, fieldChangeOutput{
			Path:       c.Path,
			Kind:       string(c.Kind),
			Old:        c.Old,
			New:        c.New,
			FieldCount: c.FieldCount,
		})
	}
	return JobSubmitOutput{
		Name:             res.Name,
		Namespace:        res.Namespace,
		DryRun:           res.DryRun,
		Ephemeral:        res.Ephemeral,
		JobID:            res.JobID,
		DeploymentStatus: res.DeploymentStatus,
		FieldCount:       res.Diff.FieldCount(),
		Diff:             changes,
	}
}

// jobSubmitSummary renders the one-line human-readable text content. It states the
// dry-run vs submitted verb plainly, names the cluster mode, and reports the
// non-blocking status (jobId when the controller already set it, else "pending
// schedule") so the agent reads the outcome without parsing the structured output.
func jobSubmitSummary(out JobSubmitOutput) string {
	verb := "submitted"
	if out.DryRun {
		verb = "validated (dry-run, nothing submitted)"
	}
	mode := "existing cluster"
	if out.Ephemeral {
		mode = "ephemeral cluster"
	}
	status := "pending schedule"
	if out.JobID != "" {
		status = fmt.Sprintf("jobId=%s", out.JobID)
	} else if out.DeploymentStatus != "" {
		status = out.DeploymentStatus
	}
	return fmt.Sprintf(
		"RayJob %q in namespace %q %s (%s); %s",
		out.Name, out.Namespace, verb, mode, status,
	)
}
