package mcp

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/risjai/ray-mcp/internal/domain"
)

// resourcesInput is the curated per-node resource shape (spec §6:
// {cpu,memory,gpu} as Kubernetes quantity strings). Empty fields are omitted.
type resourcesInput struct {
	CPU    string `json:"cpu,omitempty"    jsonschema:"CPU quantity, e.g. \"2\" or \"500m\""`
	Memory string `json:"memory,omitempty" jsonschema:"memory quantity, e.g. \"4Gi\""`
	GPU    string `json:"gpu,omitempty"    jsonschema:"GPU count mapped to nvidia.com/gpu, e.g. \"1\""`
}

// workerGroupInput is one curated worker group (spec §6:
// workerGroups[]{name,replicas,min,max,resources}).
type workerGroupInput struct {
	Name        string         `json:"name"                  jsonschema:"the worker group name (required)"`
	Replicas    int32          `json:"replicas,omitempty"    jsonschema:"desired worker count for this group"`
	MinReplicas int32          `json:"minReplicas,omitempty" jsonschema:"autoscaler floor for this group (used when enableAutoscaling)"`
	MaxReplicas int32          `json:"maxReplicas,omitempty" jsonschema:"autoscaler ceiling; clamped up to replicas when left 0"`
	Resources   resourcesInput `json:"resources,omitempty"   jsonschema:"per-worker resource quantities"`
}

// clusterCreateInput is the ray_cluster_create argument object: the thin curated
// shape (spec §6) plus the rawSpec escape hatch and the dryRun flag. RawSpec is a
// free-form object merged OVER the curated base (rawSpec wins, RFC 7396); it is
// stripped from the advertised schema when --allow-raw-spec=false (see
// clusterCreateInputSchema). dryRun maps to the pipeline's always-on DryRunAll
// short-circuit: a server-side validation with no mutation.
type clusterCreateInput struct {
	Name              string             `json:"name"                        jsonschema:"the RayCluster name (required)"`
	Namespace         string             `json:"namespace,omitempty"         jsonschema:"target namespace; defaults to the server default namespace"`
	RayVersion        string             `json:"rayVersion,omitempty"        jsonschema:"the Ray version, e.g. \"2.9.0\""`
	Image             string             `json:"image,omitempty"             jsonschema:"the Ray container image for head and workers"`
	HeadResources     resourcesInput     `json:"headResources,omitempty"     jsonschema:"resource quantities for the head node"`
	WorkerGroups      []workerGroupInput `json:"workerGroups,omitempty"      jsonschema:"the worker groups to create"`
	EnableAutoscaling bool               `json:"enableAutoscaling,omitempty" jsonschema:"enable the Ray in-tree autoscaler (honors per-group min/max)"`
	Labels            map[string]string  `json:"labels,omitempty"            jsonschema:"labels to set on the RayCluster"`
	Annotations       map[string]string  `json:"annotations,omitempty"       jsonschema:"annotations to set on the RayCluster"`
	RawSpec           map[string]any     `json:"rawSpec,omitempty"           jsonschema:"power-user escape hatch: a partial RayCluster object merged OVER the curated params (rawSpec wins, arrays replace wholesale). Removed from this schema when --allow-raw-spec=false"`
	DryRun            bool               `json:"dryRun,omitempty"            jsonschema:"validate against the live CRD schema (server-side dry-run) without creating anything"`
}

// fieldChangeOutput is one entry of the structured diff (spec §10). It mirrors
// domain.FieldChange so the agent gets the machine-readable change set alongside
// the human text summary.
type fieldChangeOutput struct {
	Path       string `json:"path"`
	Kind       string `json:"kind"` // modified | added | removed | subtree
	Old        any    `json:"old,omitempty"`
	New        any    `json:"new,omitempty"`
	FieldCount int    `json:"fieldCount,omitempty"`
}

// ClusterCreateOutput is the structured ray_cluster_create result: the identity,
// whether this was a dry-run, the field-level diff of intent-vs-server-result, and
// its headline count (spec §10). The full server object is intentionally NOT
// returned — the diff is the token-economy summary; ray_cluster_get --verbose is
// the full-object escape hatch.
type ClusterCreateOutput struct {
	Name       string              `json:"name"`
	Namespace  string              `json:"namespace"`
	DryRun     bool                `json:"dryRun"      jsonschema:"true if nothing was persisted (a server-side validation only)"`
	FieldCount int                 `json:"fieldCount"  jsonschema:"the number of fields the server set or changed relative to the submitted intent"`
	Diff       []fieldChangeOutput `json:"diff"        jsonschema:"the field-level diff of the submitted intent vs the server's view (server defaults surface here)"`
}

// addClusterWriteTools registers the mutating RayCluster tools against the domain
// ClusterWriteService. It is called by NewServer ONLY when --allow-mutations is
// set, so an unmutated server never advertises these (spec §6). allowRawSpec gates
// whether the rawSpec arg appears in the advertised schema: when false the
// power-user escape hatch is removed entirely (Gate 1 C3 hard mode).
func addClusterWriteTools(server *mcp.Server, svc *domain.ClusterWriteService, allowRawSpec bool) {
	mcp.AddTool(server, &mcp.Tool{
		Name:        "ray_cluster_create",
		Description: "Create a RayCluster from curated params (rayVersion, image, head/worker resources, worker groups, autoscaling) with an optional rawSpec escape hatch merged over them. Always server-side validated first; pass dryRun=true to validate without creating. Returns the field-level diff of your intent vs the server's view (server defaults surface here). Requires --allow-mutations.",
		// CreateResource is not idempotent: a second create of the same name is an
		// already-exists error, not a no-op. (Update/scale, which ARE idempotent
		// under SSA, set IdempotentHint true — Task 10.)
		Annotations: &mcp.ToolAnnotations{IdempotentHint: false},
		InputSchema: clusterCreateInputSchema(allowRawSpec),
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in clusterCreateInput) (*mcp.CallToolResult, ClusterCreateOutput, error) {
		if in.Name == "" {
			return nil, ClusterCreateOutput{}, errors.New("name is required")
		}
		// Defense in depth: even if a client ignores the advertised schema and sends
		// rawSpec under --allow-raw-spec=false, the hard-mode contract must hold —
		// reject rather than silently honor it.
		if !allowRawSpec && len(in.RawSpec) > 0 {
			return nil, ClusterCreateOutput{}, errors.New("rawSpec is disabled (--allow-raw-spec=false)")
		}

		res, err := svc.Create(ctx, toCreateParams(in))
		if err != nil {
			return nil, ClusterCreateOutput{}, mapDomainError(err) //nolint:wrapcheck // mapped to a clean, bounded tool error.
		}

		out := toClusterCreateOutput(in.Name, resolvedNamespace(in.Namespace, svc), res)
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: clusterCreateSummary(out)}},
		}, out, nil
	})
}

// clusterCreateInputSchema returns the advertised input schema for
// ray_cluster_create. When allowRawSpec is true it is nil, so the SDK reflects the
// full struct schema (rawSpec included). When false it reflects the struct and
// then DELETES the rawSpec property, so the escape hatch is absent from the schema
// the agent sees (spec §6: "--allow-raw-spec=false removes the rawSpec arg from
// every tool schema entirely"). Reflection failure falls back to nil (full
// schema): the handler's defense-in-depth check still rejects a rawSpec, so the
// hard-mode contract holds even if the schema could not be pruned.
func clusterCreateInputSchema(allowRawSpec bool) any {
	if allowRawSpec {
		return nil
	}
	schema, err := jsonschema.For[clusterCreateInput](nil)
	if err != nil {
		return nil
	}
	delete(schema.Properties, "rawSpec")
	return schema
}

// toCreateParams maps the decoded MCP input to the domain create params. Namespace
// is left as-given (empty allowed); the service applies the default fallback.
func toCreateParams(in clusterCreateInput) domain.ClusterCreateParams {
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
	return domain.ClusterCreateParams{
		Namespace:         in.Namespace,
		Name:              in.Name,
		RayVersion:        in.RayVersion,
		Image:             in.Image,
		HeadResources:     toResourceQuantities(in.HeadResources),
		WorkerGroups:      groups,
		EnableAutoscaling: in.EnableAutoscaling,
		Labels:            in.Labels,
		Annotations:       in.Annotations,
		RawSpec:           domain.MergedSpec(in.RawSpec),
		DryRun:            in.DryRun,
	}
}

func toResourceQuantities(r resourcesInput) domain.ResourceQuantities {
	return domain.ResourceQuantities{CPU: r.CPU, Memory: r.Memory, GPU: r.GPU}
}

// toClusterCreateOutput maps the domain apply result to the structured output,
// projecting the bounded diff and its headline field count.
func toClusterCreateOutput(name, namespace string, res domain.ApplyResult) ClusterCreateOutput {
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
	return ClusterCreateOutput{
		Name:       name,
		Namespace:  namespace,
		DryRun:     res.DryRun,
		FieldCount: res.Diff.FieldCount(),
		Diff:       changes,
	}
}

// resolvedNamespace reports the namespace the create targeted, applying the same
// default fallback the service uses so the result echoes the real namespace (not
// an empty string) when the caller omitted one.
func resolvedNamespace(ns string, svc *domain.ClusterWriteService) string {
	if ns != "" {
		return ns
	}
	return svc.DefaultNamespace()
}

// clusterCreateSummary renders the one-line human-readable text content. It states
// the dry-run vs created verb plainly and reports the diff headline so the agent
// reads the outcome without parsing the structured diff.
func clusterCreateSummary(out ClusterCreateOutput) string {
	verb := "created"
	if out.DryRun {
		verb = "validated (dry-run, nothing created)"
	}
	if out.FieldCount == 0 {
		return fmt.Sprintf("RayCluster %q in namespace %q %s", out.Name, out.Namespace, verb)
	}
	return fmt.Sprintf(
		"RayCluster %q in namespace %q %s; server set %d field(s): %s",
		out.Name, out.Namespace, verb, out.FieldCount, diffHeadline(out.Diff),
	)
}

// diffHeadline renders a compact, bounded one-line summary of the diff for the
// text content — the first few changes inline, with an honest "(+N more)" tail so
// a large diff never floods the line.
func diffHeadline(changes []fieldChangeOutput) string {
	const maxInline = 5
	parts := make([]string, 0, maxInline)
	for i, c := range changes {
		if i >= maxInline {
			parts = append(parts, fmt.Sprintf("(+%d more)", len(changes)-maxInline))
			break
		}
		parts = append(parts, fieldChangeHeadline(c))
	}
	return strings.Join(parts, ", ")
}

// fieldChangeHeadline renders one change compactly: scalar modifications show
// old→new, composites/subtrees show their field count.
func fieldChangeHeadline(c fieldChangeOutput) string {
	switch domain.ChangeKind(c.Kind) {
	case domain.ChangeModified:
		if c.FieldCount > 0 {
			return fmt.Sprintf("%s changed (%d fields)", c.Path, c.FieldCount)
		}
		return fmt.Sprintf("%s %v→%v", c.Path, c.Old, c.New)
	case domain.ChangeAdded:
		if c.FieldCount > 0 {
			return fmt.Sprintf("+%s (%d fields)", c.Path, c.FieldCount)
		}
		return fmt.Sprintf("+%s=%v", c.Path, c.New)
	case domain.ChangeRemoved:
		return fmt.Sprintf("-%s", c.Path)
	case domain.ChangeSubtree:
		return fmt.Sprintf("%s changed (%d fields)", c.Path, c.FieldCount)
	default:
		return c.Path
	}
}
