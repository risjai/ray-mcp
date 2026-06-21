package mcp

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/risjai/ray-mcp/internal/domain"
)

// The MCP edge for ray_cluster_update and ray_cluster_scale (Task 10). Both run
// the read-modify-apply-full pipeline in the domain (ClusterWriteService.Update /
// .Scale); these handlers only decode args, enforce the schema-level hard-mode
// rawSpec rule, and project the apply diff into the structured + text result.
// Their structured output mirrors ray_cluster_create (the field-level diff of
// intent vs the server's view), so an agent reads create/update/scale uniformly.

// clusterUpdateInput is the ray_cluster_update argument object: thin curated
// deltas merged onto the LIVE object. Only the set fields change (empty/omitted =
// unchanged). Worker replica counts are ray_cluster_scale's job; per-node resource
// or structural changes go through rawSpec (curated params stay thin, Gate 1 C3).
type clusterUpdateInput struct {
	Name              string            `json:"name"                        jsonschema:"the RayCluster name (required)"`
	Namespace         string            `json:"namespace,omitempty"         jsonschema:"namespace of the RayCluster; defaults to the server default namespace"`
	Image             string            `json:"image,omitempty"             jsonschema:"new container image for the head and all worker groups (Ray container only); omit to leave unchanged"`
	RayVersion        string            `json:"rayVersion,omitempty"        jsonschema:"new spec.rayVersion; omit to leave unchanged"`
	EnableAutoscaling *bool             `json:"enableAutoscaling,omitempty" jsonschema:"toggle the Ray in-tree autoscaler; omit to leave unchanged. Enabling it makes ray-mcp stop managing worker replicas (the autoscaler owns them)"`
	Labels            map[string]string `json:"labels,omitempty"            jsonschema:"labels to set/merge on the RayCluster"`
	Annotations       map[string]string `json:"annotations,omitempty"       jsonschema:"annotations to set/merge on the RayCluster"`
	RawSpec           map[string]any    `json:"rawSpec,omitempty"           jsonschema:"power-user escape hatch: a partial RayCluster object merged OVER the live object (rawSpec wins, arrays replace wholesale). Removed from this schema when --allow-raw-spec=false"`
	DryRun            bool              `json:"dryRun,omitempty"            jsonschema:"validate against the live CRD schema (server-side dry-run) without changing anything"`
}

// clusterScaleInput is the ray_cluster_scale argument object: change a worker
// group's bounds and/or desired count. minReplicas/maxReplicas are the autoscaler
// bounds (always safe to set). replicas is the desired count — REFUSED on an
// autoscaling cluster (the autoscaler owns the live count). Each is a pointer so
// "omitted" is distinct from "0" (the scale-to-zero teardown, which needs
// --allow-destructive).
type clusterScaleInput struct {
	Name        string `json:"name"                  jsonschema:"the RayCluster name (required)"`
	Namespace   string `json:"namespace,omitempty"   jsonschema:"namespace of the RayCluster; defaults to the server default namespace"`
	WorkerGroup string `json:"workerGroup"           jsonschema:"the worker group to scale (required; must already exist)"`
	Replicas    *int32 `json:"replicas,omitempty"    jsonschema:"desired worker count. Refused on an autoscaling cluster (set min/max instead). Setting 0 tears down the group's workers and requires --allow-destructive"`
	MinReplicas *int32 `json:"minReplicas,omitempty" jsonschema:"autoscaler floor for this group"`
	MaxReplicas *int32 `json:"maxReplicas,omitempty" jsonschema:"autoscaler ceiling for this group (must be >= minReplicas)"`
	DryRun      bool   `json:"dryRun,omitempty"      jsonschema:"validate against the live CRD schema (server-side dry-run) without changing anything"`
}

// addClusterUpdateTool registers ray_cluster_update. It is idempotent under SSA (a
// re-apply of the same change is a no-op), so IdempotentHint is true; it mutates,
// so it is not read-only. allowRawSpec mirrors create's hard-mode handling.
func addClusterUpdateTool(server *mcp.Server, svc *domain.ClusterWriteService, allowRawSpec bool) {
	mcp.AddTool(server, &mcp.Tool{
		Name:        "ray_cluster_update",
		Description: "Update an existing RayCluster's image, rayVersion, autoscaling toggle, or labels/annotations via Server-Side Apply (read-modify-apply of the live object, so unrelated fields are preserved). Optional rawSpec is merged over the live object. Always server-side validated first; pass dryRun=true to validate without changing anything. Returns the field-level diff. Requires --allow-mutations.",
		Annotations: &mcp.ToolAnnotations{IdempotentHint: true},
		InputSchema: clusterWriteInputSchema[clusterUpdateInput](allowRawSpec),
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in clusterUpdateInput) (*mcp.CallToolResult, ClusterWriteOutput, error) {
		if in.Name == "" {
			return nil, ClusterWriteOutput{}, errors.New("name is required")
		}
		if !allowRawSpec && len(in.RawSpec) > 0 {
			return nil, ClusterWriteOutput{}, errors.New("rawSpec is disabled (--allow-raw-spec=false)")
		}

		res, err := svc.Update(ctx, domain.ClusterUpdateParams{
			Namespace:         in.Namespace,
			Name:              in.Name,
			Image:             in.Image,
			RayVersion:        in.RayVersion,
			EnableAutoscaling: in.EnableAutoscaling,
			Labels:            in.Labels,
			Annotations:       in.Annotations,
			RawSpec:           domain.MergedSpec(in.RawSpec),
			DryRun:            in.DryRun,
		})
		if err != nil {
			return nil, ClusterWriteOutput{}, mapDomainError(err) //nolint:wrapcheck // mapped to a clean, bounded tool error.
		}

		out := toClusterWriteOutput(in.Name, resolvedNamespace(in.Namespace, svc), res)
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: clusterWriteSummary("RayCluster", "updated", out)}},
		}, out, nil
	})
}

// addClusterScaleTool registers ray_cluster_scale. Idempotent under SSA. It is
// tagged DestructiveHint:true because its scale-to-zero path is a teardown (B3);
// allowDestructive flows into the domain params so scale-to-zero is refused at
// runtime when the destructive tier is off — the confirm-fingerprint lands in
// Task 11.
func addClusterScaleTool(server *mcp.Server, svc *domain.ClusterWriteService, allowDestructive bool) {
	destructive := true
	mcp.AddTool(server, &mcp.Tool{
		Name:        "ray_cluster_scale",
		Description: "Scale a RayCluster worker group: set minReplicas/maxReplicas (autoscaler bounds) and/or replicas (desired count). On an autoscaling cluster, replicas is refused (set min/max instead). Scaling replicas to 0 tears down the group's workers and requires --allow-destructive. Server-side validated first; pass dryRun=true to preview. Returns the field-level diff. Requires --allow-mutations.",
		Annotations: &mcp.ToolAnnotations{IdempotentHint: true, DestructiveHint: &destructive},
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in clusterScaleInput) (*mcp.CallToolResult, ClusterWriteOutput, error) {
		if in.Name == "" {
			return nil, ClusterWriteOutput{}, errors.New("name is required")
		}
		if in.WorkerGroup == "" {
			return nil, ClusterWriteOutput{}, errors.New("workerGroup is required")
		}

		res, err := svc.Scale(ctx, domain.ClusterScaleParams{
			Namespace:        in.Namespace,
			Name:             in.Name,
			WorkerGroup:      in.WorkerGroup,
			Replicas:         in.Replicas,
			MinReplicas:      in.MinReplicas,
			MaxReplicas:      in.MaxReplicas,
			AllowDestructive: allowDestructive,
			DryRun:           in.DryRun,
		})
		if err != nil {
			return nil, ClusterWriteOutput{}, mapDomainError(err) //nolint:wrapcheck // mapped to a clean, bounded tool error.
		}

		out := toClusterWriteOutput(in.Name, resolvedNamespace(in.Namespace, svc), res)
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: clusterWriteSummary("RayCluster", "scaled", out)}},
		}, out, nil
	})
}

// clusterWriteInputSchema reflects T's schema and, in hard mode (allowRawSpec
// false), deletes the rawSpec property so the escape hatch is absent from the
// advertised schema (spec §6). Reflection failure falls back to nil (full schema);
// the handler's defense-in-depth check still rejects a rawSpec, so the hard-mode
// contract holds. It is the generic form of clusterCreateInputSchema, shared by
// the write tools that expose rawSpec.
func clusterWriteInputSchema[T any](allowRawSpec bool) any {
	if allowRawSpec {
		return nil
	}
	schema, err := jsonschema.For[T](nil)
	if err != nil {
		return nil
	}
	delete(schema.Properties, "rawSpec")
	return schema
}

// ClusterWriteOutput is the structured result shared by ray_cluster_update and
// ray_cluster_scale: identity, dry-run flag, and the field-level diff of the
// submitted intent vs the server's view (spec §10). It mirrors ClusterCreateOutput
// so the three write tools present a uniform shape.
type ClusterWriteOutput struct {
	Name       string              `json:"name"`
	Namespace  string              `json:"namespace"`
	DryRun     bool                `json:"dryRun"      jsonschema:"true if nothing was persisted (a server-side validation only)"`
	FieldCount int                 `json:"fieldCount"  jsonschema:"the number of fields the server set or changed relative to the submitted intent"`
	Diff       []fieldChangeOutput `json:"diff"        jsonschema:"the field-level diff of the submitted intent vs the server's view"`
}

// toClusterWriteOutput projects an apply result into the shared write output.
func toClusterWriteOutput(name, namespace string, res domain.ApplyResult) ClusterWriteOutput {
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
	return ClusterWriteOutput{
		Name:       name,
		Namespace:  namespace,
		DryRun:     res.DryRun,
		FieldCount: res.Diff.FieldCount(),
		Diff:       changes,
	}
}

// clusterWriteSummary renders the one-line human-readable text for an update/scale,
// stating the dry-run vs committed verb and the diff headline.
func clusterWriteSummary(kind, verb string, out ClusterWriteOutput) string {
	v := verb
	if out.DryRun {
		v = "validated (dry-run, nothing changed)"
	}
	if out.FieldCount == 0 {
		return fmt.Sprintf("%s %q in namespace %q %s", kind, out.Name, out.Namespace, v)
	}
	return fmt.Sprintf(
		"%s %q in namespace %q %s; %d field(s) changed: %s",
		kind, out.Name, out.Namespace, v, out.FieldCount, diffHeadline(out.Diff),
	)
}
