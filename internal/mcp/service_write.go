package mcp

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/risjai/ray-mcp/internal/domain"
)

// serviceDeployInput is the ray_service_deploy argument object: the thin curated
// shape for the RayService + its embedded rayClusterConfig, plus rawSpec and dryRun.
type serviceDeployInput struct {
	Name              string             `json:"name"                        jsonschema:"the RayService name (required)"`
	Namespace         string             `json:"namespace,omitempty"         jsonschema:"target namespace; defaults to the server default namespace"`
	ServeConfigV2     string             `json:"serveConfigV2,omitempty"     jsonschema:"the Serve config (YAML/JSON string); optional — the RayService can start without it"`
	RayVersion        string             `json:"rayVersion,omitempty"        jsonschema:"the Ray version for the embedded cluster, e.g. \"2.9.0\""`
	Image             string             `json:"image,omitempty"             jsonschema:"the Ray container image for head and workers"`
	HeadResources     resourcesInput     `json:"headResources,omitempty"     jsonschema:"resource quantities for the head node"`
	WorkerGroups      []workerGroupInput `json:"workerGroups,omitempty"      jsonschema:"the worker groups for the embedded cluster"`
	EnableAutoscaling bool               `json:"enableAutoscaling,omitempty" jsonschema:"enable the Ray in-tree autoscaler"`
	Labels            map[string]string  `json:"labels,omitempty"            jsonschema:"labels to set on the RayService"`
	Annotations       map[string]string  `json:"annotations,omitempty"       jsonschema:"annotations to set on the RayService"`
	RawSpec           map[string]any     `json:"rawSpec,omitempty"           jsonschema:"power-user escape hatch: a partial RayService object merged OVER the curated params (rawSpec wins, arrays replace wholesale). Removed from this schema when --allow-raw-spec=false"`
	DryRun            bool               `json:"dryRun,omitempty"            jsonschema:"validate against the live CRD schema (server-side dry-run) without deploying anything"`
}

// serviceUpdateInput is the ray_service_update argument object: thin curated
// deltas on the LIVE RayService. Only the set fields change.
type serviceUpdateInput struct {
	Name              string            `json:"name"                        jsonschema:"the RayService name (required)"`
	Namespace         string            `json:"namespace,omitempty"         jsonschema:"namespace of the RayService; defaults to the server default namespace"`
	ServeConfigV2     *string           `json:"serveConfigV2,omitempty"     jsonschema:"new Serve config (YAML/JSON string); omit to leave unchanged"`
	Image             string            `json:"image,omitempty"             jsonschema:"new container image for the head and all worker groups; omit to leave unchanged"`
	RayVersion        string            `json:"rayVersion,omitempty"        jsonschema:"new rayClusterConfig.rayVersion; omit to leave unchanged"`
	EnableAutoscaling *bool             `json:"enableAutoscaling,omitempty" jsonschema:"toggle the Ray in-tree autoscaler in the embedded cluster; omit to leave unchanged"`
	Labels            map[string]string `json:"labels,omitempty"            jsonschema:"labels to set/merge on the RayService"`
	Annotations       map[string]string `json:"annotations,omitempty"       jsonschema:"annotations to set/merge on the RayService"`
	RawSpec           map[string]any    `json:"rawSpec,omitempty"           jsonschema:"power-user escape hatch: a partial RayService object merged OVER the live object (rawSpec wins, arrays replace wholesale). Removed from this schema when --allow-raw-spec=false"`
	DryRun            bool              `json:"dryRun,omitempty"            jsonschema:"validate against the live CRD schema (server-side dry-run) without changing anything"`
}

// ServiceDeployOutput is the structured ray_service_deploy result.
type ServiceDeployOutput struct {
	Name       string              `json:"name"`
	Namespace  string              `json:"namespace"`
	DryRun     bool                `json:"dryRun"      jsonschema:"true if nothing was persisted (a server-side validation only)"`
	FieldCount int                 `json:"fieldCount"  jsonschema:"the number of fields the server set or changed relative to the submitted intent"`
	Diff       []fieldChangeOutput `json:"diff"        jsonschema:"the field-level diff of the submitted intent vs the server's view"`
}

// ServiceUpdateOutput is the structured ray_service_update result: includes the
// predicted operator path alongside the field-level diff.
type ServiceUpdateOutput struct {
	Name          string              `json:"name"`
	Namespace     string              `json:"namespace"`
	DryRun        bool                `json:"dryRun"         jsonschema:"true if nothing was persisted (a server-side validation only)"`
	PredictedPath string              `json:"predictedPath"  jsonschema:"the predicted operator behavior: in-place, zero-downtime-swap, or scale (no swap)"`
	FieldCount    int                 `json:"fieldCount"     jsonschema:"the number of fields the server set or changed relative to the submitted intent"`
	Diff          []fieldChangeOutput `json:"diff"           jsonschema:"the field-level diff of the submitted intent vs the server's view"`
}

// addServiceWriteTools registers the mutating RayService tools (deploy, update,
// and — under the destructive tier — delete) against the domain
// ServiceWriteService. Called by NewServer ONLY when --allow-mutations is set
// (spec §6). allowRawSpec gates the rawSpec arg in the advertised schema.
// allowDestructive gates ray_service_delete: deleting a RayService always cascades
// to its owned cluster(s), so — like ray_cluster_delete — the tool is absent unless
// the destructive tier is enabled.
func addServiceWriteTools(server *mcp.Server, svc *domain.ServiceWriteService, allowRawSpec, allowDestructive bool) {
	mcp.AddTool(server, &mcp.Tool{
		Name:        "ray_service_deploy",
		Description: "Deploy a RayService from curated params (serveConfigV2, rayVersion, image, head/worker resources, worker groups, autoscaling) with an optional rawSpec escape hatch merged over them. The embedded cluster config uses the rayClusterConfig key. Always server-side validated first; pass dryRun=true to validate without deploying. Returns the field-level diff. Requires --allow-mutations.",
		Annotations: &mcp.ToolAnnotations{IdempotentHint: false},
		InputSchema: serviceDeployInputSchema(allowRawSpec),
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in serviceDeployInput) (*mcp.CallToolResult, ServiceDeployOutput, error) {
		if in.Name == "" {
			return nil, ServiceDeployOutput{}, errors.New("name is required")
		}
		if !allowRawSpec && len(in.RawSpec) > 0 {
			return nil, ServiceDeployOutput{}, errors.New("rawSpec is disabled (--allow-raw-spec=false)")
		}

		res, err := svc.Deploy(ctx, toServiceDeployParams(in))
		if err != nil {
			return nil, ServiceDeployOutput{}, mapDomainError(err) //nolint:wrapcheck // mapped to a clean, bounded tool error.
		}

		out := toServiceDeployOutput(res)
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: serviceDeploySummary(out)}},
		}, out, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "ray_service_update",
		Description: "Update an existing RayService's serveConfigV2, image, rayVersion, autoscaling, or labels/annotations via Server-Side Apply (read-modify-apply of the live object). Reports the PREDICTED operator path: an in-place serve reconfig, a zero-downtime cluster swap, or a replicas-only scale. Optional rawSpec is merged over the live object. Always server-side validated first; pass dryRun=true to validate without changing anything. Returns the field-level diff and the predicted path. Requires --allow-mutations.",
		Annotations: &mcp.ToolAnnotations{IdempotentHint: true},
		InputSchema: serviceUpdateInputSchema(allowRawSpec),
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in serviceUpdateInput) (*mcp.CallToolResult, ServiceUpdateOutput, error) {
		if in.Name == "" {
			return nil, ServiceUpdateOutput{}, errors.New("name is required")
		}
		if !allowRawSpec && len(in.RawSpec) > 0 {
			return nil, ServiceUpdateOutput{}, errors.New("rawSpec is disabled (--allow-raw-spec=false)")
		}

		res, err := svc.Update(ctx, domain.ServiceUpdateParams{
			Namespace:         in.Namespace,
			Name:              in.Name,
			ServeConfigV2:     in.ServeConfigV2,
			Image:             in.Image,
			RayVersion:        in.RayVersion,
			EnableAutoscaling: in.EnableAutoscaling,
			Labels:            in.Labels,
			Annotations:       in.Annotations,
			RawSpec:           domain.MergedSpec(in.RawSpec),
			DryRun:            in.DryRun,
		})
		if err != nil {
			return nil, ServiceUpdateOutput{}, mapDomainError(err) //nolint:wrapcheck // mapped to a clean, bounded tool error.
		}

		out := toServiceUpdateOutput(res)
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: serviceUpdateSummary(out)}},
		}, out, nil
	})

	if allowDestructive {
		addServiceDeleteTool(server, svc)
	}
}

// serviceDeleteInput is the ray_service_delete argument object: identity, the
// confirm fingerprint (empty for preview, echoed back to commit), force (override
// the serving-traffic guard), and dryRun.
type serviceDeleteInput struct {
	Name      string `json:"name"                jsonschema:"the RayService name (required)"`
	Namespace string `json:"namespace,omitempty" jsonschema:"namespace of the RayService; defaults to the server default namespace"`
	Confirm   string `json:"confirm,omitempty"   jsonschema:"confirmation fingerprint from a prior preview (empty call); echo it back to commit the deletion"`
	Force     bool   `json:"force,omitempty"     jsonschema:"override the guard that refuses to delete a RayService that appears to be serving traffic; confirm is still required"`
	DryRun    bool   `json:"dryRun,omitempty"    jsonschema:"validate the deletion server-side without removing anything"`
}

// ServiceDeleteOutput is the unified structured result for ray_service_delete:
// preview calls carry Confirm + Message (the fingerprint to echo); commit calls
// carry DryRun. Confirm is non-empty only on a preview; DryRun is meaningful only
// on a commit — mirrors ClusterDeleteOutput.
type ServiceDeleteOutput struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	DryRun    bool   `json:"dryRun,omitempty"  jsonschema:"true if nothing was deleted (a server-side validation only)"`
	Confirm   string `json:"confirm,omitempty" jsonschema:"echo this fingerprint back to commit the deletion (preview only)"`
	Message   string `json:"message,omitempty" jsonschema:"human-readable explanation of the preview or result"`
}

// addServiceDeleteTool registers ray_service_delete. Deleting a RayService cascades
// to its owned RayCluster(s) and every serve replica/actor on them, so it is
// destructive and registered ONLY when --allow-destructive is set (tool absent
// otherwise — spec §6). Beyond the protected annotation and the two-step confirm
// fingerprint (shared with the other deletes), it adds the Gate 4 serving-traffic
// guard: a service that appears to be serving is refused unless force=true (the
// override still requires confirm). It is idempotent (a re-delete of a gone service
// is NotFound; a re-commit of the same fingerprint is safe).
func addServiceDeleteTool(server *mcp.Server, svc *domain.ServiceWriteService) {
	destructive := true
	mcp.AddTool(server, &mcp.Tool{
		Name: "ray_service_delete",
		Description: "Delete a RayService (two-step confirmed): call WITHOUT confirm to preview and " +
			"receive a confirmation fingerprint, then re-call with confirm=<fingerprint> to delete. " +
			"Deleting a RayService cascades to its owned RayCluster(s) and every serve replica on them. " +
			"If the service appears to be serving traffic (ready serve endpoints, or an upgrade/rollback " +
			"in progress) the deletion is refused unless force=true (a guardrail against deleting a live " +
			"service; confirm is still required). Honors the ray-mcp/protected annotation (refuses). " +
			"Requires --allow-destructive. Pass dryRun=true to validate without deleting.",
		Annotations: &mcp.ToolAnnotations{DestructiveHint: &destructive, IdempotentHint: true},
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in serviceDeleteInput) (*mcp.CallToolResult, ServiceDeleteOutput, error) {
		if in.Name == "" {
			return nil, ServiceDeleteOutput{}, errors.New("name is required")
		}

		err := svc.Delete(ctx, domain.ServiceDeleteParams{
			Namespace: in.Namespace,
			Name:      in.Name,
			Confirm:   in.Confirm,
			Force:     in.Force,
			DryRun:    in.DryRun,
		})

		// Preview: the domain returns a ConfirmRequiredError carrying the fingerprint.
		// A SUCCESSFUL preview (not a tool error): return nil error with the
		// fingerprint in the structured output so the agent can echo it back.
		var required *domain.ConfirmRequiredError
		if errors.As(err, &required) {
			ns := serviceResolvedNamespace(in.Namespace, svc)
			out := ServiceDeleteOutput{
				Name:      in.Name,
				Namespace: ns,
				Confirm:   required.Fingerprint,
				Message: fmt.Sprintf(
					"RayService %q will be deleted (cascades to its owned cluster(s) and serve replicas). "+
						"Re-issue with confirm=%q to commit.", in.Name, required.Fingerprint),
			}
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: out.Message}},
			}, out, nil
		}

		if err != nil {
			return nil, ServiceDeleteOutput{}, mapDomainError(err) //nolint:wrapcheck // mapped to a clean, bounded tool error.
		}

		// Commit succeeded (or dry-run validated).
		ns := serviceResolvedNamespace(in.Namespace, svc)
		verb := "deleted"
		if in.DryRun {
			verb = "validated (dry-run, not deleted)"
		}
		out := ServiceDeleteOutput{
			Name:      in.Name,
			Namespace: ns,
			DryRun:    in.DryRun,
			Message:   fmt.Sprintf("RayService %q %s", in.Name, verb),
		}
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: out.Message}},
		}, out, nil
	})
}

// serviceResolvedNamespace echoes the namespace the delete targeted, applying the
// same default fallback the service uses so the result is never blank when the
// caller omitted one (mirrors jobResolvedNamespace).
func serviceResolvedNamespace(ns string, svc *domain.ServiceWriteService) string {
	if ns != "" {
		return ns
	}
	return svc.DefaultNamespace()
}

func serviceDeployInputSchema(allowRawSpec bool) any {
	if allowRawSpec {
		return nil
	}
	schema, err := jsonschema.For[serviceDeployInput](nil)
	if err != nil {
		return nil
	}
	delete(schema.Properties, "rawSpec")
	return schema
}

func serviceUpdateInputSchema(allowRawSpec bool) any {
	if allowRawSpec {
		return nil
	}
	schema, err := jsonschema.For[serviceUpdateInput](nil)
	if err != nil {
		return nil
	}
	delete(schema.Properties, "rawSpec")
	return schema
}

func toServiceDeployParams(in serviceDeployInput) domain.ServiceDeployParams {
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
	return domain.ServiceDeployParams{
		Namespace:         in.Namespace,
		Name:              in.Name,
		ServeConfigV2:     in.ServeConfigV2,
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

func toServiceDeployOutput(res domain.ServiceDeployResult) ServiceDeployOutput {
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
	return ServiceDeployOutput{
		Name:       res.Name,
		Namespace:  res.Namespace,
		DryRun:     res.DryRun,
		FieldCount: res.Diff.FieldCount(),
		Diff:       changes,
	}
}

func toServiceUpdateOutput(res domain.ServiceUpdateResult) ServiceUpdateOutput {
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
	return ServiceUpdateOutput{
		Name:          res.Name,
		Namespace:     res.Namespace,
		DryRun:        res.DryRun,
		PredictedPath: res.PredictedPath,
		FieldCount:    res.Diff.FieldCount(),
		Diff:          changes,
	}
}

func serviceDeploySummary(out ServiceDeployOutput) string {
	verb := "deployed"
	if out.DryRun {
		verb = "validated (dry-run, nothing deployed)"
	}
	if out.FieldCount == 0 {
		return fmt.Sprintf("RayService %q in namespace %q %s", out.Name, out.Namespace, verb)
	}
	return fmt.Sprintf(
		"RayService %q in namespace %q %s; server set %d field(s): %s",
		out.Name, out.Namespace, verb, out.FieldCount, diffHeadline(out.Diff),
	)
}

func serviceUpdateSummary(out ServiceUpdateOutput) string {
	verb := "updated"
	if out.DryRun {
		verb = "validated (dry-run, nothing changed)"
	}
	path := ""
	if out.PredictedPath != "" {
		path = fmt.Sprintf("; predicted path: %s", out.PredictedPath)
	}
	if out.FieldCount == 0 {
		return fmt.Sprintf("RayService %q in namespace %q %s%s", out.Name, out.Namespace, verb, path)
	}
	return fmt.Sprintf(
		"RayService %q in namespace %q %s; %d field(s) changed: %s%s",
		out.Name, out.Namespace, verb, out.FieldCount, diffHeadline(out.Diff), path,
	)
}
