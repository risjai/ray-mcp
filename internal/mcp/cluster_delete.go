package mcp

import (
	"context"
	"errors"
	"fmt"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/risjai/ray-mcp/internal/domain"
)

// clusterDeleteInput is the ray_cluster_delete argument object: identity, the
// confirm fingerprint (empty for preview, echoed back for commit), and dryRun.
type clusterDeleteInput struct {
	Name      string `json:"name"                jsonschema:"the RayCluster name (required)"`
	Namespace string `json:"namespace,omitempty" jsonschema:"namespace of the RayCluster; defaults to the server default namespace"`
	Confirm   string `json:"confirm,omitempty"   jsonschema:"confirmation fingerprint from a prior preview (empty call); echo it back to commit the deletion"`
	DryRun    bool   `json:"dryRun,omitempty"    jsonschema:"validate the deletion server-side without removing anything"`
}

// ClusterDeleteOutput is the unified structured result for ray_cluster_delete:
// preview calls carry Confirm + Message (the fingerprint to echo); commit calls
// carry DryRun (true when dry-run validated without deleting). A single struct
// satisfies the SDK's generic output-type requirement per tool while keeping
// preview vs commit distinguishable in the JSON (Confirm is non-empty only on a
// preview; DryRun is meaningful only on a commit).
type ClusterDeleteOutput struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	DryRun    bool   `json:"dryRun,omitempty"  jsonschema:"true if nothing was deleted (a server-side validation only)"`
	Confirm   string `json:"confirm,omitempty" jsonschema:"echo this fingerprint back to commit the deletion (preview only)"`
	Message   string `json:"message,omitempty" jsonschema:"human-readable explanation of the preview or result"`
}

// addClusterDeleteTool registers ray_cluster_delete. It is destructive (tears
// down the entire RayCluster and its owned pods/services) and idempotent (a
// second delete of an already-gone cluster is a NotFound error, not a crash; and
// a re-commit of the same fingerprint is safe). It is registered ONLY when
// --allow-destructive is set (tool absent otherwise — spec §6).
func addClusterDeleteTool(server *mcp.Server, svc *domain.ClusterWriteService) {
	destructive := true
	mcp.AddTool(server, &mcp.Tool{
		Name: "ray_cluster_delete",
		Description: "Delete a RayCluster (two-step confirmed): call WITHOUT confirm to preview " +
			"and receive a confirmation fingerprint, then re-call with confirm=<fingerprint> to " +
			"delete. Honors the ray-mcp/protected annotation (refuses). Deleting a RayCluster " +
			"cascades to its head/worker pods and head service. Requires --allow-destructive. " +
			"Pass dryRun=true to validate without deleting.",
		Annotations: &mcp.ToolAnnotations{DestructiveHint: &destructive, IdempotentHint: true},
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in clusterDeleteInput) (*mcp.CallToolResult, ClusterDeleteOutput, error) {
		if in.Name == "" {
			return nil, ClusterDeleteOutput{}, errors.New("name is required")
		}

		err := svc.Delete(ctx, domain.ClusterDeleteParams{
			Namespace: in.Namespace,
			Name:      in.Name,
			Confirm:   in.Confirm,
			DryRun:    in.DryRun,
		})

		// Preview: the domain returns a ConfirmRequiredError carrying the fingerprint.
		// This is a SUCCESSFUL preview (not a tool error): return nil error with the
		// fingerprint in the structured output so the agent can echo it back.
		var required *domain.ConfirmRequiredError
		if errors.As(err, &required) {
			ns := resolvedNamespace(in.Namespace, svc)
			out := ClusterDeleteOutput{
				Name:      in.Name,
				Namespace: ns,
				Confirm:   required.Fingerprint,
				Message: fmt.Sprintf(
					"RayCluster %q will be deleted (cascades to its pods/services). "+
						"Re-issue with confirm=%q to commit.", in.Name, required.Fingerprint),
			}
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: out.Message}},
			}, out, nil
		}

		if err != nil {
			return nil, ClusterDeleteOutput{}, mapDomainError(err) //nolint:wrapcheck // mapped to a clean, bounded tool error.
		}

		// Commit succeeded (or dry-run validated).
		ns := resolvedNamespace(in.Namespace, svc)
		verb := "deleted"
		if in.DryRun {
			verb = "validated (dry-run, not deleted)"
		}
		out := ClusterDeleteOutput{
			Name:      in.Name,
			Namespace: ns,
			DryRun:    in.DryRun,
			Message:   fmt.Sprintf("RayCluster %q %s", in.Name, verb),
		}
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: out.Message}},
		}, out, nil
	})
}
