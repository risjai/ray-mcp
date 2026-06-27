package mcp

import (
	"context"
	"errors"
	"fmt"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/risjai/ray-mcp/internal/domain"
)

// jobDeleteInput is the ray_job_delete argument object: identity, the confirm
// fingerprint (empty for preview, echoed back to commit — only consulted for an
// ephemeral cascade), and dryRun. There is no allowDestructive arg: the tier is a
// server-level flag (the closure), never a per-call client choice.
type jobDeleteInput struct {
	Name      string `json:"name"                jsonschema:"the RayJob name (required)"`
	Namespace string `json:"namespace,omitempty" jsonschema:"namespace of the RayJob; defaults to the server default namespace"`
	Confirm   string `json:"confirm,omitempty"   jsonschema:"confirmation fingerprint from a prior preview (empty call); echo it back to commit an ephemeral-cluster deletion"`
	DryRun    bool   `json:"dryRun,omitempty"    jsonschema:"validate the deletion server-side without removing anything"`
}

// JobDeleteOutput is the unified structured result for ray_job_delete: an
// ephemeral preview carries Confirm + Message (the fingerprint to echo); a commit
// (or a plain existing-cluster delete) carries DryRun. Confirm is non-empty only
// on an ephemeral preview; DryRun is meaningful only on a commit — mirrors
// ClusterDeleteOutput.
type JobDeleteOutput struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	DryRun    bool   `json:"dryRun,omitempty"  jsonschema:"true if nothing was deleted (a server-side validation only)"`
	Confirm   string `json:"confirm,omitempty" jsonschema:"echo this fingerprint back to commit an ephemeral-cluster deletion (preview only)"`
	Message   string `json:"message,omitempty" jsonschema:"human-readable explanation of the preview or result"`
}

// addJobDeleteTool registers ray_job_delete (Q16a, mode-aware). Unlike
// ray_cluster_delete it is registered whenever --allow-mutations is set, NOT gated
// behind --allow-destructive: an existing-cluster RayJob (spec.clusterSelector)
// deletes as a plain write (it only removes the record). The destructive cost is
// mode-dependent and unknowable until the live object is read, so the gate is
// enforced at RUNTIME: an EPHEMERAL RayJob (spec.rayClusterSpec) cascade-deletes
// its cluster and is refused unless allowDestructive is set, then guarded by the
// same preview→commit confirm fingerprint as ray_cluster_delete. The
// DestructiveHint advertises the worst case; IdempotentHint holds (a re-delete of
// an already-gone job is NotFound, and a re-commit of the same fingerprint is
// safe).
func addJobDeleteTool(server *mcp.Server, svc *domain.JobWriteService, allowDestructive bool) {
	destructive := true
	mcp.AddTool(server, &mcp.Tool{
		Name: "ray_job_delete",
		Description: "Delete a RayJob. An existing-cluster job (clusterSelector) deletes immediately " +
			"as a plain write (the targeted cluster is untouched). An ephemeral job (clusterSpec) owns " +
			"its RayCluster, so deleting it cascades to that cluster and every actor/job on it: that is " +
			"a two-step confirmed deletion (call WITHOUT confirm to preview and receive a fingerprint, " +
			"then re-call with confirm=<fingerprint>) and requires --allow-destructive. Honors the " +
			"ray-mcp/protected annotation (refuses). Pass dryRun=true to validate without deleting. " +
			"Requires --allow-mutations.",
		Annotations: &mcp.ToolAnnotations{DestructiveHint: &destructive, IdempotentHint: true},
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in jobDeleteInput) (*mcp.CallToolResult, JobDeleteOutput, error) {
		if in.Name == "" {
			return nil, JobDeleteOutput{}, errors.New("name is required")
		}

		err := svc.Delete(ctx, domain.JobDeleteParams{
			Namespace:        in.Namespace,
			Name:             in.Name,
			Confirm:          in.Confirm,
			AllowDestructive: allowDestructive,
			DryRun:           in.DryRun,
		})

		// Ephemeral preview: the domain returns a ConfirmRequiredError carrying the
		// fingerprint. A SUCCESSFUL preview (not a tool error): return nil error with
		// the fingerprint in the structured output so the agent can echo it back.
		var required *domain.ConfirmRequiredError
		if errors.As(err, &required) {
			ns := jobResolvedNamespace(in.Namespace, svc)
			out := JobDeleteOutput{
				Name:      in.Name,
				Namespace: ns,
				Confirm:   required.Fingerprint,
				Message: fmt.Sprintf(
					"RayJob %q is ephemeral: deleting it cascades to its RayCluster (and every actor/job on it). "+
						"Re-issue with confirm=%q to commit.", in.Name, required.Fingerprint),
			}
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: out.Message}},
			}, out, nil
		}

		if err != nil {
			return nil, JobDeleteOutput{}, mapDomainError(err) //nolint:wrapcheck // mapped to a clean, bounded tool error.
		}

		// Commit succeeded (ephemeral cascade or plain existing-cluster delete), or
		// dry-run validated.
		ns := jobResolvedNamespace(in.Namespace, svc)
		verb := "deleted"
		if in.DryRun {
			verb = "validated (dry-run, not deleted)"
		}
		out := JobDeleteOutput{
			Name:      in.Name,
			Namespace: ns,
			DryRun:    in.DryRun,
			Message:   fmt.Sprintf("RayJob %q %s", in.Name, verb),
		}
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: out.Message}},
		}, out, nil
	})
}

// jobResolvedNamespace echoes the namespace the delete targeted, applying the same
// default fallback the service uses so the result is never an empty string when the
// caller omitted one. (resolvedNamespace takes a *ClusterWriteService; the job
// service exposes the same DefaultNamespace accessor.)
func jobResolvedNamespace(ns string, svc *domain.JobWriteService) string {
	if ns != "" {
		return ns
	}
	return svc.DefaultNamespace()
}
