package mcp_test

import (
	"context"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/risjai/ray-mcp/internal/config"
	"github.com/risjai/ray-mcp/internal/domain"
)

// ephemeralJob and existingClusterJob build the live JobDetail the fake backend's
// GetJob returns, carrying the mode-defining spec field (rayClusterSpec for an
// ephemeral cascade, clusterSelector for an attached job) and a uid so the confirm
// fingerprint is non-trivial. They mirror the domain suite's seededJob at the MCP
// edge.
func ephemeralJob(namespace, name, uid string) domain.JobDetail {
	return jobDetailFor(namespace, name, uid, map[string]any{
		"entrypoint":     "python main.py",
		"rayClusterSpec": map[string]any{"rayVersion": "2.9.0"},
	})
}

func existingClusterJob(namespace, name, uid string) domain.JobDetail {
	return jobDetailFor(namespace, name, uid, map[string]any{
		"entrypoint":      "python main.py",
		"clusterSelector": map[string]any{"ray.io/cluster": name + "-cluster"},
	})
}

func jobDetailFor(namespace, name, uid string, spec map[string]any) domain.JobDetail {
	return domain.JobDetail{
		JobSummary: domain.JobSummary{Name: name, Namespace: namespace},
		Raw: domain.MergedSpec{
			"apiVersion": "ray.io/v1",
			"kind":       "RayJob",
			"metadata":   map[string]any{"name": name, "namespace": namespace, "uid": uid},
			"spec":       spec,
		},
	}
}

// The ray_job_delete MCP-edge suite (Task 19, Q16a). It proves the tool's
// registration tier and the mode-aware preview/commit behavior end-to-end through
// an in-memory client. Unlike ray_cluster_delete, ray_job_delete is registered
// under --allow-mutations (an existing-cluster delete is a plain write); only the
// EPHEMERAL cascade is refused at runtime without --allow-destructive — so the
// tool is present whenever mutations are on, and the destructive gate is enforced
// per-call from the live job's mode.

// TestJobDeleteAbsentWithoutMutations asserts the tool is not advertised when
// --allow-mutations is off.
func TestJobDeleteAbsentWithoutMutations(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "default", AllowMutations: false}
	tools := toolNames(t, connectWrite(t, cfg, &fakeWriteBackend{}))
	if _, ok := tools["ray_job_delete"]; ok {
		t.Error("ray_job_delete advertised without --allow-mutations; it must be absent")
	}
}

// TestJobDeletePresentWithMutations asserts the tool IS advertised with
// --allow-mutations alone (no --allow-destructive needed for registration, since
// an existing-cluster delete is a plain write) and carries DestructiveHint +
// IdempotentHint.
func TestJobDeletePresentWithMutations(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "default", AllowMutations: true, AllowRawSpec: true}
	tools := toolNames(t, connectWrite(t, cfg, &fakeWriteBackend{}))

	tool, ok := tools["ray_job_delete"]
	if !ok {
		t.Fatal("ray_job_delete absent with --allow-mutations; it must be advertised")
	}
	if tool.Annotations == nil || tool.Annotations.DestructiveHint == nil || !*tool.Annotations.DestructiveHint {
		t.Error("ray_job_delete DestructiveHint should be true")
	}
	if tool.Annotations == nil || !tool.Annotations.IdempotentHint {
		t.Error("ray_job_delete IdempotentHint should be true")
	}
}

// TestJobDeleteExistingClusterPlainCommit drives a delete of an existing-cluster
// job (spec.clusterSelector) with NO confirm and NO --allow-destructive: it must
// succeed immediately as a plain write and record the delete.
func TestJobDeleteExistingClusterPlainCommit(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "default", AllowMutations: true, AllowRawSpec: true}
	backend := &fakeWriteBackend{job: existingClusterJob("default", "attached", "uid-1")}
	session := connectWrite(t, cfg, backend)

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "ray_job_delete",
		Arguments: map[string]any{"name": "attached"},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if res.IsError {
		t.Fatalf("existing-cluster delete reported a tool error: %+v", res)
	}
	if len(backend.deleteCalls) != 1 {
		t.Fatalf("delete calls = %d, want 1 (plain commit)", len(backend.deleteCalls))
	}
}

// TestJobDeleteEphemeralWithoutDestructiveIsError asserts deleting an ephemeral
// job (spec.rayClusterSpec) is refused at runtime without --allow-destructive, and
// the message mentions the tier.
func TestJobDeleteEphemeralWithoutDestructiveIsError(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "default", AllowMutations: true, AllowDestructive: false, AllowRawSpec: true}
	backend := &fakeWriteBackend{job: ephemeralJob("default", "ephem", "uid-2")}
	session := connectWrite(t, cfg, backend)

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "ray_job_delete",
		Arguments: map[string]any{"name": "ephem"},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if !res.IsError {
		t.Fatal("ephemeral delete without --allow-destructive did not error; it must be refused")
	}
	for _, c := range res.Content {
		if tc, ok := c.(*mcp.TextContent); ok && strings.Contains(tc.Text, "allow-destructive") {
			if len(backend.deleteCalls) != 0 {
				t.Errorf("delete recorded %d times despite refusal, want 0", len(backend.deleteCalls))
			}
			return
		}
	}
	t.Errorf("error message does not mention 'allow-destructive': %+v", res.Content)
}

// TestJobDeleteEphemeralPreviewRendersFingerprint drives a preview (empty confirm)
// of an ephemeral job WITH --allow-destructive and asserts it is not an error and
// carries the fingerprint in the structured output (and in the message).
func TestJobDeleteEphemeralPreviewRendersFingerprint(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "default", AllowMutations: true, AllowDestructive: true, AllowRawSpec: true}
	backend := &fakeWriteBackend{job: ephemeralJob("default", "ephem", "uid-3")}
	session := connectWrite(t, cfg, backend)

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "ray_job_delete",
		Arguments: map[string]any{"name": "ephem"},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if res.IsError {
		t.Fatalf("ephemeral preview reported a tool error: %+v", res)
	}
	sc, ok := res.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("StructuredContent is %T, want map[string]any", res.StructuredContent)
	}
	confirm, _ := sc["confirm"].(string)
	if confirm == "" {
		t.Fatal("ephemeral preview has no confirm fingerprint")
	}
	msg, _ := sc["message"].(string)
	if !strings.Contains(msg, confirm) {
		t.Errorf("preview message %q does not contain the fingerprint %q", msg, confirm)
	}
	if len(backend.deleteCalls) != 0 {
		t.Errorf("preview recorded a delete (%d), want 0", len(backend.deleteCalls))
	}
	assertHasText(t, res)
}

// TestJobDeleteEphemeralCommitDeletes drives the full two-call flow: preview to
// get the fingerprint, then commit with it, asserting the delete is recorded.
func TestJobDeleteEphemeralCommitDeletes(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "default", AllowMutations: true, AllowDestructive: true, AllowRawSpec: true}
	backend := &fakeWriteBackend{job: ephemeralJob("default", "ephem", "uid-4")}
	session := connectWrite(t, cfg, backend)

	preview, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "ray_job_delete",
		Arguments: map[string]any{"name": "ephem"},
	})
	if err != nil {
		t.Fatalf("preview CallTool: %v", err)
	}
	confirm := preview.StructuredContent.(map[string]any)["confirm"].(string)

	commit, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "ray_job_delete",
		Arguments: map[string]any{"name": "ephem", "confirm": confirm},
	})
	if err != nil {
		t.Fatalf("commit CallTool: %v", err)
	}
	if commit.IsError {
		t.Fatalf("commit reported a tool error: %+v", commit)
	}
	if len(backend.deleteCalls) != 1 {
		t.Fatalf("delete calls = %d, want 1 (commit)", len(backend.deleteCalls))
	}
}

// TestJobDeleteProtectedIsError asserts a protected job yields a tool error
// mentioning "protected", regardless of mode.
func TestJobDeleteProtectedIsError(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "default", AllowMutations: true, AllowDestructive: true, AllowRawSpec: true}
	job := existingClusterJob("default", "attached", "uid-5")
	job.Raw["metadata"].(map[string]any)["annotations"] = map[string]any{"ray-mcp/protected": "true"}
	backend := &fakeWriteBackend{job: job}
	session := connectWrite(t, cfg, backend)

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "ray_job_delete",
		Arguments: map[string]any{"name": "attached"},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if !res.IsError {
		t.Fatal("protected job delete did not error")
	}
	for _, c := range res.Content {
		if tc, ok := c.(*mcp.TextContent); ok && strings.Contains(tc.Text, "protected") {
			return
		}
	}
	t.Error("error message does not mention 'protected'")
}

// TestJobDeleteMissingNameIsError asserts an empty name is a validation error.
func TestJobDeleteMissingNameIsError(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "default", AllowMutations: true, AllowDestructive: true, AllowRawSpec: true}
	session := connectWrite(t, cfg, &fakeWriteBackend{})

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "ray_job_delete",
		Arguments: map[string]any{},
	})
	if err != nil {
		t.Fatalf("CallTool transport error: %v", err)
	}
	if !res.IsError {
		t.Fatalf("empty name did not produce a tool error: %+v", res)
	}
}
