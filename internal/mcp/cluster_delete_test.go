package mcp_test

import (
	"context"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/risjai/ray-mcp/internal/config"
)

// TestClusterDeleteAbsentWithoutDestructive asserts the delete tool is NOT
// advertised when --allow-destructive is false (even if --allow-mutations is set).
func TestClusterDeleteAbsentWithoutDestructive(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "default", AllowMutations: true, AllowDestructive: false, AllowRawSpec: true}
	session := connectLive(t, cfg, &liveBackend{uid: "abc-123"})
	tools := toolNames(t, session)
	if _, ok := tools["ray_cluster_delete"]; ok {
		t.Error("ray_cluster_delete advertised without --allow-destructive; it must be absent")
	}
}

// TestClusterDeletePresentWithDestructive asserts the tool is advertised with both
// flags and carries the correct annotations: DestructiveHint=true, IdempotentHint=true.
func TestClusterDeletePresentWithDestructive(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "default", AllowMutations: true, AllowDestructive: true, AllowRawSpec: true}
	session := connectLive(t, cfg, &liveBackend{uid: "abc-123"})
	tools := toolNames(t, session)

	tool, ok := tools["ray_cluster_delete"]
	if !ok {
		t.Fatal("ray_cluster_delete absent with --allow-destructive; it must be advertised")
	}
	if tool.Annotations == nil || tool.Annotations.DestructiveHint == nil || !*tool.Annotations.DestructiveHint {
		t.Error("ray_cluster_delete DestructiveHint should be true")
	}
	if tool.Annotations == nil || !tool.Annotations.IdempotentHint {
		t.Error("ray_cluster_delete IdempotentHint should be true")
	}
}

// TestClusterDeletePreviewRendersFingerprint drives a preview (empty confirm) and
// asserts it is NOT an error and carries the fingerprint in the structured output.
func TestClusterDeletePreviewRendersFingerprint(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "default", AllowMutations: true, AllowDestructive: true, AllowRawSpec: true}
	backend := &liveBackend{uid: "uid-xyz"}
	session := connectLive(t, cfg, backend)

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "ray_cluster_delete",
		Arguments: map[string]any{"name": "demo"},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if res.IsError {
		t.Fatalf("preview reported a tool error: %+v", res)
	}

	sc, ok := res.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("StructuredContent is %T, want map[string]any", res.StructuredContent)
	}
	confirm, _ := sc["confirm"].(string)
	if confirm == "" {
		t.Fatal("preview structured output has no confirm fingerprint")
	}
	msg, _ := sc["message"].(string)
	if !strings.Contains(msg, confirm) {
		t.Errorf("preview message %q does not contain the fingerprint %q", msg, confirm)
	}
	assertHasText(t, res)
}

// TestClusterDeleteMismatchIsError asserts a wrong confirm yields a tool error
// with a clean message.
func TestClusterDeleteMismatchIsError(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "default", AllowMutations: true, AllowDestructive: true, AllowRawSpec: true}
	backend := &liveBackend{uid: "uid-mismatch"}
	session := connectLive(t, cfg, backend)

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "ray_cluster_delete",
		Arguments: map[string]any{"name": "demo", "confirm": "wrong-fingerprint"},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if !res.IsError {
		t.Fatal("wrong confirm did not error; it must be a tool error")
	}
}

// TestClusterDeleteProtectedIsError asserts a protected cluster yields a tool
// error with a message mentioning "protected".
func TestClusterDeleteProtectedIsError(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "default", AllowMutations: true, AllowDestructive: true, AllowRawSpec: true}
	backend := &liveBackend{uid: "uid-protected", protected: true}
	session := connectLive(t, cfg, backend)

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "ray_cluster_delete",
		Arguments: map[string]any{"name": "demo"},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if !res.IsError {
		t.Fatal("protected cluster delete did not error")
	}
	// The error message should mention protected.
	for _, c := range res.Content {
		if tc, ok := c.(*mcp.TextContent); ok && strings.Contains(tc.Text, "protected") {
			return
		}
	}
	t.Error("error message does not mention 'protected'")
}
