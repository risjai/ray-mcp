package mcp_test

import (
	"context"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/risjai/ray-mcp/internal/adapters/kuberay"
	"github.com/risjai/ray-mcp/internal/config"
	mcpserver "github.com/risjai/ray-mcp/internal/mcp"
)

// TestCapabilitiesWorksOffline is the integration proof of the lazy-dial server-
// boot invariant: a server built from the REAL kuberay adapter (NewClient) over a
// nonexistent kubeconfig still lists and answers ray_capabilities. The capabilities
// tool needs no cluster, so it must succeed even when no cluster can be dialed —
// the controller-runtime client is built lazily, only on a ray_cluster_* call.
func TestCapabilitiesWorksOffline(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	cfg := &config.Config{
		Kubeconfig:       "/nonexistent/path/to/kubeconfig",
		Context:          "no-such-context",
		DefaultNamespace: "ray-system",
	}
	adapter := kuberay.NewClient(cfg)

	server := mcpserver.NewServer(cfg, adapter, adapter)
	serverT, clientT := mcp.NewInMemoryTransports()
	if _, err := server.Connect(ctx, serverT, nil); err != nil {
		t.Fatalf("server.Connect: %v", err)
	}
	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "v0"}, nil)
	session, err := client.Connect(ctx, clientT, nil)
	if err != nil {
		t.Fatalf("client.Connect: %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })

	// tools/list must succeed with no cluster.
	if _, err := session.ListTools(ctx, &mcp.ListToolsParams{}); err != nil {
		t.Fatalf("ListTools with no cluster: %v", err)
	}

	// ray_capabilities must answer with no cluster and report the bound context.
	res, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name:      "ray_capabilities",
		Arguments: map[string]any{},
	})
	if err != nil {
		t.Fatalf("CallTool ray_capabilities with no cluster: %v", err)
	}
	if res.IsError {
		t.Fatalf("ray_capabilities reported an error with no cluster: %+v", res)
	}
	sc, ok := res.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("StructuredContent is %T, want map[string]any", res.StructuredContent)
	}
	if got := sc["kubeContext"]; got != "no-such-context" {
		t.Errorf("kubeContext = %v, want no-such-context (config-derived, no cluster call)", got)
	}

	// A ray_cluster_list call DOES need a cluster: with the unresolvable kubeconfig
	// it must return a clean tool error (no panic, no nil-deref of the lazy client).
	listRes, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name:      "ray_cluster_list",
		Arguments: map[string]any{},
	})
	if err != nil {
		t.Fatalf("CallTool ray_cluster_list transport error: %v", err)
	}
	if !listRes.IsError {
		t.Fatalf("ray_cluster_list with no cluster did not produce a tool error: %+v", listRes)
	}
}
