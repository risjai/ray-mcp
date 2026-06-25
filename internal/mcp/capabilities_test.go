package mcp_test

import (
	"context"
	"slices"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/risjai/ray-mcp/internal/config"
	"github.com/risjai/ray-mcp/internal/domain"
	mcpserver "github.com/risjai/ray-mcp/internal/mcp"
	"github.com/risjai/ray-mcp/internal/version"
)

// fakeSource is a config-only capabilities source for the integration test. It
// stands in for the kuberay skeleton adapter (which is itself config-only) so
// the mcp package test does not import the adapter.
type fakeSource struct {
	contextName      string
	defaultNamespace string
}

func (f fakeSource) ContextName() string      { return f.contextName }
func (f fakeSource) DefaultNamespace() string { return f.defaultNamespace }

// connect wires a server (built from cfg + src) to an in-memory client session.
// The cluster tools require a KubeRayPort; these capabilities-focused tests do
// not exercise it, so an empty fake suffices.
func connect(t *testing.T, cfg *config.Config, src fakeSource) *mcp.ClientSession {
	t.Helper()
	ctx := context.Background()

	server := mcpserver.NewServer(cfg, src, &fakeKubeRay{}, &fakeKubeRay{}, mcpserver.WedgeBackend{}, domain.NopAuditSink{})
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
	return session
}

func TestListToolsShowsCapabilities(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "default"}
	session := connect(t, cfg, fakeSource{contextName: "kind-ray", defaultNamespace: "default"})

	res, err := session.ListTools(context.Background(), &mcp.ListToolsParams{})
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}

	var names []string
	for _, tool := range res.Tools {
		names = append(names, tool.Name)
	}
	if !slices.Contains(names, "ray_capabilities") {
		t.Fatalf("ray_capabilities not in tool list, got %v", names)
	}
}

func TestCallCapabilitiesStructuredAndText(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "ray-system"}
	session := connect(t, cfg, fakeSource{contextName: "prod-ctx", defaultNamespace: "ray-system"})

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "ray_capabilities",
		Arguments: map[string]any{},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if res.IsError {
		t.Fatalf("tool reported error: %+v", res)
	}

	// StructuredContent reflects config-derived fields.
	sc, ok := res.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("StructuredContent is %T, want map[string]any", res.StructuredContent)
	}
	if got := sc["kubeContext"]; got != "prod-ctx" {
		t.Errorf("kubeContext = %v, want prod-ctx", got)
	}
	if got := sc["defaultNamespace"]; got != "ray-system" {
		t.Errorf("defaultNamespace = %v, want ray-system", got)
	}
	if got := sc["serverVersion"]; got != version.Version {
		t.Errorf("serverVersion = %v, want %v", got, version.Version)
	}
	if got := sc["kubeRayTested"]; got != version.KubeRayTested {
		t.Errorf("kubeRayTested = %v, want %v", got, version.KubeRayTested)
	}

	// A non-empty text summary must be present in Content.
	var text string
	for _, c := range res.Content {
		if tc, ok := c.(*mcp.TextContent); ok {
			text = tc.Text
		}
	}
	if text == "" {
		t.Fatal("no non-empty TextContent in result")
	}
}

func TestCapabilitiesTiersReflectConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  *config.Config
		want []any
	}{
		{
			name: "read only",
			cfg:  &config.Config{DefaultNamespace: "default", AllowMutations: false, AllowDestructive: false},
			want: []any{"read"},
		},
		{
			name: "mutations and destructive on",
			cfg:  &config.Config{DefaultNamespace: "default", AllowMutations: true, AllowDestructive: true},
			want: []any{"read", "write", "destructive"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			session := connect(t, tt.cfg, fakeSource{contextName: "c", defaultNamespace: "default"})
			res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
				Name:      "ray_capabilities",
				Arguments: map[string]any{},
			})
			if err != nil {
				t.Fatalf("CallTool: %v", err)
			}
			sc, ok := res.StructuredContent.(map[string]any)
			if !ok {
				t.Fatalf("StructuredContent is %T, want map[string]any", res.StructuredContent)
			}
			got, ok := sc["enabledTiers"].([]any)
			if !ok {
				t.Fatalf("enabledTiers is %T, want []any", sc["enabledTiers"])
			}
			if !slices.Equal(got, tt.want) {
				t.Errorf("enabledTiers = %v, want %v", got, tt.want)
			}
		})
	}
}
