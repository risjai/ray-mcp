package mcp_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/risjai/ray-mcp/internal/config"
	"github.com/risjai/ray-mcp/internal/domain"
	mcpserver "github.com/risjai/ray-mcp/internal/mcp"
)

// fakeKubeRay implements domain.ClusterReader for the MCP-layer tests: the two
// cluster read methods ray_cluster_list / ray_cluster_get drive. Tests seed
// clusters + an optional continue token directly.
type fakeKubeRay struct {
	clusters   map[string]domain.ClusterDetail
	continueTo string
	events     map[string][]domain.Event
}

// compile-time proof the fake satisfies the narrow port NewServer takes.
var _ domain.ClusterReader = (*fakeKubeRay)(nil)

func clusterKey(ns, name string) string { return ns + "/" + name }

func (f *fakeKubeRay) ListClusters(_ context.Context, namespace string, opts domain.ListOptions) (domain.ClusterList, error) {
	var items []domain.ClusterSummary
	for _, c := range f.clusters {
		if opts.AllNamespaces || c.Namespace == namespace {
			items = append(items, c.ClusterSummary)
		}
	}
	return domain.ClusterList{Items: items, Continue: f.continueTo}, nil
}

func (f *fakeKubeRay) GetCluster(_ context.Context, namespace, name string) (domain.ClusterDetail, error) {
	c, ok := f.clusters[clusterKey(namespace, name)]
	if !ok {
		return domain.ClusterDetail{}, &domain.NotFoundError{Kind: domain.KindRayCluster, Namespace: namespace, Name: name}
	}
	return c, nil
}

func (f *fakeKubeRay) Events(_ context.Context, _ domain.Kind, namespace, name string, _ int) ([]domain.Event, error) {
	return f.events[clusterKey(namespace, name)], nil
}

// BuildClusterBase and Apply let the read fake also satisfy the write backend
// NewServer takes. The read-path tests run with AllowMutations=false, so the
// write tools are never registered and these stubs are never called; they exist
// only so connectCluster can pass one fake for both slices. The write path has its
// own backend fake (fakeWriteBackend, cluster_write_test.go).
func (f *fakeKubeRay) BuildClusterBase(_ domain.ClusterCreateParams) (domain.MergedSpec, error) {
	return nil, errors.New("fakeKubeRay.BuildClusterBase not used in read-path tests")
}

func (f *fakeKubeRay) Apply(_ context.Context, _ domain.Kind, _, _ string, _ domain.MergedSpec, _ domain.ApplyOptions) (domain.MergedSpec, error) {
	return nil, errors.New("fakeKubeRay.Apply not used in read-path tests")
}

func (f *fakeKubeRay) Delete(_ context.Context, _ domain.Kind, _, _ string, _ bool) error {
	return errors.New("fakeKubeRay.Delete not used in read-path tests")
}

// connectCluster wires a server (built from cfg + src + the kube fake) to an
// in-memory client session.
func connectCluster(t *testing.T, cfg *config.Config, kube domain.ClusterReader) *mcp.ClientSession {
	t.Helper()
	ctx := context.Background()

	// Read-path tests run with mutations off, so the write backend is never
	// invoked; a bare fake + nop audit satisfy NewServer's signature.
	server := mcpserver.NewServer(cfg, fakeSource{contextName: "ctx", defaultNamespace: cfg.DefaultNamespace}, kube, &fakeKubeRay{}, mcpserver.WedgeBackend{}, domain.NopAuditSink{})
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

// TestListToolsShowsClusterToolsWithReadOnlyHint asserts both cluster tools are
// registered and annotated read-only.
func TestListToolsShowsClusterToolsWithReadOnlyHint(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "default"}
	session := connectCluster(t, cfg, &fakeKubeRay{})

	res, err := session.ListTools(context.Background(), &mcp.ListToolsParams{})
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}

	byName := map[string]*mcp.Tool{}
	for _, tool := range res.Tools {
		byName[tool.Name] = tool
	}

	for _, name := range []string{"ray_cluster_list", "ray_cluster_get", "ray_cluster_events"} {
		tool, ok := byName[name]
		if !ok {
			t.Fatalf("%s not registered; got tools %v", name, res.Tools)
		}
		if tool.Annotations == nil || !tool.Annotations.ReadOnlyHint {
			t.Errorf("%s missing readOnlyHint annotation: %+v", name, tool.Annotations)
		}
	}
}

// TestClusterListStructuredRowsAndContinue asserts ray_cluster_list returns
// compact structured rows plus the "more available" continue indicator.
func TestClusterListStructuredRowsAndContinue(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "demo"}
	kube := &fakeKubeRay{
		clusters: map[string]domain.ClusterDetail{
			clusterKey("demo", "alpha"): {ClusterSummary: domain.ClusterSummary{
				Name: "alpha", Namespace: "demo", Phase: "Ready", ReadyReplicas: 2, DesiredReplicas: 2, Health: "Ready; 2/2 workers ready",
			}},
		},
		continueTo: "page-2-token",
	}
	session := connectCluster(t, cfg, kube)

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "ray_cluster_list",
		Arguments: map[string]any{},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if res.IsError {
		t.Fatalf("tool reported error: %+v", res)
	}

	sc, ok := res.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("StructuredContent is %T, want map[string]any", res.StructuredContent)
	}
	if got := sc["count"]; got != float64(1) {
		t.Errorf("count = %v, want 1", got)
	}
	if got := sc["moreAvailable"]; got != true {
		t.Errorf("moreAvailable = %v, want true", got)
	}
	if got := sc["continue"]; got != "page-2-token" {
		t.Errorf("continue = %v, want page-2-token", got)
	}
	clusters, ok := sc["clusters"].([]any)
	if !ok || len(clusters) != 1 {
		t.Fatalf("clusters = %v, want one row", sc["clusters"])
	}
	row, ok := clusters[0].(map[string]any)
	if !ok {
		t.Fatalf("row is %T, want map[string]any", clusters[0])
	}
	if row["name"] != "alpha" || row["phase"] != "Ready" {
		t.Errorf("row = %v, want alpha/Ready", row)
	}

	if textContent(res) == "" {
		t.Error("no text summary in result content")
	}
}

// TestClusterGetDistilledByDefault asserts the default get carries no raw object.
func TestClusterGetDistilledByDefault(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "demo"}
	kube := &fakeKubeRay{clusters: map[string]domain.ClusterDetail{
		clusterKey("demo", "alpha"): {
			ClusterSummary:  domain.ClusterSummary{Name: "alpha", Namespace: "demo", Phase: "Ready", Health: "Ready; 2/2 workers ready"},
			HeadServiceName: "alpha-head-svc",
			DashboardURL:    "http://alpha-head-svc.demo.svc:8265",
			Raw:             domain.MergedSpec{"kind": "RayCluster"},
		},
	}}
	session := connectCluster(t, cfg, kube)

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "ray_cluster_get",
		Arguments: map[string]any{"name": "alpha"},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if res.IsError {
		t.Fatalf("tool reported error: %+v", res)
	}

	sc := res.StructuredContent.(map[string]any)
	if _, present := sc["raw"]; present {
		t.Errorf("raw present in distilled get: %v", sc["raw"])
	}
	if sc["headServiceName"] != "alpha-head-svc" {
		t.Errorf("headServiceName = %v, want alpha-head-svc", sc["headServiceName"])
	}
	if sc["dashboardURL"] != "http://alpha-head-svc.demo.svc:8265" {
		t.Errorf("dashboardURL = %v, want the distilled URL", sc["dashboardURL"])
	}
}

// TestClusterGetVerboseIncludesRaw asserts verbose returns the full object.
func TestClusterGetVerboseIncludesRaw(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "demo"}
	kube := &fakeKubeRay{clusters: map[string]domain.ClusterDetail{
		clusterKey("demo", "alpha"): {
			ClusterSummary: domain.ClusterSummary{Name: "alpha", Namespace: "demo", Phase: "Ready"},
			Raw:            domain.MergedSpec{"kind": "RayCluster", "apiVersion": "ray.io/v1"},
		},
	}}
	session := connectCluster(t, cfg, kube)

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "ray_cluster_get",
		Arguments: map[string]any{"name": "alpha", "verbose": true},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if res.IsError {
		t.Fatalf("tool reported error: %+v", res)
	}

	sc := res.StructuredContent.(map[string]any)
	raw, ok := sc["raw"].(map[string]any)
	if !ok {
		t.Fatalf("raw is %T, want map[string]any under verbose", sc["raw"])
	}
	if raw["kind"] != "RayCluster" {
		t.Errorf("raw[kind] = %v, want RayCluster", raw["kind"])
	}
}

// TestClusterGetMissingNameValidationError asserts an omitted name yields a tool
// error (not a panic / not a successful empty result).
func TestClusterGetMissingNameValidationError(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "demo"}
	session := connectCluster(t, cfg, &fakeKubeRay{})

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "ray_cluster_get",
		Arguments: map[string]any{},
	})
	if err != nil {
		t.Fatalf("CallTool transport error: %v", err)
	}
	if !res.IsError {
		t.Fatalf("missing name did not produce a tool error: %+v", res)
	}
}

// TestClusterGetNotFoundCleanError asserts a missing cluster maps to a clean,
// bounded tool error mentioning the name + namespace.
func TestClusterGetNotFoundCleanError(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "demo"}
	session := connectCluster(t, cfg, &fakeKubeRay{})

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "ray_cluster_get",
		Arguments: map[string]any{"name": "ghost"},
	})
	if err != nil {
		t.Fatalf("CallTool transport error: %v", err)
	}
	if !res.IsError {
		t.Fatalf("not-found did not produce a tool error: %+v", res)
	}
	msg := textContent(res)
	if !containsAll(msg, "ghost", "demo") {
		t.Errorf("not-found message %q does not name the cluster + namespace", msg)
	}
}

// TestClusterEventsStructuredAndText asserts ray_cluster_events returns bounded
// structured rows (type/reason/message/count/age) plus a text summary that names
// the count and warning count.
func TestClusterEventsStructuredAndText(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "demo"}
	kube := &fakeKubeRay{events: map[string][]domain.Event{
		clusterKey("demo", "alpha"): {
			{Type: "Warning", Reason: "FailedScheduling", Message: "0/1 nodes: insufficient nvidia.com/gpu", Count: 3, LastSeen: time.Now()},
			{Type: "Normal", Reason: "Scheduled", Message: "pod scheduled", Count: 1, LastSeen: time.Now().Add(-time.Minute)},
		},
	}}
	session := connectCluster(t, cfg, kube)

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "ray_cluster_events",
		Arguments: map[string]any{"name": "alpha"},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if res.IsError {
		t.Fatalf("tool reported error: %+v", res)
	}

	sc, ok := res.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("StructuredContent is %T, want map[string]any", res.StructuredContent)
	}
	if got := sc["count"]; got != float64(2) {
		t.Errorf("count = %v, want 2", got)
	}
	if got := sc["warnings"]; got != float64(1) {
		t.Errorf("warnings = %v, want 1", got)
	}
	events, ok := sc["events"].([]any)
	if !ok || len(events) != 2 {
		t.Fatalf("events = %v, want two rows", sc["events"])
	}
	row, ok := events[0].(map[string]any)
	if !ok {
		t.Fatalf("row is %T, want map[string]any", events[0])
	}
	if row["reason"] != "FailedScheduling" || row["type"] != "Warning" {
		t.Errorf("row = %v, want the FailedScheduling Warning", row)
	}
	if row["count"] != float64(3) {
		t.Errorf("row count = %v, want 3", row["count"])
	}

	msg := textContent(res)
	if !containsAll(msg, "alpha", "demo", "2", "1 warning") {
		t.Errorf("text summary %q does not name count/namespace/warnings", msg)
	}
}

// TestClusterEventsEmptyDoesNotReadHealthy asserts that with no events the text
// summary explicitly says k8s expires events — an empty list must NOT read as a
// clean bill of health.
func TestClusterEventsEmptyDoesNotReadHealthy(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "demo"}
	session := connectCluster(t, cfg, &fakeKubeRay{})

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "ray_cluster_events",
		Arguments: map[string]any{"name": "quiet"},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if res.IsError {
		t.Fatalf("tool reported error: %+v", res)
	}

	sc := res.StructuredContent.(map[string]any)
	if got := sc["count"]; got != float64(0) {
		t.Errorf("count = %v, want 0", got)
	}
	msg := textContent(res)
	if !containsAll(msg, "no recent events", "quiet", "expires") {
		t.Errorf("empty summary %q should warn that absence is not health", msg)
	}
}

// TestClusterEventsMissingNameValidationError asserts an omitted name yields a
// tool error (not a panic / not a successful empty result).
func TestClusterEventsMissingNameValidationError(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "demo"}
	session := connectCluster(t, cfg, &fakeKubeRay{})

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "ray_cluster_events",
		Arguments: map[string]any{},
	})
	if err != nil {
		t.Fatalf("CallTool transport error: %v", err)
	}
	if !res.IsError {
		t.Fatalf("missing name did not produce a tool error: %+v", res)
	}
}

// textContent returns the first TextContent text in a result, or "".
func textContent(res *mcp.CallToolResult) string {
	for _, c := range res.Content {
		if tc, ok := c.(*mcp.TextContent); ok {
			return tc.Text
		}
	}
	return ""
}

// containsAll reports whether s contains every substring.
func containsAll(s string, subs ...string) bool {
	for _, sub := range subs {
		if !strings.Contains(s, sub) {
			return false
		}
	}
	return true
}
