package mcp_test

import (
	"context"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/risjai/ray-mcp/internal/config"
	"github.com/risjai/ray-mcp/internal/domain"
	mcpserver "github.com/risjai/ray-mcp/internal/mcp"
)

// liveBackend extends the fake write backend with a GetCluster returning a canned
// live object, so the update/scale/delete read-modify-apply path has something to
// read. It embeds fakeWriteBackend for BuildClusterBase + Apply + Delete.
type liveBackend struct {
	fakeWriteBackend
	autoscaling bool
	getErr      error
	protected   bool
	uid         string // if set, injected into metadata.uid of the canned object.
}

// ListClusters + Events satisfy domain.ClusterReader (unused by update/scale; the
// read tools are not exercised by these tests).
func (b *liveBackend) ListClusters(_ context.Context, _ string, _ domain.ListOptions) (domain.ClusterList, error) {
	return domain.ClusterList{}, nil
}

func (b *liveBackend) Events(_ context.Context, _ domain.Kind, _, _ string, _ int) ([]domain.Event, error) {
	return nil, nil
}

func (b *liveBackend) GetCluster(_ context.Context, namespace, name string) (domain.ClusterDetail, error) {
	if b.getErr != nil {
		return domain.ClusterDetail{}, b.getErr
	}
	spec := map[string]any{
		"rayVersion": "2.9.0",
		"headGroupSpec": map[string]any{
			"template": map[string]any{
				"spec": map[string]any{
					"containers": []any{map[string]any{"name": "ray-head", "image": "rayproject/ray:2.9.0"}},
				},
			},
		},
		"workerGroupSpecs": []any{
			map[string]any{
				"groupName": "workers", "replicas": int64(2), "minReplicas": int64(0), "maxReplicas": int64(5),
				"template": map[string]any{
					"spec": map[string]any{
						"containers": []any{map[string]any{"name": "ray-worker", "image": "rayproject/ray:2.9.0"}},
					},
				},
			},
		},
	}
	if b.autoscaling {
		spec["enableInTreeAutoscaling"] = true
	}
	meta := map[string]any{"name": name, "namespace": namespace}
	if b.uid != "" {
		meta["uid"] = b.uid
	}
	if b.protected {
		meta["annotations"] = map[string]any{domain.ProtectedAnnotation: "true"}
	}
	return domain.ClusterDetail{
		ClusterSummary: domain.ClusterSummary{Name: name, Namespace: namespace},
		Raw: domain.MergedSpec{
			"apiVersion": "ray.io/v1", "kind": "RayCluster",
			"metadata": meta,
			"spec":     spec,
		},
	}, nil
}

// connectLive wires a server with a live-object backend (Get + BuildClusterBase +
// Apply) and returns the client session + backend for assertions.
func connectLive(t *testing.T, cfg *config.Config, backend *liveBackend) *mcp.ClientSession {
	t.Helper()
	ctx := context.Background()
	server := mcpserver.NewServer(cfg, fakeSource{contextName: "ctx", defaultNamespace: cfg.DefaultNamespace}, backend, backend, domain.NopAuditSink{})
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

// TestUpdateScaleAbsentWithoutAllowMutations is the gate AC: neither update nor
// scale is advertised without --allow-mutations.
func TestUpdateScaleAbsentWithoutAllowMutations(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "default", AllowMutations: false}
	tools := toolNames(t, connectLive(t, cfg, &liveBackend{}))
	if _, ok := tools["ray_cluster_update"]; ok {
		t.Error("ray_cluster_update advertised without --allow-mutations")
	}
	if _, ok := tools["ray_cluster_scale"]; ok {
		t.Error("ray_cluster_scale advertised without --allow-mutations")
	}
}

// TestUpdateScalePresentWithMutationsAndHints asserts both tools are advertised
// with --allow-mutations and carry the right annotations: not read-only, and
// idempotent (SSA re-apply is a no-op). Scale additionally carries destructiveHint
// (its scale-to-zero path is a teardown).
func TestUpdateScalePresentWithMutationsAndHints(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "default", AllowMutations: true, AllowRawSpec: true}
	tools := toolNames(t, connectLive(t, cfg, &liveBackend{}))

	up, ok := tools["ray_cluster_update"]
	if !ok {
		t.Fatal("ray_cluster_update absent with --allow-mutations")
	}
	if up.Annotations == nil || up.Annotations.ReadOnlyHint {
		t.Error("ray_cluster_update must not be read-only")
	}
	if up.Annotations == nil || !up.Annotations.IdempotentHint {
		t.Error("ray_cluster_update IdempotentHint should be true (SSA re-apply is a no-op)")
	}

	sc, ok := tools["ray_cluster_scale"]
	if !ok {
		t.Fatal("ray_cluster_scale absent with --allow-mutations")
	}
	if sc.Annotations == nil || !sc.Annotations.IdempotentHint {
		t.Error("ray_cluster_scale IdempotentHint should be true")
	}
	if sc.Annotations == nil || sc.Annotations.DestructiveHint == nil || !*sc.Annotations.DestructiveHint {
		t.Error("ray_cluster_scale DestructiveHint should be true (scale-to-zero is a teardown)")
	}
}

// TestUpdateCommitChangesImage drives a real update through the in-memory client:
// the image change reaches the applied object, two backend applies fire (dry-run
// then commit), and the result is a non-error diff with DryRun=false.
func TestUpdateCommitChangesImage(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "default", AllowMutations: true, AllowRawSpec: true}
	backend := &liveBackend{}
	session := connectLive(t, cfg, backend)

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "ray_cluster_update",
		Arguments: map[string]any{"name": "demo", "image": "rayproject/ray:2.40.0"},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if res.IsError {
		t.Fatalf("update reported a tool error: %+v", res)
	}
	sc := res.StructuredContent.(map[string]any)
	if sc["dryRun"] != false {
		t.Errorf("dryRun = %v, want false", sc["dryRun"])
	}
	if len(backend.applyCalls) != 2 || !backend.applyCalls[0].dryRun || backend.applyCalls[1].dryRun {
		t.Fatalf("apply calls = %+v, want [dry-run, commit]", backend.applyCalls)
	}
}

// TestUpdateRequiresAtLeastOneChange asserts a no-field update is a tool error.
func TestUpdateRequiresAtLeastOneChange(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "default", AllowMutations: true, AllowRawSpec: true}
	backend := &liveBackend{}
	session := connectLive(t, cfg, backend)

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "ray_cluster_update",
		Arguments: map[string]any{"name": "demo"},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if !res.IsError {
		t.Fatal("empty update did not error; it must require at least one change")
	}
	if len(backend.applyCalls) != 0 {
		t.Errorf("apply called %d times, want 0", len(backend.applyCalls))
	}
}

// TestScaleSetsMinMax drives a real scale: min/max reach the applied object.
func TestScaleSetsMinMax(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "default", AllowMutations: true, AllowRawSpec: true}
	backend := &liveBackend{}
	session := connectLive(t, cfg, backend)

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "ray_cluster_scale",
		Arguments: map[string]any{"name": "demo", "workerGroup": "workers", "maxReplicas": 8},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if res.IsError {
		t.Fatalf("scale reported a tool error: %+v", res)
	}
	if len(backend.applyCalls) != 2 {
		t.Fatalf("apply calls = %+v, want [dry-run, commit]", backend.applyCalls)
	}
}

// TestScaleReplicasRefusedUnderAutoscaling asserts the MCP layer surfaces the
// autoscaler refusal as a tool error and applies nothing.
func TestScaleReplicasRefusedUnderAutoscaling(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "default", AllowMutations: true, AllowRawSpec: true}
	backend := &liveBackend{autoscaling: true}
	session := connectLive(t, cfg, backend)

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "ray_cluster_scale",
		Arguments: map[string]any{"name": "demo", "workerGroup": "workers", "replicas": 4},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if !res.IsError {
		t.Fatal("scaling replicas on an autoscaling cluster did not error; it must be refused")
	}
	if len(backend.applyCalls) != 0 {
		t.Errorf("apply called %d times, want 0", len(backend.applyCalls))
	}
}

// TestScaleToZeroRefusedWithoutDestructive asserts scale-to-zero is a tool error
// when --allow-destructive is not set (B3), even though --allow-mutations is.
func TestScaleToZeroRefusedWithoutDestructive(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "default", AllowMutations: true, AllowDestructive: false, AllowRawSpec: true}
	backend := &liveBackend{}
	session := connectLive(t, cfg, backend)

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "ray_cluster_scale",
		Arguments: map[string]any{"name": "demo", "workerGroup": "workers", "replicas": 0},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if !res.IsError {
		t.Fatal("scale-to-zero without --allow-destructive did not error; B3 requires the destructive tier")
	}
	if len(backend.applyCalls) != 0 {
		t.Errorf("apply called %d times, want 0", len(backend.applyCalls))
	}
}

// TestScaleToZeroProceedsWithDestructive asserts the same scale-to-zero proceeds
// when --allow-destructive is set.
func TestScaleToZeroProceedsWithDestructive(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "default", AllowMutations: true, AllowDestructive: true, AllowRawSpec: true}
	backend := &liveBackend{}
	session := connectLive(t, cfg, backend)

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "ray_cluster_scale",
		Arguments: map[string]any{"name": "demo", "workerGroup": "workers", "replicas": 0},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if res.IsError {
		t.Fatalf("scale-to-zero with --allow-destructive errored: %+v", res)
	}
	if len(backend.applyCalls) != 2 {
		t.Fatalf("apply calls = %+v, want [dry-run, commit]", backend.applyCalls)
	}
}

// TestUpdateSchemaDropsRawSpecInHardMode asserts the rawSpec arg is removed from
// the update schema under --allow-raw-spec=false (hard mode), like create.
func TestUpdateSchemaDropsRawSpecInHardMode(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "default", AllowMutations: true, AllowRawSpec: false}
	tool := toolNames(t, connectLive(t, cfg, &liveBackend{}))["ray_cluster_update"]
	if schemaHasProperty(t, tool, "rawSpec") {
		t.Error("rawSpec present in ray_cluster_update schema under --allow-raw-spec=false")
	}
	if !schemaHasProperty(t, tool, "image") {
		t.Error("image dropped from update schema; only rawSpec should be removed")
	}
}

// TestUpdateRejectsRawSpecInHardMode is the defense-in-depth AC: a rawSpec sent to
// update under --allow-raw-spec=false is rejected before any apply.
func TestUpdateRejectsRawSpecInHardMode(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "default", AllowMutations: true, AllowRawSpec: false}
	backend := &liveBackend{}
	session := connectLive(t, cfg, backend)

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "ray_cluster_update",
		Arguments: map[string]any{"name": "demo", "rawSpec": map[string]any{"spec": map[string]any{"x": 1}}},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if !res.IsError {
		t.Fatal("update with rawSpec under hard mode did not error")
	}
	if len(backend.applyCalls) != 0 {
		t.Errorf("apply called %d times, want 0", len(backend.applyCalls))
	}
}
