package mcp_test

import (
	"context"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/risjai/ray-mcp/internal/config"
	"github.com/risjai/ray-mcp/internal/domain"
	mcpserver "github.com/risjai/ray-mcp/internal/mcp"
)

// connectWriteWithRead is like connectWrite but accepts a custom read backend so
// tests can seed live services/clusters for update paths (the ServiceGetter type
// assertion in server.go uses the read backend).
func connectWriteWithRead(t *testing.T, cfg *config.Config, backend *fakeWriteBackend, kube domain.ClusterReader) *mcp.ClientSession {
	t.Helper()
	ctx := context.Background()

	server := mcpserver.NewServer(cfg, fakeSource{contextName: "ctx", defaultNamespace: cfg.DefaultNamespace}, kube, backend, mcpserver.WedgeBackend{}, domain.NopAuditSink{})
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

// TestServiceDeployToolAbsentWithoutAllowMutations is the gate AC:
// ray_service_deploy must NOT be advertised unless --allow-mutations is set.
func TestServiceDeployToolAbsentWithoutAllowMutations(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "default", AllowMutations: false}
	session := connectWrite(t, cfg, &fakeWriteBackend{})

	for _, name := range []string{"ray_service_deploy", "ray_service_update"} {
		if _, ok := toolNames(t, session)[name]; ok {
			t.Errorf("%s is advertised without --allow-mutations; it must be absent", name)
		}
	}
}

// TestServiceDeployToolPresentWithAllowMutations asserts both service write tools
// are advertised when --allow-mutations is set.
func TestServiceDeployToolPresentWithAllowMutations(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "default", AllowMutations: true, AllowRawSpec: true}
	session := connectWrite(t, cfg, &fakeWriteBackend{})

	for _, name := range []string{"ray_service_deploy", "ray_service_update"} {
		tool, ok := toolNames(t, session)[name]
		if !ok {
			t.Fatalf("%s absent with --allow-mutations; it must be advertised", name)
		}
		if tool.Annotations != nil && tool.Annotations.ReadOnlyHint {
			t.Errorf("%s is marked read-only; it mutates", name)
		}
	}
}

// TestServiceDeploySchemaDropsRawSpecInHardMode asserts the rawSpec arg is removed
// from the advertised schema when --allow-raw-spec=false.
func TestServiceDeploySchemaDropsRawSpecInHardMode(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "default", AllowMutations: true, AllowRawSpec: false}
	session := connectWrite(t, cfg, &fakeWriteBackend{})

	for _, name := range []string{"ray_service_deploy", "ray_service_update"} {
		tool := toolNames(t, session)[name]
		if schemaHasProperty(t, tool, "rawSpec") {
			t.Errorf("%s: rawSpec present in the schema with --allow-raw-spec=false; hard mode must remove it", name)
		}
		if !schemaHasProperty(t, tool, "name") {
			t.Errorf("%s: 'name' dropped from schema; only rawSpec should be removed in hard mode", name)
		}
	}
}

// TestServiceDeployRejectsRawSpecInHardMode asserts the tool rejects rawSpec even
// if a client sends it despite the pruned schema.
func TestServiceDeployRejectsRawSpecInHardMode(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "default", AllowMutations: true, AllowRawSpec: false}
	backend := &fakeWriteBackend{}
	session := connectWrite(t, cfg, backend)

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name: "ray_service_deploy",
		Arguments: map[string]any{
			"name":    "svc1",
			"rawSpec": map[string]any{"spec": map[string]any{"x": 1}},
		},
	})
	if err != nil {
		t.Fatalf("CallTool transport error: %v", err)
	}
	if !res.IsError {
		t.Fatal("deploy with rawSpec under --allow-raw-spec=false did not error; hard mode must reject it")
	}
	if len(backend.applyCalls) != 0 {
		t.Errorf("backend was called %d times, want 0 (rejected before apply)", len(backend.applyCalls))
	}
}

// TestServiceUpdateRejectsRawSpecInHardMode asserts ray_service_update rejects
// rawSpec even if a client sends it despite the pruned schema (defense in depth at
// the MCP edge, mirroring the deploy case).
func TestServiceUpdateRejectsRawSpecInHardMode(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "default", AllowMutations: true, AllowRawSpec: false}
	backend := &fakeWriteBackend{}
	session := connectWrite(t, cfg, backend)

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name: "ray_service_update",
		Arguments: map[string]any{
			"name":    "svc1",
			"rawSpec": map[string]any{"spec": map[string]any{"x": 1}},
		},
	})
	if err != nil {
		t.Fatalf("CallTool transport error: %v", err)
	}
	if !res.IsError {
		t.Fatal("update with rawSpec under --allow-raw-spec=false did not error; hard mode must reject it")
	}
	if len(backend.applyCalls) != 0 {
		t.Errorf("backend was called %d times, want 0 (rejected before apply)", len(backend.applyCalls))
	}
}

// TestServiceDeployCommitEndToEnd drives a full deploy through the in-memory client.
func TestServiceDeployCommitEndToEnd(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "default", AllowMutations: true, AllowRawSpec: true}
	backend := &fakeWriteBackend{serverExtra: map[string]any{"suspend": false}}
	session := connectWrite(t, cfg, backend)

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name: "ray_service_deploy",
		Arguments: map[string]any{
			"name":          "svc1",
			"rayVersion":    "2.9.0",
			"serveConfigV2": "import: serve_config",
		},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if res.IsError {
		t.Fatalf("deploy reported a tool error: %+v", res)
	}

	sc, ok := res.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("StructuredContent is %T, want map[string]any", res.StructuredContent)
	}
	if sc["dryRun"] != false {
		t.Errorf("dryRun = %v, want false for a commit", sc["dryRun"])
	}
	if sc["namespace"] != "default" {
		t.Errorf("namespace = %v, want the resolved default", sc["namespace"])
	}
	if sc["name"] != "svc1" {
		t.Errorf("name = %v, want svc1", sc["name"])
	}
	// Two backend applies: dry-run then commit.
	if len(backend.applyCalls) != 2 || !backend.applyCalls[0].dryRun || backend.applyCalls[1].dryRun {
		t.Fatalf("backend apply calls = %+v, want [dry-run, commit]", backend.applyCalls)
	}
	assertHasText(t, res)
}

// TestServiceDeployRequiresName asserts an empty name yields a tool error.
func TestServiceDeployRequiresName(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "default", AllowMutations: true, AllowRawSpec: true}
	session := connectWrite(t, cfg, &fakeWriteBackend{})

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "ray_service_deploy",
		Arguments: map[string]any{},
	})
	if err != nil {
		t.Fatalf("CallTool transport error: %v", err)
	}
	if !res.IsError {
		t.Fatal("deploy with empty name did not produce a tool error")
	}
}

// TestServiceUpdateRequiresNameMCP asserts an empty name yields a tool error.
func TestServiceUpdateRequiresNameMCP(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "default", AllowMutations: true, AllowRawSpec: true}
	session := connectWrite(t, cfg, &fakeWriteBackend{})

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "ray_service_update",
		Arguments: map[string]any{"serveConfigV2": "new"},
	})
	if err != nil {
		t.Fatalf("CallTool transport error: %v", err)
	}
	if !res.IsError {
		t.Fatal("update with empty name did not produce a tool error")
	}
}

// TestServiceUpdateNotFoundMCP asserts a not-found cluster maps to a clean tool error.
func TestServiceUpdateNotFoundMCP(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "default", AllowMutations: true, AllowRawSpec: true}
	// The fakeWriteBackend's GetService will return a NotFoundError since the service
	// is not seeded. We need to wire the read path with the service getter from the
	// fakeKubeRay (which backs the GetService interface through the type assertion in
	// server.go). The connectWrite helper uses a separate fakeKubeRay for the read
	// backend (kube arg). Let's seed it with NO services so GetService returns NotFound.
	session := connectWrite(t, cfg, &fakeWriteBackend{})

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "ray_service_update",
		Arguments: map[string]any{"name": "missing", "serveConfigV2": "x"},
	})
	if err != nil {
		t.Fatalf("CallTool transport error: %v", err)
	}
	if !res.IsError {
		t.Fatal("update on a missing service did not produce a tool error")
	}
	msg := textContent(res)
	if !containsAll(msg, "missing", "not found") {
		t.Errorf("not-found message %q does not mention the name", msg)
	}
}

// TestServiceUpdateReturnsPredicatedPath asserts the update output carries the
// predictedPath field in the structured content.
func TestServiceUpdateReturnsPredicatedPath(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "default", AllowMutations: true, AllowRawSpec: true}
	// Seed the read backend with a live service that has a known spec. We need the
	// connectWrite helper's fakeKubeRay to satisfy ServiceGetter. Let's use a
	// fakeWriteBackend that also seeds a service for GetService.
	backend := &fakeWriteBackend{}
	// Inject a live service into the read-backend fakeKubeRay used by connectWrite.
	// To do this we need the read-side fake to have the service. Looking at connectWrite,
	// it uses a separate empty &fakeKubeRay{} for the read backend. But the service
	// update tool calls GetService on the read backend (the type-asserted kube).
	// Actually: looking at server.go, the ServiceGetter type assertion is on kube
	// (the read backend), not the write backend. So we need to customize the test
	// to seed the read backend. Let's use a custom connect approach.
	_ = backend
	// Use a direct connect with a seeded read fake:
	kube := &fakeKubeRay{
		services: map[string]domain.ServiceDetail{
			"default/svc1": {
				ServiceSummary: domain.ServiceSummary{Name: "svc1", Namespace: "default"},
				Raw: domain.MergedSpec{
					"apiVersion": "ray.io/v1",
					"kind":       "RayService",
					"metadata":   map[string]any{"name": "svc1", "namespace": "default"},
					"spec": map[string]any{
						"serveConfigV2": "old",
						"rayClusterConfig": map[string]any{
							"rayVersion": "2.9.0",
							"headGroupSpec": map[string]any{
								"template": map[string]any{
									"spec": map[string]any{
										"containers": []any{
											map[string]any{"name": "ray-head", "image": "rayproject/ray:2.9.0"},
										},
									},
								},
							},
							"workerGroupSpecs": []any{
								map[string]any{
									"groupName": "workers",
									"replicas":  int64(2),
									"template": map[string]any{
										"spec": map[string]any{
											"containers": []any{
												map[string]any{"name": "ray-worker", "image": "rayproject/ray:2.9.0"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
	session := connectWriteWithRead(t, cfg, &fakeWriteBackend{}, kube)

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "ray_service_update",
		Arguments: map[string]any{"name": "svc1", "serveConfigV2": "new-config"},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if res.IsError {
		t.Fatalf("update reported a tool error: %s", textContent(res))
	}

	sc, ok := res.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("StructuredContent is %T, want map[string]any", res.StructuredContent)
	}
	pp, _ := sc["predictedPath"].(string)
	if pp != "in-place" {
		t.Errorf("predictedPath = %q, want in-place", pp)
	}
	assertHasText(t, res)
}
