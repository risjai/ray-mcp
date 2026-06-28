package mcp_test

import (
	"context"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/risjai/ray-mcp/internal/config"
	"github.com/risjai/ray-mcp/internal/domain"
)

// The RayService read tools (ray_service_list / ray_service_get) mirror the
// cluster read tools: compact list rows + honest pagination, a distilled get with
// the verbose Raw escape hatch, name-required validation, and a clean NotFound
// tool error. They share the read-backend fake (fakeKubeRay) via connectCluster,
// which also satisfies domain.ServiceReader.

func serviceKey(ns, name string) string { return ns + "/" + name }

// TestListToolsShowsServiceToolsWithReadOnlyHint asserts both service tools are
// registered and annotated read-only.
func TestListToolsShowsServiceToolsWithReadOnlyHint(t *testing.T) {
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

	for _, name := range []string{"ray_service_list", "ray_service_get"} {
		tool, ok := byName[name]
		if !ok {
			t.Fatalf("%s not registered; got tools %v", name, res.Tools)
		}
		if tool.Annotations == nil || !tool.Annotations.ReadOnlyHint {
			t.Errorf("%s missing readOnlyHint annotation: %+v", name, tool.Annotations)
		}
	}
}

// TestServiceListStructuredRowsAndContinue asserts ray_service_list returns
// compact structured rows (serve status + healthy replicas) plus the "more
// available" continue indicator.
func TestServiceListStructuredRowsAndContinue(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "demo"}
	kube := &fakeKubeRay{
		services: map[string]domain.ServiceDetail{
			serviceKey("demo", "fraud"): {ServiceSummary: domain.ServiceSummary{
				Name: "fraud", Namespace: "demo", ServiceStatus: "Running", HealthyReplicas: 3, Health: "Running; 3 serve endpoints",
			}},
		},
		serviceContinueTo: "page-2-token",
	}
	session := connectCluster(t, cfg, kube)

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "ray_service_list",
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
	services, ok := sc["services"].([]any)
	if !ok || len(services) != 1 {
		t.Fatalf("services = %v, want one row", sc["services"])
	}
	row, ok := services[0].(map[string]any)
	if !ok {
		t.Fatalf("row is %T, want map[string]any", services[0])
	}
	if row["name"] != "fraud" || row["serviceStatus"] != "Running" {
		t.Errorf("row = %v, want fraud/Running", row)
	}
	if row["healthyReplicas"] != float64(3) {
		t.Errorf("row healthyReplicas = %v, want 3", row["healthyReplicas"])
	}

	if textContent(res) == "" {
		t.Error("no text summary in result content")
	}
}

// TestServiceGetDistilledByDefault asserts the default get carries the rollout
// phase but no raw object.
func TestServiceGetDistilledByDefault(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "demo"}
	kube := &fakeKubeRay{services: map[string]domain.ServiceDetail{
		serviceKey("demo", "fraud"): {
			ServiceSummary: domain.ServiceSummary{Name: "fraud", Namespace: "demo", ServiceStatus: "Running", HealthyReplicas: 3, Health: "Running; 3 serve endpoints"},
			RolloutPhase:   "Running",
			Raw:            domain.MergedSpec{"kind": "RayService"},
		},
	}}
	session := connectCluster(t, cfg, kube)

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "ray_service_get",
		Arguments: map[string]any{"name": "fraud"},
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
	if sc["rolloutPhase"] != "Running" {
		t.Errorf("rolloutPhase = %v, want Running", sc["rolloutPhase"])
	}
	if sc["serviceStatus"] != "Running" {
		t.Errorf("serviceStatus = %v, want Running", sc["serviceStatus"])
	}
}

// TestServiceGetVerboseIncludesRaw asserts verbose returns the full object.
func TestServiceGetVerboseIncludesRaw(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "demo"}
	kube := &fakeKubeRay{services: map[string]domain.ServiceDetail{
		serviceKey("demo", "fraud"): {
			ServiceSummary: domain.ServiceSummary{Name: "fraud", Namespace: "demo", ServiceStatus: "Running"},
			RolloutPhase:   "Running",
			Raw:            domain.MergedSpec{"kind": "RayService", "apiVersion": "ray.io/v1"},
		},
	}}
	session := connectCluster(t, cfg, kube)

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "ray_service_get",
		Arguments: map[string]any{"name": "fraud", "verbose": true},
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
	if raw["kind"] != "RayService" {
		t.Errorf("raw[kind] = %v, want RayService", raw["kind"])
	}
}

// TestServiceGetMissingNameValidationError asserts an omitted name yields a tool
// error (not a panic / not a successful empty result).
func TestServiceGetMissingNameValidationError(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "demo"}
	session := connectCluster(t, cfg, &fakeKubeRay{})

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "ray_service_get",
		Arguments: map[string]any{},
	})
	if err != nil {
		t.Fatalf("CallTool transport error: %v", err)
	}
	if !res.IsError {
		t.Fatalf("missing name did not produce a tool error: %+v", res)
	}
}

// TestServiceGetNotFoundCleanError asserts a missing service maps to a clean,
// bounded tool error mentioning the name + namespace.
func TestServiceGetNotFoundCleanError(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "demo"}
	session := connectCluster(t, cfg, &fakeKubeRay{})

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "ray_service_get",
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
		t.Errorf("not-found message %q does not name the service + namespace", msg)
	}
}
