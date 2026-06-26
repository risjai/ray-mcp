package mcp_test

import (
	"context"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/risjai/ray-mcp/internal/config"
	"github.com/risjai/ray-mcp/internal/domain"
)

// The MCP edge for ray_job_submit mirrors ray_cluster_create's write-path harness
// (connectWrite + fakeWriteBackend): a real in-memory client drives the tool, and
// the fake write backend records the dry-run/commit apply sequence and echoes a
// server view so the diff + non-blocking status surface end-to-end. The domain
// service's XOR/shutdown policy is unit-tested in domain/job_submit_test.go; here
// we prove the tool is gated, schema-pruned, and maps args→params→output faithfully.

// TestJobSubmitToolAbsentWithoutAllowMutations is the gate AC: ray_job_submit must
// NOT be advertised unless --allow-mutations is set (spec §6).
func TestJobSubmitToolAbsentWithoutAllowMutations(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "default", AllowMutations: false}
	session := connectWrite(t, cfg, &fakeWriteBackend{})

	if _, ok := toolNames(t, session)["ray_job_submit"]; ok {
		t.Error("ray_job_submit is advertised without --allow-mutations; it must be absent")
	}
}

// TestJobSubmitToolPresentWithAllowMutations: with --allow-mutations the tool is
// advertised, NOT read-only (it mutates), and NOT idempotent (re-submitting the
// same name is an already-exists error, not a safe no-op — mirrors create).
func TestJobSubmitToolPresentWithAllowMutations(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "default", AllowMutations: true, AllowRawSpec: true}
	session := connectWrite(t, cfg, &fakeWriteBackend{})

	tool, ok := toolNames(t, session)["ray_job_submit"]
	if !ok {
		t.Fatal("ray_job_submit absent with --allow-mutations; it must be advertised")
	}
	if tool.Annotations != nil && tool.Annotations.ReadOnlyHint {
		t.Error("ray_job_submit is marked read-only; it mutates")
	}
	if tool.Annotations == nil || tool.Annotations.IdempotentHint {
		t.Error("ray_job_submit IdempotentHint should be false (re-submit is an error, not a no-op)")
	}
}

// TestJobSubmitSchemaIncludesRawSpecByDefault asserts the rawSpec arg is present in
// the advertised schema when --allow-raw-spec is true (the default).
func TestJobSubmitSchemaIncludesRawSpecByDefault(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "default", AllowMutations: true, AllowRawSpec: true}
	session := connectWrite(t, cfg, &fakeWriteBackend{})

	tool := toolNames(t, session)["ray_job_submit"]
	if !schemaHasProperty(t, tool, "rawSpec") {
		t.Error("rawSpec absent from the schema with --allow-raw-spec=true; it must be present")
	}
}

// TestJobSubmitSchemaDropsRawSpecInHardMode is the §6 hard-mode AC: with
// --allow-raw-spec=false the rawSpec arg is removed from the advertised schema,
// while curated fields remain.
func TestJobSubmitSchemaDropsRawSpecInHardMode(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "default", AllowMutations: true, AllowRawSpec: false}
	session := connectWrite(t, cfg, &fakeWriteBackend{})

	tool := toolNames(t, session)["ray_job_submit"]
	if schemaHasProperty(t, tool, "rawSpec") {
		t.Error("rawSpec present in the schema with --allow-raw-spec=false; hard mode must remove it")
	}
	if !schemaHasProperty(t, tool, "entrypoint") {
		t.Error("entrypoint dropped from the schema; only rawSpec should be removed in hard mode")
	}
}

// TestJobSubmitExistingClusterCommit drives a real existing-cluster submit end-to-
// end: dry-run then commit through the backend, RayJob kind, non-error summary,
// DryRun=false, Ephemeral=false, and the resolved namespace echoed back.
func TestJobSubmitExistingClusterCommit(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "default", AllowMutations: true, AllowRawSpec: true}
	backend := &fakeWriteBackend{}
	session := connectWrite(t, cfg, backend)

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name: "ray_job_submit",
		Arguments: map[string]any{
			"name":            "job1",
			"entrypoint":      "python main.py",
			"existingCluster": "existing-cluster",
		},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if res.IsError {
		t.Fatalf("submit reported a tool error: %+v", res)
	}

	sc, ok := res.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("StructuredContent is %T, want map[string]any", res.StructuredContent)
	}
	if sc["dryRun"] != false {
		t.Errorf("dryRun = %v, want false for a commit", sc["dryRun"])
	}
	if sc["ephemeral"] != false {
		t.Errorf("ephemeral = %v, want false for existing-cluster mode", sc["ephemeral"])
	}
	if sc["namespace"] != "default" {
		t.Errorf("namespace = %v, want the resolved default", sc["namespace"])
	}
	if len(backend.applyCalls) != 2 || !backend.applyCalls[0].dryRun || backend.applyCalls[1].dryRun {
		t.Fatalf("backend apply calls = %+v, want [dry-run, commit]", backend.applyCalls)
	}
	if backend.applyCalls[1].kind != domain.KindRayJob {
		t.Errorf("apply kind = %s, want RayJob", backend.applyCalls[1].kind)
	}
	assertHasText(t, res)
}

// TestJobSubmitEphemeralCommit drives the clusterSpec (ephemeral) mode and asserts
// the result is flagged Ephemeral=true.
func TestJobSubmitEphemeralCommit(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "default", AllowMutations: true, AllowRawSpec: true}
	backend := &fakeWriteBackend{}
	session := connectWrite(t, cfg, backend)

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name: "ray_job_submit",
		Arguments: map[string]any{
			"name":       "job1",
			"entrypoint": "python main.py",
			"clusterSpec": map[string]any{
				"rayVersion":   "2.9.0",
				"image":        "rayproject/ray:2.9.0",
				"workerGroups": []any{map[string]any{"name": "wg", "replicas": 1}},
			},
		},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if res.IsError {
		t.Fatalf("ephemeral submit reported a tool error: %+v", res)
	}
	sc := res.StructuredContent.(map[string]any)
	if sc["ephemeral"] != true {
		t.Errorf("ephemeral = %v, want true for clusterSpec mode", sc["ephemeral"])
	}
	if len(backend.applyCalls) != 2 {
		t.Fatalf("backend apply calls = %+v, want [dry-run, commit]", backend.applyCalls)
	}
}

// TestJobSubmitBothModesError asserts supplying both cluster targets is a tool
// error (the domain XOR), surfaced before any apply.
func TestJobSubmitBothModesError(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "default", AllowMutations: true, AllowRawSpec: true}
	backend := &fakeWriteBackend{}
	session := connectWrite(t, cfg, backend)

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name: "ray_job_submit",
		Arguments: map[string]any{
			"name":            "job1",
			"entrypoint":      "python main.py",
			"existingCluster": "c",
			"clusterSpec":     map[string]any{"rayVersion": "2.9.0"},
		},
	})
	if err != nil {
		t.Fatalf("CallTool transport error: %v", err)
	}
	if !res.IsError {
		t.Fatal("submit with both modes did not error; want a validation error")
	}
	if len(backend.applyCalls) != 0 {
		t.Errorf("backend was called %d times, want 0 (validation precedes apply)", len(backend.applyCalls))
	}
}

// TestJobSubmitNeitherModeError asserts supplying neither cluster target is a tool
// error (the other half of the XOR).
func TestJobSubmitNeitherModeError(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "default", AllowMutations: true, AllowRawSpec: true}
	backend := &fakeWriteBackend{}
	session := connectWrite(t, cfg, backend)

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "ray_job_submit",
		Arguments: map[string]any{"name": "job1", "entrypoint": "python main.py"},
	})
	if err != nil {
		t.Fatalf("CallTool transport error: %v", err)
	}
	if !res.IsError {
		t.Fatal("submit with neither mode did not error; want a validation error")
	}
	if len(backend.applyCalls) != 0 {
		t.Errorf("backend was called %d times, want 0", len(backend.applyCalls))
	}
}

// TestJobSubmitDryRun asserts dryRun=true drives only the DryRunAll (one backend
// call, dry-run) and the result is flagged DryRun=true.
func TestJobSubmitDryRun(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "default", AllowMutations: true, AllowRawSpec: true}
	backend := &fakeWriteBackend{}
	session := connectWrite(t, cfg, backend)

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name: "ray_job_submit",
		Arguments: map[string]any{
			"name": "job1", "entrypoint": "python main.py", "existingCluster": "c", "dryRun": true,
		},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if res.IsError {
		t.Fatalf("dry-run submit reported a tool error: %+v", res)
	}
	sc := res.StructuredContent.(map[string]any)
	if sc["dryRun"] != true {
		t.Errorf("dryRun = %v, want true", sc["dryRun"])
	}
	if len(backend.applyCalls) != 1 || !backend.applyCalls[0].dryRun {
		t.Fatalf("backend apply calls = %+v, want exactly one dry-run", backend.applyCalls)
	}
}

// TestJobSubmitRejectsRawSpecInHardMode is the defense-in-depth AC: even if a
// client ignores the pruned schema and sends rawSpec under --allow-raw-spec=false,
// the tool rejects it before any apply.
func TestJobSubmitRejectsRawSpecInHardMode(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "default", AllowMutations: true, AllowRawSpec: false}
	backend := &fakeWriteBackend{}
	session := connectWrite(t, cfg, backend)

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name: "ray_job_submit",
		Arguments: map[string]any{
			"name": "job1", "entrypoint": "python main.py", "existingCluster": "c",
			"rawSpec": map[string]any{"spec": map[string]any{"x": 1}},
		},
	})
	if err != nil {
		t.Fatalf("CallTool transport error: %v", err)
	}
	if !res.IsError {
		t.Fatal("submit with rawSpec under --allow-raw-spec=false did not error; hard mode must reject it")
	}
	if len(backend.applyCalls) != 0 {
		t.Errorf("backend was called %d times, want 0 (rejected before apply)", len(backend.applyCalls))
	}
}

// TestJobSubmitSurfacesJobIDAndStatus asserts the non-blocking return projects the
// read-back status (jobId + jobDeploymentStatus) when the server populated them.
func TestJobSubmitSurfacesJobIDAndStatus(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "default", AllowMutations: true, AllowRawSpec: true}
	backend := &fakeWriteBackend{serverStatus: map[string]any{
		"jobId": "raysubmit_abc", "jobDeploymentStatus": "Initializing",
	}}
	session := connectWrite(t, cfg, backend)

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name: "ray_job_submit",
		Arguments: map[string]any{
			"name": "job1", "entrypoint": "python main.py", "existingCluster": "c",
		},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if res.IsError {
		t.Fatalf("submit reported a tool error: %+v", res)
	}
	sc := res.StructuredContent.(map[string]any)
	if sc["jobId"] != "raysubmit_abc" {
		t.Errorf("jobId = %v, want raysubmit_abc", sc["jobId"])
	}
	if sc["deploymentStatus"] != "Initializing" {
		t.Errorf("deploymentStatus = %v, want Initializing", sc["deploymentStatus"])
	}
}
