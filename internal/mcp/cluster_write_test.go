package mcp_test

import (
	"context"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/risjai/ray-mcp/internal/config"
	"github.com/risjai/ray-mcp/internal/domain"
	mcpserver "github.com/risjai/ray-mcp/internal/mcp"
)

// fakeWriteBackend is a programmable ClusterWriteBackend (BuildClusterBase +
// Apply + Delete) for the MCP write-path tests. It builds a minimal
// identity-carrying base and echoes the applied spec back as the server view,
// optionally injecting an extra "added" field so the diff is non-empty. It
// records every Apply so a test can assert the dry-run/commit sequence the tool
// drove. Delete records what was deleted.
type fakeWriteBackend struct {
	applyCalls []applyRecord
	// serverExtra, when set, is merged into the returned object's spec so the
	// read-back diff surfaces a server-defaulted field.
	serverExtra map[string]any
	applyErr    error

	deleteCalls []deleteRecord
	deleteErr   error
}

type deleteRecord struct {
	kind      domain.Kind
	namespace string
	name      string
	dryRun    bool
}

func (f *fakeWriteBackend) Delete(_ context.Context, kind domain.Kind, namespace, name string, dryRun bool) error {
	f.deleteCalls = append(f.deleteCalls, deleteRecord{kind: kind, namespace: namespace, name: name, dryRun: dryRun})
	return f.deleteErr
}

type applyRecord struct {
	kind      domain.Kind
	namespace string
	name      string
	dryRun    bool
	force     bool
}

func (f *fakeWriteBackend) BuildClusterBase(p domain.ClusterCreateParams) (domain.MergedSpec, error) {
	spec := map[string]any{}
	if p.RayVersion != "" {
		spec["rayVersion"] = p.RayVersion
	}
	return domain.MergedSpec{
		"apiVersion": "ray.io/v1",
		"kind":       "RayCluster",
		"metadata":   map[string]any{"name": p.Name, "namespace": p.Namespace},
		"spec":       spec,
	}, nil
}

func (f *fakeWriteBackend) Apply(_ context.Context, kind domain.Kind, namespace, name string, spec domain.MergedSpec, opts domain.ApplyOptions) (domain.MergedSpec, error) {
	f.applyCalls = append(f.applyCalls, applyRecord{kind: kind, namespace: namespace, name: name, dryRun: opts.DryRun, force: opts.Force})
	if f.applyErr != nil {
		return nil, f.applyErr
	}
	// Echo the submitted object back as the server view, injecting serverExtra into
	// spec so the intent-vs-result diff is non-empty when configured.
	out := domain.MergedSpec(runtimeClone(spec))
	if len(f.serverExtra) > 0 {
		s, _ := out["spec"].(map[string]any)
		if s == nil {
			s = map[string]any{}
			out["spec"] = s
		}
		for k, v := range f.serverExtra {
			s[k] = v
		}
	}
	return out, nil
}

// runtimeClone is a tiny JSON-shaped deep copy so the fake never aliases the
// caller's spec map (mirrors the real adapter's isolation contract).
func runtimeClone(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		switch t := v.(type) {
		case map[string]any:
			out[k] = runtimeClone(t)
		default:
			out[k] = v
		}
	}
	return out
}

// connectWrite wires a server with the given cfg + a write backend to an in-memory
// client session. The read slice is an empty fake (these tests drive the write
// tools). Returns the session and the backend for assertions.
func connectWrite(t *testing.T, cfg *config.Config, backend *fakeWriteBackend) *mcp.ClientSession {
	t.Helper()
	ctx := context.Background()

	server := mcpserver.NewServer(cfg, fakeSource{contextName: "ctx", defaultNamespace: cfg.DefaultNamespace}, &fakeKubeRay{}, backend, domain.NopAuditSink{})
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

func toolNames(t *testing.T, session *mcp.ClientSession) map[string]*mcp.Tool {
	t.Helper()
	res, err := session.ListTools(context.Background(), &mcp.ListToolsParams{})
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	byName := map[string]*mcp.Tool{}
	for _, tool := range res.Tools {
		byName[tool.Name] = tool
	}
	return byName
}

// TestCreateToolAbsentWithoutAllowMutations is the gate AC: ray_cluster_create
// must NOT be advertised unless --allow-mutations is set (spec §6: disabled tools
// are not advertised).
func TestCreateToolAbsentWithoutAllowMutations(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "default", AllowMutations: false}
	session := connectWrite(t, cfg, &fakeWriteBackend{})

	if _, ok := toolNames(t, session)["ray_cluster_create"]; ok {
		t.Error("ray_cluster_create is advertised without --allow-mutations; it must be absent")
	}
}

// TestCreateToolPresentWithAllowMutations is the other half: with --allow-mutations
// the tool is advertised, and it is NOT marked read-only (it mutates) and NOT
// idempotent (a re-create of the same name is an already-exists error).
func TestCreateToolPresentWithAllowMutations(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "default", AllowMutations: true, AllowRawSpec: true}
	session := connectWrite(t, cfg, &fakeWriteBackend{})

	tool, ok := toolNames(t, session)["ray_cluster_create"]
	if !ok {
		t.Fatal("ray_cluster_create absent with --allow-mutations; it must be advertised")
	}
	if tool.Annotations != nil && tool.Annotations.ReadOnlyHint {
		t.Error("ray_cluster_create is marked read-only; it mutates")
	}
	if tool.Annotations == nil || tool.Annotations.IdempotentHint {
		t.Error("ray_cluster_create IdempotentHint should be false (re-create is an error, not a no-op)")
	}
}

// TestCreateSchemaIncludesRawSpecByDefault asserts the rawSpec arg is present in
// the advertised input schema when --allow-raw-spec is true (the default).
func TestCreateSchemaIncludesRawSpecByDefault(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "default", AllowMutations: true, AllowRawSpec: true}
	session := connectWrite(t, cfg, &fakeWriteBackend{})

	tool := toolNames(t, session)["ray_cluster_create"]
	if !schemaHasProperty(t, tool, "rawSpec") {
		t.Error("rawSpec absent from the schema with --allow-raw-spec=true; it must be present")
	}
}

// TestCreateSchemaDropsRawSpecInHardMode is the §6 hard-mode AC: with
// --allow-raw-spec=false the rawSpec arg is removed from the advertised schema
// entirely.
func TestCreateSchemaDropsRawSpecInHardMode(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "default", AllowMutations: true, AllowRawSpec: false}
	session := connectWrite(t, cfg, &fakeWriteBackend{})

	tool := toolNames(t, session)["ray_cluster_create"]
	if schemaHasProperty(t, tool, "rawSpec") {
		t.Error("rawSpec present in the schema with --allow-raw-spec=false; hard mode must remove it")
	}
	// Other curated fields remain.
	if !schemaHasProperty(t, tool, "image") {
		t.Error("image dropped from the schema; only rawSpec should be removed in hard mode")
	}
}

// TestCreateCommitReturnsDiff drives a real create end-to-end through the in-memory
// client: the fake server view adds a defaulted field, and the tool returns it as
// the structured diff + a non-error text summary, with DryRun=false.
func TestCreateCommitReturnsDiff(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "default", AllowMutations: true, AllowRawSpec: true}
	backend := &fakeWriteBackend{serverExtra: map[string]any{"suspend": false}}
	session := connectWrite(t, cfg, backend)

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name: "ray_cluster_create",
		Arguments: map[string]any{
			"name": "demo", "rayVersion": "2.9.0",
			"workerGroups": []any{map[string]any{"name": "wg", "replicas": 2}},
		},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if res.IsError {
		t.Fatalf("create reported a tool error: %+v", res)
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
	// The defaulted server field surfaces in the diff (fieldCount >= 1).
	if fc, _ := sc["fieldCount"].(float64); fc < 1 {
		t.Errorf("fieldCount = %v, want >= 1 (the server-defaulted suspend field)", sc["fieldCount"])
	}
	// Two backend applies: dry-run then commit.
	if len(backend.applyCalls) != 2 || !backend.applyCalls[0].dryRun || backend.applyCalls[1].dryRun {
		t.Fatalf("backend apply calls = %+v, want [dry-run, commit]", backend.applyCalls)
	}
	assertHasText(t, res)
}

// TestCreateDryRunDoesNotCommit asserts dryRun=true drives only the DryRunAll
// (one backend call, dry-run) and the result is flagged DryRun=true.
func TestCreateDryRunDoesNotCommit(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "default", AllowMutations: true, AllowRawSpec: true}
	backend := &fakeWriteBackend{}
	session := connectWrite(t, cfg, backend)

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "ray_cluster_create",
		Arguments: map[string]any{"name": "demo", "dryRun": true},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if res.IsError {
		t.Fatalf("dry-run create reported a tool error: %+v", res)
	}
	sc := res.StructuredContent.(map[string]any)
	if sc["dryRun"] != true {
		t.Errorf("dryRun = %v, want true", sc["dryRun"])
	}
	if len(backend.applyCalls) != 1 || !backend.applyCalls[0].dryRun {
		t.Fatalf("backend apply calls = %+v, want exactly one dry-run", backend.applyCalls)
	}
}

// TestCreateRejectsRawSpecInHardMode is the defense-in-depth AC: even if a client
// ignores the pruned schema and sends rawSpec under --allow-raw-spec=false, the
// tool rejects it rather than honoring the escape hatch.
func TestCreateRejectsRawSpecInHardMode(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "default", AllowMutations: true, AllowRawSpec: false}
	backend := &fakeWriteBackend{}
	session := connectWrite(t, cfg, backend)

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "ray_cluster_create",
		Arguments: map[string]any{"name": "demo", "rawSpec": map[string]any{"spec": map[string]any{"x": 1}}},
	})
	if err != nil {
		t.Fatalf("CallTool transport error: %v", err)
	}
	if !res.IsError {
		t.Fatal("create with rawSpec under --allow-raw-spec=false did not error; hard mode must reject it")
	}
	if len(backend.applyCalls) != 0 {
		t.Errorf("backend was called %d times, want 0 (rejected before apply)", len(backend.applyCalls))
	}
}

// schemaHasProperty reports whether the tool's input schema advertises a top-level
// property of the given name. From the client side the SDK delivers InputSchema as
// the default JSON marshaling (a map[string]any), so we navigate
// schema.properties.<name> as plain maps.
func schemaHasProperty(t *testing.T, tool *mcp.Tool, name string) bool {
	t.Helper()
	if tool == nil || tool.InputSchema == nil {
		t.Fatal("tool or its input schema is nil")
	}
	schema, ok := tool.InputSchema.(map[string]any)
	if !ok {
		t.Fatalf("InputSchema is %T, want map[string]any (client-side marshaling)", tool.InputSchema)
	}
	props, ok := schema["properties"].(map[string]any)
	if !ok {
		return false
	}
	_, present := props[name]
	return present
}

// assertHasText fails unless the result carries at least one non-empty text
// content block (the human-readable summary alongside the structured output).
func assertHasText(t *testing.T, res *mcp.CallToolResult) {
	t.Helper()
	for _, c := range res.Content {
		if tc, ok := c.(*mcp.TextContent); ok && tc.Text != "" {
			return
		}
	}
	t.Error("result has no non-empty text content; want a human-readable summary")
}
