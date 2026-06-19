package domain

import (
	"context"
	"errors"
	"testing"
)

// fakeBaseBuilder is a programmable ClusterBaseBuilder: it records the params it
// was handed and returns a canned base (or error), so a test can assert the
// service forwards the resolved params and composes base→merge→apply correctly.
type fakeBaseBuilder struct {
	got  ClusterCreateParams
	base MergedSpec
	err  error
}

func (f *fakeBaseBuilder) BuildClusterBase(p ClusterCreateParams) (MergedSpec, error) {
	f.got = p
	return f.base, f.err
}

// baseFor builds the curated base a real builder would produce for the given
// identity: metadata.name/namespace set so Merge's identity guard passes.
func baseFor(namespace, name string) MergedSpec {
	return MergedSpec{
		"apiVersion": "ray.io/v1",
		"kind":       "RayCluster",
		"metadata":   map[string]any{"name": name, "namespace": namespace},
		"spec": map[string]any{
			"rayVersion": "2.9.0",
		},
	}
}

// newWriteService wires a ClusterWriteService over the given base builder + applier
// with a recording audit sink, returning all three for assertions.
func newWriteService(base ClusterBaseBuilder, applier Applier, defaultNS string) (*ClusterWriteService, *recordingSink) {
	sink := &recordingSink{}
	svc := NewClusterWriteService(base, NewApplyService(applier, sink), defaultNS)
	return svc, sink
}

// TestCreateBuildsMergesAndApplies asserts the happy path composes the three
// stages: it resolves the namespace, builds the curated base, hands the merged
// spec to the applier (dry-run then commit), and returns the read-back diff.
func TestCreateBuildsMergesAndApplies(t *testing.T) {
	t.Parallel()
	base := &fakeBaseBuilder{base: baseFor("ray", "demo")}
	applier := &fakeApplier{dryRunObj: baseFor("ray", "demo"), applyObj: baseFor("ray", "demo")}
	svc, sink := newWriteService(base, applier, "ray")

	res, err := svc.Create(context.Background(), ClusterCreateParams{
		Name: "demo", RayVersion: "2.9.0", Image: "rayproject/ray:2.9.0",
		WorkerGroups: []WorkerGroupParams{{Name: "wg", Replicas: 2}},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if res.DryRun {
		t.Error("result DryRun = true, want false for a committed create")
	}
	// Namespace default was applied before building the base.
	if base.got.Namespace != "ray" {
		t.Errorf("base builder saw namespace %q, want the resolved default %q", base.got.Namespace, "ray")
	}
	// Two applier calls: the unconditional dry-run, then the commit.
	if len(applier.calls) != 2 || !applier.calls[0].dryRun || applier.calls[1].dryRun {
		t.Fatalf("applier calls = %+v, want [dry-run, commit]", applier.calls)
	}
	// The applied spec carries the identity (the merged base reached the applier).
	if applier.calls[1].name != "demo" || applier.calls[1].namespace != "ray" {
		t.Errorf("applied identity = %s/%s, want ray/demo", applier.calls[1].namespace, applier.calls[1].name)
	}
	// One audit record, tagged with the create tool name.
	if len(sink.records) != 1 || sink.records[0].Tool != "ray_cluster_create" {
		t.Fatalf("audit records = %+v, want one tagged ray_cluster_create", sink.records)
	}
}

// TestCreateDryRunDoesNotCommit asserts a dryRun create runs the DryRunAll only
// (no commit) and reports DryRun=true — the pipeline's validate-without-mutation
// path, surfaced through the create entry point.
func TestCreateDryRunDoesNotCommit(t *testing.T) {
	t.Parallel()
	base := &fakeBaseBuilder{base: baseFor("ray", "demo")}
	applier := &fakeApplier{dryRunObj: baseFor("ray", "demo")}
	svc, _ := newWriteService(base, applier, "ray")

	res, err := svc.Create(context.Background(), ClusterCreateParams{Name: "demo", DryRun: true})
	if err != nil {
		t.Fatalf("Create(dryRun): %v", err)
	}
	if !res.DryRun {
		t.Error("result DryRun = false, want true")
	}
	if len(applier.calls) != 1 || !applier.calls[0].dryRun {
		t.Fatalf("applier calls = %+v, want exactly one dry-run", applier.calls)
	}
}

// TestCreateRawSpecWins asserts the rawSpec escape hatch is merged OVER the
// curated base (rawSpec wins, Task 8a) before the apply: a rawSpec rayVersion
// overrides the curated one in the spec handed to the applier.
func TestCreateRawSpecWins(t *testing.T) {
	t.Parallel()
	base := &fakeBaseBuilder{base: baseFor("ray", "demo")} // curated rayVersion 2.9.0
	applier := &fakeApplier{dryRunObj: baseFor("ray", "demo"), applyObj: baseFor("ray", "demo")}
	svc, _ := newWriteService(base, applier, "ray")

	_, err := svc.Create(context.Background(), ClusterCreateParams{
		Name:    "demo",
		RawSpec: MergedSpec{"spec": map[string]any{"rayVersion": "2.99.0-rc1"}},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	applied := applier.calls[0].spec
	spec, _ := applied["spec"].(map[string]any)
	if spec == nil || spec["rayVersion"] != "2.99.0-rc1" {
		t.Errorf("applied spec.rayVersion = %v, want the rawSpec value 2.99.0-rc1 (rawSpec wins)", spec["rayVersion"])
	}
}

// TestCreateRejectsIdentityRetarget asserts a rawSpec that retargets the identity
// (metadata.name) is rejected by Merge's identity guard BEFORE any applier call —
// the create never reaches the cluster.
func TestCreateRejectsIdentityRetarget(t *testing.T) {
	t.Parallel()
	base := &fakeBaseBuilder{base: baseFor("ray", "demo")}
	applier := &fakeApplier{}
	svc, sink := newWriteService(base, applier, "ray")

	_, err := svc.Create(context.Background(), ClusterCreateParams{
		Name:    "demo",
		RawSpec: MergedSpec{"metadata": map[string]any{"name": "evil"}},
	})
	var ident *IdentityError
	if !errors.As(err, &ident) {
		t.Fatalf("Create error = %v, want *IdentityError", err)
	}
	if len(applier.calls) != 0 {
		t.Errorf("applier was called %d times, want 0 (identity guard precedes apply)", len(applier.calls))
	}
	// No mutation attempted → no audit record (the failure is pre-pipeline).
	if len(sink.records) != 0 {
		t.Errorf("audit records = %d, want 0 (identity rejection precedes the apply choke point)", len(sink.records))
	}
}

// TestCreateRequiresName asserts an empty name is rejected before the base build.
func TestCreateRequiresName(t *testing.T) {
	t.Parallel()
	base := &fakeBaseBuilder{base: baseFor("ray", "demo")}
	svc, _ := newWriteService(base, &fakeApplier{}, "ray")

	if _, err := svc.Create(context.Background(), ClusterCreateParams{Name: ""}); err == nil {
		t.Fatal("Create with empty name returned nil error, want a validation error")
	}
}

// TestCreateBaseBuildErrorStops asserts a base-build failure (e.g. an unparseable
// resource quantity) is returned and no apply is attempted.
func TestCreateBaseBuildErrorStops(t *testing.T) {
	t.Parallel()
	base := &fakeBaseBuilder{err: errors.New("invalid quantity \"x\"")}
	applier := &fakeApplier{}
	svc, _ := newWriteService(base, applier, "ray")

	if _, err := svc.Create(context.Background(), ClusterCreateParams{Name: "demo"}); err == nil {
		t.Fatal("Create returned nil error, want the base-build failure")
	}
	if len(applier.calls) != 0 {
		t.Errorf("applier was called %d times, want 0 (base build failed)", len(applier.calls))
	}
}

// compile-time proof the fake satisfies the base-builder port.
var _ ClusterBaseBuilder = (*fakeBaseBuilder)(nil)
