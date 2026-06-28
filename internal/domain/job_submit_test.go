package domain

import (
	"context"
	"errors"
	"testing"
)

// fakeJobBaseBuilder is a programmable JobBaseBuilder: it records the params it was
// handed and returns a canned base (or error), so a test can assert the service
// resolved the namespace / mode / shutdown default and composed base→merge→apply
// correctly without exercising the real KubeRay-typed construction (that is the
// adapter's envtest job).
type fakeJobBaseBuilder struct {
	got  JobSubmitParams
	base MergedSpec
	err  error
}

func (f *fakeJobBaseBuilder) BuildJobBase(p JobSubmitParams) (MergedSpec, error) {
	f.got = p
	return f.base, f.err
}

// compile-time proof the fake satisfies the port.
var _ JobBaseBuilder = (*fakeJobBaseBuilder)(nil)

// jobBaseFor builds the RayJob base a real builder would produce for the given
// identity: metadata.name/namespace set so Merge's identity guard passes.
func jobBaseFor(namespace, name string) MergedSpec {
	return MergedSpec{
		"apiVersion": "ray.io/v1",
		"kind":       "RayJob",
		"metadata":   map[string]any{"name": name, "namespace": namespace},
		"spec":       map[string]any{"entrypoint": "python main.py"},
	}
}

// newJobWriteService wires a JobWriteService over the given base builder + applier
// with a recording audit sink, returning the service and the sink for assertions.
func newJobWriteService(base JobBaseBuilder, applier Applier, defaultNS string) (*JobWriteService, *recordingSink) {
	sink := &recordingSink{}
	// Submit does not read or delete the live object, so the getter/deleter are nil
	// here; the delete tests wire real ones (see newJobDeleteService).
	svc := NewJobWriteService(base, nil, nil, NewApplyService(applier, sink), defaultNS)
	return svc, sink
}

// ephemeralSpec is a minimal curated ephemeral cluster spec for the mode-B tests.
func ephemeralSpec() *ClusterSubmitSpec {
	return &ClusterSubmitSpec{
		RayVersion:   "2.9.0",
		Image:        "rayproject/ray:2.9.0",
		WorkerGroups: []WorkerGroupParams{{Name: "wg", Replicas: 1}},
	}
}

func boolPtr(b bool) *bool { return &b }

// TestSubmitExistingClusterBuildsMergesAndApplies asserts the existing-cluster
// happy path: it resolves the namespace, builds the base, hands the merged RayJob
// spec to the applier (dry-run then commit) keyed by KindRayJob, and audits once.
func TestSubmitExistingClusterBuildsMergesAndApplies(t *testing.T) {
	t.Parallel()
	base := &fakeJobBaseBuilder{base: jobBaseFor("ray", "job1")}
	applier := &fakeApplier{dryRunObj: jobBaseFor("ray", "job1"), applyObj: jobBaseFor("ray", "job1")}
	svc, sink := newJobWriteService(base, applier, "ray")

	res, err := svc.Submit(context.Background(), JobSubmitParams{
		Name:            "job1",
		Entrypoint:      "python main.py",
		ExistingCluster: "existing-cluster",
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if res.DryRun {
		t.Error("result DryRun = true, want false for a committed submit")
	}
	if res.Ephemeral {
		t.Error("result Ephemeral = true, want false for existing-cluster mode")
	}
	if base.got.Namespace != "ray" {
		t.Errorf("base builder saw namespace %q, want resolved default %q", base.got.Namespace, "ray")
	}
	if len(applier.calls) != 2 || !applier.calls[0].dryRun || applier.calls[1].dryRun {
		t.Fatalf("applier calls = %+v, want [dry-run, commit]", applier.calls)
	}
	if applier.calls[1].kind != KindRayJob {
		t.Errorf("apply kind = %s, want RayJob", applier.calls[1].kind)
	}
	if len(sink.records) != 1 || sink.records[0].Tool != "ray_job_submit" {
		t.Fatalf("audit records = %+v, want one tagged ray_job_submit", sink.records)
	}
}

// TestSubmitEphemeralDefaultsShutdownTrue asserts mode-B with no explicit
// shutdownAfterJobFinishes resolves to true (Q16b divergence from KubeRay's
// false), and the resolved value reaches the builder.
func TestSubmitEphemeralDefaultsShutdownTrue(t *testing.T) {
	t.Parallel()
	base := &fakeJobBaseBuilder{base: jobBaseFor("ray", "job1")}
	applier := &fakeApplier{dryRunObj: jobBaseFor("ray", "job1"), applyObj: jobBaseFor("ray", "job1")}
	svc, _ := newJobWriteService(base, applier, "ray")

	res, err := svc.Submit(context.Background(), JobSubmitParams{
		Name:        "job1",
		Entrypoint:  "python main.py",
		ClusterSpec: ephemeralSpec(),
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if !res.Ephemeral {
		t.Error("result Ephemeral = false, want true for clusterSpec mode")
	}
	if base.got.ShutdownAfterJobFinishes == nil {
		t.Fatal("builder saw ShutdownAfterJobFinishes = nil, want a resolved default")
	}
	if *base.got.ShutdownAfterJobFinishes != true {
		t.Errorf("builder saw ShutdownAfterJobFinishes = %v, want default true (Q16b)", *base.got.ShutdownAfterJobFinishes)
	}
}

// TestSubmitEphemeralRespectsExplicitShutdownFalse asserts an explicit false is
// preserved (the "pass false to keep the cluster for debugging" hint).
func TestSubmitEphemeralRespectsExplicitShutdownFalse(t *testing.T) {
	t.Parallel()
	base := &fakeJobBaseBuilder{base: jobBaseFor("ray", "job1")}
	applier := &fakeApplier{dryRunObj: jobBaseFor("ray", "job1"), applyObj: jobBaseFor("ray", "job1")}
	svc, _ := newJobWriteService(base, applier, "ray")

	_, err := svc.Submit(context.Background(), JobSubmitParams{
		Name:                     "job1",
		Entrypoint:               "python main.py",
		ClusterSpec:              ephemeralSpec(),
		ShutdownAfterJobFinishes: boolPtr(false),
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if base.got.ShutdownAfterJobFinishes == nil || *base.got.ShutdownAfterJobFinishes != false {
		t.Errorf("builder saw ShutdownAfterJobFinishes = %v, want explicit false preserved", base.got.ShutdownAfterJobFinishes)
	}
}

// TestSubmitExistingRejectsShutdown asserts shutdownAfterJobFinishes is refused in
// existing-cluster mode (KubeRay never tears down a cluster the job does not own;
// honoring it would be a nasty surprise) — a validation error before any apply.
func TestSubmitExistingRejectsShutdown(t *testing.T) {
	t.Parallel()
	base := &fakeJobBaseBuilder{base: jobBaseFor("ray", "job1")}
	applier := &fakeApplier{}
	svc, _ := newJobWriteService(base, applier, "ray")

	_, err := svc.Submit(context.Background(), JobSubmitParams{
		Name:                     "job1",
		Entrypoint:               "python main.py",
		ExistingCluster:          "existing-cluster",
		ShutdownAfterJobFinishes: boolPtr(true),
	})
	if err == nil {
		t.Fatal("Submit with shutdown + existingCluster returned nil, want a validation error")
	}
	if len(applier.calls) != 0 {
		t.Errorf("applier called %d times, want 0 (validation precedes apply)", len(applier.calls))
	}
}

// TestSubmitRequiresExactlyOneMode asserts the XOR: both modes set, or neither, is
// a validation error before any apply (stricter than KubeRay, which ignores
// rayClusterSpec when both are set and only ValidationFails on neither).
func TestSubmitRequiresExactlyOneMode(t *testing.T) {
	t.Parallel()

	t.Run("both", func(t *testing.T) {
		t.Parallel()
		applier := &fakeApplier{}
		svc, _ := newJobWriteService(&fakeJobBaseBuilder{base: jobBaseFor("ray", "job1")}, applier, "ray")
		_, err := svc.Submit(context.Background(), JobSubmitParams{
			Name: "job1", Entrypoint: "python main.py",
			ExistingCluster: "existing-cluster", ClusterSpec: ephemeralSpec(),
		})
		if err == nil {
			t.Fatal("Submit with both modes returned nil, want a validation error")
		}
		if len(applier.calls) != 0 {
			t.Errorf("applier called %d times, want 0", len(applier.calls))
		}
	})

	t.Run("neither", func(t *testing.T) {
		t.Parallel()
		applier := &fakeApplier{}
		svc, _ := newJobWriteService(&fakeJobBaseBuilder{base: jobBaseFor("ray", "job1")}, applier, "ray")
		_, err := svc.Submit(context.Background(), JobSubmitParams{Name: "job1", Entrypoint: "python main.py"})
		if err == nil {
			t.Fatal("Submit with neither mode returned nil, want a validation error")
		}
		if len(applier.calls) != 0 {
			t.Errorf("applier called %d times, want 0", len(applier.calls))
		}
	})
}

// TestSubmitRequiresName asserts an empty name is rejected before the base build.
func TestSubmitRequiresName(t *testing.T) {
	t.Parallel()
	svc, _ := newJobWriteService(&fakeJobBaseBuilder{}, &fakeApplier{}, "ray")
	if _, err := svc.Submit(context.Background(), JobSubmitParams{Entrypoint: "python main.py", ExistingCluster: "c"}); err == nil {
		t.Fatal("Submit with empty name returned nil, want a validation error")
	}
}

// TestSubmitRequiresEntrypoint asserts an empty entrypoint is rejected client-side
// — KubeRay does not reject it (apply succeeds, then the driver fails opaquely),
// so ray-mcp must.
func TestSubmitRequiresEntrypoint(t *testing.T) {
	t.Parallel()
	applier := &fakeApplier{}
	svc, _ := newJobWriteService(&fakeJobBaseBuilder{base: jobBaseFor("ray", "job1")}, applier, "ray")
	if _, err := svc.Submit(context.Background(), JobSubmitParams{Name: "job1", ExistingCluster: "c"}); err == nil {
		t.Fatal("Submit with empty entrypoint returned nil, want a validation error")
	}
	if len(applier.calls) != 0 {
		t.Errorf("applier called %d times, want 0 (entrypoint validation precedes apply)", len(applier.calls))
	}
}

// TestSubmitRawSpecWins asserts the top-level rawSpec is merged OVER the RayJob
// base (rawSpec wins) before the apply.
func TestSubmitRawSpecWins(t *testing.T) {
	t.Parallel()
	base := &fakeJobBaseBuilder{base: jobBaseFor("ray", "job1")} // base entrypoint "python main.py"
	applier := &fakeApplier{dryRunObj: jobBaseFor("ray", "job1"), applyObj: jobBaseFor("ray", "job1")}
	svc, _ := newJobWriteService(base, applier, "ray")

	_, err := svc.Submit(context.Background(), JobSubmitParams{
		Name: "job1", Entrypoint: "python main.py", ExistingCluster: "c",
		RawSpec: MergedSpec{"spec": map[string]any{"entrypoint": "python override.py"}},
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	applied := applier.calls[0].spec
	spec, _ := applied["spec"].(map[string]any)
	if spec == nil || spec["entrypoint"] != "python override.py" {
		t.Errorf("applied spec.entrypoint = %v, want the rawSpec value (rawSpec wins)", spec["entrypoint"])
	}
}

// TestSubmitRejectsIdentityRetarget asserts a rawSpec retargeting metadata.name is
// rejected by Merge's identity guard before any apply or audit.
func TestSubmitRejectsIdentityRetarget(t *testing.T) {
	t.Parallel()
	base := &fakeJobBaseBuilder{base: jobBaseFor("ray", "job1")}
	applier := &fakeApplier{}
	svc, sink := newJobWriteService(base, applier, "ray")

	_, err := svc.Submit(context.Background(), JobSubmitParams{
		Name: "job1", Entrypoint: "python main.py", ExistingCluster: "c",
		RawSpec: MergedSpec{"metadata": map[string]any{"name": "evil"}},
	})
	var ident *IdentityError
	if !errors.As(err, &ident) {
		t.Fatalf("Submit error = %v, want *IdentityError", err)
	}
	if len(applier.calls) != 0 {
		t.Errorf("applier called %d times, want 0 (identity guard precedes apply)", len(applier.calls))
	}
	if len(sink.records) != 0 {
		t.Errorf("audit records = %d, want 0 (identity rejection precedes the apply choke point)", len(sink.records))
	}
}

// TestSubmitDryRunDoesNotCommit asserts a dryRun submit runs the DryRunAll only
// and reports DryRun=true.
func TestSubmitDryRunDoesNotCommit(t *testing.T) {
	t.Parallel()
	base := &fakeJobBaseBuilder{base: jobBaseFor("ray", "job1")}
	applier := &fakeApplier{dryRunObj: jobBaseFor("ray", "job1")}
	svc, _ := newJobWriteService(base, applier, "ray")

	res, err := svc.Submit(context.Background(), JobSubmitParams{
		Name: "job1", Entrypoint: "python main.py", ExistingCluster: "c", DryRun: true,
	})
	if err != nil {
		t.Fatalf("Submit(dryRun): %v", err)
	}
	if !res.DryRun {
		t.Error("result DryRun = false, want true")
	}
	if len(applier.calls) != 1 || !applier.calls[0].dryRun {
		t.Fatalf("applier calls = %+v, want exactly one dry-run", applier.calls)
	}
}

// TestSubmitSurfacesJobIDAndStatus asserts the non-blocking return projects the
// read-back status (jobId + jobDeploymentStatus) when the server already populated
// them — the {name, jobId-when-ready, initialStatus} contract.
func TestSubmitSurfacesJobIDAndStatus(t *testing.T) {
	t.Parallel()
	readBack := MergedSpec{
		"apiVersion": "ray.io/v1", "kind": "RayJob",
		"metadata": map[string]any{"name": "job1", "namespace": "ray"},
		"spec":     map[string]any{"entrypoint": "python main.py"},
		"status":   map[string]any{"jobId": "raysubmit_abc", "jobDeploymentStatus": "Initializing"},
	}
	base := &fakeJobBaseBuilder{base: jobBaseFor("ray", "job1")}
	applier := &fakeApplier{dryRunObj: jobBaseFor("ray", "job1"), applyObj: readBack}
	svc, _ := newJobWriteService(base, applier, "ray")

	res, err := svc.Submit(context.Background(), JobSubmitParams{
		Name: "job1", Entrypoint: "python main.py", ExistingCluster: "c",
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if res.JobID != "raysubmit_abc" {
		t.Errorf("result JobID = %q, want raysubmit_abc", res.JobID)
	}
	if res.DeploymentStatus != "Initializing" {
		t.Errorf("result DeploymentStatus = %q, want Initializing", res.DeploymentStatus)
	}
}

// TestSubmitNamespaceDefault asserts an empty namespace is resolved to the service
// default before the base build (so the builder and apply target the real ns).
func TestSubmitNamespaceDefault(t *testing.T) {
	t.Parallel()
	base := &fakeJobBaseBuilder{base: jobBaseFor("team-a", "job1")}
	applier := &fakeApplier{dryRunObj: jobBaseFor("team-a", "job1"), applyObj: jobBaseFor("team-a", "job1")}
	svc, _ := newJobWriteService(base, applier, "team-a")

	if _, err := svc.Submit(context.Background(), JobSubmitParams{
		Name: "job1", Entrypoint: "python main.py", ExistingCluster: "c",
	}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if base.got.Namespace != "team-a" {
		t.Errorf("base builder saw namespace %q, want resolved default team-a", base.got.Namespace)
	}
}
