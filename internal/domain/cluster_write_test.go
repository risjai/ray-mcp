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
	// Create does not read the live object, so a zero-value reader is unused here;
	// update/scale wire a real ClusterGetter (see cluster_scale_test.go).
	svc := NewClusterWriteService(base, &fakeReader{}, &fakeKubeRay{clusters: map[string]ClusterDetail{}}, NewApplyService(applier, sink), defaultNS)
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

// --- Delete tests ----------------------------------------------------------

// fakeDeleter is a programmable Deleter for the delete unit tests: it records
// calls and can return a configurable error.
type fakeDeleter struct {
	calls []deleteCall
	err   error
}

type deleteCall struct {
	kind      Kind
	namespace string
	name      string
	dryRun    bool
}

func (f *fakeDeleter) Delete(_ context.Context, kind Kind, namespace, name string, dryRun bool) error {
	f.calls = append(f.calls, deleteCall{kind: kind, namespace: namespace, name: name, dryRun: dryRun})
	return f.err
}

// compile-time proof the test deleter satisfies the port.
var _ Deleter = (*fakeDeleter)(nil)

// newDeleteService wires a ClusterWriteService with a seeded fake reader (so
// GetCluster returns it) and a recording audit sink, for the delete tests.
func newDeleteService(detail ClusterDetail, deleter *fakeDeleter) (*ClusterWriteService, *recordingSink) {
	reader := &fakeReader{detail: detail}
	sink := &recordingSink{}
	applySvc := NewApplyService(&fakeApplier{}, sink)
	svc := NewClusterWriteService(&fakeBaseBuilder{}, reader, deleter, applySvc, "default")
	return svc, sink
}

// seededCluster builds a ClusterDetail with metadata.uid for a non-trivial
// fingerprint, optionally protected.
func seededCluster(namespace, name, uid string, protected bool) ClusterDetail {
	meta := map[string]any{"name": name, "namespace": namespace, "uid": uid}
	if protected {
		meta["annotations"] = map[string]any{ProtectedAnnotation: "true"}
	}
	raw := MergedSpec{
		"apiVersion": "ray.io/v1",
		"kind":       "RayCluster",
		"metadata":   meta,
		"spec":       map[string]any{"rayVersion": "2.9.0"},
	}
	return ClusterDetail{ClusterSummary: ClusterSummary{Name: name, Namespace: namespace}, Raw: raw}
}

// TestClusterDeletePreviewReturnsFingerprint asserts an empty confirm (the preview
// call) returns a ConfirmRequiredError carrying the correct fingerprint derived
// from the live object, and no delete is recorded.
func TestClusterDeletePreviewReturnsFingerprint(t *testing.T) {
	t.Parallel()
	detail := seededCluster("default", "demo", "uid-123", false)
	deleter := &fakeDeleter{}
	svc, _ := newDeleteService(detail, deleter)

	err := svc.Delete(context.Background(), ClusterDeleteParams{Name: "demo"})
	var required *ConfirmRequiredError
	if !errors.As(err, &required) {
		t.Fatalf("Delete error = %v, want *ConfirmRequiredError", err)
	}
	want := Fingerprint(detail.Raw, OpDelete)
	if required.Fingerprint != want {
		t.Errorf("fingerprint = %q, want %q", required.Fingerprint, want)
	}
	if len(deleter.calls) != 0 {
		t.Errorf("deleter called %d times, want 0 (preview only)", len(deleter.calls))
	}
}

// TestClusterDeleteCommitWithMatchingConfirm asserts a matching confirm proceeds
// to delete and records the delete with the correct namespace/name.
func TestClusterDeleteCommitWithMatchingConfirm(t *testing.T) {
	t.Parallel()
	detail := seededCluster("default", "demo", "uid-123", false)
	deleter := &fakeDeleter{}
	svc, _ := newDeleteService(detail, deleter)

	fp := Fingerprint(detail.Raw, OpDelete)
	err := svc.Delete(context.Background(), ClusterDeleteParams{Name: "demo", Confirm: fp})
	if err != nil {
		t.Fatalf("Delete with matching confirm: %v", err)
	}
	if len(deleter.calls) != 1 {
		t.Fatalf("deleter calls = %d, want 1", len(deleter.calls))
	}
	if deleter.calls[0].namespace != "default" || deleter.calls[0].name != "demo" {
		t.Errorf("delete target = %s/%s, want default/demo", deleter.calls[0].namespace, deleter.calls[0].name)
	}
	if deleter.calls[0].kind != KindRayCluster {
		t.Errorf("delete kind = %s, want RayCluster", deleter.calls[0].kind)
	}
}

// TestClusterDeleteMismatchRejected asserts a wrong confirm is rejected with
// ConfirmMismatchError and no delete is recorded.
func TestClusterDeleteMismatchRejected(t *testing.T) {
	t.Parallel()
	detail := seededCluster("default", "demo", "uid-123", false)
	deleter := &fakeDeleter{}
	svc, _ := newDeleteService(detail, deleter)

	err := svc.Delete(context.Background(), ClusterDeleteParams{Name: "demo", Confirm: "wrong-fingerprint"})
	var mismatch *ConfirmMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("Delete error = %v, want *ConfirmMismatchError", err)
	}
	if len(deleter.calls) != 0 {
		t.Errorf("deleter called %d times, want 0 (mismatch rejected)", len(deleter.calls))
	}
}

// TestClusterDeleteProtectedRefused asserts a protected cluster refuses deletion
// (both preview and commit), and no delete is recorded. The protected check
// comes BEFORE confirm so a protected cluster never yields a working fingerprint.
func TestClusterDeleteProtectedRefused(t *testing.T) {
	t.Parallel()
	detail := seededCluster("default", "demo", "uid-456", true)
	deleter := &fakeDeleter{}
	svc, _ := newDeleteService(detail, deleter)

	// Preview (empty confirm) → protected error, not a fingerprint.
	err := svc.Delete(context.Background(), ClusterDeleteParams{Name: "demo"})
	if err == nil {
		t.Fatal("Delete (preview) on protected cluster returned nil, want error")
	}
	var required *ConfirmRequiredError
	if errors.As(err, &required) {
		t.Fatal("protected cluster returned a ConfirmRequiredError; the protected check must precede the confirm gate")
	}

	// Commit with a (would-be valid) fingerprint → still refused.
	fp := Fingerprint(detail.Raw, OpDelete)
	err = svc.Delete(context.Background(), ClusterDeleteParams{Name: "demo", Confirm: fp})
	if err == nil {
		t.Fatal("Delete (commit) on protected cluster returned nil, want error")
	}
	if len(deleter.calls) != 0 {
		t.Errorf("deleter called %d times, want 0 (protected refused)", len(deleter.calls))
	}
}

// TestClusterDeleteDryRunPassesThrough asserts a matching confirm with DryRun=true
// passes the dryRun flag to the Deleter.
func TestClusterDeleteDryRunPassesThrough(t *testing.T) {
	t.Parallel()
	detail := seededCluster("default", "demo", "uid-789", false)
	deleter := &fakeDeleter{}
	svc, _ := newDeleteService(detail, deleter)

	fp := Fingerprint(detail.Raw, OpDelete)
	err := svc.Delete(context.Background(), ClusterDeleteParams{Name: "demo", Confirm: fp, DryRun: true})
	if err != nil {
		t.Fatalf("Delete(dryRun): %v", err)
	}
	if len(deleter.calls) != 1 || !deleter.calls[0].dryRun {
		t.Fatalf("deleter calls = %+v, want one call with dryRun=true", deleter.calls)
	}
}

// TestClusterDeleteNotFound asserts a missing cluster propagates NotFoundError.
func TestClusterDeleteNotFound(t *testing.T) {
	t.Parallel()
	reader := &fakeReader{err: &NotFoundError{Kind: KindRayCluster, Namespace: "default", Name: "gone"}}
	sink := &recordingSink{}
	applySvc := NewApplyService(&fakeApplier{}, sink)
	deleter := &fakeDeleter{}
	svc := NewClusterWriteService(&fakeBaseBuilder{}, reader, deleter, applySvc, "default")

	err := svc.Delete(context.Background(), ClusterDeleteParams{Name: "gone"})
	var nf *NotFoundError
	if !errors.As(err, &nf) {
		t.Fatalf("Delete error = %v, want *NotFoundError", err)
	}
	if len(deleter.calls) != 0 {
		t.Errorf("deleter called %d times, want 0 (cluster not found)", len(deleter.calls))
	}
}

// TestClusterDeleteAuditEmitted asserts exactly one audit record is emitted on a
// successful delete (commit), with the correct tool name and outcome; and that a
// failing delete also emits one record with outcome=failure.
func TestClusterDeleteAuditEmitted(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()
		detail := seededCluster("default", "demo", "uid-audit", false)
		deleter := &fakeDeleter{}
		svc, sink := newDeleteService(detail, deleter)

		fp := Fingerprint(detail.Raw, OpDelete)
		if err := svc.Delete(context.Background(), ClusterDeleteParams{Name: "demo", Confirm: fp}); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		if len(sink.records) != 1 {
			t.Fatalf("audit records = %d, want 1", len(sink.records))
		}
		rec := sink.records[0]
		if rec.Tool != "ray_cluster_delete" {
			t.Errorf("audit Tool = %q, want ray_cluster_delete", rec.Tool)
		}
		if rec.Outcome != AuditOutcomeSuccess {
			t.Errorf("audit Outcome = %q, want success", rec.Outcome)
		}
		if rec.Kind != KindRayCluster || rec.Namespace != "default" || rec.Name != "demo" {
			t.Errorf("audit target = %s %s/%s, want RayCluster default/demo", rec.Kind, rec.Namespace, rec.Name)
		}
	})

	t.Run("failure", func(t *testing.T) {
		t.Parallel()
		detail := seededCluster("default", "demo", "uid-audit-fail", false)
		deleter := &fakeDeleter{err: errors.New("kube error")}
		svc, sink := newDeleteService(detail, deleter)

		fp := Fingerprint(detail.Raw, OpDelete)
		err := svc.Delete(context.Background(), ClusterDeleteParams{Name: "demo", Confirm: fp})
		if err == nil {
			t.Fatal("Delete returned nil, want the deleter error")
		}
		if len(sink.records) != 1 {
			t.Fatalf("audit records = %d, want 1", len(sink.records))
		}
		if sink.records[0].Outcome != AuditOutcomeFailure {
			t.Errorf("audit Outcome = %q, want failure", sink.records[0].Outcome)
		}
		if sink.records[0].Error == "" {
			t.Error("audit Error is empty, want the bounded failure message")
		}
	})
}
