package domain

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

// applyCall records one invocation of the fake Applier so tests can assert the
// pipeline's call sequence (the unconditional dry-run, then the commit).
type applyCall struct {
	kind      Kind
	namespace string
	name      string
	spec      MergedSpec
	dryRun    bool
}

// fakeApplier is a programmable Applier for the pure ApplyService tests. It
// records every call and returns a per-(dryRun) canned object or error, so a
// test can make the dry-run fail or return a distinct object while the commit
// returns the read-back.
type fakeApplier struct {
	calls []applyCall

	dryRunObj MergedSpec
	applyObj  MergedSpec
	dryRunErr error
	applyErr  error
}

func (f *fakeApplier) Apply(_ context.Context, kind Kind, namespace, name string, spec MergedSpec, dryRun bool) (MergedSpec, error) {
	f.calls = append(f.calls, applyCall{kind: kind, namespace: namespace, name: name, spec: spec, dryRun: dryRun})
	if dryRun {
		return f.dryRunObj, f.dryRunErr
	}
	return f.applyObj, f.applyErr
}

// recordingSink captures audit records for assertions.
type recordingSink struct {
	records []AuditRecord
}

func (s *recordingSink) Record(_ context.Context, rec AuditRecord) {
	s.records = append(s.records, rec)
}

// clusterSpec builds a minimal merged RayCluster object with a spec subtree the
// diff/pruning can operate on.
func clusterSpec(replicas int) MergedSpec {
	return MergedSpec{
		"apiVersion": "ray.io/v1",
		"kind":       "RayCluster",
		"metadata":   map[string]any{"name": "demo", "namespace": "ray"},
		"spec": map[string]any{
			"rayVersion": "2.9.0",
			"workerGroupSpecs": []any{
				map[string]any{"groupName": "wg", "replicas": int64(replicas)},
			},
		},
	}
}

func newApplyReq(spec MergedSpec, dryRun bool) ApplyRequest {
	return ApplyRequest{
		Kind:        KindRayCluster,
		Namespace:   "ray",
		Name:        "demo",
		Spec:        spec,
		DryRun:      dryRun,
		Tool:        "ray_cluster_create",
		ArgsSummary: "name=demo replicas=2",
	}
}

// TestApplyDryRunDoesNotCommit asserts a dryRun request hits ONLY the DryRunAll
// path (no second, committing Apply call) and reports DryRun=true. This is the
// "dryRun path performs DryRunAll only; no mutation" AC at the domain layer.
func TestApplyDryRunDoesNotCommit(t *testing.T) {
	t.Parallel()
	spec := clusterSpec(2)
	fake := &fakeApplier{dryRunObj: spec}
	svc := NewApplyService(fake, NopAuditSink{})

	res, err := svc.Apply(context.Background(), newApplyReq(spec, true))
	if err != nil {
		t.Fatalf("Apply(dryRun): %v", err)
	}
	if !res.DryRun {
		t.Errorf("result DryRun = false, want true")
	}
	if len(fake.calls) != 1 {
		t.Fatalf("made %d Applier calls, want exactly 1 (dry-run only)", len(fake.calls))
	}
	if !fake.calls[0].dryRun {
		t.Errorf("the single call was not a dry-run")
	}
}

// TestApplyCommitsAfterDryRun asserts a non-dryRun request runs the unconditional
// DryRunAll FIRST and then the committing apply — two calls, dry-run then commit,
// in that order — and returns DryRun=false. This is "always DryRunAll, then SSA".
func TestApplyCommitsAfterDryRun(t *testing.T) {
	t.Parallel()
	spec := clusterSpec(2)
	fake := &fakeApplier{dryRunObj: spec, applyObj: spec}
	svc := NewApplyService(fake, NopAuditSink{})

	res, err := svc.Apply(context.Background(), newApplyReq(spec, false))
	if err != nil {
		t.Fatalf("Apply(commit): %v", err)
	}
	if res.DryRun {
		t.Errorf("result DryRun = true, want false")
	}
	if len(fake.calls) != 2 {
		t.Fatalf("made %d Applier calls, want exactly 2 (dry-run then commit)", len(fake.calls))
	}
	if !fake.calls[0].dryRun {
		t.Errorf("first call must be the unconditional dry-run, got dryRun=%v", fake.calls[0].dryRun)
	}
	if fake.calls[1].dryRun {
		t.Errorf("second call must be the real apply, got dryRun=%v", fake.calls[1].dryRun)
	}
}

// TestApplyDryRunFailureSkipsCommit asserts that when the unconditional dry-run
// fails (e.g. schema validation), the pipeline returns that error and NEVER makes
// the committing call — the dry-run is the gate.
func TestApplyDryRunFailureSkipsCommit(t *testing.T) {
	t.Parallel()
	spec := clusterSpec(2)
	wantErr := errors.New("admission webhook denied the request")
	fake := &fakeApplier{dryRunErr: wantErr}
	svc := NewApplyService(fake, NopAuditSink{})

	_, err := svc.Apply(context.Background(), newApplyReq(spec, false))
	if !errors.Is(err, wantErr) {
		t.Fatalf("Apply error = %v, want the dry-run error", err)
	}
	if len(fake.calls) != 1 {
		t.Fatalf("made %d Applier calls, want 1 (dry-run failed → no commit)", len(fake.calls))
	}
}

// TestApplyReturnsReadBackDiff asserts the committing path diffs the submitted
// intent against the server read-back, scoped to spec: a replicas change shows up
// as one modified field with old→new values.
func TestApplyReturnsReadBackDiff(t *testing.T) {
	t.Parallel()
	intent := clusterSpec(2)
	// The server read-back reflects a different replica count (e.g. defaulted /
	// reconciled), which the diff must surface.
	readBack := clusterSpec(5)
	fake := &fakeApplier{dryRunObj: clusterSpec(2), applyObj: readBack}
	svc := NewApplyService(fake, NopAuditSink{})

	res, err := svc.Apply(context.Background(), newApplyReq(intent, false))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if res.Diff.Empty() {
		t.Fatalf("diff is empty, want the replicas change surfaced")
	}
	if got := res.Diff.FieldCount(); got != 1 {
		t.Errorf("diff FieldCount = %d, want 1 (replicas only)", got)
	}
	var found *FieldChange
	for i := range res.Diff.Changes {
		if res.Diff.Changes[i].Path == "workerGroupSpecs[0].replicas" {
			found = &res.Diff.Changes[i]
		}
	}
	if found == nil {
		t.Fatalf("diff did not pinpoint workerGroupSpecs[0].replicas; got %+v", res.Diff.Changes)
	}
	if found.Old != int64(2) || found.New != int64(5) {
		t.Errorf("replicas change = %v→%v, want 2→5", found.Old, found.New)
	}
}

// TestApplyEmitsAuditRecordOnSuccess asserts every committing apply emits exactly
// one success audit record carrying the tool, identity, kind/ns/name, and dryRun
// flag (the audit-every-mutation invariant, spec §8).
func TestApplyEmitsAuditRecordOnSuccess(t *testing.T) {
	t.Parallel()
	spec := clusterSpec(2)
	fake := &fakeApplier{dryRunObj: spec, applyObj: spec}
	sink := &recordingSink{}
	svc := NewApplyService(fake, sink)

	if _, err := svc.Apply(context.Background(), newApplyReq(spec, false)); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if len(sink.records) != 1 {
		t.Fatalf("emitted %d audit records, want exactly 1", len(sink.records))
	}
	rec := sink.records[0]
	if rec.Outcome != AuditOutcomeSuccess {
		t.Errorf("audit Outcome = %q, want success", rec.Outcome)
	}
	if rec.DryRun {
		t.Errorf("audit DryRun = true, want false for a committed apply")
	}
	if rec.Tool != "ray_cluster_create" {
		t.Errorf("audit Tool = %q, want ray_cluster_create", rec.Tool)
	}
	if rec.Kind != KindRayCluster || rec.Namespace != "ray" || rec.Name != "demo" {
		t.Errorf("audit target = %s %s/%s, want RayCluster ray/demo", rec.Kind, rec.Namespace, rec.Name)
	}
	if rec.Caller != DefaultCaller {
		t.Errorf("audit Caller = %q, want the stdio default %q", rec.Caller, DefaultCaller)
	}
}

// TestApplyEmitsAuditRecordOnFailure asserts a failed apply still emits exactly
// one audit record, marked failure with the bounded error — so "what did the
// agent attempt?" is answerable even when the write was rejected.
func TestApplyEmitsAuditRecordOnFailure(t *testing.T) {
	t.Parallel()
	spec := clusterSpec(2)
	fake := &fakeApplier{dryRunErr: errors.New("forbidden: cannot patch rayclusters")}
	sink := &recordingSink{}
	svc := NewApplyService(fake, sink)

	if _, err := svc.Apply(context.Background(), newApplyReq(spec, false)); err == nil {
		t.Fatal("Apply returned nil error, want the dry-run failure")
	}

	if len(sink.records) != 1 {
		t.Fatalf("emitted %d audit records, want exactly 1 (the failure)", len(sink.records))
	}
	rec := sink.records[0]
	if rec.Outcome != AuditOutcomeFailure {
		t.Errorf("audit Outcome = %q, want failure", rec.Outcome)
	}
	if rec.Error == "" {
		t.Errorf("audit Error is empty, want the bounded failure message")
	}
}

// TestApplyAuditCarriesContextCaller asserts the caller identity set on the
// context (the seam Task 24 fills for HTTP) flows into the audit record,
// overriding the stdio default.
func TestApplyAuditCarriesContextCaller(t *testing.T) {
	t.Parallel()
	spec := clusterSpec(2)
	fake := &fakeApplier{dryRunObj: spec, applyObj: spec}
	sink := &recordingSink{}
	svc := NewApplyService(fake, sink)

	ctx := WithCaller(context.Background(), "system:serviceaccount:ray:agent")
	if _, err := svc.Apply(ctx, newApplyReq(spec, false)); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if got := sink.records[0].Caller; got != "system:serviceaccount:ray:agent" {
		t.Errorf("audit Caller = %q, want the context-supplied SA", got)
	}
}

// TestApplyDoesNotMutateIntentSpec asserts the pipeline never mutates the
// caller's submitted spec map (it diffs against it after the Applier returns a
// possibly-aliased object). A defensive guard: the Applier contract is to not
// retain/alias, but the service must not corrupt intent either.
func TestApplyDoesNotMutateIntentSpec(t *testing.T) {
	t.Parallel()
	intent := clusterSpec(2)
	snapshot := cloneMap(intent)
	fake := &fakeApplier{dryRunObj: clusterSpec(5), applyObj: clusterSpec(5)}
	svc := NewApplyService(fake, NopAuditSink{})

	if _, err := svc.Apply(context.Background(), newApplyReq(intent, false)); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if !reflect.DeepEqual(map[string]any(intent), snapshot) {
		t.Errorf("Apply mutated the caller's intent spec:\n got  %v\n want %v", intent, snapshot)
	}
}

// TestApplyDiffsWholeObjectWhenNoSpec exercises the diff's graceful fallback:
// when neither the intent nor the read-back carries a spec object, diffSpec
// compares the whole objects rather than silently producing an empty diff. Here
// a top-level field changes and must still be surfaced.
func TestApplyDiffsWholeObjectWhenNoSpec(t *testing.T) {
	t.Parallel()
	intent := MergedSpec{"metadata": map[string]any{"name": "demo", "namespace": "ray"}, "topLevel": "before"}
	readBack := MergedSpec{"metadata": map[string]any{"name": "demo", "namespace": "ray"}, "topLevel": "after"}
	fake := &fakeApplier{dryRunObj: readBack, applyObj: readBack}
	svc := NewApplyService(fake, NopAuditSink{})

	res, err := svc.Apply(context.Background(), newApplyReq(intent, false))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if res.Diff.Empty() {
		t.Fatal("diff is empty; the whole-object fallback should surface the topLevel change")
	}
	var found bool
	for _, c := range res.Diff.Changes {
		if c.Path == "topLevel" && c.Old == "before" && c.New == "after" {
			found = true
		}
	}
	if !found {
		t.Errorf("diff did not surface topLevel before→after; got %+v", res.Diff.Changes)
	}
}

// TestNewApplyServiceNilSinkIsSafe asserts passing a nil sink does not panic and
// does not skip the apply — the constructor substitutes a no-op so the
// audit-every-mutation invariant has a sink to call.
func TestNewApplyServiceNilSinkIsSafe(t *testing.T) {
	t.Parallel()
	spec := clusterSpec(2)
	fake := &fakeApplier{dryRunObj: spec, applyObj: spec}
	svc := NewApplyService(fake, nil)

	if _, err := svc.Apply(context.Background(), newApplyReq(spec, false)); err != nil {
		t.Fatalf("Apply with nil sink: %v", err)
	}
}

// compile-time proof the production fake and the full port both satisfy Applier.
var (
	_ Applier = (*fakeApplier)(nil)
	_ Applier = (*fakeKubeRay)(nil)
)
