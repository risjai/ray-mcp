package domain

import (
	"context"
	"errors"
	"testing"
)

// The mode-aware RayJob delete unit suite (Task 19, Q16a). It proves the blast-
// radius tiering the spec mandates: an EPHEMERAL job (owns its cluster via
// spec.rayClusterSpec) deletes with cascade → destructive tier (AllowDestructive
// + confirm-fingerprint), while an EXISTING-CLUSTER job (spec.clusterSelector)
// just removes the record → plain write (no tier, no confirm). The both-set case
// is the safety-critical one: it must classify as existing/plain (clusterSelector
// wins, matching KubeRay), never mis-tier as a cascade.

// seededJob builds a JobDetail whose Raw carries the identity (uid for a
// non-trivial fingerprint), the protected annotation when asked, and the mode-
// defining spec field: spec.rayClusterSpec for ephemeral, spec.clusterSelector
// for existing-cluster.
func seededJob(namespace, name, uid string, ephemeral, protected bool) JobDetail {
	meta := map[string]any{"name": name, "namespace": namespace, "uid": uid}
	if protected {
		meta["annotations"] = map[string]any{ProtectedAnnotation: "true"}
	}
	spec := map[string]any{"entrypoint": "python main.py"}
	if ephemeral {
		spec["rayClusterSpec"] = map[string]any{"rayVersion": "2.9.0"}
	} else {
		spec["clusterSelector"] = map[string]any{"ray.io/cluster": name + "-cluster"}
	}
	raw := MergedSpec{
		"apiVersion": "ray.io/v1",
		"kind":       "RayJob",
		"metadata":   meta,
		"spec":       spec,
	}
	return JobDetail{JobSummary: JobSummary{Name: name, Namespace: namespace}, Raw: raw}
}

// newJobDeleteService wires a JobWriteService with a fake getter seeded with the
// given job (so GetJob returns it, and any other name is NotFound), a recording
// deleter, and a recording audit sink — mirroring newDeleteService for clusters.
func newJobDeleteService(detail JobDetail, deleter *fakeDeleter) (*JobWriteService, *recordingSink) {
	getter := &fakeKubeRay{jobs: map[string]JobDetail{key(detail.Namespace, detail.Name): detail}}
	sink := &recordingSink{}
	applySvc := NewApplyService(&fakeApplier{}, sink)
	svc := NewJobWriteService(&fakeJobBaseBuilder{}, getter, deleter, applySvc, "default")
	return svc, sink
}

// TestJobDeleteEphemeralPreviewReturnsFingerprint asserts an ephemeral job under
// the destructive tier with an empty confirm returns a ConfirmRequiredError
// carrying the correct fingerprint, and no delete is recorded.
func TestJobDeleteEphemeralPreviewReturnsFingerprint(t *testing.T) {
	t.Parallel()
	detail := seededJob("default", "ephem", "uid-1", true, false)
	deleter := &fakeDeleter{}
	svc, _ := newJobDeleteService(detail, deleter)

	err := svc.Delete(context.Background(), JobDeleteParams{Name: "ephem", AllowDestructive: true})
	var required *ConfirmRequiredError
	if !errors.As(err, &required) {
		t.Fatalf("Delete error = %v, want *ConfirmRequiredError", err)
	}
	if want := Fingerprint(detail.Raw, OpDelete); required.Fingerprint != want {
		t.Errorf("fingerprint = %q, want %q", required.Fingerprint, want)
	}
	if len(deleter.calls) != 0 {
		t.Errorf("deleter called %d times, want 0 (preview only)", len(deleter.calls))
	}
}

// TestJobDeleteEphemeralCommitWithMatchingConfirm asserts a matching confirm
// commits the cascade delete keyed by KindRayJob.
func TestJobDeleteEphemeralCommitWithMatchingConfirm(t *testing.T) {
	t.Parallel()
	detail := seededJob("default", "ephem", "uid-1", true, false)
	deleter := &fakeDeleter{}
	svc, _ := newJobDeleteService(detail, deleter)

	fp := Fingerprint(detail.Raw, OpDelete)
	if err := svc.Delete(context.Background(), JobDeleteParams{Name: "ephem", Confirm: fp, AllowDestructive: true}); err != nil {
		t.Fatalf("Delete with matching confirm: %v", err)
	}
	if len(deleter.calls) != 1 {
		t.Fatalf("deleter calls = %d, want 1", len(deleter.calls))
	}
	if deleter.calls[0].kind != KindRayJob || deleter.calls[0].namespace != "default" || deleter.calls[0].name != "ephem" {
		t.Errorf("delete target = %s %s/%s, want RayJob default/ephem", deleter.calls[0].kind, deleter.calls[0].namespace, deleter.calls[0].name)
	}
}

// TestJobDeleteEphemeralMismatchRejected asserts a wrong confirm on an ephemeral
// job is a ConfirmMismatchError and records no delete.
func TestJobDeleteEphemeralMismatchRejected(t *testing.T) {
	t.Parallel()
	detail := seededJob("default", "ephem", "uid-1", true, false)
	deleter := &fakeDeleter{}
	svc, _ := newJobDeleteService(detail, deleter)

	err := svc.Delete(context.Background(), JobDeleteParams{Name: "ephem", Confirm: "wrong", AllowDestructive: true})
	var mismatch *ConfirmMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("Delete error = %v, want *ConfirmMismatchError", err)
	}
	if len(deleter.calls) != 0 {
		t.Errorf("deleter called %d times, want 0 (mismatch rejected)", len(deleter.calls))
	}
}

// TestJobDeleteEphemeralRequiresAllowDestructive asserts an ephemeral job is
// refused without the destructive tier — BEFORE any fingerprint is minted (so the
// refusal is the tier gate, not a confirm preview), and a would-be-valid confirm
// is still refused. No delete is recorded either way.
func TestJobDeleteEphemeralRequiresAllowDestructive(t *testing.T) {
	t.Parallel()
	detail := seededJob("default", "ephem", "uid-1", true, false)
	deleter := &fakeDeleter{}
	svc, _ := newJobDeleteService(detail, deleter)

	// No tier, empty confirm → refused, and NOT a ConfirmRequiredError preview.
	err := svc.Delete(context.Background(), JobDeleteParams{Name: "ephem", AllowDestructive: false})
	if err == nil {
		t.Fatal("ephemeral Delete without --allow-destructive returned nil, want a refusal")
	}
	var required *ConfirmRequiredError
	if errors.As(err, &required) {
		t.Fatal("ephemeral Delete without the tier yielded a ConfirmRequiredError; the tier gate must precede the confirm preview")
	}

	// No tier, with a (would-be valid) confirm → still refused.
	fp := Fingerprint(detail.Raw, OpDelete)
	if err := svc.Delete(context.Background(), JobDeleteParams{Name: "ephem", Confirm: fp, AllowDestructive: false}); err == nil {
		t.Fatal("ephemeral Delete with confirm but without the tier returned nil, want a refusal")
	}
	if len(deleter.calls) != 0 {
		t.Errorf("deleter called %d times, want 0 (tier gate refused)", len(deleter.calls))
	}
}

// TestJobDeleteEphemeralProtectedRefusedBeforeConfirm asserts a protected
// ephemeral job is refused before the confirm gate — never yielding a fingerprint
// — even with the destructive tier enabled.
func TestJobDeleteEphemeralProtectedRefusedBeforeConfirm(t *testing.T) {
	t.Parallel()
	detail := seededJob("default", "ephem", "uid-2", true, true)
	deleter := &fakeDeleter{}
	svc, _ := newJobDeleteService(detail, deleter)

	err := svc.Delete(context.Background(), JobDeleteParams{Name: "ephem", AllowDestructive: true})
	if err == nil {
		t.Fatal("protected ephemeral Delete returned nil, want a refusal")
	}
	var required *ConfirmRequiredError
	if errors.As(err, &required) {
		t.Fatal("protected job yielded a ConfirmRequiredError; the protected check must precede the confirm gate")
	}
	if len(deleter.calls) != 0 {
		t.Errorf("deleter called %d times, want 0 (protected refused)", len(deleter.calls))
	}
}

// TestJobDeleteExistingClusterPlainDelete asserts an existing-cluster job deletes
// immediately as a plain write: no destructive tier, no confirm fingerprint
// required. It only removes the RayJob record (the targeted cluster is untouched).
func TestJobDeleteExistingClusterPlainDelete(t *testing.T) {
	t.Parallel()
	detail := seededJob("default", "attached", "uid-3", false, false)
	deleter := &fakeDeleter{}
	svc, _ := newJobDeleteService(detail, deleter)

	// Empty confirm, no tier → deletes straight away (plain write).
	if err := svc.Delete(context.Background(), JobDeleteParams{Name: "attached", AllowDestructive: false}); err != nil {
		t.Fatalf("existing-cluster Delete: %v", err)
	}
	if len(deleter.calls) != 1 {
		t.Fatalf("deleter calls = %d, want 1 (plain delete)", len(deleter.calls))
	}
	if deleter.calls[0].kind != KindRayJob || deleter.calls[0].name != "attached" {
		t.Errorf("delete target = %s %s, want RayJob attached", deleter.calls[0].kind, deleter.calls[0].name)
	}
}

// TestJobDeleteBothTargetsTreatedAsExisting is the safety-critical classification:
// a RayJob with BOTH rayClusterSpec and clusterSelector set must be treated as
// existing-cluster (plain write, no tier) — clusterSelector wins, mirroring
// KubeRay, so an attached job is NEVER mis-tiered as an ephemeral cascade.
func TestJobDeleteBothTargetsTreatedAsExisting(t *testing.T) {
	t.Parallel()
	detail := seededJob("default", "both", "uid-4", false, false)
	// Add an ephemeral rayClusterSpec ON TOP of the existing clusterSelector.
	spec := detail.Raw["spec"].(map[string]any)
	spec["rayClusterSpec"] = map[string]any{"rayVersion": "2.9.0"}

	deleter := &fakeDeleter{}
	svc, _ := newJobDeleteService(detail, deleter)

	// No tier, empty confirm → must delete as a plain write (existing wins).
	if err := svc.Delete(context.Background(), JobDeleteParams{Name: "both", AllowDestructive: false}); err != nil {
		t.Fatalf("both-targets Delete: %v (must classify as existing/plain, not a gated cascade)", err)
	}
	if len(deleter.calls) != 1 {
		t.Fatalf("deleter calls = %d, want 1 (both-set classifies as plain existing-cluster delete)", len(deleter.calls))
	}
}

// TestJobDeleteExistingClusterProtectedRefused asserts the protected guard applies
// regardless of mode: a protected existing-cluster job is refused even though it
// is otherwise a plain write.
func TestJobDeleteExistingClusterProtectedRefused(t *testing.T) {
	t.Parallel()
	detail := seededJob("default", "attached", "uid-5", false, true)
	deleter := &fakeDeleter{}
	svc, _ := newJobDeleteService(detail, deleter)

	if err := svc.Delete(context.Background(), JobDeleteParams{Name: "attached"}); err == nil {
		t.Fatal("protected existing-cluster Delete returned nil, want a refusal")
	}
	if len(deleter.calls) != 0 {
		t.Errorf("deleter called %d times, want 0 (protected refused)", len(deleter.calls))
	}
}

// TestJobDeleteDryRunPassesThrough asserts the dryRun flag reaches the Deleter on
// the plain existing-cluster path.
func TestJobDeleteDryRunPassesThrough(t *testing.T) {
	t.Parallel()
	detail := seededJob("default", "attached", "uid-6", false, false)
	deleter := &fakeDeleter{}
	svc, _ := newJobDeleteService(detail, deleter)

	if err := svc.Delete(context.Background(), JobDeleteParams{Name: "attached", DryRun: true}); err != nil {
		t.Fatalf("Delete(dryRun): %v", err)
	}
	if len(deleter.calls) != 1 || !deleter.calls[0].dryRun {
		t.Fatalf("deleter calls = %+v, want one call with dryRun=true", deleter.calls)
	}
}

// TestJobDeleteNotFound asserts a missing job propagates NotFoundError and records
// no delete.
func TestJobDeleteNotFound(t *testing.T) {
	t.Parallel()
	detail := seededJob("default", "present", "uid-7", false, false)
	deleter := &fakeDeleter{}
	svc, _ := newJobDeleteService(detail, deleter)

	err := svc.Delete(context.Background(), JobDeleteParams{Name: "gone"})
	var nf *NotFoundError
	if !errors.As(err, &nf) {
		t.Fatalf("Delete error = %v, want *NotFoundError", err)
	}
	if len(deleter.calls) != 0 {
		t.Errorf("deleter called %d times, want 0 (job not found)", len(deleter.calls))
	}
}

// TestJobDeleteAuditEmitted asserts exactly one audit record tagged ray_job_delete
// is emitted on success, and one with outcome=failure when the deleter errors.
func TestJobDeleteAuditEmitted(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()
		detail := seededJob("default", "attached", "uid-ok", false, false)
		deleter := &fakeDeleter{}
		svc, sink := newJobDeleteService(detail, deleter)

		if err := svc.Delete(context.Background(), JobDeleteParams{Name: "attached"}); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		if len(sink.records) != 1 {
			t.Fatalf("audit records = %d, want 1", len(sink.records))
		}
		rec := sink.records[0]
		if rec.Tool != "ray_job_delete" {
			t.Errorf("audit Tool = %q, want ray_job_delete", rec.Tool)
		}
		if rec.Outcome != AuditOutcomeSuccess {
			t.Errorf("audit Outcome = %q, want success", rec.Outcome)
		}
		if rec.Kind != KindRayJob || rec.Namespace != "default" || rec.Name != "attached" {
			t.Errorf("audit target = %s %s/%s, want RayJob default/attached", rec.Kind, rec.Namespace, rec.Name)
		}
	})

	t.Run("failure", func(t *testing.T) {
		t.Parallel()
		detail := seededJob("default", "attached", "uid-fail", false, false)
		deleter := &fakeDeleter{err: errors.New("kube error")}
		svc, sink := newJobDeleteService(detail, deleter)

		if err := svc.Delete(context.Background(), JobDeleteParams{Name: "attached"}); err == nil {
			t.Fatal("Delete returned nil, want the deleter error")
		}
		if len(sink.records) != 1 {
			t.Fatalf("audit records = %d, want 1", len(sink.records))
		}
		if sink.records[0].Outcome != AuditOutcomeFailure {
			t.Errorf("audit Outcome = %q, want failure", sink.records[0].Outcome)
		}
	})
}
