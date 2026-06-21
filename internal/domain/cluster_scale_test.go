package domain

import (
	"context"
	"errors"
	"testing"
)

// liveCluster builds the live full-object map an adapter GetCluster returns under
// .Raw: identity metadata + a spec with one worker group, optionally autoscaling.
// It mirrors what the create path persisted, including server-shaped noise
// (status, a managedFields stub) so the read-modify-apply-full path is exercised
// against a realistic object and proven to strip that noise before applying.
func liveCluster(namespace, name string, autoscaling bool, group string, replicas, minR, maxR int64) ClusterDetail {
	raw := MergedSpec{
		"apiVersion": "ray.io/v1",
		"kind":       "RayCluster",
		"metadata": map[string]any{
			"name":            name,
			"namespace":       namespace,
			"resourceVersion": "12345",
			"managedFields":   []any{map[string]any{"manager": "ray-mcp"}},
			"labels":          map[string]any{"team": "ml"},
		},
		"spec": map[string]any{
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
					"groupName":   group,
					"replicas":    replicas,
					"minReplicas": minR,
					"maxReplicas": maxR,
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
		"status": map[string]any{"state": "ready"},
	}
	if autoscaling {
		raw["spec"].(map[string]any)["enableInTreeAutoscaling"] = true
	}
	return ClusterDetail{ClusterSummary: ClusterSummary{Name: name, Namespace: namespace}, Raw: raw}
}

// fakeReader is a programmable ClusterGetter: it returns a canned live object (or
// error) and records the get target, so a scale/update test can assert the
// read-modify-apply-full read happened against the resolved identity.
type fakeReader struct {
	detail  ClusterDetail
	err     error
	gotNS   string
	gotName string
}

func (f *fakeReader) GetCluster(_ context.Context, namespace, name string) (ClusterDetail, error) {
	f.gotNS, f.gotName = namespace, name
	if f.err != nil {
		return ClusterDetail{}, f.err
	}
	return f.detail, nil
}

// newScaleService wires a write service with a fake reader + applier for the
// scale/update unit tests. The base builder is unused by update/scale (they
// read-modify the live object), so a no-op builder is fine.
func newScaleService(reader ClusterGetter, applier Applier, defaultNS string) (*ClusterWriteService, *fakeApplier, *recordingSink) {
	fa, _ := applier.(*fakeApplier)
	sink := &recordingSink{}
	svc := NewClusterWriteService(&fakeBaseBuilder{}, reader, NewApplyService(applier, sink), defaultNS)
	return svc, fa, sink
}

func i32(v int32) *int32 { return &v }

// appliedSpec returns the spec subtree of the last committed apply call.
func appliedSpec(t *testing.T, fa *fakeApplier) map[string]any {
	t.Helper()
	if len(fa.calls) == 0 {
		t.Fatal("no applier calls recorded")
	}
	spec, ok := fa.calls[len(fa.calls)-1].spec["spec"].(map[string]any)
	if !ok {
		t.Fatalf("last applied object has no spec map: %+v", fa.calls[len(fa.calls)-1].spec)
	}
	return spec
}

// firstWorkerGroup returns spec.workerGroupSpecs[0] of the last applied object.
func firstWorkerGroup(t *testing.T, fa *fakeApplier) map[string]any {
	t.Helper()
	groups, ok := appliedSpec(t, fa)["workerGroupSpecs"].([]any)
	if !ok || len(groups) == 0 {
		t.Fatalf("applied spec has no worker groups: %+v", appliedSpec(t, fa))
	}
	wg, _ := groups[0].(map[string]any)
	return wg
}

// TestScaleSetsMinMaxReadModifyApplyFull is the core scale AC: a non-autoscaling
// cluster's worker group min/max are changed by reading the live object,
// modifying only those fields, and applying the FULL object back (so SSA does not
// prune the rest of the spec ray-mcp owns).
func TestScaleSetsMinMaxReadModifyApplyFull(t *testing.T) {
	t.Parallel()
	reader := &fakeReader{detail: liveCluster("ray", "demo", false, "workers", 2, 0, 5)}
	svc, fa, _ := newScaleService(reader, &fakeApplier{applyObj: MergedSpec{}}, "ray")

	_, err := svc.Scale(context.Background(), ClusterScaleParams{
		Name: "demo", WorkerGroup: "workers", MinReplicas: i32(1), MaxReplicas: i32(8),
	})
	if err != nil {
		t.Fatalf("Scale: %v", err)
	}
	// Read targeted the resolved identity.
	if reader.gotNS != "ray" || reader.gotName != "demo" {
		t.Errorf("read target = %s/%s, want ray/demo", reader.gotNS, reader.gotName)
	}
	wg := firstWorkerGroup(t, fa)
	if wg["minReplicas"] != int64(1) || wg["maxReplicas"] != int64(8) {
		t.Errorf("applied min/max = %v/%v, want 1/8", wg["minReplicas"], wg["maxReplicas"])
	}
	// The rest of the spec ray-mcp owns survived (apply-FULL, no pruning): the head
	// group and rayVersion are still present.
	if appliedSpec(t, fa)["rayVersion"] != "2.9.0" {
		t.Error("rayVersion missing from applied object; scale must apply the FULL spec, not a partial")
	}
	if _, ok := appliedSpec(t, fa)["headGroupSpec"]; !ok {
		t.Error("headGroupSpec missing from applied object; scale must not prune unrelated fields")
	}
}

// TestScaleStripsServerNoiseBeforeApply asserts the read-modify-apply path sends a
// clean intent: status, managedFields, and resourceVersion from the live read are
// NOT carried into the SSA apply body (they would be invalid / cause ownership
// churn). Identity + labels ray-mcp owns are preserved.
func TestScaleStripsServerNoiseBeforeApply(t *testing.T) {
	t.Parallel()
	reader := &fakeReader{detail: liveCluster("ray", "demo", false, "workers", 2, 0, 5)}
	svc, fa, _ := newScaleService(reader, &fakeApplier{applyObj: MergedSpec{}}, "ray")

	if _, err := svc.Scale(context.Background(), ClusterScaleParams{
		Name: "demo", WorkerGroup: "workers", MaxReplicas: i32(9),
	}); err != nil {
		t.Fatalf("Scale: %v", err)
	}
	obj := fa.calls[len(fa.calls)-1].spec
	if _, ok := obj["status"]; ok {
		t.Error("applied object carries status; the apply intent must drop status")
	}
	meta, _ := obj["metadata"].(map[string]any)
	if meta == nil {
		t.Fatal("applied object has no metadata")
	}
	if _, ok := meta["managedFields"]; ok {
		t.Error("applied object metadata carries managedFields; must be dropped from the SSA body")
	}
	if _, ok := meta["resourceVersion"]; ok {
		t.Error("applied object metadata carries resourceVersion; apply must not pin it")
	}
	if meta["name"] != "demo" || meta["namespace"] != "ray" {
		t.Errorf("applied identity = %v/%v, want ray/demo", meta["namespace"], meta["name"])
	}
}

// TestScaleRefusesReplicasUnderAutoscaling is the autoscaler-safety AC: on an
// autoscaling cluster, setting replicas is refused with an actionable error (the
// autoscaler owns the live replica count; min/max are the knobs) and NOTHING is
// applied.
func TestScaleRefusesReplicasUnderAutoscaling(t *testing.T) {
	t.Parallel()
	reader := &fakeReader{detail: liveCluster("ray", "demo", true, "workers", 2, 0, 5)}
	svc, fa, _ := newScaleService(reader, &fakeApplier{}, "ray")

	_, err := svc.Scale(context.Background(), ClusterScaleParams{
		Name: "demo", WorkerGroup: "workers", Replicas: i32(4),
	})
	if err == nil {
		t.Fatal("Scale replicas on an autoscaling cluster returned nil error; it must be refused")
	}
	if len(fa.calls) != 0 {
		t.Errorf("applier called %d times, want 0 (refused before apply)", len(fa.calls))
	}
}

// TestScaleAllowsMinMaxUnderAutoscaling asserts min/max ARE settable on an
// autoscaling cluster (those are the autoscaler's bounds, which ray-mcp owns) —
// only replicas is refused.
func TestScaleAllowsMinMaxUnderAutoscaling(t *testing.T) {
	t.Parallel()
	reader := &fakeReader{detail: liveCluster("ray", "demo", true, "workers", 2, 1, 5)}
	svc, fa, _ := newScaleService(reader, &fakeApplier{applyObj: MergedSpec{}}, "ray")

	if _, err := svc.Scale(context.Background(), ClusterScaleParams{
		Name: "demo", WorkerGroup: "workers", MinReplicas: i32(2), MaxReplicas: i32(10),
	}); err != nil {
		t.Fatalf("Scale min/max under autoscaling: %v", err)
	}
	wg := firstWorkerGroup(t, fa)
	if wg["minReplicas"] != int64(2) || wg["maxReplicas"] != int64(10) {
		t.Errorf("applied min/max = %v/%v, want 2/10", wg["minReplicas"], wg["maxReplicas"])
	}
}

// TestScaleRejectsMinAboveMax is the client-side guard the server-side DryRunAll
// does NOT enforce in KubeRay v1.6.1 (min<=max is checked in the operator
// reconcile, not an admission webhook). A min>max scale is rejected before apply.
func TestScaleRejectsMinAboveMax(t *testing.T) {
	t.Parallel()
	reader := &fakeReader{detail: liveCluster("ray", "demo", false, "workers", 2, 0, 5)}
	svc, fa, _ := newScaleService(reader, &fakeApplier{}, "ray")

	_, err := svc.Scale(context.Background(), ClusterScaleParams{
		Name: "demo", WorkerGroup: "workers", MinReplicas: i32(6), MaxReplicas: i32(3),
	})
	if err == nil {
		t.Fatal("Scale with minReplicas > maxReplicas returned nil; it must be rejected client-side")
	}
	if len(fa.calls) != 0 {
		t.Errorf("applier called %d times, want 0 (rejected before apply)", len(fa.calls))
	}
}

// TestScaleRejectsMinAboveExistingMax catches the case where only minReplicas is
// supplied and it exceeds the group's EXISTING maxReplicas (the effective max
// after the change). The guard must consider the merged result, not just the args.
func TestScaleRejectsMinAboveExistingMax(t *testing.T) {
	t.Parallel()
	reader := &fakeReader{detail: liveCluster("ray", "demo", false, "workers", 2, 0, 5)}
	svc, fa, _ := newScaleService(reader, &fakeApplier{}, "ray")

	_, err := svc.Scale(context.Background(), ClusterScaleParams{
		Name: "demo", WorkerGroup: "workers", MinReplicas: i32(9), // existing max is 5
	})
	if err == nil {
		t.Fatal("Scale minReplicas=9 against existing maxReplicas=5 returned nil; must reject")
	}
	if len(fa.calls) != 0 {
		t.Errorf("applier called %d times, want 0", len(fa.calls))
	}
}

// TestScaleRejectsReplicasAboveMax is the create-path-parity guard: setting
// replicas above the group's effective maxReplicas is rejected client-side (the
// create path clamps max>=replicas; scale must not silently commit an object that
// violates that invariant, since DryRunAll won't catch it on a default install).
func TestScaleRejectsReplicasAboveMax(t *testing.T) {
	t.Parallel()
	reader := &fakeReader{detail: liveCluster("ray", "demo", false, "workers", 2, 0, 5)}
	svc, fa, _ := newScaleService(reader, &fakeApplier{}, "ray")

	_, err := svc.Scale(context.Background(), ClusterScaleParams{
		Name: "demo", WorkerGroup: "workers", Replicas: i32(10), // existing max is 5
	})
	if err == nil {
		t.Fatal("Scale replicas=10 against maxReplicas=5 returned nil; must reject (replicas <= max)")
	}
	if len(fa.calls) != 0 {
		t.Errorf("applier called %d times, want 0", len(fa.calls))
	}
}

// TestScaleReplicasWithRaisedMax asserts replicas above the OLD max is allowed when
// the same call also raises maxReplicas to cover it (the effective bounds are
// evaluated after the overlay, not against the stale live max).
func TestScaleReplicasWithRaisedMax(t *testing.T) {
	t.Parallel()
	reader := &fakeReader{detail: liveCluster("ray", "demo", false, "workers", 2, 0, 5)}
	svc, fa, _ := newScaleService(reader, &fakeApplier{applyObj: MergedSpec{}}, "ray")

	if _, err := svc.Scale(context.Background(), ClusterScaleParams{
		Name: "demo", WorkerGroup: "workers", Replicas: i32(10), MaxReplicas: i32(12),
	}); err != nil {
		t.Fatalf("Scale replicas=10 with maxReplicas=12: %v", err)
	}
	wg := firstWorkerGroup(t, fa)
	if wg["replicas"] != int64(10) || wg["maxReplicas"] != int64(12) {
		t.Errorf("applied replicas/max = %v/%v, want 10/12", wg["replicas"], wg["maxReplicas"])
	}
}

// TestScaleToZeroNeedsDestructive is the B3 gate at the domain layer: a
// scale-to-zero (replicas=0) on a non-autoscaling cluster is refused unless
// AllowDestructive is set — nothing is applied.
func TestScaleToZeroNeedsDestructive(t *testing.T) {
	t.Parallel()
	reader := &fakeReader{detail: liveCluster("ray", "demo", false, "workers", 2, 0, 5)}
	svc, fa, _ := newScaleService(reader, &fakeApplier{}, "ray")

	_, err := svc.Scale(context.Background(), ClusterScaleParams{
		Name: "demo", WorkerGroup: "workers", Replicas: i32(0), AllowDestructive: false,
	})
	if err == nil {
		t.Fatal("scale-to-zero without AllowDestructive returned nil; it must be refused (B3)")
	}
	if len(fa.calls) != 0 {
		t.Errorf("applier called %d times, want 0 (scale-to-zero gated)", len(fa.calls))
	}
}

// TestScaleToZeroProceedsWithDestructive asserts the same scale-to-zero proceeds
// when AllowDestructive is set (the confirm-fingerprint itself lands in Task 11).
func TestScaleToZeroProceedsWithDestructive(t *testing.T) {
	t.Parallel()
	reader := &fakeReader{detail: liveCluster("ray", "demo", false, "workers", 2, 0, 5)}
	svc, fa, _ := newScaleService(reader, &fakeApplier{applyObj: MergedSpec{}}, "ray")

	if _, err := svc.Scale(context.Background(), ClusterScaleParams{
		Name: "demo", WorkerGroup: "workers", Replicas: i32(0), AllowDestructive: true,
	}); err != nil {
		t.Fatalf("scale-to-zero with AllowDestructive: %v", err)
	}
	if firstWorkerGroup(t, fa)["replicas"] != int64(0) {
		t.Errorf("applied replicas = %v, want 0", firstWorkerGroup(t, fa)["replicas"])
	}
}

// TestNonZeroScaleIsPlainWrite asserts a non-zero replicas scale on a
// non-autoscaling cluster does NOT require AllowDestructive (B3: only scale-to-zero
// is destructive; a non-zero scale stays a plain write).
func TestNonZeroScaleIsPlainWrite(t *testing.T) {
	t.Parallel()
	reader := &fakeReader{detail: liveCluster("ray", "demo", false, "workers", 2, 0, 5)}
	svc, fa, _ := newScaleService(reader, &fakeApplier{applyObj: MergedSpec{}}, "ray")

	if _, err := svc.Scale(context.Background(), ClusterScaleParams{
		Name: "demo", WorkerGroup: "workers", Replicas: i32(3), AllowDestructive: false,
	}); err != nil {
		t.Fatalf("non-zero scale should be a plain write: %v", err)
	}
	if firstWorkerGroup(t, fa)["replicas"] != int64(3) {
		t.Errorf("applied replicas = %v, want 3", firstWorkerGroup(t, fa)["replicas"])
	}
}

// TestScaleUnknownWorkerGroup asserts scaling a group that does not exist on the
// live cluster is an actionable error, not a silent no-op or a new group.
func TestScaleUnknownWorkerGroup(t *testing.T) {
	t.Parallel()
	reader := &fakeReader{detail: liveCluster("ray", "demo", false, "workers", 2, 0, 5)}
	svc, fa, _ := newScaleService(reader, &fakeApplier{}, "ray")

	_, err := svc.Scale(context.Background(), ClusterScaleParams{
		Name: "demo", WorkerGroup: "does-not-exist", MaxReplicas: i32(3),
	})
	if err == nil {
		t.Fatal("scaling an unknown worker group returned nil; it must be an error")
	}
	if len(fa.calls) != 0 {
		t.Errorf("applier called %d times, want 0", len(fa.calls))
	}
}

// TestScaleReadErrorStops asserts a failed live read (e.g. NotFound) stops the
// scale before any apply, surfacing the read error.
func TestScaleReadErrorStops(t *testing.T) {
	t.Parallel()
	reader := &fakeReader{err: &NotFoundError{Kind: KindRayCluster, Namespace: "ray", Name: "demo"}}
	svc, fa, _ := newScaleService(reader, &fakeApplier{}, "ray")

	_, err := svc.Scale(context.Background(), ClusterScaleParams{Name: "demo", WorkerGroup: "workers", MaxReplicas: i32(3)})
	var nf *NotFoundError
	if !errors.As(err, &nf) {
		t.Fatalf("Scale error = %v, want the read NotFoundError", err)
	}
	if len(fa.calls) != 0 {
		t.Errorf("applier called %d times, want 0 (read failed)", len(fa.calls))
	}
}

// TestScaleRequiresName asserts an empty name is rejected before any read.
func TestScaleRequiresName(t *testing.T) {
	t.Parallel()
	reader := &fakeReader{detail: liveCluster("ray", "demo", false, "workers", 2, 0, 5)}
	svc, _, _ := newScaleService(reader, &fakeApplier{}, "ray")

	if _, err := svc.Scale(context.Background(), ClusterScaleParams{WorkerGroup: "workers", MaxReplicas: i32(3)}); err == nil {
		t.Fatal("Scale with empty name returned nil error, want a validation error")
	}
}

// TestScaleRequiresWorkerGroup asserts an empty worker-group name is rejected.
func TestScaleRequiresWorkerGroup(t *testing.T) {
	t.Parallel()
	reader := &fakeReader{detail: liveCluster("ray", "demo", false, "workers", 2, 0, 5)}
	svc, _, _ := newScaleService(reader, &fakeApplier{}, "ray")

	if _, err := svc.Scale(context.Background(), ClusterScaleParams{Name: "demo", MaxReplicas: i32(3)}); err == nil {
		t.Fatal("Scale with empty worker group returned nil error, want a validation error")
	}
}

// TestScaleDryRunDoesNotCommit asserts dryRun flows to the apply pipeline (one
// dry-run call, no commit).
func TestScaleDryRunDoesNotCommit(t *testing.T) {
	t.Parallel()
	reader := &fakeReader{detail: liveCluster("ray", "demo", false, "workers", 2, 0, 5)}
	svc, fa, _ := newScaleService(reader, &fakeApplier{dryRunObj: MergedSpec{}}, "ray")

	res, err := svc.Scale(context.Background(), ClusterScaleParams{
		Name: "demo", WorkerGroup: "workers", MaxReplicas: i32(7), DryRun: true,
	})
	if err != nil {
		t.Fatalf("Scale(dryRun): %v", err)
	}
	if !res.DryRun {
		t.Error("result DryRun = false, want true")
	}
	if len(fa.calls) != 1 || !fa.calls[0].dryRun {
		t.Fatalf("applier calls = %+v, want exactly one dry-run", fa.calls)
	}
}
