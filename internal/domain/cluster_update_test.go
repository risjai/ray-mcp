package domain

import (
	"context"
	"errors"
	"testing"
)

// headImage returns the head container image of the last applied object.
func headImage(t *testing.T, fa *fakeApplier) string {
	t.Helper()
	spec := appliedSpec(t, fa)
	head, _ := spec["headGroupSpec"].(map[string]any)
	tmpl, _ := head["template"].(map[string]any)
	podSpec, _ := tmpl["spec"].(map[string]any)
	containers, _ := podSpec["containers"].([]any)
	if len(containers) == 0 {
		t.Fatalf("applied head group has no containers: %+v", head)
	}
	c0, _ := containers[0].(map[string]any)
	img, _ := c0["image"].(string)
	return img
}

// TestUpdateChangesImageReadModifyApplyFull is the core update AC: changing the
// image reads the live object, overlays the new image onto head + worker
// containers, and applies the FULL object (no pruning of the rest of the spec).
func TestUpdateChangesImageReadModifyApplyFull(t *testing.T) {
	t.Parallel()
	reader := &fakeReader{detail: liveCluster("ray", "demo", false, "workers", 2, 0, 5)}
	svc, fa, _ := newScaleService(reader, &fakeApplier{applyObj: MergedSpec{}}, "ray")

	_, err := svc.Update(context.Background(), ClusterUpdateParams{
		Name: "demo", Image: "rayproject/ray:2.40.0",
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if reader.gotName != "demo" {
		t.Errorf("read target name = %q, want demo", reader.gotName)
	}
	if got := headImage(t, fa); got != "rayproject/ray:2.40.0" {
		t.Errorf("applied head image = %q, want the updated image", got)
	}
	// The worker container image was updated too.
	wg := firstWorkerGroup(t, fa)
	tmpl, _ := wg["template"].(map[string]any)
	podSpec, _ := tmpl["spec"].(map[string]any)
	containers, _ := podSpec["containers"].([]any)
	c0, _ := containers[0].(map[string]any)
	if c0["image"] != "rayproject/ray:2.40.0" {
		t.Errorf("applied worker image = %v, want the updated image", c0["image"])
	}
	// Full apply: rayVersion (unchanged) still present.
	if appliedSpec(t, fa)["rayVersion"] != "2.9.0" {
		t.Error("rayVersion missing; update must apply the FULL spec, not a partial")
	}
}

// TestUpdateChangesRayVersion asserts a rayVersion-only update overlays the new
// version and applies the full object.
func TestUpdateChangesRayVersion(t *testing.T) {
	t.Parallel()
	reader := &fakeReader{detail: liveCluster("ray", "demo", false, "workers", 2, 0, 5)}
	svc, fa, _ := newScaleService(reader, &fakeApplier{applyObj: MergedSpec{}}, "ray")

	if _, err := svc.Update(context.Background(), ClusterUpdateParams{Name: "demo", RayVersion: "2.40.0"}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if appliedSpec(t, fa)["rayVersion"] != "2.40.0" {
		t.Errorf("applied rayVersion = %v, want 2.40.0", appliedSpec(t, fa)["rayVersion"])
	}
}

// TestUpdatePreservesLiveReplicasUnderAutoscaling is the autoscaler-safety AC for
// update: spec.workerGroupSpecs is an ATOMIC SSA list, so the apply must resend
// each worker group whole — and update resends the LIVE replicas value verbatim
// (whatever the autoscaler last set), never stripping or changing it. Stripping
// would reset the field to its zero default on the atomic list (clobbering the
// autoscaler DOWN); re-asserting the live value leaves it untouched. min/max remain.
func TestUpdatePreservesLiveReplicasUnderAutoscaling(t *testing.T) {
	t.Parallel()
	reader := &fakeReader{detail: liveCluster("ray", "demo", true, "workers", 4, 1, 5)}
	svc, fa, _ := newScaleService(reader, &fakeApplier{applyObj: MergedSpec{}}, "ray")

	if _, err := svc.Update(context.Background(), ClusterUpdateParams{Name: "demo", Image: "rayproject/ray:2.40.0"}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	wg := firstWorkerGroup(t, fa)
	if wg["replicas"] != int64(4) {
		t.Errorf("applied replicas = %v, want 4 (the live autoscaler value re-asserted, not stripped)", wg["replicas"])
	}
	// min/max are ray-mcp-owned bounds — they must survive.
	if wg["minReplicas"] != int64(1) || wg["maxReplicas"] != int64(5) {
		t.Errorf("applied min/max = %v/%v, want 1/5 (bounds must survive)", wg["minReplicas"], wg["maxReplicas"])
	}
}

// TestUpdateKeepsReplicasWithoutAutoscaling asserts that WITHOUT autoscaling, the
// existing replicas are preserved in the full apply (ray-mcp owns them; omitting
// them on the atomic list would prune/reset them).
func TestUpdateKeepsReplicasWithoutAutoscaling(t *testing.T) {
	t.Parallel()
	reader := &fakeReader{detail: liveCluster("ray", "demo", false, "workers", 3, 0, 5)}
	svc, fa, _ := newScaleService(reader, &fakeApplier{applyObj: MergedSpec{}}, "ray")

	if _, err := svc.Update(context.Background(), ClusterUpdateParams{Name: "demo", Image: "rayproject/ray:2.40.0"}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if firstWorkerGroup(t, fa)["replicas"] != int64(3) {
		t.Errorf("applied replicas = %v, want 3 preserved (no autoscaling → ray-mcp owns it)", firstWorkerGroup(t, fa)["replicas"])
	}
}

// TestUpdateRawSpecWins asserts a rawSpec is merged OVER the live object (rawSpec
// wins), reaching the applied spec — the escape hatch for fields curated params
// don't cover.
func TestUpdateRawSpecWins(t *testing.T) {
	t.Parallel()
	reader := &fakeReader{detail: liveCluster("ray", "demo", false, "workers", 2, 0, 5)}
	svc, fa, _ := newScaleService(reader, &fakeApplier{applyObj: MergedSpec{}}, "ray")

	_, err := svc.Update(context.Background(), ClusterUpdateParams{
		Name:    "demo",
		RawSpec: MergedSpec{"spec": map[string]any{"rayVersion": "9.9.9-raw"}},
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if appliedSpec(t, fa)["rayVersion"] != "9.9.9-raw" {
		t.Errorf("applied rayVersion = %v, want the rawSpec value (rawSpec wins)", appliedSpec(t, fa)["rayVersion"])
	}
}

// TestUpdateStripsWorkersToDelete asserts the read-modify-apply path drops
// spec.workerGroupSpecs[].scaleStrategy.workersToDelete from the apply body.
// That field is transient command state the Ray autoscaler/KubeRay author and
// the operator consumes; KubeRay v1.6.1 clears it in-memory only (no persisting
// Update), so a populated-but-already-actioned list can linger on the live spec.
// ray-mcp never authors it, so re-asserting it as ray-mcp-owned intent could
// re-trigger a targeted pod deletion on the next reconcile. We strip it (like
// status) so ray-mcp declines to own a deletion list it did not issue. The
// autoscaler-owned live replicas value must still survive.
func TestUpdateStripsWorkersToDelete(t *testing.T) {
	t.Parallel()
	detail := liveCluster("ray", "demo", true, "workers", 4, 1, 5)
	// Seed a populated, already-actioned workersToDelete on the live worker group.
	wg := detail.Raw["spec"].(map[string]any)["workerGroupSpecs"].([]any)[0].(map[string]any)
	wg["scaleStrategy"] = map[string]any{"workersToDelete": []any{"demo-worker-abc12"}}
	reader := &fakeReader{detail: detail}
	svc, fa, _ := newScaleService(reader, &fakeApplier{applyObj: MergedSpec{}}, "ray")

	if _, err := svc.Update(context.Background(), ClusterUpdateParams{Name: "demo", Image: "rayproject/ray:2.40.0"}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	applied := firstWorkerGroup(t, fa)
	if ss, ok := applied["scaleStrategy"].(map[string]any); ok {
		if _, has := ss["workersToDelete"]; has {
			t.Errorf("applied worker group carries scaleStrategy.workersToDelete = %v; it must be stripped (ray-mcp never authors it)", ss["workersToDelete"])
		}
	}
	// The autoscaler-owned live replicas value must be untouched by the strip.
	if applied["replicas"] != int64(4) {
		t.Errorf("applied replicas = %v, want 4 preserved (strip must not disturb the live value)", applied["replicas"])
	}
}

// TestScaleStripsWorkersToDelete asserts the same strip on the scale path: a scale
// of min/max must not re-assert a stale workersToDelete carried from the live read.
func TestScaleStripsWorkersToDelete(t *testing.T) {
	t.Parallel()
	detail := liveCluster("ray", "demo", false, "workers", 2, 0, 5)
	wg := detail.Raw["spec"].(map[string]any)["workerGroupSpecs"].([]any)[0].(map[string]any)
	wg["scaleStrategy"] = map[string]any{"workersToDelete": []any{"demo-worker-xyz"}}
	reader := &fakeReader{detail: detail}
	svc, fa, _ := newScaleService(reader, &fakeApplier{applyObj: MergedSpec{}}, "ray")

	if _, err := svc.Scale(context.Background(), ClusterScaleParams{Name: "demo", WorkerGroup: "workers", MaxReplicas: i32(8)}); err != nil {
		t.Fatalf("Scale: %v", err)
	}
	applied := firstWorkerGroup(t, fa)
	if ss, ok := applied["scaleStrategy"].(map[string]any); ok {
		if _, has := ss["workersToDelete"]; has {
			t.Errorf("applied worker group carries scaleStrategy.workersToDelete = %v; it must be stripped", ss["workersToDelete"])
		}
	}
}

// TestUpdateRejectsIdentityRetarget asserts a rawSpec that retargets identity is
// rejected by the merge identity guard before any apply.
func TestUpdateRejectsIdentityRetarget(t *testing.T) {
	t.Parallel()
	reader := &fakeReader{detail: liveCluster("ray", "demo", false, "workers", 2, 0, 5)}
	svc, fa, _ := newScaleService(reader, &fakeApplier{}, "ray")

	_, err := svc.Update(context.Background(), ClusterUpdateParams{
		Name:    "demo",
		RawSpec: MergedSpec{"metadata": map[string]any{"name": "evil"}},
	})
	var ident *IdentityError
	if !errors.As(err, &ident) {
		t.Fatalf("Update error = %v, want *IdentityError", err)
	}
	if len(fa.calls) != 0 {
		t.Errorf("applier called %d times, want 0 (identity guard precedes apply)", len(fa.calls))
	}
}

// TestUpdateEnableAutoscaling asserts toggling autoscaling on overlays
// enableInTreeAutoscaling. Replicas are preserved (the live value is re-asserted on
// the atomic list), not stripped — once the autoscaler starts, it owns the value
// and ray-mcp re-asserting the current count leaves it untouched.
func TestUpdateEnableAutoscaling(t *testing.T) {
	t.Parallel()
	reader := &fakeReader{detail: liveCluster("ray", "demo", false, "workers", 3, 1, 5)}
	svc, fa, _ := newScaleService(reader, &fakeApplier{applyObj: MergedSpec{}}, "ray")

	enable := true
	if _, err := svc.Update(context.Background(), ClusterUpdateParams{Name: "demo", EnableAutoscaling: &enable}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if appliedSpec(t, fa)["enableInTreeAutoscaling"] != true {
		t.Errorf("applied enableInTreeAutoscaling = %v, want true", appliedSpec(t, fa)["enableInTreeAutoscaling"])
	}
	if firstWorkerGroup(t, fa)["replicas"] != int64(3) {
		t.Errorf("applied replicas = %v, want 3 preserved (atomic list; live value re-asserted)", firstWorkerGroup(t, fa)["replicas"])
	}
}

// TestUpdateRequiresAtLeastOneChange asserts an update with no fields set is a
// no-op error (don't apply an unchanged object and churn ownership for nothing).
func TestUpdateRequiresAtLeastOneChange(t *testing.T) {
	t.Parallel()
	reader := &fakeReader{detail: liveCluster("ray", "demo", false, "workers", 2, 0, 5)}
	svc, fa, _ := newScaleService(reader, &fakeApplier{}, "ray")

	if _, err := svc.Update(context.Background(), ClusterUpdateParams{Name: "demo"}); err == nil {
		t.Fatal("Update with no changes returned nil; it must require at least one field")
	}
	if len(fa.calls) != 0 {
		t.Errorf("applier called %d times, want 0 (no-op update)", len(fa.calls))
	}
}

// TestUpdateReadErrorStops asserts a failed live read stops the update.
func TestUpdateReadErrorStops(t *testing.T) {
	t.Parallel()
	reader := &fakeReader{err: &NotFoundError{Kind: KindRayCluster, Namespace: "ray", Name: "demo"}}
	svc, fa, _ := newScaleService(reader, &fakeApplier{}, "ray")

	_, err := svc.Update(context.Background(), ClusterUpdateParams{Name: "demo", Image: "x"})
	var nf *NotFoundError
	if !errors.As(err, &nf) {
		t.Fatalf("Update error = %v, want the read NotFoundError", err)
	}
	if len(fa.calls) != 0 {
		t.Errorf("applier called %d times, want 0", len(fa.calls))
	}
}

// TestUpdateAuditTaggedUpdate asserts the update audit record is tagged with the
// update tool name (the apply choke point fires for update too).
func TestUpdateAuditTaggedUpdate(t *testing.T) {
	t.Parallel()
	reader := &fakeReader{detail: liveCluster("ray", "demo", false, "workers", 2, 0, 5)}
	svc, _, sink := newScaleService(reader, &fakeApplier{applyObj: MergedSpec{}}, "ray")

	if _, err := svc.Update(context.Background(), ClusterUpdateParams{Name: "demo", Image: "x"}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if len(sink.records) != 1 || sink.records[0].Tool != "ray_cluster_update" {
		t.Fatalf("audit records = %+v, want one tagged ray_cluster_update", sink.records)
	}
}

// TestScaleForcesOnConflictRetry asserts the lost-update guard: when the first
// commit returns a ConflictError (another manager owns the atomic worker list),
// scale re-reads and re-applies ONCE with force. The retry commit must carry
// force=true; the dry-run preview is never forced.
func TestScaleForcesOnConflictRetry(t *testing.T) {
	t.Parallel()
	reader := &fakeReader{detail: liveCluster("ray", "demo", false, "workers", 2, 0, 5)}
	svc, fa, _ := newScaleService(reader, &fakeApplier{applyObj: MergedSpec{}, commitConflictOnce: true}, "ray")

	if _, err := svc.Scale(context.Background(), ClusterScaleParams{Name: "demo", WorkerGroup: "workers", MaxReplicas: i32(8)}); err != nil {
		t.Fatalf("Scale (should recover via force-retry): %v", err)
	}
	// Call sequence: dry-run, commit#1 (conflict), dry-run, commit#2 (forced).
	var commits []applyCall
	for _, c := range fa.calls {
		if !c.dryRun {
			commits = append(commits, c)
		}
	}
	if len(commits) != 2 {
		t.Fatalf("commit applies = %d, want 2 (conflict then forced retry)", len(commits))
	}
	if commits[0].force {
		t.Error("first commit was forced; the initial apply must be non-force")
	}
	if !commits[1].force {
		t.Error("retry commit was not forced; the conflict-retry must set force")
	}
	// The retry's dry-run preview must ALSO be forced (SSA dry-run does conflict
	// detection, so a non-forced preview would re-conflict before the forced
	// commit). The first attempt's preview is non-forced.
	var dryRuns []applyCall
	for _, c := range fa.calls {
		if c.dryRun {
			dryRuns = append(dryRuns, c)
		}
	}
	if len(dryRuns) != 2 {
		t.Fatalf("dry-run previews = %d, want 2 (one per attempt)", len(dryRuns))
	}
	if dryRuns[0].force {
		t.Error("first attempt's dry-run was forced; the initial preview must be non-force")
	}
	if !dryRuns[1].force {
		t.Error("retry's dry-run was not forced; it must inherit force so it doesn't re-conflict")
	}
}

// TestScaleConflictNotRetriedOnDryRun asserts a dry-run that conflicts is NOT
// force-retried (a dry-run mutates nothing, so there is nothing to force) — the
// conflict surfaces as-is.
func TestScaleConflictNotRetriedOnDryRun(t *testing.T) {
	t.Parallel()
	reader := &fakeReader{detail: liveCluster("ray", "demo", false, "workers", 2, 0, 5)}
	// dryRunErr is a conflict; with DryRun=true the pipeline never reaches a commit.
	fa := &fakeApplier{dryRunErr: &ConflictError{Kind: KindRayCluster, Namespace: "ray", Name: "demo", Detail: "x"}}
	svc, _, _ := newScaleService(reader, fa, "ray")

	_, err := svc.Scale(context.Background(), ClusterScaleParams{Name: "demo", WorkerGroup: "workers", MaxReplicas: i32(8), DryRun: true})
	var conflict *ConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("Scale(dryRun) error = %v, want the ConflictError surfaced (no force-retry on dry-run)", err)
	}
	for _, c := range fa.calls {
		if c.force {
			t.Error("a dry-run conflict triggered a forced apply; it must not")
		}
	}
}

// TestScaleAuditTaggedScale asserts the scale audit record is tagged with the
// scale tool name.
func TestScaleAuditTaggedScale(t *testing.T) {
	t.Parallel()
	reader := &fakeReader{detail: liveCluster("ray", "demo", false, "workers", 2, 0, 5)}
	svc, _, sink := newScaleService(reader, &fakeApplier{applyObj: MergedSpec{}}, "ray")

	if _, err := svc.Scale(context.Background(), ClusterScaleParams{Name: "demo", WorkerGroup: "workers", MaxReplicas: i32(6)}); err != nil {
		t.Fatalf("Scale: %v", err)
	}
	if len(sink.records) != 1 || sink.records[0].Tool != "ray_cluster_scale" {
		t.Fatalf("audit records = %+v, want one tagged ray_cluster_scale", sink.records)
	}
}
