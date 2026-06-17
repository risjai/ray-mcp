package domain

import (
	"testing"
)

// findChange returns the entry at the given path (and whether it was present).
func findChange(d DiffResult, path string) (FieldChange, bool) {
	for _, c := range d.Changes {
		if c.Path == path {
			return c, true
		}
	}
	return FieldChange{}, false
}

// TestDiffScalarModifiedInline asserts a changed scalar is reported inline with
// old→new values (spec §10: "replicas 2→5").
func TestDiffScalarModifiedInline(t *testing.T) {
	t.Parallel()

	before := MergedSpec{"spec": map[string]any{"replicas": int64(2)}}
	after := MergedSpec{"spec": map[string]any{"replicas": int64(5)}}

	d := Diff(before, after, DefaultDiffMaxDepth)
	c, ok := findChange(d, "spec.replicas")
	if !ok {
		t.Fatalf("no change at spec.replicas; got %+v", d.Changes)
	}
	if c.Kind != ChangeModified || c.Old != int64(2) || c.New != int64(5) {
		t.Errorf("spec.replicas change = %+v, want modified 2→5", c)
	}
	if d.FieldCount() != 1 {
		t.Errorf("FieldCount = %d, want 1", d.FieldCount())
	}
}

// TestDiffAddedAndRemovedScalars asserts added/removed keys are classified and
// carry the relevant value side.
func TestDiffAddedAndRemovedScalars(t *testing.T) {
	t.Parallel()

	before := MergedSpec{"a": "keep", "gone": "x"}
	after := MergedSpec{"a": "keep", "added": "y"}

	d := Diff(before, after, DefaultDiffMaxDepth)
	if c, ok := findChange(d, "added"); !ok || c.Kind != ChangeAdded || c.New != "y" {
		t.Errorf("added entry = %+v ok=%v, want added New=y", c, ok)
	}
	if c, ok := findChange(d, "gone"); !ok || c.Kind != ChangeRemoved || c.Old != "x" {
		t.Errorf("removed entry = %+v ok=%v, want removed Old=x", c, ok)
	}
	if _, ok := findChange(d, "a"); ok {
		t.Errorf("unchanged key 'a' should not appear in the diff")
	}
}

// TestDiffArrayElementWiseByIndex asserts arrays are compared element-wise so the
// summary can pinpoint workerGroupSpecs[0].replicas (spec §10), independent of the
// merge's arrays-replace rule.
func TestDiffArrayElementWiseByIndex(t *testing.T) {
	t.Parallel()

	before := MergedSpec{"spec": map[string]any{"workerGroupSpecs": []any{
		map[string]any{"groupName": "small", "replicas": int64(2)},
	}}}
	after := MergedSpec{"spec": map[string]any{"workerGroupSpecs": []any{
		map[string]any{"groupName": "small", "replicas": int64(5)},
	}}}

	d := Diff(before, after, DefaultDiffMaxDepth)
	c, ok := findChange(d, "spec.workerGroupSpecs[0].replicas")
	if !ok {
		t.Fatalf("no change at spec.workerGroupSpecs[0].replicas; got %+v", d.Changes)
	}
	if c.Kind != ChangeModified || c.Old != int64(2) || c.New != int64(5) {
		t.Errorf("change = %+v, want modified 2→5", c)
	}
}

// TestDiffArrayGrowthAndShrink asserts appended/removed array elements are
// reported as added/removed at their index.
func TestDiffArrayGrowthAndShrink(t *testing.T) {
	t.Parallel()

	before := MergedSpec{"items": []any{"a"}}
	after := MergedSpec{"items": []any{"a", "b"}}

	d := Diff(before, after, DefaultDiffMaxDepth)
	if c, ok := findChange(d, "items[1]"); !ok || c.Kind != ChangeAdded || c.New != "b" {
		t.Errorf("items[1] = %+v ok=%v, want added New=b", c, ok)
	}

	d2 := Diff(after, before, DefaultDiffMaxDepth)
	if c, ok := findChange(d2, "items[1]"); !ok || c.Kind != ChangeRemoved || c.Old != "b" {
		t.Errorf("items[1] = %+v ok=%v, want removed Old=b", c, ok)
	}
}

// TestDiffSubtreeCollapsedPastMaxDepth asserts a deep changed object collapses to
// one ChangeSubtree entry with a leaf count (spec §10: "workerGroupSpecs[0].template
// changed (3 fields)"), instead of recursing into every leaf.
func TestDiffSubtreeCollapsedPastMaxDepth(t *testing.T) {
	t.Parallel()

	// template is at value-depth 4 (spec.workerGroupSpecs[0].template); at
	// maxDepth=4 the composite under it collapses.
	mkTemplate := func(image string) MergedSpec {
		return MergedSpec{"spec": map[string]any{"workerGroupSpecs": []any{
			map[string]any{"template": map[string]any{
				"spec": map[string]any{"containers": []any{
					map[string]any{"image": image, "name": "ray", "imagePullPolicy": "Always"},
				}},
			}},
		}}}
	}

	d := Diff(mkTemplate("ray:2.9.0"), mkTemplate("ray:2.10.0"), DefaultDiffMaxDepth)
	c, ok := findChange(d, "spec.workerGroupSpecs[0].template")
	if !ok {
		t.Fatalf("expected a collapsed subtree at spec.workerGroupSpecs[0].template; got %+v", d.Changes)
	}
	if c.Kind != ChangeSubtree {
		t.Errorf("template change kind = %s, want subtree (collapsed past maxDepth)", c.Kind)
	}
	if c.FieldCount != 1 {
		t.Errorf("template subtree FieldCount = %d, want 1 (only image changed within)", c.FieldCount)
	}
	// And it must NOT have recursed into the leaf.
	if _, deep := findChange(d, "spec.workerGroupSpecs[0].template.spec.containers[0].image"); deep {
		t.Errorf("subtree should be collapsed, but a deep leaf change was emitted")
	}
}

// TestDiffScalarToCompositeTypeChange asserts a leaf changing from a scalar to
// an object is reported as a modification carrying a FieldCount for the composite
// side (diff.go's type-change branch), and is counted once in the headline.
func TestDiffScalarToCompositeTypeChange(t *testing.T) {
	t.Parallel()

	before := MergedSpec{"spec": map[string]any{"replicas": int64(2)}}
	after := MergedSpec{"spec": map[string]any{"replicas": map[string]any{"min": int64(1), "max": int64(5)}}}

	d := Diff(before, after, DefaultDiffMaxDepth)
	c, ok := findChange(d, "spec.replicas")
	if !ok {
		t.Fatalf("no change at spec.replicas; got %+v", d.Changes)
	}
	if c.Kind != ChangeModified {
		t.Errorf("type-change kind = %s, want modified", c.Kind)
	}
	if c.Old != int64(2) {
		t.Errorf("Old = %v, want scalar 2 (the prior scalar side)", c.Old)
	}
	if c.New != nil {
		t.Errorf("New = %v, want nil (composite side is summarized, not inlined)", c.New)
	}
	// A scalar↔composite type change is summarized as a single changed node
	// (the value at this path changed), not the leaf-count of the new structure.
	if c.FieldCount != 1 {
		t.Errorf("FieldCount = %d, want 1 (type change counts as one changed field)", c.FieldCount)
	}
	if d.FieldCount() != 1 {
		t.Errorf("headline FieldCount = %d, want 1", d.FieldCount())
	}
}

// TestDiffArrayElementCollapsedPastMaxDepth asserts the slice-side depth-collapse
// branch: an array whose element pair differs past maxDepth collapses to one
// ChangeSubtree, rather than recursing into the element's leaves.
func TestDiffArrayElementCollapsedPastMaxDepth(t *testing.T) {
	t.Parallel()

	// An array sitting AT maxDepth collapses regardless of element contents. Put
	// a list at value-depth 4 (a.b.c.list) with maxDepth=4.
	mk := func(v string) MergedSpec {
		return MergedSpec{"a": map[string]any{"b": map[string]any{"c": map[string]any{
			"list": []any{map[string]any{"x": v, "y": "same"}},
		}}}}
	}

	d := Diff(mk("old"), mk("new"), DefaultDiffMaxDepth)
	c, ok := findChange(d, "a.b.c.list")
	if !ok {
		t.Fatalf("expected a collapsed subtree at a.b.c.list; got %+v", d.Changes)
	}
	if c.Kind != ChangeSubtree || c.FieldCount != 1 {
		t.Errorf("list change = %+v, want subtree with FieldCount 1 (only x changed)", c)
	}
	if _, deep := findChange(d, "a.b.c.list[0].x"); deep {
		t.Errorf("array element should be collapsed, but a deep leaf change was emitted")
	}
}

// TestDiffAddedCompositeCountsLeaves asserts an added object is reported as added
// with FieldCount = its leaf count (so the renderer can say "+annotations (2 fields)").
func TestDiffAddedCompositeCountsLeaves(t *testing.T) {
	t.Parallel()

	before := MergedSpec{"metadata": map[string]any{"name": "demo"}}
	after := MergedSpec{"metadata": map[string]any{
		"name":        "demo",
		"annotations": map[string]any{"team": "ml", "tier": "gold"},
	}}

	d := Diff(before, after, DefaultDiffMaxDepth)
	c, ok := findChange(d, "metadata.annotations")
	if !ok {
		t.Fatalf("no change at metadata.annotations; got %+v", d.Changes)
	}
	if c.Kind != ChangeAdded || c.FieldCount != 2 {
		t.Errorf("annotations change = %+v, want added with FieldCount 2", c)
	}
}

// TestDiffEmptyWhenEqual asserts equal objects produce no changes.
func TestDiffEmptyWhenEqual(t *testing.T) {
	t.Parallel()

	obj := MergedSpec{"spec": map[string]any{"replicas": int64(3), "image": "ray:2.9.0"}}
	// Distinct deep copy with the same content.
	same := MergedSpec(cloneMap(obj))

	d := Diff(obj, same, DefaultDiffMaxDepth)
	if !d.Empty() {
		t.Errorf("Diff of equal objects = %+v, want empty", d.Changes)
	}
	if d.FieldCount() != 0 {
		t.Errorf("FieldCount = %d, want 0", d.FieldCount())
	}
}

// TestDiffMultipleChangesCounted asserts the headline field count sums scalar and
// collapsed-subtree changes (spec §10: "3 fields changed: …").
func TestDiffMultipleChangesCounted(t *testing.T) {
	t.Parallel()

	before := MergedSpec{"spec": map[string]any{
		"replicas": int64(2),
		"image":    "ray:2.9.0",
	}}
	after := MergedSpec{"spec": map[string]any{
		"replicas":    int64(5),
		"image":       "ray:2.10.0",
		"annotations": map[string]any{"team": "ml"},
	}}

	d := Diff(before, after, DefaultDiffMaxDepth)
	// replicas (1) + image (1) + added annotations (1 leaf) = 3.
	if d.FieldCount() != 3 {
		t.Errorf("FieldCount = %d, want 3 (replicas, image, +annotations.team)", d.FieldCount())
	}
}

// TestDiffDeterministicOrder asserts changes are sorted by path so the rendered
// summary is stable across runs.
func TestDiffDeterministicOrder(t *testing.T) {
	t.Parallel()

	before := MergedSpec{"z": "1", "a": "1", "m": "1"}
	after := MergedSpec{"z": "2", "a": "2", "m": "2"}

	d := Diff(before, after, DefaultDiffMaxDepth)
	if len(d.Changes) != 3 {
		t.Fatalf("got %d changes, want 3", len(d.Changes))
	}
	if d.Changes[0].Path != "a" || d.Changes[1].Path != "m" || d.Changes[2].Path != "z" {
		t.Errorf("paths not sorted: %s, %s, %s", d.Changes[0].Path, d.Changes[1].Path, d.Changes[2].Path)
	}
}
