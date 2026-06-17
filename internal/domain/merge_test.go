package domain

import (
	"errors"
	"reflect"
	"testing"
)

// id is the fixed tool-arg identity used across the merge tests.
func id() Identity { return Identity{Namespace: "ray-system", Name: "demo"} }

// base is a minimal curated-derived base map carrying the identity metadata the
// caller is expected to set before merging.
func base() MergedSpec {
	return MergedSpec{
		"metadata": map[string]any{"name": "demo", "namespace": "ray-system"},
		"spec": map[string]any{
			"rayVersion": "2.9.0",
			"headGroupSpec": map[string]any{
				"rayStartParams": map[string]any{"dashboard-host": "0.0.0.0"},
			},
			"workerGroupSpecs": []any{
				map[string]any{"groupName": "small", "replicas": int64(2)},
			},
		},
	}
}

// TestMergeRawSpecWins asserts a colliding scalar in rawSpec overrides the base
// (spec §7.C step 3: rawSpec wins).
func TestMergeRawSpecWins(t *testing.T) {
	t.Parallel()

	raw := MergedSpec{"spec": map[string]any{"rayVersion": "2.10.0"}}
	merged, err := Merge(base(), raw, id())
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}
	got, _ := nestedString(merged, "spec", "rayVersion")
	if got != "2.10.0" {
		t.Errorf("rayVersion = %q, want rawSpec value 2.10.0", got)
	}
}

// TestMergeObjectsMergeRecursively asserts object values deep-merge: a rawSpec
// key adds alongside base keys at the same object, not replacing the whole object.
func TestMergeObjectsMergeRecursively(t *testing.T) {
	t.Parallel()

	raw := MergedSpec{"spec": map[string]any{
		"headGroupSpec": map[string]any{
			"rayStartParams": map[string]any{"num-cpus": "0"},
		},
	}}
	merged, err := Merge(base(), raw, id())
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}
	rsp, _ := nested(merged, "spec", "headGroupSpec", "rayStartParams")
	rspMap, _ := rsp.(map[string]any)
	if rspMap["dashboard-host"] != "0.0.0.0" {
		t.Errorf("base key dashboard-host lost on recursive merge: %+v", rspMap)
	}
	if rspMap["num-cpus"] != "0" {
		t.Errorf("rawSpec key num-cpus not merged in: %+v", rspMap)
	}
}

// TestMergeArraysReplaceWholesale asserts that setting an array in rawSpec
// replaces the entire base array — you own the whole list (spec §6, documented
// loudly). No element-wise merge.
func TestMergeArraysReplaceWholesale(t *testing.T) {
	t.Parallel()

	raw := MergedSpec{"spec": map[string]any{
		"workerGroupSpecs": []any{
			map[string]any{"groupName": "gpu", "replicas": int64(1)},
			map[string]any{"groupName": "cpu", "replicas": int64(8)},
		},
	}}
	merged, err := Merge(base(), raw, id())
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}
	wgs, _ := nested(merged, "spec", "workerGroupSpecs")
	wgsSlice, _ := wgs.([]any)
	if len(wgsSlice) != 2 {
		t.Fatalf("workerGroupSpecs len = %d, want 2 (array replaced wholesale, not merged)", len(wgsSlice))
	}
	first, _ := wgsSlice[0].(map[string]any)
	if first["groupName"] != "gpu" {
		t.Errorf("workerGroupSpecs[0].groupName = %v, want the rawSpec list to fully replace base", first["groupName"])
	}
}

// TestMergeNullDeletes asserts an RFC 7396 null in rawSpec deletes the key.
func TestMergeNullDeletes(t *testing.T) {
	t.Parallel()

	raw := MergedSpec{"spec": map[string]any{
		"headGroupSpec": map[string]any{"rayStartParams": nil},
	}}
	merged, err := Merge(base(), raw, id())
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}
	if _, ok := nested(merged, "spec", "headGroupSpec", "rayStartParams"); ok {
		t.Errorf("rayStartParams should have been deleted by a null rawSpec value")
	}
}

// TestMergePreservesNewerThanBaselineFields asserts a field unknown to the
// compiled KubeRay baseline survives the merge (no typed round-trip drops it) —
// the wedge (spec §7.C step 5).
func TestMergePreservesNewerThanBaselineFields(t *testing.T) {
	t.Parallel()

	raw := MergedSpec{"spec": map[string]any{
		"someFutureKubeRayField": map[string]any{"enabled": true},
	}}
	merged, err := Merge(base(), raw, id())
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}
	v, ok := nested(merged, "spec", "someFutureKubeRayField", "enabled")
	if !ok || v != true {
		t.Errorf("future field dropped: got %v ok=%v, want true (unstructured preserves it)", v, ok)
	}
}

// TestMergeObjectReplacesScalarAndStripsNestedNulls asserts the trickiest RFC
// 7396 branch: a patch object landing where the target key is absent (or a
// scalar) creates a fresh object, and nested nulls within that patch object are
// stripped (a null means "absent", so it must not survive into the result).
func TestMergeObjectReplacesScalarAndStripsNestedNulls(t *testing.T) {
	t.Parallel()

	// Base has `spec.rayVersion` as a scalar; rawSpec replaces it with an object
	// carrying a nested null. RFC 7396: the object replaces the scalar, and the
	// null key is dropped.
	raw := MergedSpec{"spec": map[string]any{
		"rayVersion": map[string]any{"keep": "yes", "drop": nil},
	}}
	merged, err := Merge(base(), raw, id())
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}
	rv, ok := nested(merged, "spec", "rayVersion")
	rvMap, isMap := rv.(map[string]any)
	if !ok || !isMap {
		t.Fatalf("spec.rayVersion = %v (%T), want an object replacing the scalar", rv, rv)
	}
	if rvMap["keep"] != "yes" {
		t.Errorf("nested keep lost: %+v", rvMap)
	}
	if _, present := rvMap["drop"]; present {
		t.Errorf("nested null key 'drop' should be stripped, got %+v", rvMap)
	}
}

// TestMergeIdentityGuardRejectsNonMapMetadata asserts type confusion cannot
// bypass the guard: a rawSpec that overwrites metadata with a non-map value
// yields an empty name and is rejected (not silently applied to a metadata-less
// object).
func TestMergeIdentityGuardRejectsNonMapMetadata(t *testing.T) {
	t.Parallel()

	raw := MergedSpec{"metadata": "not-a-map"}
	_, err := Merge(base(), raw, id())
	var ie *IdentityError
	if !errors.As(err, &ie) {
		t.Fatalf("Merge error = %v, want *IdentityError when metadata is overwritten by a scalar", err)
	}
	if ie.Field != "name" || ie.Got != "" {
		t.Errorf("IdentityError = %+v, want field=name got empty (metadata no longer a map)", ie)
	}
}

// TestMergeIdentityGuardRejectsNameChange asserts a rawSpec that retargets
// metadata.name is a hard error (spec §7.C step 4), and reports the field.
func TestMergeIdentityGuardRejectsNameChange(t *testing.T) {
	t.Parallel()

	raw := MergedSpec{"metadata": map[string]any{"name": "evil"}}
	_, err := Merge(base(), raw, id())
	var ie *IdentityError
	if !errors.As(err, &ie) {
		t.Fatalf("Merge error = %v, want *IdentityError", err)
	}
	if ie.Field != "name" || ie.Got != "evil" || ie.Want != "demo" {
		t.Errorf("IdentityError = %+v, want field=name got=evil want=demo", ie)
	}
}

// TestMergeIdentityGuardRejectsNamespaceChange asserts the guard also covers
// metadata.namespace.
func TestMergeIdentityGuardRejectsNamespaceChange(t *testing.T) {
	t.Parallel()

	raw := MergedSpec{"metadata": map[string]any{"namespace": "kube-system"}}
	_, err := Merge(base(), raw, id())
	var ie *IdentityError
	if !errors.As(err, &ie) {
		t.Fatalf("Merge error = %v, want *IdentityError for namespace", err)
	}
	if ie.Field != "namespace" {
		t.Errorf("IdentityError.Field = %q, want namespace", ie.Field)
	}
}

// TestMergeIdentityGuardRejectsNameRemoval asserts a rawSpec that NULLs out
// metadata.name is rejected (Got empty) rather than silently applied to a
// nameless object.
func TestMergeIdentityGuardRejectsNameRemoval(t *testing.T) {
	t.Parallel()

	raw := MergedSpec{"metadata": map[string]any{"name": nil}}
	_, err := Merge(base(), raw, id())
	var ie *IdentityError
	if !errors.As(err, &ie) {
		t.Fatalf("Merge error = %v, want *IdentityError for removed name", err)
	}
	if ie.Got != "" {
		t.Errorf("IdentityError.Got = %q, want empty (name was removed)", ie.Got)
	}
}

// TestMergeNilRawSpecClonesBase asserts a curated-only apply (nil rawSpec)
// returns a clean clone equal to base.
func TestMergeNilRawSpecClonesBase(t *testing.T) {
	t.Parallel()

	b := base()
	merged, err := Merge(b, nil, id())
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}
	if !reflect.DeepEqual(map[string]any(merged), map[string]any(b)) {
		t.Errorf("nil-rawSpec merge = %+v, want a clone equal to base", merged)
	}
}

// TestMergeDoesNotMutateInputs asserts Merge clones: mutating the result does not
// reach back into base or rawSpec, so callers may reuse them.
func TestMergeDoesNotMutateInputs(t *testing.T) {
	t.Parallel()

	b := base()
	raw := MergedSpec{"spec": map[string]any{"rayVersion": "2.10.0"}}
	merged, err := Merge(b, raw, id())
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}

	// Mutate the merged result's nested map; base must be untouched.
	mergedSpec, _ := merged["spec"].(map[string]any)
	mergedSpec["rayVersion"] = "MUTATED"

	baseVer, _ := nestedString(b, "spec", "rayVersion")
	if baseVer != "2.9.0" {
		t.Errorf("base mutated through merge result: rayVersion = %q, want untouched 2.9.0", baseVer)
	}
	if raw["spec"].(map[string]any)["rayVersion"] != "2.10.0" {
		t.Errorf("rawSpec mutated through merge result")
	}
}

// nested reads a value at a key path, reporting whether it was found. Test helper
// (the production nestedString only returns strings).
func nested(m map[string]any, path ...string) (any, bool) {
	var cur any = m
	for _, p := range path {
		cm, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		cur, ok = cm[p]
		if !ok {
			return nil, false
		}
	}
	return cur, true
}
