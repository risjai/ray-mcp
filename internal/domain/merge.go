package domain

import "fmt"

// The pure heart of the unified apply pipeline (spec §7.C, steps 3-5). This file
// owns the I/O-free merge: rawSpec applied over a curated base as an RFC 7396
// JSON Merge Patch, the identity guard, and the unstructured (map) result. It
// imports no Kubernetes packages — the curated-params → typed KubeRay object →
// base-map construction (step 1) lives in the adapter/tool layer where the
// KubeRay types exist; this core receives that base already as a MergedSpec and
// never round-trips through a typed struct, so fields newer than the compiled
// KubeRay baseline survive (the wedge, step 5).

// Identity is the tool-arg (namespace, name) the merged spec must not move away
// from. Merge enforces it after applying rawSpec so a rawSpec can never retarget
// an apply at a different object (spec §7.C, step 4).
type Identity struct {
	Namespace string
	Name      string
}

// IdentityError reports that the merged spec's metadata.name/namespace differs
// from the tool-arg identity — a rawSpec tried to retarget (or remove) the
// apply target. It is a hard error, never a silent ignore (spec §7.C, step 4).
type IdentityError struct {
	Field string // "name" or "namespace".
	Want  string // the tool-arg value the field must keep.
	Got   string // what the merged spec carried ("" if rawSpec removed it).
}

func (e *IdentityError) Error() string {
	if e.Got == "" {
		return fmt.Sprintf("rawSpec removed metadata.%s; it must stay %q (identity is fixed by the tool args)", e.Field, e.Want)
	}
	return fmt.Sprintf("rawSpec changed metadata.%s to %q; it must stay %q (identity is fixed by the tool args)", e.Field, e.Got, e.Want)
}

// Merge applies rawSpec over base as an RFC 7396 JSON Merge Patch and enforces
// the identity guard (spec §7.C, steps 3-4). Semantics:
//
//   - rawSpec WINS on any key collision;
//   - objects merge recursively, but ARRAYS REPLACE WHOLESALE — set
//     workerGroupSpecs in rawSpec and you own the entire list (documented loudly,
//     spec §6);
//   - a null value in rawSpec DELETES that key (RFC 7396);
//   - the result stays a plain map (unstructured): no typed round-trip runs, so
//     fields newer than the compiled KubeRay baseline are preserved (step 5).
//
// base is expected to already carry metadata.name/namespace equal to id (the
// caller builds it from the curated params + tool-arg identity). After merging,
// the identity guard rejects any result whose metadata.name or metadata.namespace
// differs from id. base and rawSpec are not mutated — Merge deep-clones as it
// goes — so callers may reuse them. rawSpec may be nil (a curated-only apply),
// in which case the result is a clean clone of base.
func Merge(base, rawSpec MergedSpec, id Identity) (MergedSpec, error) {
	merged := cloneMap(base)
	mergePatch(merged, rawSpec)

	name, _ := nestedString(merged, "metadata", "name")
	if name != id.Name {
		return nil, &IdentityError{Field: "name", Want: id.Name, Got: name}
	}
	namespace, _ := nestedString(merged, "metadata", "namespace")
	if namespace != id.Namespace {
		return nil, &IdentityError{Field: "namespace", Want: id.Namespace, Got: namespace}
	}

	return merged, nil
}

// mergePatch applies an RFC 7396 merge patch (patch over target) in place on
// target, which the caller must already own (cloned). Per RFC 7396: an object
// value merges recursively into an object target; a null deletes the key;
// everything else (scalars and arrays) replaces wholesale. Replaced values are
// deep-cloned so the result never aliases patch.
func mergePatch(target map[string]any, patch map[string]any) {
	for k, pv := range patch {
		if pv == nil {
			delete(target, k)
			continue
		}

		pvMap, pvIsMap := pv.(map[string]any)
		if !pvIsMap {
			// Scalar or array: replace wholesale.
			target[k] = cloneValue(pv)
			continue
		}

		// Patch value is an object. Merge into the target object if it is one;
		// otherwise the patch object replaces the (absent or non-object) target.
		// Either way the recursion strips any nested nulls in the patch.
		if tvMap, ok := target[k].(map[string]any); ok {
			mergePatch(tvMap, pvMap)
		} else {
			fresh := map[string]any{}
			mergePatch(fresh, pvMap)
			target[k] = fresh
		}
	}
}

// cloneMap deep-copies a map of JSON-compatible values. A nil map clones to an
// empty (non-nil) map so the merge result is always a usable object.
func cloneMap(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = cloneValue(v)
	}
	return out
}

// cloneValue deep-copies a JSON-compatible value (map, slice, or scalar). Scalars
// are immutable value types and pass through unchanged.
func cloneValue(v any) any {
	switch t := v.(type) {
	case map[string]any:
		return cloneMap(t)
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = cloneValue(e)
		}
		return out
	default:
		return v
	}
}

// nestedString reads a string at the given key path, reporting whether a string
// was actually found there (false if any segment is missing or not a map, or the
// leaf is not a string).
func nestedString(m map[string]any, path ...string) (string, bool) {
	var cur any = m
	for _, p := range path {
		cm, ok := cur.(map[string]any)
		if !ok {
			return "", false
		}
		cur, ok = cm[p]
		if !ok {
			return "", false
		}
	}
	s, ok := cur.(string)
	return s, ok
}
