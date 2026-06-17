package domain

import (
	"reflect"
	"sort"
	"strconv"
)

// Field-level diff summarization (spec §10, the apply pipeline's step-7 output).
// Pure and I/O-free: it compares two unstructured objects (the before/after of an
// apply, or intent-vs-dry-run-result) and produces a BOUNDED, agent-readable
// summary — scalar changes inline, large nested subtrees collapsed to a count —
// not the full structural diff. The consumer is an LLM with a finite context
// budget, so the summary IS the token-economy mechanism: "3 fields changed:
// replicas 2→5, image X→Y, +annotation Z" rather than a wall of YAML. The full
// structural diff (behind `verbose`) is just the before/after objects, which the
// caller already holds; this module owns only the summary.
//
// reflect is stdlib (reflect.DeepEqual for value equality); the hexagonal
// invariant forbids k8s.io / sigs.k8s.io / net/http, not the standard library.

// DefaultDiffMaxDepth is the default collapse depth: a changed object/array at or
// below this value-depth is summarized as one "path changed (N fields)" entry
// instead of being recursed into. It is tuned for the RayCluster shape — deep
// enough to inline scalar changes on a worker group
// (spec.workerGroupSpecs[i].replicas, value-depth 4) while collapsing the pod
// template subtree (spec.workerGroupSpecs[i].template, value-depth 4) to a count.
// Scalars always inline regardless of depth; only composite-vs-composite nodes
// collapse. The apply tool may pass a different depth.
const DefaultDiffMaxDepth = 4

// ChangeKind classifies one entry in a diff summary.
type ChangeKind string

const (
	// ChangeModified is a changed leaf: a scalar (or a type change) whose value
	// differs. Old/New carry the scalar values (nil on a side that is composite).
	ChangeModified ChangeKind = "modified"
	// ChangeAdded is a key/index present only in the after object.
	ChangeAdded ChangeKind = "added"
	// ChangeRemoved is a key/index present only in the before object.
	ChangeRemoved ChangeKind = "removed"
	// ChangeSubtree is a changed nested object/array collapsed past maxDepth.
	// FieldCount carries the number of leaf changes summarized under Path.
	ChangeSubtree ChangeKind = "subtree"
)

// FieldChange is one entry in a diff summary. Path is dotted with [i] for array
// indices (e.g. "spec.workerGroupSpecs[0].replicas"). For a modified/added/
// removed SCALAR, Old/New hold the scalar values. For a composite that was added,
// removed, or collapsed (ChangeSubtree, or an added/removed object/array),
// Old/New are nil and FieldCount carries the count of leaf fields involved so the
// renderer can say "(N fields)".
type FieldChange struct {
	Path       string
	Kind       ChangeKind
	Old        any // scalar prior value; nil for added or for a composite side.
	New        any // scalar new value; nil for removed or for a composite side.
	FieldCount int // leaf-field count for subtree/composite changes; 0 for a scalar leaf.
}

// DiffResult is the summarized field-level diff. Changes are ordered
// deterministically (sorted by path) so the rendered summary is stable.
type DiffResult struct {
	Changes []FieldChange
}

// FieldCount totals the leaf-level fields changed across all entries — the "N
// fields changed" headline (spec §10). A scalar leaf counts as 1; a collapsed or
// added/removed composite counts as its FieldCount.
func (d DiffResult) FieldCount() int {
	total := 0
	for _, c := range d.Changes {
		switch c.Kind {
		case ChangeSubtree:
			total += c.FieldCount
		case ChangeAdded, ChangeRemoved, ChangeModified:
			if c.FieldCount > 0 {
				total += c.FieldCount
			} else {
				total++
			}
		}
	}
	return total
}

// Empty reports whether the two objects were equivalent (no changes).
func (d DiffResult) Empty() bool { return len(d.Changes) == 0 }

// Diff summarizes the change from before to after at the given collapse depth
// (use DefaultDiffMaxDepth for the tuned default). Semantics:
//
//   - scalar leaf changed       → ChangeModified (Old→New), at any depth;
//   - key/index only in after   → ChangeAdded;
//   - key/index only in before  → ChangeRemoved;
//   - object/array changed at value-depth ≥ maxDepth → ChangeSubtree (collapsed,
//     FieldCount = leaf changes within);
//   - arrays are compared ELEMENT-WISE BY INDEX (so the summary can say
//     workerGroupSpecs[0].replicas), independent of the merge's arrays-replace
//     rule, which governs construction, not reporting.
//
// before/after are not mutated.
func Diff(before, after MergedSpec, maxDepth int) DiffResult {
	var out []FieldChange
	walkDiff("", 0, maxDepth, map[string]any(before), map[string]any(after), &out)
	sort.SliceStable(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return DiffResult{Changes: out}
}

// walkDiff recursively compares b and a at the given path/value-depth, appending
// summary entries. It descends into matching object/array pairs until maxDepth,
// where a still-differing composite collapses to one ChangeSubtree entry.
func walkDiff(path string, depth, maxDepth int, b, a any, out *[]FieldChange) {
	if reflect.DeepEqual(b, a) {
		return
	}

	bMap, bIsMap := b.(map[string]any)
	aMap, aIsMap := a.(map[string]any)
	if bIsMap && aIsMap {
		if depth >= maxDepth {
			*out = append(*out, FieldChange{Path: path, Kind: ChangeSubtree, FieldCount: countChanges(b, a)})
			return
		}
		for _, k := range unionKeys(bMap, aMap) {
			bv, bok := bMap[k]
			av, aok := aMap[k]
			kp := childPath(path, k)
			switch {
			case bok && !aok:
				emitPresence(kp, ChangeRemoved, bv, out)
			case !bok && aok:
				emitPresence(kp, ChangeAdded, av, out)
			default:
				walkDiff(kp, depth+1, maxDepth, bv, av, out)
			}
		}
		return
	}

	bSlice, bIsSlice := b.([]any)
	aSlice, aIsSlice := a.([]any)
	if bIsSlice && aIsSlice {
		if depth >= maxDepth {
			*out = append(*out, FieldChange{Path: path, Kind: ChangeSubtree, FieldCount: countChanges(b, a)})
			return
		}
		n := len(bSlice)
		if len(aSlice) > n {
			n = len(aSlice)
		}
		for i := 0; i < n; i++ {
			ip := indexPath(path, i)
			switch {
			case i < len(bSlice) && i >= len(aSlice):
				emitPresence(ip, ChangeRemoved, bSlice[i], out)
			case i >= len(bSlice) && i < len(aSlice):
				emitPresence(ip, ChangeAdded, aSlice[i], out)
			default:
				walkDiff(ip, depth+1, maxDepth, bSlice[i], aSlice[i], out)
			}
		}
		return
	}

	// Scalar leaf, or a type change (scalar↔composite). Report a modification;
	// carry scalar values where present, and a FieldCount when a composite side
	// is involved so the renderer can summarize it.
	fc := 0
	if isComposite(b) || isComposite(a) {
		fc = countChanges(b, a)
	}
	*out = append(*out, FieldChange{Path: path, Kind: ChangeModified, Old: scalarOf(b), New: scalarOf(a), FieldCount: fc})
}

// emitPresence appends an added/removed entry. A scalar carries its value; a
// composite carries nil with FieldCount = its leaf count ("+spec.foo (3 fields)").
func emitPresence(path string, kind ChangeKind, v any, out *[]FieldChange) {
	fc := 0
	var scalar any
	if isComposite(v) {
		fc = countLeaves(v)
	} else {
		scalar = v
	}
	c := FieldChange{Path: path, Kind: kind, FieldCount: fc}
	if kind == ChangeAdded {
		c.New = scalar
	} else {
		c.Old = scalar
	}
	*out = append(*out, c)
}

// countChanges counts leaf-level differences between b and a (full depth, no
// collapse). Used for ChangeSubtree FieldCount and added/removed composites.
func countChanges(b, a any) int {
	if reflect.DeepEqual(b, a) {
		return 0
	}
	bMap, bIsMap := b.(map[string]any)
	aMap, aIsMap := a.(map[string]any)
	if bIsMap && aIsMap {
		total := 0
		for _, k := range unionKeys(bMap, aMap) {
			bv, bok := bMap[k]
			av, aok := aMap[k]
			switch {
			case bok && !aok:
				total += countLeaves(bv)
			case !bok && aok:
				total += countLeaves(av)
			default:
				total += countChanges(bv, av)
			}
		}
		return total
	}
	bSlice, bIsSlice := b.([]any)
	aSlice, aIsSlice := a.([]any)
	if bIsSlice && aIsSlice {
		total := 0
		n := len(bSlice)
		if len(aSlice) > n {
			n = len(aSlice)
		}
		for i := 0; i < n; i++ {
			switch {
			case i < len(bSlice) && i >= len(aSlice):
				total += countLeaves(bSlice[i])
			case i >= len(bSlice) && i < len(aSlice):
				total += countLeaves(aSlice[i])
			default:
				total += countChanges(bSlice[i], aSlice[i])
			}
		}
		return total
	}
	// Scalar or type change: one leaf differs.
	return 1
}

// countLeaves counts scalar leaves in a value. An empty object/array has 0
// leaves; a scalar (or nil) is 1.
func countLeaves(v any) int {
	switch t := v.(type) {
	case map[string]any:
		n := 0
		for _, e := range t {
			n += countLeaves(e)
		}
		return n
	case []any:
		n := 0
		for _, e := range t {
			n += countLeaves(e)
		}
		return n
	default:
		return 1
	}
}

// isComposite reports whether v is an object or array (vs a scalar leaf).
func isComposite(v any) bool {
	switch v.(type) {
	case map[string]any, []any:
		return true
	default:
		return false
	}
}

// scalarOf returns v if it is a scalar, else nil (composites are summarized via
// FieldCount, not carried inline).
func scalarOf(v any) any {
	if isComposite(v) {
		return nil
	}
	return v
}

// unionKeys returns the sorted union of two maps' keys, for deterministic output.
func unionKeys(a, b map[string]any) []string {
	seen := make(map[string]struct{}, len(a)+len(b))
	for k := range a {
		seen[k] = struct{}{}
	}
	for k := range b {
		seen[k] = struct{}{}
	}
	keys := make([]string, 0, len(seen))
	for k := range seen {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// childPath joins a parent path and an object key.
func childPath(parent, key string) string {
	if parent == "" {
		return key
	}
	return parent + "." + key
}

// indexPath appends an array index to a path: parent[i].
func indexPath(parent string, i int) string {
	return parent + "[" + strconv.Itoa(i) + "]"
}
