package domain

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"
)

// Destructive-op guards (spec §7, Q9/Q10/Q14; Decision Gate 2b/B1). Two stateless
// primitives the destructive tools (delete, scale-to-zero) compose:
//
//   - confirm-fingerprint — a content-derived handshake. The preview call returns
//     a fingerprint recomputed FROM THE LIVE OBJECT; the commit call echoes it
//     back and the server recomputes-and-compares. No pending state is stored, so
//     the HTTP deployment stays trivially horizontally scalable (spec §14, Q14).
//   - protected annotation — self-gating: a write that REMOVES or weakens
//     `ray-mcp/protected` is itself refused unless a valid unprotect fingerprint
//     is presented, so the annotation is not a decorative strip-then-delete bypass
//     (spec §7, Q10).
//
// It imports no Kubernetes/HTTP packages: it reads the live object as the plain
// MergedSpec map the KubeRay adapter already hands back under ClusterDetail.Raw.
// These are pure functions — the destructive tools (Task 12+) call them at their
// own entry points; the apply pipeline is unchanged.

// ProtectedAnnotation is the metadata annotation that self-gates destructive ops.
// Set to the literal "true" it makes delete and destructive scale-down refuse,
// and removing/weakening it is itself a confirmed (fingerprinted) act (spec §7,
// Q10). It is a fat-finger guard, NOT a security control — real protection is RBAC.
const ProtectedAnnotation = "ray-mcp/protected"

// protectedValue is the only annotation value that engages the guard. Any other
// value (or absence) is unprotected.
const protectedValue = "true"

// Operation names the destructive act a fingerprint authorizes. It is bound into
// the hash so a confirm minted for one op cannot be replayed to authorize another
// on the same object.
type Operation string

const (
	// OpDelete is a resource deletion (ray_cluster_delete et al.). Per B1 its
	// fingerprint is IDENTITY-ONLY (hash(UID + op), no resourceVersion): a delete
	// targets a resource by identity, and binding the churning resourceVersion would
	// livelock the confirm on a busy autoscaling cluster (Decision Gate 2b).
	OpDelete Operation = "delete"
	// OpScaleToZero is a worker-group teardown to zero replicas. It mutates spec, so
	// its fingerprint binds resourceVersion for free TOCTOU / optimistic-concurrency
	// rejection (a resource changed between preview and commit flips the hash).
	OpScaleToZero Operation = "scale-to-zero"
	// OpUnprotect is the removal/weakening of the protected annotation. Like
	// scale-to-zero it mutates spec/metadata, so it binds resourceVersion.
	OpUnprotect Operation = "unprotect"
)

// bindsResourceVersion reports whether op's fingerprint includes resourceVersion.
// Delete is identity-only (B1); every content-mutating op binds it for TOCTOU.
func (op Operation) bindsResourceVersion() bool {
	return op != OpDelete
}

// ConfirmRequiredError is the preview half of the two-step handshake: the caller
// supplied no confirm, so the destructive op did NOT run. It carries the
// fingerprint the caller must echo back as `confirm` to commit, and the operation
// it authorizes. It is a typed error so the tool layer can render the preview
// (diff + "pass confirm=<fingerprint> to proceed") rather than a generic failure.
type ConfirmRequiredError struct {
	Operation   Operation
	Fingerprint string
}

func (e *ConfirmRequiredError) Error() string {
	return fmt.Sprintf("%s requires confirmation: re-issue with confirm=%q (recomputed from the live object; a stale value is rejected)", e.Operation, e.Fingerprint)
}

// ConfirmMismatchError reports a non-empty confirm that does not match the
// recomputed fingerprint — either a wrong value or a STALE one (the resource
// changed since the preview, flipping a resourceVersion-bound fingerprint). It
// deliberately does NOT echo the current fingerprint: a mismatch must force the
// caller to re-preview and re-examine the changed resource, not blindly retry.
type ConfirmMismatchError struct {
	Operation Operation
}

func (e *ConfirmMismatchError) Error() string {
	return fmt.Sprintf("%s confirmation does not match the live object (wrong or stale) — re-preview to get a current fingerprint", e.Operation)
}

// Fingerprint computes the stateless content-derived confirmation token for a
// destructive op against the live object: hex(sha256(uid [+ resourceVersion] +
// op)). The op and uid are always bound (no cross-op replay, no cross-resource
// replay); resourceVersion is bound for every op EXCEPT delete (B1). The inputs
// are length-prefixed so no concatenation collision can forge a match across
// different field boundaries.
func Fingerprint(live MergedSpec, op Operation) string {
	h := sha256.New()
	writeField(h, string(op))
	writeField(h, metaString(live, "uid"))
	if op.bindsResourceVersion() {
		writeField(h, metaString(live, "resourceVersion"))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// RequireConfirm gates a destructive op on a matching confirm. An empty confirm is
// the preview: it returns a ConfirmRequiredError carrying the fingerprint to echo
// back. A non-empty confirm equal to the recomputed fingerprint commits (nil). Any
// other value is a ConfirmMismatchError (covers both wrong and stale).
func RequireConfirm(live MergedSpec, op Operation, confirm string) error {
	want := Fingerprint(live, op)
	if confirm == "" {
		return &ConfirmRequiredError{Operation: op, Fingerprint: want}
	}
	if confirm != want {
		return &ConfirmMismatchError{Operation: op}
	}
	return nil
}

// IsProtected reports whether the live object carries ray-mcp/protected="true".
func IsProtected(live MergedSpec) bool {
	return annotation(live, ProtectedAnnotation) == protectedValue
}

// GuardProtectedChange refuses a write that REMOVES or weakens the protected
// annotation unless a valid OpUnprotect confirm is presented (computed against the
// `before` live object). Strengthening (adding protection) or leaving it unchanged
// is always allowed without a confirm — you may tighten a guard freely; only
// loosening it is the confirmed act. This closes the strip-then-delete bypass that
// would make the annotation decorative (spec §7, Q10).
func GuardProtectedChange(before, after MergedSpec, confirm string) error {
	if !IsProtected(before) || IsProtected(after) {
		// Was not protected, or stays protected: no weakening, nothing to gate.
		return nil
	}
	return RequireConfirm(before, OpUnprotect, confirm)
}

// writeField hashes a length-prefixed field so the boundary between concatenated
// inputs is unambiguous (e.g. uid="a"+rv="bc" cannot collide with uid="ab"+rv="c").
func writeField(h hash.Hash, s string) {
	var lenBuf [8]byte
	binary.LittleEndian.PutUint64(lenBuf[:], uint64(len(s)))
	_, _ = h.Write(lenBuf[:])
	_, _ = h.Write([]byte(s))
}

// metaString reads a string field from metadata (uid, resourceVersion), tolerating
// a missing metadata map or a non-string value (returns "").
func metaString(live MergedSpec, key string) string {
	meta, _ := live["metadata"].(map[string]any)
	s, _ := meta[key].(string)
	return s
}

// annotation reads metadata.annotations[key] as a string, tolerating missing maps.
func annotation(live MergedSpec, key string) string {
	meta, _ := live["metadata"].(map[string]any)
	ann, _ := meta["annotations"].(map[string]any)
	v, _ := ann[key].(string)
	return v
}
